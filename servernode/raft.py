import time
import random
import threading
import logging
import protos.raft_pb2 as raft_pb2
import protos.raft_pb2_grpc as raft_pb2_grpc
from concurrent import futures
import grpc


class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, host, port, raft_config):
        self.host = host
        self.port = port
        self.peers = raft_config['peers']  # 其他节点地址
        self.state = "FOLLOWER"
        self.term = 0
        self.voted_for = None
        self.logs = []  # 存储日志条目
        self.commit_index = 0  # 已提交的日志索引
        self.last_heartbeat = time.time()

        # 配置日志文件
        self.setup_logger()

        # RPC 服务配置
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(f'{self.host}:{self.port}')

    def setup_logger(self):
        """设置日志配置"""
        log_filename = f"raft{self.port}.log"
        self.logger = logging.getLogger(f"RaftNode{self.port}")
        self.logger.setLevel(logging.DEBUG)

        # 创建文件处理器和日志格式
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)

    def start(self):
        """启动Raft节点"""
        self.server.start()
        self.logger.info(f"RaftNode {self.host}:{self.port} is running")

        if not self.peers:  # 如果没有其他节点，直接成为领导者
            self.logger.info("No peers detected. This node is running as the sole leader.")
            self.state = "LEADER"
            self.start_heartbeat()
        else:
            self.run_election_timeout()

    def run_election_timeout(self):
        """每隔一段时间检查选举超时"""
        while True:
            if self.state != "LEADER" and time.time() - self.last_heartbeat > random.uniform(1.5, 3.0):
                self.start_election()
            time.sleep(1)

    def start_election(self):
        """发起选举"""
        self.state = "CANDIDATE"
        self.term += 1
        self.voted_for = self.host
        self.last_heartbeat = time.time()

        self.logger.info(f"Node {self.host}:{self.port} is starting an election for term {self.term}")

        # 如果没有其他节点，直接成为领导者
        if not self.peers:
            self.logger.info(f"Node {self.host}:{self.port} became leader by default (no peers).")
            self.state = "LEADER"
            self.start_heartbeat()
            return

        # 请求投票
        votes = 1  # 默认投给自己
        request = raft_pb2.RequestVoteRequest(
            term=self.term,
            candidate_id=self.host,
            last_log_index=len(self.logs),
            last_log_term=self.logs[-1]['term'] if self.logs else 0
        )

        for peer in self.peers:
            response = self.send_request_vote(peer, request)
            if response and response.vote_granted:
                votes += 1
            if votes > len(self.peers) // 2:
                self.state = "LEADER"
                self.logger.info(f"Node {self.host}:{self.port} became leader with term {self.term}")
                self.start_heartbeat()
                return

    def send_request_vote(self, peer, request):
        """发送投票请求"""
        try:
            channel = grpc.insecure_channel(f'{peer}')
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            response = stub.RequestVote(request)
            return response
        except grpc.RpcError as e:
            self.logger.error(f"Failed to send vote request to {peer}: {e}")
            return None

    def start_heartbeat(self):
        """领导者开始发送心跳给跟随者"""
        def send_heartbeat():
            while self.state == "LEADER":
                # 给自己发送心跳
                self.last_heartbeat = time.time()

                if len(self.peers) > 0:
                    for peer in self.peers:
                        # 发送心跳
                        self.logger.info(f"Node {self.host}:{self.port} sending heartbeat to {peer} in term {self.term}")
                        request = raft_pb2.AppendEntriesRequest(
                            term=self.term,
                            leader_id=self.host,
                            prev_log_index=len(self.logs),
                            prev_log_term=self.logs[-1]['term'] if self.logs else 0,
                            entries=[],  # 心跳时不包含日志条目
                            leader_commit=self.commit_index
                        )
                        self.send_append_entries(peer, request)

                time.sleep(0.3)  # 每300ms发送一次心跳

        threading.Thread(target=send_heartbeat, daemon=True).start()

    def send_append_entries(self, peer, request):
        """发送日志追加请求"""
        try:
            channel = grpc.insecure_channel(f'{peer}')
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            stub.AppendEntries(request)
            self.logger.info(f"Node {self.host}:{self.port} successfully sent AppendEntries to {peer}")
        except grpc.RpcError as e:
            self.logger.error(f"Failed to send append entries to {peer}: {e}")

    def RequestVote(self, request, context):
        """处理投票请求"""
        self.logger.info(f"Node {self.host}:{self.port} received vote request from {request.candidate_id} for term {request.term}")

        if request.term > self.term:
            self.term = request.term
            self.state = "FOLLOWER"
            self.voted_for = None

        if request.term >= self.term and (self.voted_for is None or self.voted_for == request.candidate_id):
            self.voted_for = request.candidate_id
            self.logger.info(f"Node {self.host}:{self.port} voted for {request.candidate_id} in term {self.term}")
            return raft_pb2.RequestVoteResponse(term=self.term, vote_granted=True)
        self.logger.info(f"Node {self.host}:{self.port} rejected vote request from {request.candidate_id} in term {self.term}")
        return raft_pb2.RequestVoteResponse(term=self.term, vote_granted=False)

    def AppendEntries(self, request, context):
        """处理日志追加请求"""
        self.logger.info(f"Node {self.host}:{self.port} received AppendEntries from leader {request.leader_id} in term {request.term}")

        if request.term < self.term:
            return raft_pb2.AppendEntriesResponse(term=self.term, success=False)

        self.term = request.term
        self.state = "FOLLOWER"
        self.last_heartbeat = time.time()

        return raft_pb2.AppendEntriesResponse(term=self.term, success=True)



# 运行 Raft 节点
if __name__ == '__main__':
    raft_config = {
        'peers': ['localhost:5001', ],
        'host': 'localhost',
        'port': 5000
    }
    node = RaftNode(raft_config['host'], raft_config['port'], raft_config)
    node.start()
