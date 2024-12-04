import grpc
import protos.kvstore_pb2 as kvstore_pb2
import protos.kvstore_pb2_grpc as kvstore_pb2_grpc
import threading
import logging
import time
from collections import deque

class RaftNode:
    def __init__(self, server_node, config):
        self.server_node = server_node
        self.config = config
        self.log = deque()  # Raft 日志
        self.leader = None  # 领导者节点
        self.current_term = 0
        self.voted_for = None
        self.state = 'FOLLOWER'  # 节点状态: FOLLOWER, CANDIDATE, LEADER
        self.setup_logging()

    def setup_logging(self):
        """设置日志记录"""
        self.logger = logging.getLogger("RaftNode")
        self.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler("raftnode.log")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(message)s"))
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def append_log(self, log_entry):
        """将日志条目添加到 Raft 日志中。"""
        self.log.append(log_entry)
        self.logger.info(f"Appended log entry: {log_entry}")

    def become_leader(self):
        """将当前节点变为领导者"""
        self.state = 'LEADER'
        self.leader = self.server_node.host
        self.logger.info(f"Node {self.server_node.host} became the leader.")

    def become_follower(self):
        """将当前节点变为跟随者"""
        self.state = 'FOLLOWER'
        self.leader = None
        self.logger.info(f"Node {self.server_node.host} is now a follower.")

    def become_candidate(self):
        """将当前节点变为候选人"""
        self.state = 'CANDIDATE'
        self.logger.info(f"Node {self.server_node.host} is now a candidate.")

    def handle_heartbeat(self):
        """处理领导者的心跳"""
        if self.state != 'LEADER':
            self.logger.info(f"Node {self.server_node.host} is not the leader, ignoring heartbeat.")
            return
        self.logger.info(f"Leader {self.server_node.host} sending heartbeat to followers.")
        for follower in self.config['nodes']:
            if follower != self.server_node.host:
                threading.Thread(target=self.send_heartbeat, args=(follower,)).start()

    def send_heartbeat(self, peer):
        """向特定的跟随者节点发送心跳"""
        try:
            with grpc.insecure_channel(f"{peer}:5000") as channel:
                stub = kvstore_pb2_grpc.KVStoreServiceStub(channel)
                stub.List(kvstore_pb2.ListRequest(operation_id="heartbeat"))
                self.logger.info(f"Heartbeat sent to {peer}")
        except Exception as e:
            self.logger.error(f"Failed to send heartbeat to {peer}: {e}")

    def election_timeout(self):
        """每个节点会在此函数中等待选举超时，然后变为候选人"""
        self.logger.info(f"Node {self.server_node.host} waiting for election timeout.")
        time.sleep(self.config['election_timeout'])
        if self.state == 'FOLLOWER':
            self.become_candidate()

    def request_vote(self, peer, request):
        """向其他节点请求投票"""
        try:
            with grpc.insecure_channel(f"{peer}:5000") as channel:
                stub = kvstore_pb2_grpc.KVStoreServiceStub(channel)
                response = stub.List(kvstore_pb2.ListRequest(operation_id="vote_request"))
                if response.success:
                    self.logger.info(f"Vote granted by {peer}")
        except Exception as e:
            self.logger.error(f"Vote request failed to {peer}: {e}")

    def start_election(self):
        """开始选举，成为候选人并请求投票"""
        self.logger.info(f"Node {self.server_node.host} starting election.")
        self.become_candidate()
        votes = 1  # We always vote for ourselves
        for peer in self.config['nodes']:
            if peer != self.server_node.host:
                self.request_vote(peer, {"term": self.current_term})
                votes += 1
        if votes > len(self.config['nodes']) // 2:
            self.become_leader()
        else:
            self.logger.info(f"Node {self.server_node.host} failed to become leader.")

    def handle_append_entries(self, entries):
        """接收来自其他节点的日志条目"""
        if self.state != 'LEADER':
            self.append_log(entries)
            self.logger.info(f"Node {self.server_node.host} appended entries: {entries}")
        else:
            self.logger.info(f"Node {self.server_node.host} is the leader, ignoring append entries.")

# 启动 Raft 节点
def start_raft_node(server_node, raft_config):
    raft_node = RaftNode(server_node, raft_config)
    threading.Thread(target=raft_node.election_timeout).start()  # 启动选举超时
    return raft_node
