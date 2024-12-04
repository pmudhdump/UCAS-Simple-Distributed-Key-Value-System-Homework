import threading
import time
import random
import logging
import os
import json

class RaftNode:
    def __init__(self, server, config):
        """
        初始化 Raft 节点。
        Args:
            server (DistributedKVServerNode): 所属的服务器实例。
            config (dict): 节点配置，包括节点 ID 和集群地址。
        """
        self.server = server
        self.node_id = config["node_id"]
        self.cluster = config["cluster"]  # 集群中所有节点的地址列表
        self.state = "follower"  # 节点状态: follower, candidate, leader
        self.current_term = 0  # 当前任期
        self.voted_for = None  # 当前任期投票的候选人
        self.commit_index = 0  # 已提交的最后一个日志索引
        self.last_applied = 0  # 最后应用到状态机的日志索引

        # 日志文件路径
        self.log_file = f"raft_log_{self.node_id}.json"
        self.lock = threading.Lock()

        # 用于选举计时
        self.election_timeout = random.uniform(1, 3)  # 1-3 秒的选举超时
        self.last_heartbeat = time.time()

        # 状态管理
        self.leader_id = None  # 当前的领导者 ID

        # 日志记录
        self.logger = logging.getLogger(f"RaftNode-{self.node_id}")
        self.setup_logging()

        # 启动后台线程
        threading.Thread(target=self.run_raft, daemon=True).start()

    def setup_logging(self):
        """设置日志记录"""
        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(f"[%(asctime)s] [%(levelname)s] [Node-{self.node_id}] %(message)s"))
        self.logger.addHandler(console_handler)

    def load_logs(self):
        """从文件加载日志条目"""
        if os.path.exists(self.log_file):
            with open(self.log_file, 'r') as f:
                return json.load(f)
        return []

    def save_logs(self):
        """将日志条目保存到文件"""
        with open(self.log_file, 'w') as f:
            json.dump(self.log, f)

    def run_raft(self):
        """运行 Raft 主循环，处理选举和心跳"""
        while True:
            with self.lock:
                if self.state == "follower" and time.time() - self.last_heartbeat > self.election_timeout:
                    self.start_election()
                elif self.state == "candidate" and time.time() - self.last_heartbeat > self.election_timeout:
                    self.start_election()
                elif self.state == "leader":
                    self.send_heartbeats()
                    self.replicate_logs()  # 领导者执行日志复制
            time.sleep(0.1)

    def start_election(self):
        """启动选举过程"""
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.logger.info(f"Starting election for term {self.current_term}")

        votes = 1  # 自己给自己投票
        for peer in self.cluster:
            if peer != self.server.host:
                success = self.request_vote(peer)
                if success:
                    votes += 1

        if votes > len(self.cluster) // 2:
            self.state = "leader"
            self.leader_id = self.node_id
            self.logger.info(f"Node {self.node_id} became leader for term {self.current_term}")

    def request_vote(self, peer):
        """
        请求其他节点投票。
        Args:
            peer (str): 对端节点地址。
        Returns:
            bool: 投票成功返回 True。
        """
        # 模拟投票请求
        self.logger.info(f"Requesting vote from {peer} for term {self.current_term}")
        # 假设对端总是投票，实际中需要通过 RPC 或网络调用
        return True

    def send_heartbeats(self):
        """发送心跳消息给集群中的其他节点"""
        self.logger.info(f"Node {self.node_id} is sending heartbeats")
        for peer in self.cluster:
            if peer != self.server.host:
                self.send_heartbeat(peer)

    def send_heartbeat(self, peer):
        """
        向对端节点发送心跳。
        Args:
            peer (str): 对端节点地址。
        """
        self.logger.info(f"Sending heartbeat to {peer}")
        # 实际实现中这里应发送 RPC 或消息，更新对端的日志状态

    def replicate_logs(self):
        """领导者节点将日志复制到所有跟随者节点"""
        if self.state != "leader":
            return

        # 对于所有跟随者节点
        for peer in self.cluster:
            if peer != self.server.host:
                self.replicate_log_to_peer(peer)

    def replicate_log_to_peer(self, peer):
        """将日志条目复制到指定的跟随者节点"""
        # 获取最新日志条目
        log_entries = self.log[self.commit_index:]

        if log_entries:  # 只有当有新日志条目时才复制
            self.logger.info(f"Replicating log to {peer}: {log_entries}")
            # 模拟日志复制的 RPC 调用
            # 假设跟随者节点成功复制了日志
            self.commit_index = len(self.log)  # 如果日志条目复制成功，则更新已提交的日志索引

    def append_log(self, command):
        """
        添加日志条目。
        Args:
            command (dict): 客户端命令。
        Returns:
            dict: 操作结果。
        """
        with self.lock:
            if self.state != "leader":
                return {"status": "error", "message": "Not the leader", "leader_id": self.leader_id}

            # 读取现有的日志并追加新的条目
            self.log = self.load_logs()
            self.log.append({"term": self.current_term, "command": command})
            self.logger.info(f"Appended log: {command}")

            # 保存新的日志
            self.save_logs()

            # 复制日志到所有跟随者
            self.replicate_logs()

            return {"status": "success", "message": "Log appended"}

    def query_log(self, query):
        """
        查询日志或状态。
        Args:
            query (dict): 查询请求。
        Returns:
            dict: 查询结果。
        """
        if query["action"] == "get":
            key = query["key"]
            for entry in reversed(self.log):
                if entry["command"]["action"] == "put" and entry["command"]["key"] == key:
                    return {"status": "success", "value": entry["command"]["value"]}
            return {"status": "error", "message": "Key not found"}
        elif query["action"] == "list":
            keys = [entry["command"]["key"] for entry in self.log if entry["command"]["action"] == "put"]
            return {"status": "success", "data": keys}
        elif query["action"] == "clear":
            self.log = []
            self.save_logs()
            return {"status": "success", "message": "Logs cleared"}
        else:
            return {"status": "error", "message": "Unknown query"}
