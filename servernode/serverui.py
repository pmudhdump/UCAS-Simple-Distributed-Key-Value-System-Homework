import socket
import threading
import logging
from concurrent.futures import ThreadPoolExecutor
from utils.socket_protocol import SocketProtocol
from servernode.raft import RaftNode  # 假设你有 Raft 协议实现模块
from servernode.consistent_hash import ConsistentHashing  # 引入一致性哈希


class DistributedKVServerNode:
    def __init__(self, host="127.0.0.1", port=5000, datanodes=None, raft_config=None):
        """
        初始化 ServerNode。
        Args:
            host (str): 服务器主机地址。
            port (int): 服务器端口号。
            datanodes (list): 可用的 DataNode 地址列表。
            raft_config (dict): Raft 配置参数。
        """
        self.host = host
        self.port = port
        self.datanodes = datanodes or []
        self.protocol = SocketProtocol()
        self.raft_node = RaftNode(self, raft_config)  # 初始化 Raft 节点
        self.setup_logging()

        # 使用一致性哈希来选择节点
        self.consistent_hashing = ConsistentHashing(
            [f"{datanode[0]}:{datanode[1]}" for datanode in datanodes])  # 使用节点的host:port

    def start_server(self):
        """启动 ServerNode 服务器"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        self.logger.info(f"ServerNode started at {self.host}:{self.port}")

        while True:
            client_socket, addr = server_socket.accept()
            self.logger.info(f"Accepted connection from {addr}")
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def setup_logging(self):
        """设置日志记录"""
        self.logger = logging.getLogger("DistributedKVServerNode")
        self.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler("servernode.log")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(message)s"))

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def handle_client(self, client_socket):
        """
        处理来自客户端的请求。
        Args:
            client_socket (socket.socket): 客户端套接字。
        """
        try:
            request = self.protocol.receive(client_socket)
            self.logger.info(f"Received request: {request}")

            # 根据请求类型处理
            action = request.get("action")
            if action == "put":
                key, value = request["key"], request["value"]
                response = self.handle_put_request(key, value)
            elif action == "get":
                key = request["key"]
                response = self.handle_get_request(key)
            elif action == "delete":
                key = request["key"]
                response = self.handle_delete_request(key)
            elif action == "list":
                response = self.raft_node.query_log({"action": "list"})
            elif action == "clear":
                response = self.raft_node.append_log({"action": "clear"})
                # 清除数据时同步到 DataNode
                self.clear_data_on_datanodes()
            else:
                response = {"status": "error", "message": "Unknown action."}

            self.protocol.send(client_socket, response)
            self.logger.info(f"Sent response: {response}")

        except Exception as e:
            self.logger.error(f"Error handling client request: {e}")
        finally:
            client_socket.close()

    def handle_put_request(self, key, value):
        """
        处理 PUT 请求，并将数据存储到目标节点。
        同时返回存储的节点。
        """
        # 获取目标节点及副本节点
        target_node = self.consistent_hashing.get_node(key)
        node_index = self.consistent_hashing.nodes.index(target_node)
        replica_nodes = [
            self.consistent_hashing.nodes[(node_index + 1) % len(self.consistent_hashing.nodes)],
            self.consistent_hashing.nodes[(node_index + 2) % len(self.consistent_hashing.nodes)]
        ]

        # 存储数据到目标节点和副本节点
        request = {"action": "put", "key": key, "value": value}
        self.send_data_to_datanodes(request)

        # 记录操作到 Raft
        self.raft_node.append_log({"action": "put", "key": key, "value": value})

        # 返回存储成功的节点信息
        return {"status": "success", "message": f"Data stored at node {target_node}.", "node": replica_nodes}

    def handle_delete_request(self, key):
        """
        处理 DELETE 请求，并将数据从目标节点删除。
        同时返回删除的节点。
        """
        # 获取目标节点及副本节点
        target_node = self.consistent_hashing.get_node(key)
        node_index = self.consistent_hashing.nodes.index(target_node)
        replica_nodes = [
            self.consistent_hashing.nodes[(node_index + 1) % len(self.consistent_hashing.nodes)],
            self.consistent_hashing.nodes[(node_index + 2) % len(self.consistent_hashing.nodes)]
        ]

        # 删除数据
        request = {"action": "delete", "key": key}
        self.send_data_to_datanodes(request)

        # 记录操作到 Raft
        self.raft_node.append_log({"action": "delete", "key": key})

        # 返回删除成功的节点信息
        return {"status": "success", "message": f"Data deleted from node {target_node}.", "node": replica_nodes}

    def send_data_to_datanodes(self, request):
        """将数据同步到多个副本节点"""
        key = request.get("key")
        # 计算目标节点（包括副本节点）
        target_node = self.consistent_hashing.get_node(key)
        # 获取该节点的索引位置
        node_index = self.consistent_hashing.nodes.index(target_node)
        # 获取该节点的下一个和上一个节点作为副本
        replica_nodes = [
            self.consistent_hashing.nodes[(node_index + 1) % len(self.consistent_hashing.nodes)],
            self.consistent_hashing.nodes[(node_index + 2) % len(self.consistent_hashing.nodes)]
        ]

        # 同步数据到主副本节点
        for node in [target_node] + replica_nodes:
            peer_host, peer_port = node.split(":")
            peer = (peer_host, int(peer_port))
            threading.Thread(target=self.sync_with_peer, args=(peer, request)).start()

    def sync_with_peer(self, peer, request):
        """与副本节点同步"""
        peer_host, peer_port = peer
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer_socket:
            peer_socket.connect((peer_host, peer_port))
            self.protocol.send(peer_socket, request)
            response = self.protocol.receive(peer_socket)
            self.logger.info(f"Sent request to DataNode {peer}: {response}")

    def clear_data_on_datanodes(self):
        """清除数据并通知所有副本节点"""
        for node in self.consistent_hashing.nodes:
            peer_host, peer_port = node.split(":")
            peer = (peer_host, int(peer_port))
            threading.Thread(target=self.sync_with_peer, args=(peer, {"action": "clear"})).start()

    def handle_get_request(self, key):
        """处理 GET 请求，并确保从两个副本节点获取数据并验证一致性"""
        # 获取目标节点及其副本节点
        target_node = self.consistent_hashing.get_node(key)
        node_index = self.consistent_hashing.nodes.index(target_node)
        replica_nodes = [
            self.consistent_hashing.nodes[(node_index + 1) % len(self.consistent_hashing.nodes)],
            self.consistent_hashing.nodes[(node_index + 2) % len(self.consistent_hashing.nodes)]
        ]

        # 获取第一个副本节点的数据
        data_from_target = self.get_data_from_peer(target_node, key)
        # 获取第二个副本节点的数据
        data_from_replica = self.get_data_from_peer(replica_nodes[0], key)

        if data_from_target != data_from_replica:
            return {"status": "error", "message": "Inconsistent data from replicas."}

        # 如果一致，返回数据
        return {"status": "success", "key": key, "data": data_from_target}

    def get_data_from_peer(self, peer, key):
        """从指定副本节点获取数据"""
        peer_host, peer_port = peer.split(":")
        peer = (peer_host, int(peer_port))

        request = {"action": "get", "key": key}
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer_socket:
            peer_socket.connect((peer_host, peer_port))
            self.protocol.send(peer_socket, request)
            response = self.protocol.receive(peer_socket)

        if response.get("status") == "success":
            return response.get("data")
        else:
            return None
