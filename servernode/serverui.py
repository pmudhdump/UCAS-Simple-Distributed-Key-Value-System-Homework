import threading
import grpc
import logging
from concurrent import futures
import time
import protos.kvstore_pb2 as kvstore_pb2
import protos.kvstore_pb2_grpc as kvstore_pb2_grpc
from servernode.raft import RaftNode  # 假设你有 Raft 协议实现模块
from servernode.consistent_hash import ConsistentHashing  # 引入一致性哈希

class DistributedKVServerNode(kvstore_pb2_grpc.KVStoreServiceServicer):
    def __init__(self, host="127.0.0.1", port=5000, datanodes=None, raft_config=None):
        """
        初始化 ServerNode。
        Args:
            host (str): 服务器主机地址。
            port (int): 服务器端口号。
            datanodes (list): 可用的 DataNode 地址列表。
            raft_config (dict): Raft 配置参数。
        """
        if raft_config is None:
            raft_config = {"node_id": 1, "cluster": ["127.0.0.1", 5000]}
        if datanodes is None:
            self.datanodes = [["127.0.0.1", 6000], ["127.0.0.1", 6001], ["127.0.0.1", 6002]]
        self.host = host
        self.port = port
        self.raft_node = RaftNode(self.host,self.port ,raft_config)  # 初始化 Raft 节点
        self.setup_logging()
        self.raft_node.start()

        # 使用一致性哈希来选择节点
        self.consistent_hashing = ConsistentHashing([f"{datanode[0]}:{datanode[1]}" for datanode in self.datanodes])  # 使用节点的host:port

    def setup_logging(self):
        """设置日志记录"""
        self.logger = logging.getLogger("DistributedKVServerNode")
        self.logger.setLevel(logging.DEBUG)  # 改为 DEBUG 级别以便记录更多细节
        # 创建日志文件的处理器
        file_handler = logging.FileHandler("servernode.log")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        # 创建控制台的处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        # 添加处理器到日志记录器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    # gRPC服务方法
    def Put(self, request, context):
        """处理 PUT 请求"""
        key = request.key
        value = request.value
        operation_id = request.operation_id
        self.logger.info(f"Received PUT request: Key = {key}, Value = {value}, Operation ID = {operation_id}")
        response = self.handle_put_request(key, value, operation_id)
        return kvstore_pb2.PutResponse(success=response['status'] == "success", message=response.get('message', ''))

    def Get(self, request, context):
        """处理 GET 请求"""
        key = request.key
        operation_id = request.operation_id
        self.logger.info(f"Received GET request: Key = {key}, Operation ID = {operation_id}")
        response = self.handle_get_request(key, operation_id)
        return kvstore_pb2.GetResponse(success=response['status'] == "success", value=response.get('data', ''), message=response.get('message', ''))

    def Delete(self, request, context):
        """处理 DELETE 请求"""
        key = request.key
        operation_id = request.operation_id
        self.logger.info(f"Received DELETE request: Key = {key}, Operation ID = {operation_id}")
        response = self.handle_delete_request(key, operation_id)
        return kvstore_pb2.DeleteResponse(success=response['status'] == "success", message=response.get('message', ''))

    def List(self, request, context):
        """处理 LIST 请求"""
        operation_id = request.operation_id
        self.logger.info(f"Received LIST request: Operation ID = {operation_id}")
        response = self.raft_node.query_log({"action": "list"})
        return kvstore_pb2.ListResponse(success=response['status'] == "success", keys=response.get('data', []), message=response.get('message', ''))

    def Clear(self, request, context):
        """处理 CLEAR 请求"""
        operation_id = request.operation_id
        self.logger.info(f"Received CLEAR request: Operation ID = {operation_id}")
        response = self.raft_node.append_log({"action": "clear"})
        self.clear_data_on_datanodes()
        return kvstore_pb2.ClearResponse(success=response['status'] == "success", message=response.get('message', ''))

    # 内部处理方法（PUT, GET, DELETE等）
    def handle_put_request(self, key, value, operation_id):
        """处理 PUT 请求"""
        target_node = self.consistent_hashing.get_node(key)
        node_index = self.consistent_hashing.nodes.index(target_node)
        replica_nodes = [
            self.consistent_hashing.nodes[(node_index + 1) % len(self.consistent_hashing.nodes)]
        ]
        # 存储数据到目标节点和副本节点
        self.logger.debug(f"Storing data at target node {target_node} and replica nodes {replica_nodes}.")
        request = {"action": "put", "key": key, "value": value, "operation_id": operation_id}
        self.send_data_to_datanodes(request)
        self.raft_node.append_log({"action": "put", "key": key, "value": value, "operation_id": operation_id})
        return {"status": "success", "message": f"Data stored at node {target_node} and {replica_nodes}."}

    def handle_get_request(self, key, operation_id):
        """处理 GET 请求"""
        target_node = self.consistent_hashing.get_node(key)
        data_from_target = self.get_data_from_peer(target_node, key)
        self.logger.debug(f"Retrieved data from node {target_node}: {data_from_target}")
        node_index = self.consistent_hashing.nodes.index(target_node)
        replica_nodes = [
            self.consistent_hashing.nodes[(node_index + 1) % len(self.consistent_hashing.nodes)]
        ]
        for node in replica_nodes:
            data_from_replica = self.get_data_from_peer(node, key)
            if data_from_replica != data_from_target:
                self.logger.warning(f"Data inconsistency found between target node {target_node} and replica node {node}.")
                return {"status": "error", "message": "Data inconsistency found"}
        return {"status": "success", "data": data_from_target, "message": "Key found"}

    def handle_delete_request(self, key, operation_id):
        """处理 DELETE 请求"""
        target_node = self.consistent_hashing.get_node(key)
        self.logger.debug(f"Deleting data from node {target_node}.")
        request = {"action": "delete", "key": key, "operation_id": operation_id}
        self.send_data_to_datanodes(request)
        self.raft_node.append_log({"action": "delete", "key": key, "operation_id": operation_id})
        return {"status": "success", "message": f"Data deleted from node {target_node}."}

    def send_data_to_datanodes(self, request):
        """将数据同步到多个副本节点"""
        key = request.get("key")
        target_node = self.consistent_hashing.get_node(key)
        node_index = self.consistent_hashing.nodes.index(target_node)
        replica_nodes = [
            self.consistent_hashing.nodes[(node_index + 1) % len(self.consistent_hashing.nodes)]
        ]
        self.logger.debug(f"Sending data to target node {target_node} and replica nodes {replica_nodes}.")
        for node in [target_node] + replica_nodes:
            peer_host, peer_port = node.split(":")
            peer = (peer_host, int(peer_port))
            threading.Thread(target=self.sync_with_peer, args=(peer, request)).start()

    def sync_with_peer(self, peer, request):
        """与副本节点同步"""
        peer_host, peer_port = peer
        self.logger.debug(f"Synchronizing data with peer {peer_host}:{peer_port}.")
        with grpc.insecure_channel(f'{peer_host}:{peer_port}') as channel:
            stub = kvstore_pb2_grpc.KVStoreServiceStub(channel)
            if request['action'] == 'put':
                stub.Put(kvstore_pb2.PutRequest(key=request['key'], value=request['value'], operation_id=request['operation_id']))
            elif request['action'] == 'delete':
                stub.Delete(kvstore_pb2.DeleteRequest(key=request['key'], operation_id=request['operation_id']))

    def clear_data_on_datanodes(self):
        """清除数据并通知所有副本节点"""
        self.logger.info("Clearing data on all data nodes.")
        for node in self.consistent_hashing.nodes:
            peer_host, peer_port = node.split(":")
            peer = (peer_host, int(peer_port))
            threading.Thread(target=self.sync_with_peer, args=(peer, {"action": "clear"})).start()

    def get_data_from_peer(self, peer, key):
        """从指定副本节点获取数据"""
        peer_host, peer_port = peer.split(":")
        with grpc.insecure_channel(f'{peer_host}:{peer_port}') as channel:
            stub = kvstore_pb2_grpc.KVStoreServiceStub(channel)
            response = stub.Get(kvstore_pb2.GetRequest(key=key, operation_id=""))
            self.logger.debug(f"Data from peer {peer_host}:{peer_port}: {response.value}")
            return response.value

# 启动 gRPC 服务器
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_KVStoreServiceServicer_to_server(DistributedKVServerNode(), server)
    server.add_insecure_port('[::]:5000')
    server.start()
    print("Server running on port 5000")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
