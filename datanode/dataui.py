import sys
import grpc
import logging
from concurrent import futures
import protos.kvstore_pb2 as kvstore_pb2
# from protos import kvstore_pb2 as kvstore_pb2
import protos.kvstore_pb2_grpc as kvstore_pb2_grpc
from datanode.rocks import RocksDictStore  # 引入 RocksDictStore 类


class DistributedKVDataNode(kvstore_pb2_grpc.KVStoreServiceServicer):
    def __init__(self, host="127.0.0.1", port=5001):
        """
        初始化 DataNode。
        Args:
            host (str): DataNode 主机地址。
            port (int): DataNode 端口号。
            db_path (str): RocksDB 数据库路径。
        """
        self.host = host
        self.port = port
        self.db_path = "./data/{}".format(self.port)
        self.db = RocksDictStore(self.db_path)  # 使用 RocksDictStore 进行本地存储
        self.setup_logging()

    def setup_logging(self):
        """设置日志记录"""
        self.logger = logging.getLogger("DistributedKVDataNode")
        self.logger.setLevel(logging.DEBUG)  # 允许DEBUG级别日志
        # 创建日志文件的处理器
        file_handler = logging.FileHandler("datanode{}.log".format(self.port))
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        # 创建控制台的处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        # 添加处理器到日志记录器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    # gRPC 服务方法
    def Put(self, request, context):
        """处理 PUT 请求"""
        key = request.key
        value = request.value
        self.logger.info(f"Received PUT request: Key = {key}, Value = {value}")
        response = self.handle_put_request(key, value)
        return kvstore_pb2.PutResponse(success=response['status'] == "success", message=response.get('message', ''))

    def Get(self, request, context):
        """处理 GET 请求"""
        key = request.key
        self.logger.info(f"Received GET request: Key = {key}")
        response = self.handle_get_request(key)
        return kvstore_pb2.GetResponse(success=response['status'] == "success", value=response.get('data', ''),
                                       message=response.get('message', ''))

    def Delete(self, request, context):
        """处理 DELETE 请求"""
        key = request.key
        self.logger.info(f"Received DELETE request: Key = {key}")
        response = self.handle_delete_request(key)
        return kvstore_pb2.DeleteResponse(success=response['status'] == "success", message=response.get('message', ''))

    def Clear(self, request, context):
        """处理 CLEAR 请求"""
        self.logger.info("Received CLEAR request.")
        response = self.handle_clear_request()
        return kvstore_pb2.ClearResponse(success=response['status'] == "success", message=response.get('message', ''))

    # 内部处理方法（PUT, GET, DELETE等）
    def handle_put_request(self, key, value):
        """处理 PUT 请求，并存储数据"""
        try:
            self.db.put(key, value)
            self.logger.info(f"Key '{key}' stored successfully.")
            return {"status": "success", "message": f"Key '{key}' stored successfully."}
        except Exception as e:
            self.logger.error(f"Error storing key '{key}': {e}")
            return {"status": "error", "message": f"Error storing key '{key}': {e}"}

    def handle_get_request(self, key):
        """处理 GET 请求，从存储中获取数据"""
        try:
            value = self.db.get(key)
            if value is not None:
                self.logger.info(f"Retrieved key '{key}' with value '{value}'.")
                return {"status": "success", "key": key, "data": value}
            else:
                self.logger.warning(f"Key '{key}' not found.")
                return {"status": "error", "message": f"Key '{key}' not found."}
        except Exception as e:
            self.logger.error(f"Error retrieving key '{key}': {e}")
            return {"status": "error", "message": f"Error retrieving key '{key}': {e}"}

    def handle_delete_request(self, key):
        """处理 DELETE 请求，从存储中删除数据"""
        try:
            self.db.delete(key)
            self.logger.info(f"Key '{key}' deleted successfully.")
            return {"status": "success", "message": f"Key '{key}' deleted successfully."}
        except Exception as e:
            self.logger.error(f"Error deleting key '{key}': {e}")
            return {"status": "error", "message": f"Error deleting key '{key}': {e}"}

    def handle_clear_request(self):
        """处理 CLEAR 请求，清空所有数据"""
        try:
            self.db.clear()
            self.logger.info("All data cleared.")
            return {"status": "success", "message": "All data cleared."}
        except Exception as e:
            self.logger.error(f"Error clearing data: {e}")
            return {"status": "error", "message": f"Error clearing data: {e}"}


# 启动 gRPC 服务器
def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_KVStoreServiceServicer_to_server(DistributedKVDataNode(host="127.0.0.1", port=port), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"DataNode running on port {port}")
    server.wait_for_termination()


def serve_auto():
    server1 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_KVStoreServiceServicer_to_server(DistributedKVDataNode(host="127.0.0.1", port=6000), server1)
    server1.add_insecure_port('[::]:6000')  # 端口6000
    server1.start()

    server2 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_KVStoreServiceServicer_to_server(DistributedKVDataNode(host="127.0.0.1", port=6001), server2)
    server2.add_insecure_port('[::]:6001')  # 端口6001
    server2.start()

    server3 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_KVStoreServiceServicer_to_server(DistributedKVDataNode(host="127.0.0.1", port=6002), server3)
    server3.add_insecure_port('[::]:6002')  # 端口6002
    server3.start()

    print("DataNode running on ports 6000, 6001, 6002")
    server1.wait_for_termination()
    server2.wait_for_termination()
    server3.wait_for_termination()


def argv_check():
    if len(sys.argv) != 2:
        print("Usage: python datanode.py <port>")
        sys.exit(1)


def main():
    argv_check()
    port = int(sys.argv[1])
    serve(port)

# if __name__ == "__main__":
#     serve_auto()

if __name__ == '__main__':
    main()