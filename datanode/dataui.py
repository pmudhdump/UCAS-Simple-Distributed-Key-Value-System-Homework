import socket
import threading
import logging
from utils.socket_protocol import SocketProtocol
from datanode.rocks import RocksDictStore  # 引入 RocksDictStore 类

class DistributedKVDataNode:
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
        self.protocol = SocketProtocol()
        self.db = RocksDictStore(self.db_path)  # 使用 RocksDictStore 进行本地存储
        self.setup_logging()

    def setup_logging(self):
        """设置日志记录"""
        self.logger = logging.getLogger("DistributedKVDataNode")
        self.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler("datanode.log")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(message)s"))

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def start_server(self):
        """启动 DataNode 服务器"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        self.logger.info(f"DataNode started at {self.host}:{self.port}")

        while True:
            client_socket, addr = server_socket.accept()
            self.logger.info(f"Accepted connection from {addr}")
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        """
        处理来自 ServerNode 的请求。
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
            elif action == "clear":
                response = self.handle_clear_request()
            else:
                response = {"status": "error", "message": "Unknown action."}

            self.protocol.send(client_socket, response)
            self.logger.info(f"Sent response: {response}")

        except Exception as e:
            self.logger.error(f"Error handling client request: {e}")
        finally:
            client_socket.close()

    def handle_put_request(self, key, value):
        """处理 PUT 请求，并存储数据"""
        try:
            self.db.put(key, value)
            return {"status": "success", "message": f"Key '{key}' stored successfully."}
        except Exception as e:
            self.logger.error(f"Error storing key '{key}': {e}")
            return {"status": "error", "message": f"Error storing key '{key}': {e}"}

    def handle_get_request(self, key):
        """处理 GET 请求，从存储中获取数据"""
        value = self.db.get(key)
        if value is not None:
            return {"status": "success", "key": key, "data": value}
        else:
            return {"status": "error", "message": f"Key '{key}' not found."}

    def handle_delete_request(self, key):
        """处理 DELETE 请求，从存储中删除数据"""
        try:
            self.db.delete(key)
            return {"status": "success", "message": f"Key '{key}' deleted successfully."}
        except Exception as e:
            self.logger.error(f"Error deleting key '{key}': {e}")
            return {"status": "error", "message": f"Error deleting key '{key}': {e}"}

    def handle_clear_request(self):
        """处理 CLEAR 请求，清空所有数据"""
        try:
            self.db.clear()
            return {"status": "success", "message": "All data cleared."}
        except Exception as e:
            self.logger.error(f"Error clearing data: {e}")
            return {"status": "error", "message": f"Error clearing data: {e}"}


if __name__ == "__main__":
    datanode = DistributedKVDataNode(host="127.0.0.1", port=5001)
    datanode.start_server()
