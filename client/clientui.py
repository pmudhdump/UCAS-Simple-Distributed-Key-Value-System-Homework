import grpc
import logging
from utils.uuid_tag import generate_operation_id
import protos.kvstore_pb2 as kvstore_pb2
import protos.kvstore_pb2_grpc as kvstore_pb2_grpc


class DistributedKVClient:
    def __init__(self, host="127.0.0.1", port=5000):
        """
        初始化客户端。
        Args:
            host (str): 主节点的主机地址，默认为本地地址。
            port (int): 主节点的端口号，默认为5000。
        """
        self.host = host
        self.port = port
        self.setup_logging()

    def setup_logging(self):
        """设置日志记录"""
        self.logger = logging.getLogger("DistributedKVClient")
        self.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler("client.log")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        console_handler = logging.StreamHandler()  # 添加终端日志输出
        console_handler.setFormatter(logging.Formatter("%(message)s"))

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def send_request(self, request):
        """
        发送请求到服务器并接收响应。
        Args:
            request (dict): 请求数据。
        Returns:
            dict: 服务器返回的响应。
        """
        try:
            with grpc.insecure_channel(f"{self.host}:{self.port}") as channel:
                stub = kvstore_pb2_grpc.KVStoreServiceStub(channel)
                if request["action"] == "put":
                    response = stub.Put(kvstore_pb2.PutRequest(
                        key=request["key"],
                        value=request["value"],
                        operation_id=request["operation_id"]
                    ))
                elif request["action"] == "get":
                    response = stub.Get(kvstore_pb2.GetRequest(
                        key=request["key"],
                        operation_id=request["operation_id"]
                    ))
                elif request["action"] == "delete":
                    response = stub.Delete(kvstore_pb2.DeleteRequest(
                        key=request["key"],
                        operation_id=request["operation_id"]
                    ))
                elif request["action"] == "list":
                    response = stub.List(kvstore_pb2.ListRequest(operation_id=request["operation_id"]))
                elif request["action"] == "clear":
                    response = stub.Clear(kvstore_pb2.ClearRequest(operation_id=request["operation_id"]))
                else:
                    raise ValueError("Invalid action.")

                return {
                    "status": "success" if response.success else "error",
                    "message": response.message,
                    "value": response.value if hasattr(response, 'value') else None,
                    "data": response.keys if hasattr(response, 'keys') else None
                }

        except Exception as e:
            self.logger.error(f"Failed to connect to server: {e}")
            return {"status": "error", "message": str(e)}

    def handle_response(self, response):
        """
        处理服务器返回的响应。
        Args:
            response (dict): 服务器返回的响应。
        """
        if response.get("status") == "success":
            print(f"Success: {response.get('message', response.get('value', response.get('data', 'No details.')))}")
        else:
            print(f"Error: {response.get('message', 'Unknown error.')}")

    def run_cli(self):
        """运行命令行交互界面"""
        print("Welcome to the Distributed KV Store CLI.")
        print("Available commands: PUT, GET, DELETE, LIST, CLEAR, EXIT")

        while True:
            try:
                command = input("Enter command: ").strip().upper()
                if command == "EXIT":
                    print("Exiting CLI. Goodbye!")
                    break

                request = None
                if command == "PUT":
                    key = input("Enter key: ").strip()
                    value = input("Enter value: ").strip()
                    request = {
                        "action": "put",
                        "key": key,
                        "value": value,
                        "operation_id": generate_operation_id()
                    }
                elif command == "GET":
                    key = input("Enter key: ").strip()
                    request = {
                        "action": "get",
                        "key": key,
                        "operation_id": generate_operation_id()
                    }
                elif command == "DELETE":
                    key = input("Enter key: ").strip()
                    request = {
                        "action": "delete",
                        "key": key,
                        "operation_id": generate_operation_id()
                    }
                elif command == "LIST":
                    request = {
                        "action": "list",
                        "operation_id": generate_operation_id()
                    }
                elif command == "CLEAR":
                    request = {
                        "action": "clear",
                        "operation_id": generate_operation_id()
                    }
                else:
                    print("Invalid command. Try again.")
                    continue

                # 发送请求并接收响应
                self.logger.info(f"Sending request: {request}")
                response = self.send_request(request)

                # 处理响应
                self.handle_response(response)
                self.logger.info(f"Received response: {response}")

            except KeyboardInterrupt:
                print("\nExiting CLI. Goodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")
                self.logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    client = DistributedKVClient()
    client.run_cli()
