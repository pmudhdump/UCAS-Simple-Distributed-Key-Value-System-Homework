import json

class SocketProtocol:
    def send(self, conn, data):
        """发送数据到客户端"""
        message = json.dumps(data).encode("utf-8")
        conn.sendall(message + b"\n")

    def receive(self, conn):
        """接收客户端的数据"""
        data = b""
        while not data.endswith(b"\n"):
            data += conn.recv(1024)
        return json.loads(data.decode("utf-8"))
