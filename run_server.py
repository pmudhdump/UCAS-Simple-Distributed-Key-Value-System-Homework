import argparse
import sys
from servernode.serverui import DistributedKVServerNode
# python run_server.py server 127.0.0.1 5001 --raft 127.0.0.1:5002,127.0.0.1:5003 --datanode 127.0.0.1:5003,127.0.0.1:5004 --node_id 1



def start_server(host, port, raft_addresses, datanode_addresses, node_id):
    # 解析Raft配置
    raft_config = {"node_id": node_id, "cluster": raft_addresses.split(",")}

    # 解析datanode配置
    datanodes = [(address.split(":")[0], int(address.split(":")[1])) for address in datanode_addresses.split(",")]

    # 初始化Raft节点
    server_node = DistributedKVServerNode(
        host=host,
        port=port,
        datanodes=datanodes,
        raft_config=raft_config
    )

    # 启动服务器
    server_node.start_server()


def parse_arguments():
    parser = argparse.ArgumentParser(description="Distributed KV Store Server")

    subparsers = parser.add_subparsers(dest="command")

    # 定义 server 启动命令
    server_parser = subparsers.add_parser("server", help="Start server")
    server_parser.add_argument("host", type=str, help="Server host address")
    server_parser.add_argument("port", type=int, help="Server port")
    server_parser.add_argument("--raft", type=str, required=True, help="Comma-separated list of Raft cluster addresses")
    server_parser.add_argument("--datanode", type=str, required=True, help="Comma-separated list of DataNode addresses")
    server_parser.add_argument("--node_id", type=int, required=True, help="Node ID for the Raft node")

    return parser.parse_args()


def main():
    args = parse_arguments()

    if args.command == "server":
        start_server(args.host, args.port, args.raft, args.datanode, args.node_id)
    else:
        print("Invalid command. Please use 'server' to start the server.")
        sys.exit(1)


if __name__ == "__main__":
    main()
