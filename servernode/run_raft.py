from raft import *

if __name__ == '__main__':
    raft_config = {
        'peers': ['localhost:7000', ],
        'host': 'localhost',
        'port': 7001
    }
    node = RaftNode(raft_config['host'], raft_config['port'], raft_config)
    node.start()