import hashlib
import bisect


class ConsistentHashing:
    def __init__(self, nodes):
        """初始化一致性哈希环
        :param nodes: 节点列表，每个节点可以是节点的标识符（例如，IP+端口）
        """
        self.nodes = sorted(nodes)  # 节点列表按哈希值排序
        self.ring = self.create_ring(nodes)

    def _hash(self, key):
        """计算哈希值"""
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def create_ring(self, nodes):
        """根据节点列表创建哈希环
        :param nodes: 节点列表
        :return: 排序后的哈希值列表
        """
        ring = []
        for node in nodes:
            # 每个节点根据其自身的哈希值在环中占有一个位置
            ring.append((self._hash(node), node))
        ring.sort()  # 按哈希值排序
        return ring

    def get_node(self, key):
        """根据 key 查找目标节点"""
        if not self.ring:
            return None

        # 计算 key 的哈希值
        hash_value = self._hash(key)

        # 查找哈希环中第一个比 key 哈希值大的节点
        idx = bisect.bisect(self.ring, (hash_value, ''))

        # 如果没有找到，返回第一个节点
        if idx == len(self.ring):
            return self.ring[0][1]

        return self.ring[idx][1]
