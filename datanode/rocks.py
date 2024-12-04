import threading
from rocksdict import Rdict


class RocksDictStore:
    def __init__(self, db_path):
        self.db_path = db_path
        self.db = Rdict(db_path)
        self.lock = threading.Lock()  # 创建一个锁来确保线程安全

    def put(self, key, value):
        """存储一个键值对"""
        try:
            with self.lock:  # 使用锁来保护数据库操作
                self.db[key] = value
        except Exception as e:
            print(f"Error storing key '{key}': {e}")
            # 你可以选择记录日志到文件或其他错误处理措施

    def get(self, key):
        """根据键获取值"""
        try:
            with self.lock:  # 使用锁来保护数据库操作
                return self.db.get(key, None)
        except Exception as e:
            print(f"Error getting key '{key}': {e}")
            return None

    def delete(self, key):
        """删除一个键值对"""
        try:
            with self.lock:  # 使用锁来保护数据库操作
                if key in self.db:
                    del self.db[key]
        except Exception as e:
            print(f"Error deleting key '{key}': {e}")

    def list_all(self, batch_size=100):
        """列出所有键值对，分页返回"""
        try:
            with self.lock:  # 使用锁来保护数据库操作
                return {key: value for key, value in self.db.items()}
        except Exception as e:
            print(f"Error listing all keys: {e}")

    def clear(self):
        """清空所有键值对"""
        try:
            with self.lock:  # 使用锁来保护数据库操作
                for key in self.db:
                    del self.db[key]
        except Exception as e:
            print(f"Error clearing the database: {e}")

    def kill(self):
        """关闭数据库"""
        try:
            with self.lock:  # 使用锁来保护数据库操作
                self.db.close()
                Rdict.destroy(self.db_path)  # 删除数据库文件
        except Exception as e:
            print(f"Error closing the database: {e}")
