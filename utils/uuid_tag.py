import uuid
import time


def generate_operation_id():
    """生成唯一操作 ID"""
    return str(uuid.uuid4())


def has_handled(self, operation_id):
    """检查操作是否已处理"""
    return operation_id in self.handled_operations


def mark_handled(self, operation_id):
    """记录操作为已处理"""
    self.handled_operations[operation_id] = time.time()
