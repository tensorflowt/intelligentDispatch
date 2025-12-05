import threading
from collections import defaultdict
from typing import List, Any, Dict, Tuple, Optional, Set
from KvCacheIndex.base_prefix_cache import BasePrefixCache

from utils.logger import logger

class TreeNode:
    counter = 0
    def __init__(self, id: Optional[int] = None):
        self.children = defaultdict(TreeNode)
        self.parent: TreeNode = None
        self.key: List[int] = None
        self.value: List[int] = None
        self.lock = threading.RLock()  # 节点级锁，保证局部线程安全

        self.id = TreeNode.counter if id is None else id
        TreeNode.counter += 1

    def delete_subtree(self):
        """安全地递归删除当前节点及其所有子节点"""
        with self.lock:
            for k, child in list(self.children.items()):
                child.delete_subtree()
            self.children.clear()

            if self.parent is not None:
                with self.parent.lock:
                    for k, v in list(self.parent.children.items()):
                        if v is self:
                            del self.parent.children[k]
                            break
                self.parent = None
            self.key = None
            self.value = None


class RadixTree(BasePrefixCache):
    def __init__(self, instance_id):
        self.root = TreeNode()
        self.instance_id = instance_id

    
    def _print_tree(self):
        self._print_helper(self.root, 0)
    
    def _print_helper(self, node: TreeNode, indent: int):
        logger.info("execute _print_helper")
        """Prints the radix tree in a human-readable format."""
        stack = [(node, indent)]
        while stack:
            current_node, current_indent = stack.pop()
            logger.info("{}  {}  {}".format(" "* current_indent, 0 if current_node.key is None else len(current_node.key), [] if current_node.key is None else current_node.key[:10] ))
            for _, child in current_node.children.items():
                stack.append((child, current_indent + 2))



    def recover_tree_from_dict(self, tree_info: dict):
        # 外部调用接口
        # 创建节点
        self.root = self._build_tree_from_dict(tree_info)
        self._print_tree()

    def _build_tree_from_dict(self, tree_info: dict, parent: TreeNode = None) -> TreeNode:
        node = TreeNode()
        # 属性
        node.key = tree_info.get("key", [])
        node.value = tree_info.get("value", [])
        node.parent = parent

        # 递归构建 children
        for child_dict in tree_info.get("children", []):
            child = self._build_tree_from_dict(child_dict, parent=node)
            # ★ 用 child.key[0] 作为 children 的 key
            if child.key is None or len(child.key) == 0:
                raise ValueError("子节点 key 不能为空，无法作为 children 的字典 key。")
            idx = child.key[0]  # 关键点：取第 0 个元素
            node.children[idx] = child
        return node


    def apply_op(self, op: Dict):
        op_type = op["op_type"]
        if op_type == "insert_node":
            self._insert_node(op["parent_path"], op["insert_key"], op["insert_value"])
        elif op_type == "delete_node":
            self._delete_node(op["parent_path"])
        elif op_type == "split_node":
            self._split_node(op["parent_path"], op["split_length"])
        else:
            logger.warning(f"Unknown op_type: {op_type}")

    def _find_node(self, parent_path):
        node = self.root
        for seg in parent_path:
            key = seg[0]
            with node.lock:
                if key not in node.children:
                    return None
                node = node.children[key]
        return node

    def _insert_node(self, parent_path: list, node_key: list, node_value: list):
        logger.info(f"insert node: {node_key}")


        node = self._find_node(parent_path)
        logger.info("node is None?:{}".format(node is None))
        if node:
            with node.lock:
                new_node = TreeNode()
                new_node.key = node_key
                new_node.value = node_value
                new_node.parent = node
                node.children[node_key[0]] = new_node
            self._print_tree()
            return True
        else:
            return False

    def _split_node(self, parent_path: list, split_length: int):
        node = self._find_node(parent_path)
        logger.info("node is None?:{}".format(node is None))
        if node:
            with node.lock:
                new_node = TreeNode()
                key = node.key[0] # 这里先提出key，这个key是 分割节点 的parent children 的key  node.parent.children[key] = node
                new_node.children[node.key[split_length]] = node
                new_node.parent = node.parent
                new_node.key = node.key[:split_length]
                new_node.value = node.value[:split_length]
                node.parent = new_node
                node.key = node.key[split_length:]
                node.value = node.value[split_length:]
                with new_node.parent.lock:
                    new_node.parent.children[key] = new_node
                new_node.key = node.key[:split_length]
                new_node.value = node.value[:split_length]
            self._print_tree()
            return True
        else:
            return False

    def _delete_node(self, parent_path: list):
        node = self._find_node(parent_path)
        logger.info("node is None?:{}".format(node is None))
        for k, v in node.parent.items():
            if v == node:
                break
        del node.parent.children[k] # 更radix tree的删除方式保持一致
