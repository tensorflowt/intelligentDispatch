from collections import defaultdict
from typing import List, Any, Dict, Tuple, Optional
import threading
import concurrent.futures
from wsgiref.util import request_uri

from Tree.safe_dict import ThreadSafeDict
from persistence.walmanager import WalManager


# TODO 添加反分词服务
class TreeNode:
    counter = 0

    def __init__(self, id: Optional[int] = None):
        self.children = defaultdict(TreeNode)  # key: token(int) → value: TreeNode
        self.parent: Optional[TreeNode] = None
        self.key: Optional[List[int]] = None  # 当前节点的token序列（如[101, 202, 303]）
        self.value = ThreadSafeDict()  # key: pod标识 → value: 相关信息（用户自行维护）
        self.decode_string: Optional[List[str]] = None  # 反分词结果
        self.id = TreeNode.counter if id is None else id
        TreeNode.counter += 1
        self.lock = threading.RLock()  # 节点级读写锁

        # 版本控制相关（核心保留）
        self.version = 0  # 节点最后修改的版本号
        self.old_info: Optional[Dict] = None  # 缓存“快照版本前”的旧状态
        self.status_delete = False  # 在执行快照期间如果被删除，那么会暂时缓存
        """
        old_info结构：
        {
            "version": 缓存时的快照版本,
            "key": 快照版本时的key,
            "value": 快照版本时的value（深拷贝）,
            "decode_string": 快照版本时的decode_string,
            "children": 快照版本时的子节点映射（token→node.id）,
            "parent_id": 快照版本时的父节点ID
        }
        """

    def cache_old_info(self, snap_version: int):
        """缓存旧状态（仅当节点版本 < 快照版本时，避免重复缓存）"""
        with self.lock:
            if self.version >= snap_version or self.old_info is not None:
                return
            # 深拷贝value，避免后续修改影响旧状态
            old_value = {}
            for k, v in self.value.items():
                old_value[k] = v.copy() if isinstance(v, (dict, list)) else v
            # 缓存当前状态作为旧状态（快照版本时的状态）
            self.old_info = {
                "version": snap_version,
                "key": self.key.copy() if self.key else None,
                "value": old_value,
                "decode_string": self.decode_string.copy() if self.decode_string else None,
                "children": [(token, child.id) for token, child in self.children.items()],
                "parent_id": self.parent.id if self.parent else None
            }
            # 更新节点版本为快照版本（标记已缓存）
            self.version = snap_version

    def get_snap_state(self, snap_version: int) -> Optional[Dict]:
        """获取快照版本对应的状态（优先返回old_info，无则返回当前状态）"""
        with self.lock:
            if self.old_info and self.old_info["version"] == snap_version:
                return self.old_info
            # 若节点未被快照版本修改，直接返回当前状态（深拷贝）
            if self.version < snap_version:
                current_value = {}
                for k, v in self.value.items():
                    current_value[k] = v.copy() if isinstance(v, (dict, list)) else v
                return {
                    "version": snap_version,
                    "key": self.key.copy() if self.key else None,
                    "value": current_value,
                    "decode_string": self.decode_string.copy() if self.decode_string else None,
                    "children": [(token, child.id) for token, child in self.children.items()],
                    "parent_id": self.parent.id if self.parent else None
                }
            return None  # 无对应版本状态（理论上不会触发）

    def clear_old_info(self):
        """清空旧状态（快照完成后调用）"""
        with self.lock:
            self.old_info = None


class MergePrefixTree:  # 合并树
    def __init__(self, wal_manager: WalManager = None, root: TreeNode = None):
        if root is None:
            self.root = TreeNode()
        else:
            self.root = root
        self.wal_manager = wal_manager
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=16)
        self.global_version = 0  # 全局版本
        self.global_version_lock = threading.Lock()

        self.active_snapshots: bool = False
        self.snapshot_trigger_version = 0  # 触发快照时的锁定版本

        self.current_finish_version = 0  # 当前完成变更的version
        self.freeze_finished_version = 0  # 执行快照时冻结的finished_version
        self.completed_finish = set()
        self.finish_lock = threading.Lock()

    def on_task_finished(self, task_id):
        """变更任务完成时调用"""
        with self.finish_lock:
            self.completed_finish.add(task_id)

            # 尝试向前推进 finish_id
            next_id = self.current_finish_version + 1
            while next_id in self.completed_finish:
                self.completed_finish.remove(next_id)
                self.current_finish_version = next_id
                next_id += 1

    def _get_global_version(self) -> int:
        with self.global_version_lock:
            self.global_version += 1
            return self.global_version

    def freeze_trigger_version(self):  # 给外部调用
        with self.global_version_lock:
            self.snapshot_trigger_version = self.global_version
            self.active_snapshots = True
        with self.finish_lock:
            self.freeze_finished_version = self.current_finish_version  # 后续如果大于这个版本并且小于snapshot_trigger_version 的都将记录到
        return self.snapshot_trigger_version, self.freeze_finished_version

    def update_prefix_tree(self, data: Dict[str, Any], write_wal=True):
        for update_info in data["updates"]:
            version = self._get_global_version()  # 为每一个node设置一个version
            if write_wal:
                self.wal_manager.append(update_info)  # 写WAL，异步非阻塞的，恢复是不写wal的
            if update_info["op_type"] == "insert_node":
                self._executor.submit(self.insert_prompt,
                                      update_info["prompt"],
                                      update_info["prompt_value"],
                                      update_info["instance_id"],
                                      version)
            elif update_info["op_type"] == "delete_node":
                self._executor.submit(self.evict_prompt,
                                      update_info["prompt"],
                                      update_info["length"],
                                      update_info["instance_id"],
                                      version)
            else:
                result = {"result": "failed", "message": "Not support"}

    def insert_prompt(self, key_list: list, value_list: list, instance_id: str, version: int):
        """
        :param key_list:  完整的prompt的 key
        :param value_list:  完整的prompt的 value
        :param instance_id:
        :param version:  暂时不用
        :return:
        """
        # 如果存在 还要加入当前节点 在匹配过程中
        current_node = self.root
        while len(key_list) > 0 and key_list[0] in current_node.children.keys():
            current_node.lock.acquire()
            child_node = current_node.children[key_list[0]]
            child_node.lock.acquire()
            length = self._match_length(key_list, child_node.key)  # 看看key_list 和value_list 有多长是相同的
            if length < len(child_node.key):
                # split node  split 并不改变树，所以也不影响version
                new_node = TreeNode()
                with new_node.lock:
                    if self.active_snapshots:
                        new_node.version = child_node.version  # 如果位于执行快照期间version不变
                    else:
                        new_node.version = version  #

                new_node.key = child_node.key[:length] # 新节点的key
                child_node.key = child_node.key[length:] # 就节点的key 做split
                for keys, values in child_node.value.items():
                    child_node.value[keys] = values[length:]  # 新的节点拿 length的
                    new_node.value[keys] = values[:length]  # 原节点只有剩下的
                new_node.parent = child_node.parent  # 新的节点父节点是当前节点的父节点
                child_node.parent.children[key_list[0]] = new_node  # 原来的父节点指向新的节点
                child_node.parent = new_node  # 当前节点的父节点更改为新的节点
                new_node.children[child_node.key[0]] = new_node  # 新节点的子节点是当前节点，但是需要更换索引，也就是现在的child_node.key[0]

                child_node.lock.release()  # 释放锁
                current_node.lock.release()  # 释放锁

                key_list = key_list[:length]  # 更新剩余prompt
                value_list = value_list[:length]  # 更新剩余prompt对应的value
                current_node = new_node  # 更新现在的节点为 new_node
            else:
                # child_node的value 加入一个instance_id value_list，无论是否存在都直接修改
                child_node.value[instance_id] = value_list[:length]

                value_list = value_list[length:]  # 更新剩下的value
                key_list = key_list[length:]  # 更新剩下的key

                child_node.lock.release()  # 释放子节点的锁
                current_node.lock.release()  # 释放父节点的锁
                current_node = child_node  # 更新current_node

        if len(key_list) > 0:
            # 还有剩余，那么就需要构建一个新的子节点
            with current_node.lock:  # 这里用with就可以了
                new_node = TreeNode()
                new_node.key = key_list
                new_node.value[instance_id] = value_list
                new_node.parent = current_node  # 更新父节点关系
                current_node.children[key_list[0]] = new_node  # 在父节点下添加新的子节点

        self.on_task_finished(task_id=version)  # 第一个更新完成版本

    @staticmethod
    def _match_length(key_value, node_value):
        length = 0
        while length < len(node_value) and length < len(key_value):
            if key_value[length] == node_value[length]:
                length += 1
            else:
                break
        return length

    def evict_prompt(self, key_list: list, instance_id: str, delete_length: int, version: int):
        # 删除逻辑是 删除有的，如果没找到，并不会报错
        # 在推理实例中并不是完整的删除一整条路径，而是删除叶子节点
        current_node = self.root
        int
        match_count = 0
        while len(key_list) > 0 and key_list[0] in current_node.children.keys():

            current_node.lock.acquire()
            child_node = current_node.children[key_list[0]]
            child_node.lock.acquire()
            length = self._match_length(key_list, child_node.key)
            match_count += length
            if not instance_id in child_node.value.keys():  # 如果这个节点里面并没有这个节点的信息
                break  # 那就是未找到
            if match_count <= delete_length:
                child_node.lock.release()
                current_node.lock.release()

                key_list = key_list[:length]  # 截断key_list
                current_node = child_node
            else:
                index = delete - (match_count - length)
                if len(child_node.value) == 1:  # 只有这一个节点用于它
                    if index == 0:
                        # 删除整个节点及其所有子节点
                        child_node.lock.release()
                        current_node.lock.release()
                        self._delete_next_all_node(current_node) # 删除这个节点及其所有子节点

                        pass
                    else:
                        # TODO 删除这个节点后面所有的节点（注意要释放内存）
                        child_node.key = child_node.key[:index]
                        child_node.value[instance_id] = child_node.value[instance_id][:index]
                        child_node.lock.release()
                        current_node.lock.release()
                        for key, value in child_node.children.items():
                            self._delete_next_all_node(value)

                else: # 有两个及以上节点拥有该Key
                    if index == 0:
                        del child_node.value[instance_id] # 删除这个value
                        # TODO 删除这个节点后面所有节点里面 instace_id的信息（理论上不会有这一步）
                    else:
                        # TODO Split
                        new_node = TreeNode()
                        new_node.key = child_node.key[:index]
                        child_node.key = child_node.key[index:]
                        for keys, values in child_node.value.items():
                            child_node.value[keys] = values[index:]  # 新的节点拿 length的
                            new_node.value[keys] = values[:index]  # 原节点只有剩下的

                        new_node.parent = child_node.parent  # 新的节点父节点是当前节点的父节点
                        child_node.parent = new_node  # 当前节点的父节点更改为新的节点

                        child_node.parent.children[key_list[0]] = new_node  # 原来的父节点指向新的节点
                        new_node.children[child_node.key[0]] = new_node  # 新节点的子节点是当前节点，但是需要更换索引，也就是现在的child_node.key[0]

                        child_node.lock.release()  # 释放锁
                        current_node.lock.release()  # 释放锁

                        key_list = key_list[index:]  # 更新剩余prompt
                        current_node = new_node  # 更新现在的节点为 new_node

                        match_count = match_count - length + index # 继续进入下次循环，进入循环之前重置下match_count


    def _delete_next_all_node(self, node: TreeNode):
        # 递归删除一个节点及其下面的所有节点
        if len(node.children) == 0:
            parent_node = node.parent
            parent_node.lock.acquire()
            for key, value in parent_node.children.items():
                if value == node:
                    break
            del parent_node.children[key]
            parent_node.lock.release()
        else:
            for key, value in node.children.items():
                self._delete_next_all_node(value)


    def _evict_prompt_by_instance(self, node:TreeNode, instace_id: str):
        #
            node.lock.acquire()
            if instace_id not in node.value.keys():
                return
            else:
                if len(node.value) == 1:
                    node.lock.release()
                    self._delete_next_all_node(node)
                else:
                    del node.value[instace_id] # value里面删除 instance_id
                    node.lock.release()
                    for key, value in node.children.items():
                        self._evict_prompt_by_instance(value, instace_id)
                    # node.lock.release() 加在这里可能会死锁

