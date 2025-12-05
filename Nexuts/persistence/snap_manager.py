import os
import threading
import pickle
from datetime import datetime
import time
from collections import deque
from os import chown

from Tree.tree import MergePrefixTree, TreeNode
from Tree.safe_dict import ThreadSafeDict
from persistence.walmanager import WalManager
from utils.logger import logger



class SnapshotManager:
    def __init__(self,
                 tree: MergePrefixTree,
                 wal_manager: WalManager,
                 snapshot_dir: str,
                 interval_seconds: int = 600,
                 resume: bool = True):
        self.tree = tree
        self.wal_manager = wal_manager

        self.snapshot_dir = snapshot_dir
        self.interval_seconds = interval_seconds
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread = None
        os.makedirs(snapshot_dir, exist_ok=True)
        self.start_auto_snapshot()
        if resume:  
            self.load_snapshot()  
            wal_recover = self.wal_manager.load_resume_records() # 加载wal日志  
            for ops_info in wal_recover:  
                # 将WAL记录包装成update_prefix_tree期望的格式  
                wrapped_ops_info = {  
                    "updates": [ops_info]  # WAL记录包装在updates列表中  
                }  
                self.tree.update_prefix_tree(wrapped_ops_info, write_wal=False) # 不写wal了


    def _snapshot_filename(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"snapshot_{timestamp}.pkl"

    def _snapshot_path(self, filename: str) -> str:
        return os.path.join(self.snapshot_dir, filename)

    def start_auto_snapshot(self):
        self._stop_event.clear()
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(target=self._auto_snapshot_loop, daemon=True)
            self._thread.start()

    def stop_auto_snapshot(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join()
            self._thread = None

    def _auto_snapshot_loop(self):
        while not self._stop_event.is_set():
            self._stop_event.wait(self.interval_seconds)
            self.take_snapshot()

    # ------------------------------
    # BFS方式收集快照
    # ------------------------------
    def _serialize_tree_bfs(self, root: TreeNode, snapshot_version: int):
        if not root:
            return list()
        queue = deque()
        node_list = []
        parent_node = root.parent
        queue.append((root, parent_node))

        while queue:
            node, parent_node = queue.popleft()
            if parent_node is not None:
                parent_node.lock.acquire()
            node.lock.acquire()
            # 保存旧信息
            children_ids = [child.id for child in node.children.values()] # 这个地方记录的是 节点的id
            if node.version <= snapshot_version:  # 直接记录，小于快照版本
                node_node = {
                    "id": node.id,
                    "key": node.key.copy() if node.key else None,
                    "value": node.value.copy() if node.value else None,
                    "decode_string": node.decode_string.copy() if node.decode_string else None,
                    "children": children_ids,
                    "version": node.version  # TODO 是不是不用recover的时候这个东西了？
                }
                node_list.append(node_node)
            else:  # 如果版本大于
                node_node = {
                    "id": node.old_info.id,
                    "key": node.old_info.key.copy() if node.old_info else None,
                    "value": node.old_info.value.copy() if node.old_info else None,
                    "decode_string": node.old_info.decode_string.copy() if node.old_info.decode_string else None,
                    "children": children_ids,
                    "version": node.old_info.version  #
                }
                node_list.append(node_node)

            if parent_node is not None:
                parent_node.lock.release()
            node.lock.release()

            for child in node.children.values():
                queue.append((child, node))

        return node_list


    # ------------------------------
    # 生成快照
    # ------------------------------
    def take_snapshot(self) -> str:
        with self._lock:
            snapshot_trigger_version, freeze_finish_version = self.tree.freeze_trigger_version()  # 执行快照时的版本、finish（完成变更）版本
            assert (snapshot_trigger_version - freeze_finish_version) >= 0, "error version in {snapshot_trigger_version - freeze_finish_version}"
            result_holder = {}  # 存放 snapshot_data
            done_event = threading.Event()
            def bfs_job():
                snapshot_data = self._serialize_tree_bfs(self.tree.root, snapshot_trigger_version) #
                result_holder["data"] = snapshot_data
                done_event.set()

            threading.Thread(target=bfs_job, daemon=True).start()

            # 将log.logs的 snapshot_trigger_version - freeze_finish_version 行，重定向输入到 log2.logs 里面
            logger.info("snapshot_trigger_version:{} freeze_finish_version:{}".format(snapshot_trigger_version, freeze_finish_version))
            self.wal_manager.rotate(line_n=snapshot_trigger_version - freeze_finish_version)  # 更换记录文件，同时记录在执行快照期间发生的变更

            filename = self._snapshot_filename()
            temp_path = self._snapshot_path(filename + ".tmp")
            final_path = self._snapshot_path(filename)

            done_event.wait()  # 阻塞直到后台 BFS 完成
            snapshot_data = result_holder["data"]

            with open(temp_path, "wb") as f:
                pickle.dump(snapshot_data, f)
            os.replace(temp_path, final_path)

            # 生成快照后需要将   tree.active_snapshots 改成false
            self.tree.active_snapshots = False

            self.wal_manager.commit_new_log()  # 删除旧文件，更新文件的名字
            self._cleanup_old_snapshots(final_path)
            return final_path


    def _cleanup_old_snapshots(self, keep_file: str):
        for fname in os.listdir(self.snapshot_dir):
            path = os.path.join(self.snapshot_dir, fname)
            if path != keep_file and fname.endswith(".pkl"):
                try:
                    os.remove(path)
                except Exception as e:
                    print(f"Failed to remove old snapshot {path}: {e}")


    # ------------------------------
    # BFS迭代恢复树
    # ------------------------------
    def load_snapshot(self, path: str = None) -> MergePrefixTree:
        if path is None:
            pkl_files = [f for f in os.listdir(self.snapshot_dir) if f.endswith(".pkl")]
            if not pkl_files:
                logger.info(f"No snapshot found at {self.snapshot_dir}")
                return
            pkl_files.sort()
            path = os.path.join(self.snapshot_dir, pkl_files[-1])

        with open(path, "rb") as f:
            snapshot_data = pickle.load(f)

        logger.info(f"Loading snapshot snapshot_data {snapshot_data}")
        # BFS迭代恢复树
        if snapshot_data is None:
            return
        node_map = {}
        node_children_map = {}
        root_node = None
        for i, node_info in enumerate(snapshot_data):
            new_node = TreeNode(id=node_info["id"])
            new_node.version = node_info.get("version", 0)
            new_node.key = node_info.get("key", None)
            new_node.value = ThreadSafeDict()
            new_node.decode_string = node_info.get("decode_string", None)
            if node_info.get("value") is not None:
                for k, v in node_info.get("value", {}).items():
                    root_node.value[k] = v
            node_map[new_node.id] = new_node
            node_children_map[new_node.id] = node_info.get("children", [])
            if i == 0:
                root_node = new_node
        node_map[root_node.id] = root_node

        queue = deque()
        queue.append((root_node, node_children_map[root_node.id]))

        while queue:
            parent_node, child_id_map = queue.popleft()
            parent_node.children = {}
            for child_id in child_id_map: # children是 list呀 是child的 id
                child_node = node_map[child_id]
                key_index0 = child_node.key[0]
                parent_node.children[key_index0] = child_node
                child_node.parent = parent_node
                queue.append((child_node, node_children_map[child_node.id]))
        self.tree.root = root_node
