import os
import json
import pickle
import lz4.frame
import queue
import time
from datetime import datetime
from collections import defaultdict
from typing import List, Any, Dict, Tuple, Optional, Set
import threading
import concurrent.futures
from safe_dict import ThreadSafeDict

# 前缀树节点类（保留version+old_info，删除节点逻辑用户自行处理）
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


# 持久化管理器（调整WAL写入时机配合任务执行，保留核心快照逻辑）
class PersistenceManager:
    def __init__(
            self,
            data_dir: str = "./prefix_tree_persist",
            wal_max_size: int = 512 * 1024 * 1024,  # 单WAL文件最大512MB
            snap_interval: int = 1800,  # 快照间隔30分钟（可调整）
            snap_keep_count: int = 1  # 只保留最新1个快照（核心配置）
    ):
        self.data_dir = data_dir
        self.wal_dir = os.path.join(data_dir, "wal")
        self.snap_dir = os.path.join(data_dir, "snapshots")
        os.makedirs(self.wal_dir, exist_ok=True)
        os.makedirs(self.snap_dir, exist_ok=True)

        # WAL配置（按用户要求：任务执行成功后才写入）
        self.wal_max_size = wal_max_size
        self.current_wal_file: Optional[str] = None
        self.wal_lock = threading.Lock()  # WAL文件写入锁
        self._init_current_wal()

        # 快照配置
        self.snap_interval = snap_interval
        self.snap_keep_count = snap_keep_count
        self.tree: Optional["MergePrefixTree"] = None  # 关联的前缀树实例
        self.snap_thread = threading.Thread(target=self._snapshot_loop, daemon=True)
        self.snap_thread.start()

    def _init_current_wal(self):
        """初始化当前WAL文件（按时间戳命名）"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.current_wal_file = os.path.join(self.wal_dir, f"wal_{timestamp}.log")

    def _rotate_wal(self):
        """WAL轮转（快照触发时强制轮转，确保快照后任务写入新WAL）"""
        with self.wal_lock:
            old_wal = self.current_wal_file
            self._init_current_wal()
            print(f"[WAL] 强制轮转：旧文件{os.path.basename(old_wal)} → 新文件{os.path.basename(self.current_wal_file)}")

    def write_log(self, log_entry: Dict):
        """写入WAL日志（仅任务执行成功后调用）"""
        with self.wal_lock:
            # 按大小自动轮转
            if self.current_wal_file and os.path.exists(self.current_wal_file):
                if os.path.getsize(self.current_wal_file) >= self.wal_max_size:
                    self._init_current_wal()
                    print(f"[WAL] 大小触发轮转，新文件：{os.path.basename(self.current_wal_file)}")
            # 写入日志（JSON每行一条，便于回放）
            with open(self.current_wal_file, "a", encoding="utf-8") as f:
                json.dump(log_entry, f, ensure_ascii=False)
                f.write("\n")
        print(f"[WAL] 写入日志：{log_entry['op_type']}（instance：{log_entry['instance_id']}）")

    def bind_tree(self, tree: "MergePrefixTree"):
        """绑定前缀树实例（服务启动时调用）"""
        self.tree = tree

    def _snapshot_loop(self):
        """后台快照循环：确保同一时间仅1个快照任务"""
        while True:
            time.sleep(self.snap_interval)
            if self.tree and not self.tree.is_snap_running():
                print(f"\n[快照] 开始生成快照（间隔：{self.snap_interval}s）")
                self.create_snapshot()
            else:
                print(f"[快照] 跳过本次：树未绑定或快照正在运行")

    def create_snapshot(self):
        """生成一致性快照（版本控制）+ WAL轮转+清理旧数据"""
        if not self.tree:
            print(f"[快照] 未绑定树实例，生成失败")
            return

        # 步骤1：生成全局快照版本（时间戳，确保唯一性）
        snap_version = int(time.time() * 1000)
        try:
            # 步骤2：标记快照开始（阻止并发快照）
            self.tree.set_snap_running(True)

            # 步骤3：强制轮转WAL（关键！快照后任务写入新WAL）
            self._rotate_wal()

            # 步骤4：序列化一致性快照（基于snap_version）
            print(f"[快照] 开始序列化（版本：{snap_version}）...")
            snapshot_data = self._serialize_consistent_tree(snap_version)
            node_count = len(snapshot_data["nodes"])
            print(f"[快照] 序列化完成，共{node_count}个节点")

            # 步骤5：写入快照文件（lz4压缩）
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            snap_file = os.path.join(
                self.snap_dir, f"snap_{timestamp}_{snap_version}_{node_count}.snap"
            )
            with open(snap_file, "wb") as f:
                compressed_data = lz4.frame.compress(pickle.dumps(snapshot_data))
                f.write(compressed_data)

            # 步骤6：清理旧快照（只留最新1个）
            self._clean_expired_snaps()

            # 步骤7：清理过期WAL（删除所有早于当前快照的WAL，仅留新WAL）
            self._clean_expired_wal(snap_version)

            print(f"[快照] 生成成功：{os.path.basename(snap_file)}")

        except Exception as e:
            print(f"[快照] 生成失败：{e}")
        finally:
            # 步骤8：清理资源（清空所有节点old_info）
            self.tree.clear_snap_resources()
            self.tree.set_snap_running(False)
            print(f"[快照] 资源清理完成（版本：{snap_version}）\n")

    def _serialize_consistent_tree(self, snap_version: int) -> Dict:
        """序列化一致性快照（遍历所有节点，基于快照版本获取状态）"""
        snapshot_data = {
            "root_id": self.tree.root.id,
            "nodes": {},
            "snap_version": snap_version,
            "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        visited = set()
        queue = [self.tree.root]

        while queue:
            node = queue.pop(0)
            if node.id in visited:
                continue
            visited.add(node.id)

            # 获取该节点在快照版本的状态
            node_state = node.get_snap_state(snap_version)
            if not node_state:
                continue

            # 序列化节点状态
            snapshot_data["nodes"][node.id] = {
                "parent_id": node_state["parent_id"],
                "key": node_state["key"],
                "decode_string": node_state["decode_string"],
                "value": node_state["value"],
                "children": node_state["children"]  # token→node.id映射
            }

            # 遍历子节点（基于快照版本的子节点列表）
            for _, child_id in node_state["children"]:
                # 递归查找子节点（用户自行维护节点存在性，这里假设节点未被物理删除）
                child_node = self._find_node_by_id(child_id, self.tree.root, visited)
                if child_node and child_node.id not in visited:
                    queue.append(child_node)

        return snapshot_data

    def _find_node_by_id(self, target_id: int, current_node: TreeNode, visited: Set[int]) -> Optional[TreeNode]:
        """递归查找节点（基于节点ID）"""
        if current_node.id == target_id:
            return current_node
        visited.add(current_node.id)
        for child in current_node.children.values():
            if child.id not in visited:
                result = self._find_node_by_id(target_id, child, visited)
                if result:
                    return result
        return None

    def _clean_expired_snaps(self):
        """清理旧快照：只保留最新1个"""
        snaps = sorted(
            [f for f in os.listdir(self.snap_dir) if f.startswith("snap_")],
            reverse=True  # 按版本号倒序（最新在前）
        )
        if len(snaps) > self.snap_keep_count:
            for snap in snaps[self.snap_keep_count:]:
                snap_path = os.path.join(self.snap_dir, snap)
                try:
                    os.remove(snap_path)
                    print(f"[快照清理] 删除旧快照：{snap}")
                except Exception as e:
                    print(f"[快照清理] 失败：{snap} → {e}")

    def _clean_expired_wal(self, snap_version: int):
        """清理过期WAL：删除所有早于快照版本的WAL（仅留快照后新生成的WAL）"""
        keep_wal = self.current_wal_file
        wal_files = [f for f in os.listdir(self.wal_dir) if f.startswith("wal_")]
        deleted_count = 0

        for wal_file in wal_files:
            wal_path = os.path.join(self.wal_dir, wal_file)
            if wal_path == keep_wal:
                continue  # 保留快照后的新WAL
            try:
                os.remove(wal_path)
                deleted_count += 1
                print(f"[WAL清理] 删除过期日志：{wal_file}")
            except Exception as e:
                print(f"[WAL清理] 删除失败：{wal_file} → {e}")

        print(f"[WAL清理] 完成，共删除{deleted_count}个过期WAL，保留新WAL：{os.path.basename(keep_wal)}")

    def recover_tree(self) -> "MergePrefixTree":
        """从持久化数据恢复树：最新快照 + 快照后的WAL"""
        print("\n[恢复] 开始从持久化数据恢复...")
        tree = MergePrefixTree(persist_manager=self)

        # 步骤1：加载最新快照（全量数据）
        latest_snap = self._get_latest_snapshot()
        if latest_snap:
            try:
                snap_version = self._deserialize_snap(latest_snap, tree)
                print(f"[恢复] 成功加载快照：{os.path.basename(latest_snap)}（版本：{snap_version}）")
            except Exception as e:
                print(f"[恢复] 快照加载失败，创建新树：{e}")
                return MergePrefixTree(persist_manager=self)
        else:
            print("[恢复] 无快照文件，创建新树")
            return MergePrefixTree(persist_manager=self)

        # 步骤2：回放快照后的WAL（增量数据，仅回放新WAL）
        wal_files = self._get_wal_after_snap(latest_snap)
        if wal_files:
            print(f"[恢复] 发现{len(wal_files)}个需回放的WAL文件（快照后增量）")
            for wal_file in wal_files:
                self._replay_wal(wal_file, tree)
        else:
            print("[恢复] 无快照后的增量WAL，恢复完成")

        return tree

    def _get_latest_snapshot(self) -> Optional[str]:
        """获取最新的快照文件"""
        snaps = [f for f in os.listdir(self.snap_dir) if f.startswith("snap_")]
        if not snaps:
            return None
        # 按版本号倒序（最新快照版本最大）
        snaps.sort(reverse=True, key=lambda x: int(x.split("_")[2]))
        return os.path.join(self.snap_dir, snaps[0])

    def _deserialize_snap(self, snap_file: str, tree: "MergePrefixTree") -> int:
        """反序列化快照到树，返回快照版本"""
        with open(snap_file, "rb") as f:
            compressed_data = f.read()
            snapshot_data = pickle.loads(lz4.frame.decompress(compressed_data))

        # 1. 重建节点字典
        node_map: Dict[int, TreeNode] = {}
        for node_id, node_data in snapshot_data["nodes"].items():
            node = TreeNode(id=node_id)
            node.key = node_data["key"]
            node.decode_string = node_data["decode_string"]
            # 恢复value（深拷贝）
            for k, v in node_data["value"].items():
                node.value[k] = v.copy() if isinstance(v, (dict, list)) else v
            node_map[node_id] = node

        # 2. 重建父子关系和子节点映射
        for node_id, node_data in snapshot_data["nodes"].items():
            node = node_map[node_id]
            parent_id = node_data["parent_id"]
            if parent_id is not None and parent_id in node_map:
                node.parent = node_map[parent_id]
            # 重建子节点映射（token→node）
            for child_token, child_id in node_data["children"]:
                if child_id in node_map:
                    node.children[child_token] = node_map[child_id]

        # 3. 设置根节点并重置计数器
        tree.root = node_map[snapshot_data["root_id"]]
        TreeNode.counter = max(node_map.keys()) + 1 if node_map else 0

        return snapshot_data["snap_version"]

    def _get_wal_after_snap(self, snap_file: str) -> List[str]:
        """获取快照后的WAL文件（仅新生成的WAL）"""
        # 快照文件名格式：snap_20251117_100000_123456_100.snap
        snap_timestamp = snap_file.split("_")[1] + snap_file.split("_")[2][:6]
        wal_files = []
        for wal_file in os.listdir(self.wal_dir):
            if not wal_file.startswith("wal_"):
                continue
            # WAL文件名格式：wal_20251117_100000.log
            wal_timestamp = wal_file.split("_")[1]
            if wal_timestamp >= snap_timestamp:
                wal_files.append(os.path.join(self.wal_dir, wal_file))
        # 按时间正序排列（先回放旧的）
        return sorted(wal_files, key=lambda x: x.split("_")[1])

    def _replay_wal(self, wal_file: str, tree: "MergePrefixTree"):
        """回放单个WAL文件（增量恢复）"""
        print(f"[恢复] 回放WAL：{os.path.basename(wal_file)}")
        with open(wal_file, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                try:
                    log_entry = json.loads(line.strip())
                    self._apply_log_entry(log_entry, tree)
                except Exception as e:
                    print(f"[恢复] WAL行{line_num}回放失败：{e} → 内容：{line.strip()}")

    def _apply_log_entry(self, log_entry: Dict, tree: "MergePrefixTree"):
        """应用单个日志条目到树（恢复时直接执行，无需再写WAL）"""
        op_type = log_entry.get("op_type")
        if op_type == "insert_token":
            tree.insert_prompt(
                key_list=log_entry["key_list"],
                value_list=log_entry["value_list"],
                instance_id=log_entry["instance_id"],
                skip_wal=True  # 恢复时跳过WAL写入
            )
        elif op_type == "delete_token":
            tree.evict_prompt(
                key_list=log_entry["key_list"],
                instance_id=log_entry["instance_id"],
                skip_wal=True
            )
        elif op_type == "delete_instance":
            tree.evict_prompt_by_instance(
                instance_id=log_entry["instance_id"],
                skip_wal=True
            )


# 合并前缀树（核心调整：任务执行完成后写入WAL，保留version管理）
class MergePrefixTree:
    def __init__(self, root: Optional[TreeNode] = None, persist_manager: Optional[PersistenceManager] = None):
        self.root = TreeNode() if root is None else root
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=16)  # 并发线程池
        self.persist_manager = persist_manager or PersistenceManager()
        self.persist_manager.bind_tree(self)  # 绑定到持久化管理器

        # 快照控制
        self._snap_running = False
        self._snap_version: Optional[int] = None  # 当前快照版本（仅快照时有效）
        self.snap_lock = threading.RLock()

    # ------------------------------ 快照状态控制 ------------------------------
    def is_snap_running(self) -> bool:
        with self.snap_lock:
            return self._snap_running

    def set_snap_running(self, running: bool):
        with self.snap_lock:
            self._snap_running = running
            # 快照结束时清空快照版本
            if not running:
                self._snap_version = None

    def get_current_snap_version(self) -> Optional[int]:
        with self.snap_lock:
            return self._snap_version

    def clear_snap_resources(self):
        """清空所有节点的old_info（快照完成后调用）"""

        # 递归遍历所有节点，清空old_info
        def _clear(node: TreeNode, visited: Set[int]):
            if node.id in visited:
                return
            visited.add(node.id)
            node.clear_old_info()
            for child in node.children.values():
                _clear(child, visited)

        _clear(self.root, set())
        print("[快照] 已清空所有节点old_info")

    # ------------------------------ 核心操作（调整WAL写入时机） ------------------------------
    def update_prefix_tree(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """统一更新入口：提交任务→等待执行完成→成功后写入WAL（同步执行）"""
        results = []
        for update_info in data.get("info", []):
            op_type = update_info.get("op_type")
            instance_id = update_info.get("instance_id")
            if not op_type or not instance_id:
                results.append({"result": "failed", "message": "缺少op_type或instance_id"})
                continue

            try:
                # 生成WAL日志条目（先准备，执行成功后写入）
                log_entry = {
                    "op_type": op_type,
                    "timestamp": int(time.time() * 1000),
                    "instance_id": instance_id,
                    "key_list": update_info.get("insert_key", []),
                    "value_list": update_info.get("insert_value", [])
                }

                # 提交任务到线程池并等待执行完成（同步阻塞，确保执行成功）
                future = None
                if op_type == "insert_token":
                    future = self._executor.submit(
                        self.insert_prompt,
                        key_list=update_info["insert_key"],
                        value_list=update_info["insert_value"],
                        instance_id=instance_id,
                        skip_wal=True  # 执行时不写WAL，统一在成功后写入
                    )
                elif op_type == "delete_token":
                    future = self._executor.submit(
                        self.evict_prompt,
                        key_list=update_info["insert_key"],
                        instance_id=instance_id,
                        skip_wal=True
                    )
                elif op_type == "delete_instance":
                    future = self._executor.submit(
                        self.evict_prompt_by_instance,
                        instance_id=instance_id,
                        skip_wal=True
                    )

                # 等待任务完成，检查是否成功
                if future:
                    future.result(timeout=30)  # 超时30秒，防止任务卡死
                    # 任务执行成功，写入WAL
                    self.persist_manager.write_log(log_entry)
                    results.append({"result": "success", "message": f"{op_type}任务执行成功并写入WAL"})

            except Exception as e:
                # 任务执行失败，不写入WAL
                print(f"[任务失败] {op_type}（instance：{instance_id}）：{e}")
                results.append({"result": "failed", "message": f"任务执行失败：{str(e)}"})

        return {"total": len(results), "details": results}

    def insert_prompt(self, key_list: List[int], value_list: List[Any], instance_id: str, skip_wal: bool = False):
        """插入token序列到前缀树（线程安全+版本控制）"""
        current_node = self.root
        key_list = key_list.copy()
        value_list = value_list.copy()
        snap_version = self.get_current_snap_version()

        while len(key_list) > 0 and key_list[0] in current_node.children:
            current_node.lock.acquire()
            child_node = current_node.children[key_list[0]]
            child_node.lock.acquire()

            # 若快照正在运行，缓存旧状态（基于快照版本）
            if snap_version is not None:
                child_node.cache_old_info(snap_version)

            # 计算匹配长度
            length = self._match_length(key_list, child_node.key)
            if length < len(child_node.key):
                # 分裂节点
                new_node = TreeNode()
                new_node.key = child_node.key[:length]
                # 复制value到新节点（深拷贝）
                for inst_id, vals in child_node.value.items():
                    new_node.value[inst_id] = vals.copy() if isinstance(vals, (dict, list)) else vals
                    child_node.value[inst_id] = vals[length:] if isinstance(vals, list) else vals
                # 维护父子关系
                new_node.parent = child_node.parent
                child_node.parent.children[key_list[0]] = new_node
                child_node.parent = new_node
                new_node.children[child_node.key[length]] = child_node

                # 快照运行时，缓存新节点的旧状态
                if snap_version is not None:
                    new_node.cache_old_info(snap_version)

                # 释放锁并更新当前节点
                child_node.lock.release()
                current_node.lock.release()
                key_list = key_list[length:]
                value_list = value_list[length:]
                current_node = new_node
            else:
                # 完全匹配，更新value
                if snap_version is not None:
                    child_node.cache_old_info(snap_version)
                # 深拷贝value，避免外部修改影响
                child_node.value[instance_id] = value_list[:length].copy() if isinstance(value_list,
                                                                                         list) else value_list
                value_list = value_list[length:]
                key_list = key_list[length:]

                # 释放锁并更新当前节点
                child_node.lock.release()
                current_node.lock.release()
                current_node = child_node

        # 剩余key生成新节点
        if len(key_list) > 0:
            current_node.lock.acquire()

            # 快照运行时，缓存当前节点旧状态
            if snap_version is not None:
                current_node.cache_old_info(snap_version)

            # 创建新节点
            new_node = TreeNode()
            new_node.key = key_list.copy()
            new_node.value[instance_id] = value_list.copy() if isinstance(value_list, list) else value_list
            new_node.parent = current_node
            current_node.children[key_list[0]] = new_node

            # 快照运行时，缓存新节点的旧状态
            if snap_version is not None:
                new_node.cache_old_info(snap_version)

            current_node.lock.release()

    def evict_prompt(self, key_list: List[int], instance_id: str, skip_wal: bool = False):
        """删除指定token序列和instance_id的记录（仅修改value，用户自行处理节点删除）"""
        current_node = self.root
        key_list = key_list.copy()
        snap_version = self.get_current_snap_version()

        while len(key_list) > 0 and key_list[0] in current_node.children:
            current_node.lock.acquire()
            child_node = current_node.children[key_list[0]]
            child_node.lock.acquire()

            # 若快照正在运行，缓存旧状态（删除前先缓存）
            if snap_version is not None:
                child_node.cache_old_info(snap_version)

            # 计算匹配长度
            length = self._match_length(key_list, child_node.key)
            if length == len(child_node.key):
                # 完全匹配，删除value中的instance记录（不删除节点）
                if instance_id in child_node.value:
                    del child_node.value[instance_id]
                key_list = key_list[length:]

            # 释放锁并更新当前节点
            child_node.lock.release()
            current_node.lock.release()
            current_node = child_node

        print(f"[删除] 已移除instance {instance_id} 在key {key_list} 下的记录（仅修改value，未删除节点）")

    def evict_prompt_by_instance(self, instance_id: str, skip_wal: bool = False):
        """删除指定instance_id的所有记录（仅修改value，用户自行处理节点删除）"""
        queue = [self.root]
        visited = set()
        snap_version = self.get_current_snap_version()

        while queue:
            node = queue.pop(0)
            if node.id in visited:
                continue
            visited.add(node.id)

            node.lock.acquire()
            # 若快照正在运行，缓存旧状态（删除前先缓存）
            if snap_version is not None:
                node.cache_old_info(snap_version)
            # 删除instance_id对应的value记录
            if instance_id in node.value:
                del node.value[instance_id]
            # 遍历子节点
            queue.extend(node.children.values())
            node.lock.release()

        print(f"[删除] 已移除instance {instance_id} 在所有节点的记录（仅修改value，未删除节点）")

    def _match_length(self, key_list: List[int], node_key: Optional[List[int]]) -> int:
        """计算key_list和node_key的匹配长度"""
        if not node_key:
            return 0
        length = 0
        max_len = min(len(key_list), len(node_key))
        while length < max_len and key_list[length] == node_key[length]:
            length += 1
        return length
    
    def search_instances_with_prefix(self, key_list: List[int]) -> List[str]:  
        """搜索包含指定前缀的所有实例ID"""  
        matched_instances = set()  
        
        def dfs_search(node: TreeNode, remaining_key: List[int]):  
            if not remaining_key:  
                # 收集该节点及所有子节点的实例  
                self._collect_instances_from_node(node, matched_instances)  
                return  
            
            first_token = remaining_key[0]  
            if first_token in node.children:  
                child = node.children[first_token]  
                length = self._match_length(remaining_key, child.key)  
                
                if length == len(child.key):  
                    # 完全匹配，继续搜索剩余key  
                    dfs_search(child, remaining_key[length:])  
                elif length > 0:  
                    # 部分匹配，收集该节点的实例  
                    self._collect_instances_from_node(child, matched_instances)  
        
        dfs_search(self.root, key_list)  
        return list(matched_instances)  
  
    def _collect_instances_from_node(self, node: TreeNode, instances: set):  
        """收集节点及其子节点的所有实例ID"""  
        instances.update(node.value.keys())  
        for child in node.children.values():  
            self._collect_instances_from_node(child, instances)


# 服务启动示例（直接运行即可）
def main():
    # 初始化持久化管理器（可自定义配置）
    persist_manager = PersistenceManager(
        data_dir="./prefix_tree_persist",
        wal_max_size=512 * 1024 * 1024,  # 单WAL最大512MB
        snap_interval=1800,  # 30分钟生成一次快照
        snap_keep_count=1  # 只保留最新1个快照（核心配置）
    )

    # 尝试从持久化数据恢复树（崩溃后重启会自动恢复）
    try:
        tree = persist_manager.recover_tree()
        print("\n=== 服务启动成功（已从持久化数据恢复）===")
    except Exception as e:
        print(f"\n=== 恢复失败，创建新树：{e} ===")
        tree = MergePrefixTree(persist_manager=persist_manager)

    # 示例：模拟主线程提交任务（同步等待执行完成后写入WAL）
    def mock_main_thread_tasks():
        """模拟主线程批量提交任务"""
        import random
        task_count = 0
        while True:
            # 随机生成任务
            op_type = random.choice(["insert_token", "delete_token", "delete_instance"])
            instance_id = f"pod_{random.randint(1000, 9999)}"
            insert_key = [random.randint(100, 999) for _ in range(random.randint(1, 3))]
            insert_value = {"status": "running", "timestamp": int(time.time())}

            op_data = {
                "info": [
                    {
                        "op_type": op_type,
                        "instance_id": instance_id,
                        "insert_key": insert_key,
                        "insert_value": insert_value
                    }
                ]
            }

            # 提交任务（同步等待执行完成）
            result = tree.update_prefix_tree(op_data)
            task_count += 1
            print(f"\n[主线程] 第{task_count}个任务完成：{result['details'][0]['message']}")
            print(f"[任务详情] op_type={op_type}, instance={instance_id}, key={insert_key}")

            time.sleep(random.uniform(0.5, 1.5))  # 模拟任务提交间隔

    # 启动主线程任务模拟（单线程提交，线程池执行）
    mock_main_thread_tasks()


if __name__ == "__main__":
    # 安装依赖：pip install lz4
    main()