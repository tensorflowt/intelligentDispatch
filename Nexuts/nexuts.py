import threading
import time
from typing import Dict, Any, Optional
from collections import defaultdict
from curl_cffi import requests
from utils.logger import logger
from typing import List 

from Tree.tree import MergePrefixTree
from Sentry_manager.Sentry import Sentry
from persistence.snap_manager import SnapshotManager
from persistence.walmanager import WalManager
from persistence.sqlite_storage import SQLiteStorage
from utils.metrics_collector import InstanceMetricsCollector


class InformationCenter:
    """信息中心核心类"""

    def __init__(self, nexuts_config):
        logger.info("Nexuts Config : {}".format(nexuts_config))
        self.instances_status: Dict[str, Any] = {}
        self.sentry_instance: Dict[str, Sentry] = {}
        self.sentry_hearbeat = nexuts_config.get('sentry_hearbeat', 5)


        self.lock_instances = threading.Lock()
        self.lock_sentry_instance = threading.Lock()

        # 添加metrics相关字段  
        self.instances_metrics: Dict[str, Dict[str, float]] = {}  
        self.lock_metrics = threading.Lock()  
          
        # 从配置中读取权重  
        self.load_balancing_weights = nexuts_config.get("load_balancing_weights", {  
            "prealloc": 0.3,  
            "inflight": 0.7  
        })  
          
        # 初始化metrics收集器  
        self.metrics_collector = InstanceMetricsCollector(  
            prealloc_weight=self.load_balancing_weights["prealloc"],  
            inflight_weight=self.load_balancing_weights["inflight"]  
        )

        # 初始化SnapshotManager、WalManager
        wal_manager_path = nexuts_config.get('WalManager_dir', "/data/nexuts/wal_dir")
        snapshot_dir = nexuts_config.get("snapshot_dir", "/data/snapshots") # "/data/snapshots"
        snapshot_interval_seconds = nexuts_config.get("snapshot_interval_seconds", 600) # 10分钟一次
        resume = nexuts_config.get('resume', True) # 是否是异常恢复的


        self.wal_manager = WalManager(walmanager_path=wal_manager_path)
        self.tree = MergePrefixTree(wal_manager=self.wal_manager)

        self.snapshot_manager = SnapshotManager(
            tree=self.tree,
            wal_manager=self.wal_manager,
            snapshot_dir=snapshot_dir,
            interval_seconds=snapshot_interval_seconds,
            resume=resume) # 这个启动后会读取快照并恢复


        # SQLiteStorage
        self.db = SQLiteStorage(nexuts_config.get("db_path", "/data/info_center.db"))
        self.loss_pod_waiting_time = 10

        if resume:
            self._recover_from_db() # 恢复注册的Sentry
        else:
            self.db.clear_all() # 清除
        # TODO 恢复树从 wal和snapshot里面
 
    def is_system_balanced(self, threshold: float = 0.3) -> bool:  
        """判断系统负载是否均衡"""
        if not self.instances_metrics:  
            return True  
        
        loads = [m["weighted_load"] for m in self.instances_metrics.values()]  
        max_load = max(loads)  
        min_load = min(loads)  
        
        # 如果最大负载和最小负载差异小于阈值，认为均衡  
        return (max_load - min_load) < threshold
    
    def find_worker_by_cache(self, prompt_tokens: List[int]) -> Optional[str]:  
        """基于前缀匹配查找包含缓存的worker"""  
        # 在 MergePrefixTree 中查找匹配的实例  
        matched_instances = self.tree.search_instances_with_prefix(prompt_tokens)  
        
        if not matched_instances:  
            return None  
        
        # 从匹配的实例中选择负载最低的  
        available_matched = []  
        for instance_id in matched_instances:  
            if self.instances_status.get(instance_id, False):  
                metrics = self.instances_metrics.get(instance_id)  
                if metrics:  
                    available_matched.append({  
                        "instance_id": instance_id,  
                        "weighted_load": metrics["weighted_load"]  
                    })  
        
        if available_matched:  
            return min(available_matched, key=lambda x: x["weighted_load"])["instance_id"]  
        
        return None

    async def get_instance_metrics(self, instance_id: str) -> Optional[Dict[str, float]]:
        """
        获取指定实例的实时metrics信息。
        
        通过遍历sentry_instance查找目标实例，如果实例不存在于instances_status中则返回None。
        
        Args:
            instance_id: 实例的唯一标识符
            
        Returns:
                Optional[Dict[str, float]]: 实例的metrics信息字典，如果实例不存在则返回None
        """
        if instance_id not in self.instances_status:
            return None
              
        # 从sentry_instance中获取实例信息
        instance_info = None
        for sentry_id, sentry in self.sentry_instance.items():
            if instance_id in sentry.prefill_list:
                instance_info = sentry.prefill_list[instance_id]
                instance_type = "prefill"
                break
            elif instance_id in sentry.decode_list:  
                instance_info = sentry.decode_list[instance_id]  
                instance_type = "decode" 
                break  
                  
        if not instance_info:  
            return None  
              
        instance_ip = instance_info.get("node_ip", "127.0.0.1")  
        service_port = instance_info.get("service_port")  
          
        if not service_port:  
            return None  
              
        metrics = await self.metrics_collector.get_instance_load(instance_ip, service_port, instance_type)  
          
        with self.lock_metrics:  
            if metrics:  
                self.instances_metrics[instance_id] = metrics  
            else:  
                # 获取失败时返回None，不使用缓存  
                logger.warning(f"Failed to get metrics from {instance_ip}:{service_port}")    
                return None  
                  
        return metrics

    def call_back_function(self, sentry_id):
        """
        # sentry失联的回调函数
        :param sentry_id:
        :return:
        """
        with self.lock_sentry_instance:
            self.sentry_instance[sentry_id].stop()  # 先停止这个函数
            for key in self.sentry_instance[sentry_id].prefill_list.keys():
                with self.lock_instances:
                    # 设置为不可调度  # TODO self.tree 需要什么变化吗？直接清除的话，不太合适，做线程计时器吧，如果比如10分钟无法连上就删除
                    self.instances_status[key] = False

            for key in self.sentry_instance[sentry_id].decode_list.keys():
                with self.lock_instances:
                    self.instances_status[key] = False


    def update_prefix_tree(self, sentry_info):
        """
        :param sentry_info:
        :return:
        """
        with self.lock_sentry_instance:
            if sentry_info.get('sentry_id') not in self.sentry_instance:
                logger.info("update prefix tree result :{}".format({"result": "failed"}))
                return {"result": "failed"}
            else:
                self.tree.update_prefix_tree(sentry_info)
                return {"result": "ok"}

    def register_instance(self, data: Dict[str, Any]):
        """
        data: RegisterRequest.dict()
        """
        sentry_id = data.get("sentry_id")
        instance_id = data.get("instance_id")
        ip = data.get("node_ip")
        port = data.get("sentry_port")
        pod_type = data.get("instance_type")  # prefill / decode
        service_port = data.get("service_port")
        tp_size = data.get("tp_size", 1)
        base_gpu_id = data.get("base_gpu_id", 0)
        step = data.get("step", 1)
        # ---------------- 保存 sentry 到数据库 ----------------


        with self.lock_sentry_instance:
            if sentry_id not in self.sentry_instance:
                # 创建新 Sentry
                s_info = {"sentry_id": sentry_id, "ip": ip, "port": port}
                self.sentry_instance[sentry_id] = Sentry(s_info, self.sentry_hearbeat, self.call_back_function)
                # self.sentry_instance[sentry_id].start_heartbeat() 实例化的时候就启动了
                self.db.save_sentry(sentry_id, ip, port)
            else:
                # sentry_id 在，但是之前停掉了
                if not self.sentry_instance[sentry_id].running:
                    self.sentry_instance[sentry_id].running = True
                    self.sentry_instance[sentry_id].start_heartbeat()



        instance_info = {
            "instance_id": instance_id,
            "pod_type": pod_type,
            "service_port": service_port,
            "tp_size": tp_size,
            "base_gpu_id": base_gpu_id,
            "step": step,
            "status": 1
        }

        # ---------------- 保存实例 ----------------
        if data.get("pod_type") == "prefill":
            if instance_id not in self.sentry_instance[sentry_id].prefill_list:
                self.sentry_instance[sentry_id].prefill_list[instance_id] = instance_info
                self.db.save_instance(sentry_id, instance_info)  # 会覆盖
        else:
            if instance_id not in self.sentry_instance[sentry_id].decode_list:
                self.sentry_instance[sentry_id].decode_list[instance_id] = instance_info
                self.db.save_instance(sentry_id, instance_info)  # 会覆盖

        # 设置状态可用
        with self.lock_instances:
            self.instances_status[instance_id] = True
        
        # 初始化metrics
        with self.lock_metrics:  
            self.instances_metrics[instance_id] = {  
                "prealloc_queue": 0,  
                "infight_queue": 0,  
                "weighted_load": 0  
            }  

        # 持久化实例
        logger.info(f"[Register] sentry={sentry_id}, instance={instance_id}, type={pod_type} 注册成功")
        return {"result": "ok"}

    def set_status(self, info):
        sentry_id = info["sentry_id"]
        instance_id = info["instance_id"]

        if sentry_id not in self.sentry_instance:
            return {"result": "failed", "message": "sentry_id not exist"}
        if instance_id not in self.sentry_instance[sentry_id].decode_list and instance_id not in  self.sentry_instance[
            sentry_id].prefill_list:
            return {"result": "failed", "message": "instance_id not exist"}

        self.instances_status[instance_id] = info["status"]  # 置为False
        return {"result": "ok"}


    def deregister_instance(self, info):
        sentry_id = info["sentry_id"]
        instance_id = info["instance_id"]

        self.sentry_instance[sentry_id].prefill_list.pop(instance_id, None)  # 实例信息删除
        self.sentry_instance[sentry_id].decode_list.pop(instance_id, None)  # 实例信息删除
        del self.instances_status[instance_id]  # 状态删除

        # TODO delete instance tree，同时删除的操作要记录到wal中
        return {"result": "ok"}


    def _recover_from_db(self):
        """
        recover from db
        :return:
        """
        data = self.db.load_all()

        for sid, info in data.items():
            sentry_info = {
                "sentry_id": sid,
                "ip": info["ip"],
                "port": info["port"],
            }
            sentry_obj = Sentry(sentry_info, self.sentry_hearbeat, self.call_back_function)
            self.sentry_instance[sid] = sentry_obj
            #sentry_obj.start_heartbeat()

            # 恢复实例列表
            for inst in info["instances"]:
                iid = inst["instance_id"]
                pod_type = inst["pod_type"]

                if pod_type == "prefill":
                    sentry_obj.prefill_list[iid] = inst
                else:
                    sentry_obj.decode_list[iid] = inst

                self.instances_status[iid] = inst["status"]

        logger.info(f"[Recovery] 恢复了 {len(data)} 个 sentry 节点")
