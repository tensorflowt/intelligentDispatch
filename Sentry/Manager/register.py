import json
from typing import Dict, Any
import threading
import time

from curl_cffi import requests
#import requests


from utils.logger import logger
from Manager.instance_manager import InstanceManager
from Manager.InstanceDB import InstanceRegistryDB


class InstanceInfo:
    def __init__(self, info: Dict[str, Any]):
        self.instance_id = info["instance_id"]
        self.manager = None
        self.info = info
        self._stop_event = threading.Event()
        self._health_thread = None
        self.loss_status = False # 默认是false


class Registry:
    def __init__(self,
                 instance_db: InstanceRegistryDB,
                 call_back_update_info,
                 call_back_for_loss,
                 call_back_set_loss_status,
                 call_back_deal_re_register_pod,
                 health_interval=10.0
                 ):
        # instance_id -> InstanceInfo
        self.instances: Dict[str, InstanceInfo] = {}  # 维护整个节点上的推理实例，包括P和D
        self.health_interval = health_interval
        self.lock = threading.Lock()
        self.instance_db = instance_db
        self.call_back = call_back_update_info
        self.call_back_for_loss = call_back_for_loss
        self.call_back_set_loss_status = call_back_set_loss_status
        self.call_back_deal_re_register_pod = call_back_deal_re_register_pod

        self.load_from_sqlite()  # load all instance

    def load_from_sqlite(self):
        all_instances = self.instance_db.get_all_instances()  # [{'instance_id': '11112222', 'instance_type': 'prefill',
        # 'node_ip': '172.16.16.63', 'service_port': '9000', 'tp_size': 1, 'base_gpu_id': 0, 'step': 1}]
        if all_instances:
            for info in all_instances:
                url = f"http://127.0.0.1:{info['service_port']}/v1/pdserver/health"
                if not self._check_health_once(url, info["instance_id"]):
                    self.instance_db.delete_instance(info["instance_id"])  # 直接删除，Sentry恢复时，这个实例不在服务
                    continue

                #url = f"http://{info['node_ip']}:{info['service_port']}/v1/pdserver/status"
                url = f"http://127.0.0.1:{info['service_port']}/v1/pdserver/status"
                logger.info(url)
                try:
                    r = requests.get(url, timeout=1).json()
                    if r["instance_type"] == "prefill": # 如果实例是P节点，就要拉取树
                        inst = InstanceInfo(info)
                        inst.manager = InstanceManager(info, self.call_back)
                        # TODO 是因为sentry异常，然后重启导致的  需要拉取对应的信息 等待PD Server提供接口
                        #url = f"http://{info['node_ip']}:{info['service_port']}/v1/radixtree/full"
                        url = f"http://127.0.0.1:{info['service_port']}/v1/radixtree/full"
                        r = requests.get(url, timeout=1).json()

                        logger.info("tree info:{}".format(r))
                        inst.manager.recover_tree(r["tree"])
                        inst.manager.ops_id_next = r["ops_id_finished"] + 1 # 拿到当前树对应的ops_finish
                        self.instances[info["instance_id"]] = inst
                        self._start_heartbeat_check(inst)
                    else:
                        self.register(info=info, write_db=False)  # 直接走重新注册维护实例，然后不写db
                    self.call_back_deal_re_register_pod(info) # 重新注册给Nexuts，Nexuts那边会处理
                except Exception as e:
                    logger.warning(f"[HealthCheck] ({info['instance_id']}) health check failed: {e}")
                    self.instance_db.delete_instance(info["instance_id"]) # 直接删除，Sentry恢复时，这个实例不在服务
                    self.call_back_for_loss(instance_id) # 通知push_manager 告知Nexuts 进行删除


    def register(self, info: Dict[str, Any], write_db=True):

        try:
            instance_id = info["instance_id"] # 如果节点已经存在，就是
            if instance_id in self.instances and self.instances[instance_id].loss_status:
                self.instances[instance_id].loss_status = False
                return True

            inst = InstanceInfo(info)
            if info["instance_type"] == "prefill":
                inst.manager = InstanceManager(info, self.call_back)  # prefill节点才需要

            with self.lock:
                if instance_id in self.instances:
                    logger.info(f"Re-registering instance {instance_id}, replacing info")
                self.instances[instance_id] = inst
            self._start_heartbeat_check(inst)
            if write_db:
                self.instance_db.upsert_instance(info)  # 记录注册实例的信息到sqlite

        except Exception as e:
            logger.info("registration failed: {}  info:{}".format(e, info))
            return False
        return True


    def get(self, instance_id: str) -> InstanceInfo:
        return self.instances.get(instance_id)

    def remove(self, instance_id: str):
        """
        注销实例
        1  PD Server 异常  PD Server异常直接删除所有维护的内容
        """
        with self.lock:
            inst = self.instances.pop(instance_id, None)
            if inst:
                inst._stop_event.set()
                logger.info(f"[Registry] Removed instance {instance_id}")
            else:
                logger.warning(f"[Registry] Tried to remove nonexistent instance {instance_id}")
            self.instance_db.delete_instance(instance_id)  # 从sqlite里面删除

            self.call_back_for_loss(instance_id) # 通知push_manager 进行删除


    def _health_check_loop(self, inst):
        instance_id = inst.info["instance_id"]
        #url = f"http://{inst.info['node_ip']}:{inst.info['service_port']}/v1/pdserver/health"
        url = f"http://127.0.0.1:{inst.info['service_port']}/v1/pdserver/health"
        logger.info(url)
        while not inst._stop_event.is_set():
            time.sleep(self.health_interval)
            healthy = self._check_health_once(url, instance_id)
            if not healthy:
                recover = False
                for retry in range(5):
                    if not self._check_health_once(url, instance_id):
                        time.sleep(1)
                        continue
                    else:
                        recover = True
                        break
                if recover:
                    continue
                else:
                    self.instances[instance_id].loss_status = True
                    self.call_back_set_loss_status(instance_id, True) # 推送给事件中心，该实例状态暂时设置为不可调度
                    total_time = 30 # 30秒
                    while total_time > 0 and  self.instances[instance_id].loss_status: #
                        # 不再ping了，直接判断有没有走重新注册
                        if self.instances[instance_id].loss_status:
                            total_time -= 5
                            time.sleep(5)
                        else:
                            break
                    if self.instances[instance_id].loss_status:# 如果30秒
                        self.remove(instance_id)  # 删除实例
                    else:
                        #TODO 回调设置为正常状态  清空信息中心这个节点的信息
                        #self.remove(instance_id)
                        self.call_back_set_loss_status(instance_id, False)


    def _start_heartbeat_check(self, inst: InstanceInfo):
        thread = threading.Thread(
            target=self._health_check_loop,
            args=(inst,),
            daemon=True,
            name=f"HealthCheck-{inst.info['instance_id']}"
        )
        inst._health_thread = thread
        thread.start()
        logger.info(f"[Registry] Started health thread for {inst.info['instance_id']}")

    def _check_health_once(self, url, instance_id):
        #logger.info("url:{}".format(url))
        try:
            r = requests.get(url, timeout=1)   # 这里应该要加instance_id 做check
            #logger.info("instance_id:{}  url:{}  health_result   status_code:{}".format(instance_id, url, r.status_code))
            if r.status_code == 200:
                return True
        except Exception as e:
            logger.warning(f"[HealthCheck] ({instance_id}) health check failed: {e}")
        return False
