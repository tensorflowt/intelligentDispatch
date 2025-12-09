import threading
import time
import json
import redis
from curl_cffi import requests
import string
import random
from typing import List
from utils.logger import logger
from datetime import datetime


class PushToNexuts:
    def __init__(self, config: dict, sentry_id:str, sentry_port:int):
        """
        :param config:
            nexuts_api_url:
            nexuts_cycle:
            nexuts_register_api_url:
            nexuts_update_api_url:
            redis_config:
                host:
                port:
                db:
        :param redis_config: {"host": str, "port": int, "db": int}
        :param nexuts_api_url: {"post_update": str}
        :param sent_nexuts_cycle: 循环周期
        """
        self.sentry_id = sentry_id # sentry_id
        self.sentry_port = sentry_port

        prefix = f"http://{config['nexuts_api_url']['ip']}:{config['nexuts_api_url']['port']}"
        self.nexuts_register_api_url = f"{prefix}{config['nexuts_api_url']['resister_pod']}"
        self.nexuts_update_api_url = f"{prefix}{config['nexuts_api_url']['post_update']}"
        self.nexuts_deregister_api_url = f"{prefix}{config['nexuts_api_url']['deregister_pod']}"
        self.set_status_api_url = f"{prefix}{config['nexuts_api_url']['set_status']}"
        self.send_nexuts_cycle = config['send_nexuts_cycle']
        # Redis

        self.r = redis.StrictRedis(
            host=config['redis']['redis_host'],
            port=config['redis']['redis_port'],
            db=config['redis']['redis_db'],
            decode_responses=True
        )
        self.redis_lock = threading.Lock()
        if config['redis']['clear'] == 1:
            self.r.flushdb()

        logger.info(
            "Nexuts & Redis Config:\n"
            "  nexuts_register_api_url: {}\n"
            "  nexuts_update_api_url: {}\n"
            "  nexuts_deregister_api_url: {}\n"
            "  set_status_api_url: {}\n"
            "  send_nexuts_cycle: {}\n"
            "  redis_host: {}\n"
            "  redis_port: {}\n"
            "  redis_db: {}\n"
            "  redis_clear: {}\n",
            self.nexuts_register_api_url,
            self.nexuts_update_api_url,
            self.nexuts_deregister_api_url,
            self.set_status_api_url,
            self.send_nexuts_cycle,
            config["redis"]["redis_host"],
            config["redis"]["redis_port"],
            config["redis"]["redis_db"],
            config["redis"]["clear"]
        )


        # Buffer
        self._active_buffer: List[dict] = []
        self._buffer_lock = threading.Lock()

        # ops_id
        self.ops_id_sentry = int(self.r.get("sentry:ops_id_sentry") or 0)
        logger.info(f"ops_id_sentry: {self.ops_id_sentry}")
        self.ops_id_sentry_finish = int(self.r.get("sentry:ops_id_sentry_finish") or 0)
        logger.info(f"ops_id_sentry_finish: {self.ops_id_sentry_finish}")

        self._stop_event = threading.Event()


        # 如果注册失败则反复注册, 没间隔5秒注册一次
        self.fail_register_lock = threading.Lock()
        self.fail_register_instance = {"register":dict(), "deregister":dict(), "set_status":dict()}
        self._stop_register_event = threading.Event()



        # 线程
        self._collect_thread = threading.Thread(target=self._collect_loop, daemon=True)
        self._send_thread = threading.Thread(target=self._send_loop, daemon=True)
        self._collect_thread.start()
        self._send_thread.start()


    def _recycle_register(self):
        while not self._stop_register_event.is_set():
            time.sleep(5)
            copy_data = None
            with self.fail_register_lock:
                copy_data = self.fail_register_instance.copy()
            for key,value in copy_data.items():
                for instance_id, item in value.items():
                    if key == "register":
                        result = self.register_pod_to_nexuts(item)
                    if key == "deregister":
                        result = self.deal_loss_pod(instance_id)
                    if result:
                        with self.fail_register_lock:
                            del self.fail_register_instance[key][instance_id]



    def register_pod_to_nexuts(self, info, try_again=False):
        """
        :param info:
        :try_again: 是否要加入fail_register_instance 然后进行重试
        :return:
        class RegisterRequest(BaseModel):
            instance_type: str
            instance_id: str
            sentry_id: str
            node_ip: str
            sentry_port: int
            service_port: int
            tp_size: Optional[int] = 1
            base_gpu_id: Optional[int] = 0
            step: Optional[int] = 1
        """
        logger.info("execute register_pod_to_nexuts")
        info["sentry_id"] = self.sentry_id
        info["sentry_port"] = self.sentry_port
        logger.info("register info:{}".format(info))
        try:
            requests.post(self.nexuts_register_api_url, json=info)
        except Exception as e:
            if try_again:
                with self.fail_register_lock:
                    self.fail_register_instance["register"][info["instance_id"]] = info
            logger.info(e)


    def deal_loss_pod(self, instance_id, try_again=False):
        try:
            info = {"instance_id": instance_id,
                    "sentry_id": self.sentry_id}
            requests.post(self.nexuts_deregister_api_url, json=info) # TODO 增加结果确认
            if instance_id in self.fail_register_instance["register"]: # 如果这两个在 其他两个的话，直接删除
                del self.fail_register_instance["register"][instance_id]
            if instance_id in self.fail_register_instance["deregister"]:
                del self.fail_register_instance["deregister"][instance_id]
            return True

        except Exception as e:
            if try_again:
                with self.fail_register_lock:
                    self.fail_register_instance["deregister"][info["instance_id"]] = info
            logger.info(e)
            return False

    def set_status(self, instance_id, status, try_again=False):
        try:
            info = {"instance_id": instance_id,
                    "sentry_id": self.sentry_id,
                    "status": status}
            requests.post(self.set_status_api_url, json=info)
            return True
        except Exception as e:
            if try_again:
                with self.fail_register_lock:
                    self.fail_register_instance["set_status"][info["instance_id"]] = info
            logger.info(e)
            return False

    def add_active_callback(self, info: dict):
        """
        class RadixRequest(BaseModel):
            ops_id: int
            timestamp: str
            node_ip: str
            server_port: int
            instance_id: str
            info: List[RadixOp]

        {'ops_id': 1, 'timestamp': '2025-11-27T12:34:37.384411+00:00', 'node_ip': '172.16.16.63', 'server_port': 9993,
         'instance_id': 'test_prefill_wzs', 'info': [{'op_type': 'insert_node', 'parent_path': [],
                                                        'prompt': [0, 1, 2, 3], 'prompt_value': [1, 2, 3, 4]}]
        """
        """把回调数据放入 buffer"""
        #logger.info("add active callback info:{}".format(info))
        for radix_update_info in info["info"]:
            if radix_update_info["op_type"] == "split":
                continue
            data = {
                "instance_id": info["instance_id"],
                "ops_id": info["ops_id"],
                "op_type": radix_update_info["op_type"],
                "prompt": radix_update_info.get("prompt")
            }
            if radix_update_info["op_type"] == "insert_node":
                data["prompt_value"] = radix_update_info.get("prompt_value")
            if radix_update_info["op_type"] == "delete_node":
                data["length"] = radix_update_info.get("split_length")

            # 转换为Nexuts格式
            nexuts_data = self._prepare_update_for_nexuts(data)  

            with self._buffer_lock:
                self._active_buffer.append(nexuts_data)
        logger.info("add active callback done")
    
    def _prepare_update_for_nexuts(self, update: dict) -> dict:  
        """准备发送到 Nexuts 的更新数据，转换操作类型和字段名"""  
        nexuts_update = update.copy()  
        
        # 转换操作类型  
        if nexuts_update.get('op_type') == 'insert_node':  
            nexuts_update['op_type'] = 'insert_token'  
            # 映射字段名  
            if 'prompt' in nexuts_update:  
                nexuts_update['insert_key'] = nexuts_update.pop('prompt')  
            if 'prompt_value' in nexuts_update:  
                nexuts_update['insert_value'] = nexuts_update.pop('prompt_value')  
        
        return nexuts_update
 
    def _collect_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self.send_nexuts_cycle)
            self._flush_to_queue()

    def _flush_to_queue(self):
        with self._buffer_lock:
            if not self._active_buffer:
                #logger.info("flush to queue 为空")
                return
            self.ops_id_sentry += 1
            payload = {
                "sentry_ops_id": self.ops_id_sentry,
                "timestamp": datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "sentry_id": self.sentry_id,
                "updates": self._active_buffer,
            }
            self._active_buffer = []
            logger.info("flush to queue:{}".format(payload))
        with self.redis_lock:
            self.r.rpush("sentry:queue", json.dumps(payload))
            self.r.set("sentry:ops_id_sentry", self.ops_id_sentry)

    # === Send Loop ===
    def _send_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self.send_nexuts_cycle)
            self._try_send_one()

    def _try_send_one(self):
        with self.redis_lock:
            data = self.r.lindex("sentry:queue", 0)
        if not data:
            return
        item = json.loads(data)
        sentry_ops_id = item["sentry_ops_id"]

        logger.info("sentry_ops_id:{}  item:{}".format(sentry_ops_id, item))
        try:
            self._send_to_central(item)
            # 发送成功 -> 从队列移除
            self.ops_id_sentry_finish = sentry_ops_id
            with self.redis_lock:
                self.r.lpop("sentry:queue")
                self.r.set("sentry:ops_id_sentry_finish", self.ops_id_sentry_finish)
        except Exception as e:
            # 可根据需要记录错误
            logger.info("push_to_nexuts error: %s" % e)
            pass

    def _send_to_central(self, item: dict):
        requests.post(self.nexuts_update_api_url, json=item)

    def stop(self):
        self._stop_event.set()
        self._collect_thread.join()
        self._send_thread.join()

