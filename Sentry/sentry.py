# Sentry
import redis
import string
import random

from curl_cffi import requests
from utils.utils import load_config
from Manager.register import Registry
from Manager.InstanceDB import InstanceRegistryDB
from PushWithNexuts.push_to_nexuts import PushToNexuts
from PushWithNexuts.NexutsMetric import MetricsHTTPServer
"""
Sentry 发送给信息中心的只有插入和删除两种情况，而且只需要告知删除的内容
"""

class Sentry:
    def __init__(self, args):
        self.sentry_instance_id = "104369026" # self._random_str() # sentry_id
        self.args = args # 启动参数
        self.sentry_config = load_config(args.config_path) # Sentry的配置 "./utils/SentryConfig.json"
        instance_db = InstanceRegistryDB(self.sentry_config["instance_db_path"]) # 注册信息的DB
        health_interval = float(self.sentry_config["health_interval"])  # 心跳延迟
        #self.send_nexuts_cycle = self.sentry_config.get("send_nexuts_cycle", 1.0)  # 默认1.0
        sentry_port = args.port
        self.push_manager = PushToNexuts(self.sentry_config, self.sentry_instance_id, sentry_port)

        # metrics_port = self.sentry_config.get("metrics", {}).get("port", 9100)  
        # metrics_server = MetricsHTTPServer(self.push_manager, port=metrics_port)
        # metrics_server.start()

        # 注册管理  self.push_manager.add_active_callback 这个是变更的回调函数
        self.register = Registry(instance_db,
                                 self.push_manager.add_active_callback,
                                 self.deal_loss_inference_pod,
                                 self.deal_set_status,
                                 self.deal_re_register_pod,
                                 health_interval)  # 注册DB

    @staticmethod
    def _random_str(length=13):
        chars = string.ascii_letters + string.digits  # 包含大小写字母和数字
        return ''.join(random.choices(chars, k=length))


    def register_inference(self, info: dict):
        result = self.register.register(info)
        '''
        if result:
            """
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
            info["sentry_id"] = self.ops_id_sentry
            info["sentry_port"] = self.args.port
            requests.post(self.nexuts_api_url["resister_pod"], json=info)
            # TODO 注册到信息中心，逻辑判断
        '''
        self.push_manager.register_pod_to_nexuts(info)
        return result

    def update_radix(self, info: dict):
        instance_id = info["instance_id"]
        if instance_id not in self.register.instances:
            # TODO 如果不存在，得让他注册  这种情况出现在 pod启动，但是sentry未启动，但是服务已经有请求，请求已经会推送了，这时候sentry起来，pd服务的注册没那么快，但是推送消息已经过来了
            return {"result": "error", "message": "pod not register"}

        inst = self.register.get(instance_id)
        inst.manager.update_tree(info)
        return {"result": "ok"}

    def deal_loss_inference_pod(self, instance_id):  # 删除实例
        self.push_manager.deal_loss_pod(instance_id)

    def deal_set_status(self, instance_id, status): # 暂时设置为服务不可调度
        self.push_manager.set_status(instance_id, status)

    def deal_re_register_pod(self, info): # Sentry启动从数据库读取的数据
        self.push_manager.register_pod_to_nexuts(info)
