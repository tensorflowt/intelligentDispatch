from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from loguru import logger
import time
import asyncio

from Manager.register import Registry
from Manager.instance_manager import InstanceManager
from ApiServer.request_data import RadixRequest, RegisterRequest
from sentry import Sentry
from utils.logger import logger


class APIServer:
    """
    封装 FastAPI 的业务逻辑类。
    支持模型注册、任务插入、健康检查等接口。
    """

    def __init__(self, args):
        self.app = FastAPI(title="Sentry Service", version="1.0")
        self.sentry = Sentry(args)
        # 注册路由
        self._register_routes()

    def _register_routes(self):
        """内部方法：注册所有路由"""
        app = self.app

        @app.post("/v1/Sentry/register_inference_info")
        async def register_inference(req: RegisterRequest):
            """注册推理实例，并创建对应的 InstanceManager"""
            info = req.dict()
            instance_id = info["instance_id"]
            self.sentry.register_inference(info)
            logger.info(f"Instance {instance_id} registered and manager started.")
            return {"status": "registered", "instance_id": instance_id, "ts": time.time()}

        @app.post("/v1/radixtree/update")
        async def update_node(req: RadixRequest):

            """插入前缀树节点"""

            #logger.info("update radixtree:{}".format(req.dict()))
            info = req.dict()
            instance_id = info["instance_id"]
            self.sentry.update_radix(req.dict())
            instance_id = req.dict()["instance_id"]

            return {"status": "enqueued", "instance_id": instance_id}

        @app.get("/v1/health")
        async def health_check():
            """服务健康检查"""
            return {"status": "ok", "timestamp": time.time()}

        @app.get("/v1/instances")
        async def list_instances():
            """查看已注册实例"""
            return {"instances": list(self.sentry.register.instances.keys())}

    def get_app(self):
        """返回 FastAPI 应用实例"""
        return self.app

