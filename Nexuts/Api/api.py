from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from nexuts import InformationCenter
from Api.request_data import RegisterRequest, UpdateRequest, DeregisterRequest, SetStatus
from utils.utils import load_config
from utils.logger import logger


# 实例化信息中心（全局单例）
class APIServer:
    def __init__(self, args):
        self.args = args
        self.config = load_config(args.config_path)
        self.app = FastAPI(title="Information Center", version="1.0.0")
        self.info_center = InformationCenter(self.config)
        self._register_routes()

    def get_app(self):
        """返回 FastAPI 应用实例"""
        return self.app


    def _register_routes(self):
        """在此注册所有路由"""
        app = self.app

        @app.post("/v1/Nexuts/register")
        async def register_instance(request: RegisterRequest):
            """注册推理实例"""
            info = request.dict()
            result = self.info_center.register_instance(info)
            return result

        @app.post("/v1/Nexuts/set_status")
        async def set_status(request: SetStatus):
            info = request.dict()
            result = self.info_center.set_status(info)
            return result

        @app.post("/v1/Nexuts/deregister")
        async def delete_pod(request: DeregisterRequest):
            # TODO 删除实例
            info = request.dict()
            result = self.info_center.deregister_instance(info)
            return result

        @app.post("/v1/Nexuts/update_prefix_tree")
        async def update_prefix_tree(request: UpdateRequest):
            """接收 Sentry 节点上报的前缀树更新"""
            data = request.dict()
            logger.info("update prefix tree input:{}".format(data))

            result = self.info_center.update_prefix_tree(data)
            return JSONResponse(result)

        # @app.get("/instances")
        # async def list_instances():
        #     """查看当前注册的实例"""
        #     return JSONResponse(info_center.instances)

        @app.get("/v1/Nexuts/health")
        async def health_check():
            """简单健康检测"""
            return JSONResponse({"status": "ok"})

        @app.get("/v1/Nexuts/get_best_instance")  
        async def get_best_instance():  
            """基于加权metrics获取最佳实例"""  
              
            # 获取所有可用实例的metrics  
            available_instances = []  
              
            for instance_id, status in self.info_center.instances_status.items():  
                if not status:  # 跳过不可用的实例  
                    continue  
                      
                metrics = await self.info_center.get_instance_metrics(instance_id)  
                if metrics:  
                    available_instances.append({  
                        "instance_id": instance_id,  
                        "weighted_load": metrics["weighted_load"],  
                        "prealloc_queue": metrics["prealloc_queue"],  
                        "infight_queue": metrics["infight_queue"]  
                    })  
                    # 添加调试信息  
                    logger.info(f"instances_status: {self.info_center.instances_status}")  
                    logger.info(f"instances_metrics: {self.info_center.instances_metrics}")  
              
            if not available_instances:  
                return {"instance_id": None, "message": "No available instances"}  
              
            # 添加tie-breaking：先按加权负载排序，再按instance_id字典序  
            best_instance = min(  
                available_instances,   
                key=lambda x: (x["weighted_load"], x["instance_id"])  
            )   
            
            # 添加调试信息  
            # logger.info(f"instances_status: {self.info_center.instances_status}")  
            # logger.info(f"instances_metrics: {self.info_center.instances_metrics}")  
            
            return {  
                "instance_id": best_instance["instance_id"],  
                "load_info": {  
                    "weighted_load": best_instance["weighted_load"],  
                    "prealloc_queue": best_instance["prealloc_queue"],  
                    "infight_queue": best_instance["infight_queue"],  
                    "formula": "prealloc_queue * 0.3 + inflight_queue * 0.7"  
                }  
            }
