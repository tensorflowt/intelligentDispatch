from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from nexuts import InformationCenter
from Api.request_data import RegisterRequest, UpdateRequest, DeregisterRequest, SetStatus
from utils.utils import load_config
from utils.logger import logger

from typing import Optional, List, Dict, Any    


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
        async def get_best_instance(prompt_tokens: Optional[List[int]] = None):  
            """双重策略路由：缓存感知 + 负载均衡"""  
              
            # 解析查询参数中的token列表  
            token_list = None  
            if prompt_tokens:  
                try:  
                    # 将字符串 "100,200,300" 转换为列表 [100, 200, 300]  
                    token_list = [int(x.strip()) for x in prompt_tokens.split(',')]  
                except ValueError:  
                    return {"error": "Invalid prompt_tokens format"}
        
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
                    # 打印实例信息  
                    logger.info(f"instances_status: {self.info_center.instances_status}")  
                    logger.info(f"instances_metrics: {self.info_center.instances_metrics}")  
              
            if not available_instances:  
                return {"instance_id": None, "message": "No available instances"}  
              
            # 双重策略决策  
            if token_list and self.info_center.is_system_balanced():  
                # 尝试缓存感知路由  
                cache_worker = self.info_center.find_worker_by_cache(token_list)  
                if cache_worker:  
                    return {  
                        "instance_id": cache_worker,  
                        "routing_strategy": "cache_aware",  
                        "load_info": next(m for m in available_instances if m["instance_id"] == cache_worker)  
                    }  
            
            # 回退到负载均衡路由  
            best_instance = min(  
                available_instances,  
                key=lambda x: (x["weighted_load"], x["instance_id"])  
            )  
            
            return {  
                "instance_id": best_instance["instance_id"],  
                "routing_strategy": "load_balanced",  
                "load_info": best_instance  
            }
