from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from typing import Optional
import requests
import socket
import uvicorn
import random
import time
import argparse
import sys
import os

# ---------- FastAPI应用初始化 ----------
app = FastAPI(title="Mock Inference Service", version="1.0.0")

# ---------- 数据模型 ----------
class RegisterRequest(BaseModel):
    """向Sentry注册的请求数据模型"""
    instance_type: str
    instance_id: str
    node_ip: str
    service_port: int
    tp_size: Optional[int] = 1
    base_gpu_id: Optional[int] = 0
    step: Optional[int] = 1

# ---------- 配置类（替代全局变量）----------
class Config:
    """配置管理类"""
    def __init__(self):
        self.sentry_port = int(os.environ.get("SENTRY_PORT", "9992"))
        self.service_port = int(os.environ.get("SERVICE_PORT", "9000"))
        self.instance_type = os.environ.get("INSTANCE_TYPE", "prefill")
        self.max_retries = int(os.environ.get("REGISTER_RETRIES", "3"))
        self.retry_delay = int(os.environ.get("REGISTER_RETRY_DELAY", "2"))
    
    def update_from_args(self, args):
        """使用命令行参数更新配置"""
        self.sentry_port = args.sentry_port
        self.service_port = args.port
        self.instance_type = args.type

# 创建全局配置实例
config = Config()

# ---------- 全局异常处理器 ----------
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """统一处理Pydantic验证错误"""
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
    )

# ---------- 路由函数 ----------
@app.get("/v1/pdserver/health")
async def health():
    """Sentry健康检查端点"""
    return {"status": "healthy", "timestamp": int(time.time())}

@app.get("/v1/pdserver/status")
async def status_endpoint():
    """实例状态查询端点"""
    return {
        "instance_type": config.instance_type,
        "instance_id": f"mock_{config.instance_type}_{config.service_port}",
        "status": "running",
        "timestamp": int(time.time()),
        "service_port": config.service_port,
        "node_ip": socket.gethostbyname(socket.gethostname())
    }

@app.get("/metrics")
async def metrics():
    """Prometheus格式的metrics端点"""
    prealloc = random.uniform(0, 10)
    inflight = random.uniform(0, 10)
    
    metrics_text = f"""# HELP sglang_num_prefill_prealloc_queue_reqs Prefill prealloc queue requests
# TYPE sglang_num_prefill_prealloc_queue_reqs gauge
sglang_num_prefill_prealloc_queue_reqs{{instance_id="mock_{config.instance_type}_{config.service_port}",instance_type="{config.instance_type}"}} {prealloc}

# HELP sglang_num_prefill_inflight_queue_reqs Prefill inflight queue requests
# TYPE sglang_num_prefill_inflight_queue_reqs gauge
sglang_num_prefill_inflight_queue_reqs{{instance_id="mock_{config.instance_type}_{config.service_port}",instance_type="{config.instance_type}"}} {inflight}
"""
    return metrics_text

# ---------- 注册逻辑 ----------
def register_self_with_retry(sentry_port, service_port, instance_type, max_retries=3, retry_delay=2):
    """向Sentry注册自身信息，包含重试机制"""
    node_ip = socket.gethostbyname(socket.gethostname())
    instance_id = f"mock_{instance_type}_{service_port}"
    url = f"http://127.0.0.1:{sentry_port}/v1/Sentry/register_inference_info"
    
    payload = RegisterRequest(
        instance_type=instance_type,
        instance_id=instance_id,
        node_ip=node_ip,
        service_port=service_port,
        tp_size=1,
        base_gpu_id=0,
        step=1
    ).dict()
    
    print(f"[注册] 实例信息: type={instance_type}, id={instance_id}, ip={node_ip}:{service_port}")
    print(f"[注册] Sentry地址: {url}")
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[注册] 尝试第 {attempt}/{max_retries} 次注册...")
            resp = requests.post(url, json=payload, timeout=10)
            
            if resp.status_code == 200:
                print(f"[注册] 成功! 响应: {resp.text}")
                return True
            else:
                print(f"[注册] 失败! 状态码: {resp.status_code}, 响应: {resp.text}")
                
        except requests.exceptions.ConnectionError as e:
            print(f"[注册] 连接失败 (尝试 {attempt}/{max_retries}): {e}")
        
        if attempt < max_retries:
            delay = retry_delay * attempt
            print(f"[注册] 等待 {delay} 秒后重试...")
            time.sleep(delay)
    
    print(f"[注册] 所有 {max_retries} 次尝试均失败，将继续启动服务...")
    return False

# ---------- 主启动逻辑 ----------
def main():
    """主启动函数"""
    parser = argparse.ArgumentParser(
        description="Mock Inference Service for Sentry",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument("--port", type=int, default=config.service_port, 
                       help=f"Mock service端口 (默认: {config.service_port})")
    parser.add_argument("--type", type=str, default=config.instance_type, 
                       choices=["prefill", "decode"], 
                       help=f"实例类型 (默认: {config.instance_type})")
    parser.add_argument("--sentry-port", type=int, default=config.sentry_port, 
                       help=f"Sentry服务端口 (默认: {config.sentry_port})")
    parser.add_argument("--no-register", action="store_true",
                       help="跳过向Sentry注册")
    
    args = parser.parse_args()
    
    # 更新配置
    config.update_from_args(args)
    
    # 打印启动信息
    print("=" * 60)
    print(f"Mock Inference Service")
    print("=" * 60)
    print(f"配置:")
    print(f"  - 实例类型: {config.instance_type}")
    print(f"  - 服务端口: {config.service_port}")
    print(f"  - Sentry端口: {config.sentry_port}")
    print("=" * 60)
    
    # 注册服务
    if not args.no_register:
        print("\n[启动阶段] 正在向Sentry注册服务...")
        register_success = register_self_with_retry(
            sentry_port=config.sentry_port,
            service_port=config.service_port,
            instance_type=config.instance_type,
            max_retries=config.max_retries,
            retry_delay=config.retry_delay
        )
        if not register_success:
            print("[警告] 服务注册失败，但将继续启动...")
    
    # 启动服务器
    print(f"\n[启动阶段] 启动HTTP服务器 0.0.0.0:{config.service_port}")
    
    try:
        uvicorn.run(
            app, 
            host="0.0.0.0", 
            port=config.service_port,
            log_level="info"
        )
    except KeyboardInterrupt:
        print("\n[关闭] 收到中断信号，正在关闭服务...")

# ---------- 程序入口 ----------
if __name__ == "__main__":
    main()