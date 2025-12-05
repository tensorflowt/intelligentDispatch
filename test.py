from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import requests
import socket
import uvicorn

app = FastAPI()

# ---------- 数据模型 ----------
class RegisterRequest(BaseModel):
    instance_type: str
    instance_id: str
    node_ip: str
    service_port: int
    tp_size: Optional[int] = 1
    base_gpu_id: Optional[int] = 0
    step: Optional[int] = 1


# ---------- 健康检查路由 ----------
@app.get("/v1/model/health")
def health():
    return {"status": "ok"}


# ---------- 注册逻辑 ----------
def register_self(service_port: int):
    node_ip = socket.gethostbyname(socket.gethostname())
    url = "http://127.0.0.1:8080/v1/Sentry/register_inference_info"

    payload = RegisterRequest(
        instance_type="prefill",
        instance_id="11112222",
        node_ip=node_ip,
        service_port=service_port,
        tp_size=1,
        base_gpu_id=0,
        step=1
    ).dict()

    try:
        print(f"[register] sending to {url} ...")
        resp = requests.post(url, json=payload, timeout=5)
        print(f"[register] status={resp.status_code}, response={resp.text}")
    except Exception as e:
        print(f"[register] failed: {e}")


# ---------- 启动后注册 ----------
@app.on_event("startup")
def on_startup():
    port = 9000  # 你的服务端口
    register_self(port)


# ---------- 启动入口 ----------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000)
