from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

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


class DeregisterRequest(BaseModel):
    sentry_id: str
    instance_id: str


class SetStatus(DeregisterRequest):
    status: bool = False # True 代表失联，False代表恢复正常

class UpdateRequest(BaseModel):
    timestamp: str
    sentry_ops_id: int
    sentry_id: str
    updates: List[Dict[str, Any]]
    '''
    data = dict()
    data["instance_id"] = info["instance_id"]
    data["ops_id"] = info["ops_id"]
    data["prompt"] = update_info["prompt"]
    if update_info["op_type"] == "insert_node":
        data["insert_value"] = update_info["insert_value"]
    if update_info["op_type"] == "delete_node":
        data["length"] = update_info["split_length"]
    '''


