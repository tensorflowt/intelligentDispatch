from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class RegisterRequest(BaseModel):
    instance_type: str
    instance_id: str
    node_ip: str
    service_port: int
    tp_size: Optional[int] = 1
    base_gpu_id: Optional[int] = 0
    step: Optional[int] = 1

class RadixOp(BaseModel):
    op_type: str
    parent_path: List[List[int]] = Field(default_factory=list)
    prompt: list[int] = Field(default_factory=list) # 完整的prompt
    prompt_value: List[int] = Field(default_factory=list) # prompt value
    insert_key: List[int] = Field(default_factory=list) # 插入节点的key —— token index
    insert_value: List[int] = Field(default_factory=list) # 插入节点的value —— token kvcache index
    split_length: int = 0 # 分裂节点，该节点的长度     如果节点操作是删除的话，就是删除的分离节点
    depth: int = 0

class RadixRequest(BaseModel):
    ops_id: int
    timestamp: str
    node_ip: str
    server_port: int
    instance_id: str
    info: List[RadixOp]

