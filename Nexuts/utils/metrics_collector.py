import asyncio  
import aiohttp  
import re  
from typing import Dict, Optional  
from utils.logger import logger  
  
class InstanceMetricsCollector:  
    def __init__(self, prealloc_weight=0.3, inflight_weight=0.7):  
        self.session = None  
        self.prealloc_weight = prealloc_weight  
        self.inflight_weight = inflight_weight  
          
    async def get_instance_load(self, instance_ip: str, metrics_port: int, instance_type: str = "prefill") -> Optional[Dict[str, float]]:  
        """获取实例的负载指标，支持 prefill 和 decode 类型"""
        if not self.session:  
            self.session = aiohttp.ClientSession()  
              
        url = f"http://{instance_ip}:{metrics_port}/metrics"  
          
        try:  
            async with self.session.get(url, timeout=2) as resp:  
                text = await resp.text()  
                  
            if instance_type == "decode":  
                # Decode 实例使用不同的指标  
                prealloc_queue = self._extract_metric(text, "sglang:num_decode_prealloc_queue_reqs")  
                infight_queue = self._extract_metric(text, "sglang:num_decode_transfer_queue_reqs")  
            else:  
                # Prefill 实例使用原有指标  
                prealloc_queue = self._extract_metric(text, "sglang:num_prefill_prealloc_queue_reqs")  
                infight_queue = self._extract_metric(text, "sglang:num_prefill_inflight_queue_reqs")  
              
            # 使用加权算法计算总负载  
            weighted_load = (prealloc_queue * self.prealloc_weight +   
                           infight_queue * self.inflight_weight)  
              
            return {  
                "prealloc_queue": prealloc_queue,  
                "infight_queue": infight_queue,  
                "weighted_load": weighted_load  
            }  
              
        except Exception as e:  
            logger.error(f"Failed to get metrics from {instance_ip}:{metrics_port}: {e}")  
            return None  
              
    def _extract_metric(self, metrics_text: str, metric_name: str) -> float:  
        """从Prometheus格式文本中提取指标值"""  
        pattern = f"{metric_name}{{.*?}}\\s+([0-9.]+)"  
        match = re.search(pattern, metrics_text)  
        return float(match.group(1)) if match else 0.0