import threading
from typing import Dict, Any, Optional
import time

from curl_cffi import requests
from utils.logger import logger



class Sentry:
    """负责节点心跳检测"""
    def __init__(self, sentry_info: Dict[str, Any], time_cycle, on_unconnection_callback):
        self.sentry_id = sentry_info.get('sentry_id')
        self.ip = sentry_info.get('ip')
        self.port = str(sentry_info.get('port'))
        self.health_url = f"http://{self.ip}:{self.port}/v1/health"
        self.prefill_list = dict()
        self.decode_list = dict()
        self.on_failure = on_unconnection_callback
        self.running = True
        self.time_cycle = time_cycle
        self.thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.thread.start()

    def start_heartbeat(self):
        """启动或重启心跳检测线程"""  
        if not hasattr(self, 'thread') or not self.thread.is_alive():  
            # 创建新线程  
            self.thread = threading.Thread(target=self._heartbeat_loop, daemon=True)  
            self.thread.start()

    def _heartbeat_loop(self):
        """后台线程执行心跳检测"""
        while self.running:
            healthy = self._ping()
            if not healthy:
                # 通知信息中心
                self.on_failure(self.sentry_id)
                # 发现故障后立即停止自己，避免重复通知
                self.running = False
            time.sleep(self.time_cycle)

    def _ping(self) -> bool:

        """模拟心跳检测"""
        try:
            resp = requests.get(self.health_url, timeout=1)  # 2秒超时
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "ok":
                    #logger.info(f"[Heartbeat] {self.health_url} returned non-ok: {data}")
                    return True
                else:
                    logger.info(f"[Heartbeat] {self.health_url} returned non-ok: {data}")
            else:
                logger.info(f"[Heartbeat] {self.health_url} bad status {resp.status_code}")
        except requests.exceptions.RequestException as e:
            logger.info(f"[Heartbeat] {self.health_url} FAILED ❌: {e}")
        return False


    def stop(self):
        self.running = False

