import asyncio
import heapq
import threading
from concurrent.futures import ThreadPoolExecutor
import time
from typing import Dict, Any, List
import httpx

from KvCacheIndex.radix_tree import RadixTree
from utils.logger import logger


class InstanceManager:
    def __init__(self, instance_info: Dict[str, Any], call_back):
        self.info = instance_info
        self.radix_tree = RadixTree(self.info["instance_id"])  # 实例化一颗树
        self.ops_lock = threading.Lock()
        self.ops_id_next = 1
        self.queue_lock = threading.Lock()
        self.cond = threading.Condition(self.queue_lock)
        self.task_queue: List[tuple[int, dict]] = []  # 小根堆 (ops_id, update_info)
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.stop_flag = False
        self.call_back = call_back # 回掉函数，执行变更的info都会在Sentry执行回调，然后添加到Sentry的队列中
        self.consumer_thread = threading.Thread(target=self._consumer_loop, daemon=True)
        self.consumer_thread.start()

    def recover_tree(self, tree_info):
        self.radix_tree.recover_tree_from_dict(tree_info)



    def update_tree(self, update_info: Dict[str, Any]):
        """生产者接口：接收外部请求，把任务放入优先队列"""
        ops_id = update_info["ops_id"]
        #logger.info("update info: {}".format(update_info))
        with self.cond:
            heapq.heappush(self.task_queue, (ops_id, update_info))
            logger.info(f"[Producer] Received ops_id={ops_id}")
            self.cond.notify_all()

    def _consumer_loop(self):
        """消费者线程：保证按 ops_id 顺序取出任务"""
        while not self.stop_flag:
            with self.cond:
                while (not self.task_queue or self.task_queue[0][0] != self.ops_id_next) and not self.stop_flag:
                    self.cond.wait(timeout=0.1)
                if self.stop_flag:
                    break

                ops_id, update_info = heapq.heappop(self.task_queue)
                #logger.info(f"[Consumer] Received ops_id={ops_id}   and  update_info={update_info}")
            # 把任务丢给线程池执行
            self.executor.submit(self._process_update, update_info)
            with self.ops_lock:
                self.ops_id_next += 1

    def _process_update(self, update_info: Dict[str, Any]):
        """处理一次 update_info 内部的多个操作"""
        info_list = update_info.get("info", [])
        futures = []
        """
        class RadixRequest(BaseModel):
            ops_id: int
            timestamp: str
            node_ip: str
            server_port: int
            instance_id: str
            info: List[RadixOp]
        """
        for op in info_list:
            #logger.info("op:{}".format(op))
            futures.append(self.executor.submit(self.radix_tree.apply_op, op))
        self.call_back(update_info)
        # 等待所有操作完成
        for f in futures:
            f.result()

        # TODO 看看这里怎么维护 ops_id_finished

        logger.info(f"[InstanceManager] ops_id={update_info['ops_id']} finished")

    def shutdown(self):
        self.stop_flag = True
        with self.cond:
            self.cond.notify_all()
        self.executor.shutdown(wait=True)
