# sqlite_storage.py
import sqlite3
import threading
import time
from typing import Dict, Any



class SQLiteStorage:
    """负责持久化 sentry 和 instance 信息"""

    def __init__(self, DB_PATH="/data/info_center.db"):
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.lock = threading.Lock()
        self._create_tables()

    def _create_tables(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS sentry (
                    sentry_id TEXT PRIMARY KEY,
                    ip TEXT,
                    port INTEGER,
                    last_update REAL
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS instance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sentry_id TEXT,
                    instance_id TEXT,
                    pod_type TEXT,
                    service_port INTEGER,
                    tp_size INTEGER,
                    base_gpu_id INTEGER,
                    step INTEGER,
                    status INTEGER,
                    last_update REAL
                )
            """)

    # ------------------ 保存 / 更新 ------------------
    def save_sentry(self, sentry_id: str, ip: str, port: int):
        with self.lock, self.conn:
            self.conn.execute("""
                INSERT INTO sentry(sentry_id, ip, port, last_update)
                VALUES(?,?,?,?)
                ON CONFLICT(sentry_id)
                DO UPDATE SET ip=excluded.ip, port=excluded.port, last_update=excluded.last_update
            """, (sentry_id, ip, port, time.time()))

    def save_instance(self, sentry_id: str, instance_info: Dict[str, Any]):
        with self.lock, self.conn:
            self.conn.execute("""
                INSERT INTO instance(sentry_id, instance_id, pod_type, service_port, tp_size, base_gpu_id, step, status, last_update)
                VALUES(?,?,?,?,?,?,?,?,?)
            """, (
                sentry_id,
                instance_info["instance_id"],
                instance_info["pod_type"],
                instance_info.get("service_port", 0),
                instance_info.get("tp_size", 1),
                instance_info.get("base_gpu_id", 0),
                instance_info.get("step", 1),
                int(instance_info.get("status", 1)),
                time.time()
            ))

    # ------------------ 读取数据 ------------------
    def load_all(self) -> Dict[str, Any]:
        """返回 {sentry_id: {ip, port, instances: [...]}}"""

        with self.lock, self.conn:
            sentries = self.conn.execute("SELECT sentry_id, ip, port FROM sentry").fetchall()
            instances = self.conn.execute(
                "SELECT sentry_id, instance_id, pod_type, service_port, tp_size, base_gpu_id, step, status FROM instance"
            ).fetchall()

        result = {}
        for sid, ip, port in sentries:
            result[sid] = {
                "ip": ip,
                "port": port,
                "instances": []
            }

        for sid, iid, pod_type, service_port, tp_size, base_gpu_id, step, status in instances:
            if sid in result:
                result[sid]["instances"].append({
                    "instance_id": iid,
                    "pod_type": pod_type,
                    "service_port": service_port,
                    "tp_size": tp_size,
                    "base_gpu_id": base_gpu_id,
                    "step": step,
                    "status": bool(status)
                })

        return result

    # ------------------ 删除失联 ------------------
    def delete_sentry(self, sentry_id: str):
        with self.lock, self.conn:
            self.conn.execute("DELETE FROM sentry WHERE sentry_id=?", (sentry_id,))
            self.conn.execute("DELETE FROM instance WHERE sentry_id=?", (sentry_id,))

    def delete_instance(self, sentry_id: str, instance_id: str):
        """删除某个 sentry 下的指定 instance"""
        with self.lock, self.conn:
            self.conn.execute("""
                DELETE FROM instance 
                WHERE sentry_id=? AND instance_id=?
            """, (sentry_id, instance_id))


    def clear_all(self):
        """清空所有表内容，但不删除表结构"""
        with self.lock, self.conn:
            self.conn.execute("DELETE FROM sentry")
            self.conn.execute("DELETE FROM instance")