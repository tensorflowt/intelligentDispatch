import os
import sqlite3
import threading

class InstanceRegistryDB:
    def __init__(self, db_path="/data/sentry_instances.db"):
        self.db_path = db_path
        self.lock = threading.Lock()

        # 如果数据库不存在，才初始化
        if not os.path.exists(self.db_path):
            self._init_db()
        else:
            # 可选：也可以检测表是否存在（防止外部破坏）
            self._check_table()

    def _init_db(self):
        """初始化数据库表"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS instances (
                    instance_id TEXT PRIMARY KEY,
                    instance_type TEXT NOT NULL,
                    node_ip TEXT NOT NULL,
                    service_port INTEGER NOT NULL,
                    tp_size INTEGER NOT NULL,
                    base_gpu_id INTEGER NOT NULL,
                    step INTEGER NOT NULL
                )
            """)
            conn.commit()
        print(f"[DB] 初始化完成：{self.db_path}")

    def _check_table(self):
        """检查表是否存在，防止文件被破坏"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='instances'
            """)
            exists = cursor.fetchone()
            if not exists:
                print("[DB] 表缺失，重新初始化...")
                self._init_db()

    def upsert_instance(self, info: dict):
        """插入或更新实例信息"""
        with self.lock, sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO instances (instance_id, instance_type, node_ip, service_port, tp_size, base_gpu_id, step)
                VALUES (:instance_id, :instance_type, :node_ip, :service_port, :tp_size, :base_gpu_id, :step)
                ON CONFLICT(instance_id) DO UPDATE SET
                    instance_type=excluded.instance_type,
                    node_ip=excluded.node_ip,
                    service_port=excluded.service_port,
                    tp_size=excluded.tp_size,
                    base_gpu_id=excluded.base_gpu_id,
                    step=excluded.step
            """, info)
            conn.commit()

    def get_all_instances(self):
        """读取所有已注册实例"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT * FROM instances")
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_instance(self, instance_id: str):
        """按 ID 获取实例信息"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT * FROM instances WHERE instance_id=?", (instance_id,))
            row = cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
            return None

    def delete_instance(self, instance_id: str) -> bool:
        """根据 instance_id 删除实例，返回是否删除成功"""
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM instances WHERE instance_id=?", (instance_id,))
            conn.commit()
            # cursor.rowcount 返回删除的行数，0 表示没有找到对应记录
            return cursor.rowcount > 0