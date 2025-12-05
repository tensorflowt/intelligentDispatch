import os
import json
import threading
import collections
import time
from typing import Optional, Any, List

_fdatasync = getattr(os, "fdatasync", os.fsync)


class _WalEntry:
    __slots__ = ("data", "event")
    def __init__(self, data: bytes):
        self.data = data
        self.event = threading.Event()


class WalManager:
    def __init__(self,
                 walmanager_path: str = "/data/nexuts/wal_dir",
                 log_path: str = "log.logs",
                 rotated_path: str = "log2.logs",
                 flush_interval: float = 0.01,
                 max_batch: int = 4096):
        self.log_path = os.path.join(walmanager_path, log_path)
        self.rotated_path = os.path.join(walmanager_path, rotated_path)


        self.flush_interval = float(flush_interval)
        self.max_batch = int(max_batch)

        # queue + cond
        self._q = collections.deque()  # deque[_WalEntry]
        self._q_lock = threading.Lock()
        self._q_cond = threading.Condition(self._q_lock)

        # file lock + fd
        self._file_lock = threading.Lock()
        os.makedirs(os.path.dirname(self.log_path) or ".", exist_ok=True)
        self._fd = os.open(self.log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)

        # flusher thread
        self._stop_flag = False
        self._flusher = threading.Thread(target=self._flusher_loop, name="wal-flusher", daemon=True)
        self._flusher.start()

        self.metrics = {
            "append_count": 0,
            "sync_count": 0,
            "fsync_count": 0,
            "written_bytes": 0,
            "batches": 0,
        }

    # -------------------------
    # public API
    # -------------------------
    def append(self, record: Any, sync: bool = False, timeout: Optional[float] = None) -> bool:
        data = self._normalize(record)
        entry = _WalEntry(data)
        with self._q_cond:
            self._q.append(entry)
            self._q_cond.notify()
        self.metrics["append_count"] += 1
        self.metrics["written_bytes"] += len(data)
        if sync:
            ok = entry.event.wait(timeout=timeout)
            if ok:
                self.metrics["sync_count"] += 1
            return ok
        return True

    def load_resume_records(self):
        """Load all records from log.logs and log2.logs without modifying files."""
        result = []

        # read log.logs
        if os.path.exists(self.log_path):
            result.extend(self._read_and_parse_all(self.log_path))

        # read log2.logs if exists
        if os.path.exists(self.rotated_path):
            result.extend(self._read_and_parse_all(self.rotated_path))

        return result


    def _barrier_flush(self):
        """Insert a sentinel entry and wait until it's persisted, used before rotate/commit."""
        entry = _WalEntry(b"")
        with self._q_cond:
            self._q.append(entry)
            self._q_cond.notify()
        entry.event.wait()

    def rotate(self, new_path: Optional[str] = None, line_n: int = 0):
        """Switch active writing file to new_path (default rotated_path).
        Optionally copy last `line_n` lines from current log to new log at head.
        """
        new_path = new_path or self.rotated_path
        self._barrier_flush()
        with self._file_lock:
            try:
                _fdatasync(self._fd)
            except Exception:
                pass
            try:
                os.close(self._fd)
            except Exception:
                pass

            parent = os.path.dirname(new_path) or "."
            os.makedirs(parent, exist_ok=True)

            prepend_data = b""
            if line_n > 0 and os.path.exists(self.log_path):
                lines = self._read_complete_lines(self.log_path)
                last_lines = lines[-line_n:] if line_n <= len(lines) else lines
                prepend_data = b"".join(last_lines)

            with open(new_path, "wb") as f:
                if prepend_data:
                    f.write(prepend_data)

            self._fd = os.open(new_path, os.O_WRONLY | os.O_APPEND)

    def commit_new_log(self):
        self._barrier_flush()
        with self._file_lock:
            try:
                _fdatasync(self._fd)
            except Exception:
                pass
            try:
                os.close(self._fd)
            except Exception:
                pass

            os.replace(self.rotated_path, self.log_path)
            self._fsync_dir(self.log_path)

            self._fd = os.open(self.log_path, os.O_WRONLY | os.O_APPEND)

    def read_all_records(self, start_percent: float = 0.0, end_percent: float = 100.0) -> List[Any]:
        if not (0.0 <= start_percent <= 100.0 and 0.0 <= end_percent <= 100.0):
            raise ValueError("percent must be in [0,100]")
        if start_percent >= end_percent:
            return []

        lines = self._read_complete_lines(self.log_path)
        total = len(lines)
        if total == 0:
            return []

        start_idx = int(total * start_percent / 100.0)
        end_idx = int(total * end_percent / 100.0)
        if end_idx <= start_idx:
            return []

        out = []
        for line in lines[start_idx:end_idx]:
            parsed = self._parse_line(line)
            if parsed is not None:
                out.append(parsed)
        return out

    def recover(self) -> List[Any]:
        result = []
        if os.path.exists(self.rotated_path):
            self._ensure_truncate_to_last_newline(self.log_path)
            self._ensure_truncate_to_last_newline(self.rotated_path)
            result.extend(self._read_and_parse_all(self.log_path))
            result.extend(self._read_and_parse_all(self.rotated_path))
        else:
            self._ensure_truncate_to_last_newline(self.log_path)
            result.extend(self._read_and_parse_all(self.log_path))
        return result

    def close(self, wait: float = 5.0):
        self._stop_flag = True
        with self._q_cond:
            self._q_cond.notify_all()
        self._flusher.join(timeout=wait)
        with self._file_lock:
            try:
                _fdatasync(self._fd)
            except Exception:
                pass
            try:
                os.close(self._fd)
            except Exception:
                pass

    # -------------------------
    # internal helpers
    # -------------------------
    def _normalize(self, record: Any) -> bytes:
        if isinstance(record, dict):
            s = json.dumps(record, ensure_ascii=False)
            b = s.encode("utf-8")
        elif isinstance(record, str):
            b = record.encode("utf-8")
        elif isinstance(record, (bytes, bytearray)):
            b = bytes(record)
        else:
            raise TypeError("record must be dict/str/bytes")
        if not b.endswith(b"\n"):
            b += b"\n"
        return b

    def _parse_line(self, line: bytes) -> Optional[Any]:
        line = line.rstrip(b"\n")
        if not line:
            return None
        try:
            return json.loads(line.decode("utf-8"))
        except Exception:
            return None

    def _collect_batch(self) -> List[_WalEntry]:
        batch = []
        while self._q and len(batch) < self.max_batch:
            batch.append(self._q.popleft())
        return batch

    def _flusher_loop(self):
        while True:
            with self._q_cond:
                if not self._q and not self._stop_flag:
                    self._q_cond.wait(timeout=self.flush_interval)
                if not self._q and self._stop_flag:
                    break
                batch = self._collect_batch()

            if not batch:
                continue

            total_bytes = b"".join(entry.data for entry in batch if entry.data)
            with self._file_lock:
                try:
                    written = 0
                    while written < len(total_bytes):
                        n = os.write(self._fd, total_bytes[written:])
                        if n == 0:
                            raise OSError("os.write returned 0")
                        written += n
                    try:
                        _fdatasync(self._fd)
                        self.metrics["fsync_count"] += 1
                    except Exception:
                        pass
                except Exception:
                    pass

            for entry in batch:
                entry.event.set()
            self.metrics["batches"] += 1

        remaining = []
        with self._q_lock:
            while self._q:
                remaining.append(self._q.popleft())
        if remaining:
            total_bytes = b"".join(entry.data for entry in remaining if entry.data)
            with self._file_lock:
                try:
                    written = 0
                    while written < len(total_bytes):
                        n = os.write(self._fd, total_bytes[written:])
                        if n == 0:
                            raise OSError("os.write returned 0")
                        written += n
                    try:
                        _fdatasync(self._fd)
                        self.metrics["fsync_count"] += 1
                    except Exception:
                        pass
                except Exception:
                    pass
            for entry in remaining:
                entry.event.set()
            self.metrics["batches"] += 1

    def _read_complete_lines(self, path: str) -> List[bytes]:
        if not os.path.exists(path):
            return []
        with open(path, "rb") as f:
            data = f.read()
        if not data:
            return []
        raw_lines = data.splitlines(keepends=True)
        complete = [ln for ln in raw_lines if ln.endswith(b"\n")]
        return complete

    def _read_and_parse_all(self, path: str) -> List[Any]:
        lines = self._read_complete_lines(path)
        out = []
        for ln in lines:
            p = self._parse_line(ln)
            if p is not None:
                out.append(p)
        return out

    def _ensure_truncate_to_last_newline(self, path: str):
        if not os.path.exists(path):
            return
        try:
            fd = os.open(path, os.O_RDWR)
        except Exception:
            return
        try:
            st = os.fstat(fd)
            if st.st_size == 0:
                os.close(fd)
                return
            data = os.pread(fd, st.st_size, 0)
            if not data.endswith(b"\n"):
                idx = data.rfind(b"\n")
                if idx == -1:
                    try:
                        os.ftruncate(fd, 0)
                        _fdatasync(fd)
                    except Exception:
                        pass
                else:
                    try:
                        os.ftruncate(fd, idx + 1)
                        _fdatasync(fd)
                    except Exception:
                        pass
        finally:
            try:
                os.close(fd)
            except Exception:
                pass

    def _fsync_dir(self, path: str):
        dirpath = os.path.dirname(path) or "."
        try:
            dirfd = os.open(dirpath, os.O_DIRECTORY | os.O_RDONLY)
            try:
                os.fsync(dirfd)
            finally:
                os.close(dirfd)
        except Exception:
            pass


if __name__ == "__main__":
    # --- 清理旧文件 ---
    for f in ["log.logs", "log2.logs"]:
        if os.path.exists(f):
            os.remove(f)

    print("=== 创建 WAL 实例 ===")
    wal = WalManager(log_path="log.logs", rotated_path="log2.logs")

    print("\n=== 测试 1：异步 append ===")
    for i in range(50):
        wal.append({"msg": f"async-{i}"})
    #time.sleep(0.1)
    print(open("log.logs").read())

    # print("\n=== 测试 2：同步 append ===")
    # wal.append({"msg": "sync-write"}, sync=True)
    # print(open("log.logs").read())
    #
    # print("\n=== 测试 3：百分比读取 0~50 ===")
    # recs = wal.read_all_records(0, 50)
    # print("读取结果：", recs)
    #
    # print("\n=== 测试 4：rotate，拷贝最后两行 ===")
    # wal.rotate(line_n=2)
    # wal.append({"msg": "after-rotate-1"}, sync=True)
    # wal.append({"msg": "after-rotate-2"}, sync=True)
    # print("log2.logs 内容：")
    # print(open("log2.logs").read())
    #
    # print("\n=== 测试 5：commit_new_log ===")
    # wal.commit_new_log()
    # print("log.logs 内容：")
    # print(open("log.logs").read())
    #
    # print("\n=== 测试 6：模拟半行崩溃 ===")
    # with open("log.logs", "ab") as f:
    #     f.write(b'{"msg": "crash-partial"')
    # print(open("log.logs").read())
    #
    # print("\n=== 测试 recover ===")
    # out = wal.recover()
    # print(out)
    # print(open("log.logs").read())
    #
    # print("\n=== 关闭 WAL ===")
    # wal.close()
    # print("\n=== metrics ===")
    # print(wal.metrics)