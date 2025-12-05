import threading
from collections.abc import MutableMapping

class ThreadSafeDict(MutableMapping):
    """线程安全版 dict，基于 MutableMapping 实现"""
    def __init__(self, *args, **kwargs):
        self._dict = dict(*args, **kwargs)
        self._lock = threading.RLock()

    def __getitem__(self, key):
        with self._lock:
            return self._dict[key]

    def __setitem__(self, key, value):
        with self._lock:
            self._dict[key] = value

    def __delitem__(self, key):
        with self._lock:
            del self._dict[key]

    def __iter__(self):
        with self._lock:
            # 返回快照，避免迭代中修改异常
            return iter(list(self._dict.keys()))

    def __len__(self):
        with self._lock:
            return len(self._dict)

    def __repr__(self):
        with self._lock:
            return f"ThreadSafeDict({self._dict!r})"

    def get(self, key, default=None):
        with self._lock:
            return self._dict.get(key, default)

    def pop(self, key, default=None):
        with self._lock:
            return self._dict.pop(key, default)

    def popitem(self):
        with self._lock:
            return self._dict.popitem()

    def clear(self):
        with self._lock:
            self._dict.clear()

    def update(self, *args, **kwargs):
        with self._lock:
            self._dict.update(*args, **kwargs)

    def setdefault(self, key, default=None):
        with self._lock:
            return self._dict.setdefault(key, default)

    def copy(self):
        with self._lock:
            return dict(self._dict)

    def items(self):
        with self._lock:
            return list(self._dict.items())

    def keys(self):
        with self._lock:
            return list(self._dict.keys())

    def values(self):
        with self._lock:
            return list(self._dict.values())