from __future__ import annotations

import pathlib
import queue
import threading
import time
from typing import Dict, List, Optional


class Monitor:
    def __init__(self, directory: pathlib.Path):
        self.dir = directory
        self._timed_cache: Dict[pathlib.Path, float] = {}
        self._in_queue: queue.Queue = queue.Queue()
        self._out_queue: queue.Queue = queue.Queue()
        self.thread: Optional[threading.Thread] = None

    def _check(self) -> List[pathlib.Path]:
        new_files: Dict[pathlib.Path, float] = {
            f: f.stat().st_mtime
            for f in self.dir.glob("**/*")
            if not self._timed_cache.get(f) or self._timed_cache[f] == f.stat().st_mtime
        }
        if not new_files:
            return []
        self._timed_cache.update(new_files)
        return list(new_files.keys())

    def monitor(self, sleep: int = 10, in_thread: bool = False):
        if in_thread:
            self.thread = threading.Thread(
                target=self._monitor, args=(sleep,), name=f"{self.dir} monitor thread"
            )
        else:
            self._monitor(sleep)

    def _monitor(self, sleep: int):
        while True:
            if new_files := self._check():
                for f in new_files:
                    self._out_queue.put(f)
            time.sleep(sleep)
