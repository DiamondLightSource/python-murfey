from __future__ import annotations

import logging
import pathlib
import queue
import threading
import time
from typing import Dict, List, Optional

logger = logging.getLogger("transferscript.utils.monitor")


class Monitor:
    def __init__(self, directory: pathlib.Path):
        self.dir = directory
        self._timed_cache: Dict[pathlib.Path, float] = {}
        self._file_queue: queue.Queue = queue.Queue()
        self.thread: Optional[threading.Thread] = None
        self.free: bool = True

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
            logger.info(
                f"Starting to monitor {self.dir} in separate thread {self.thread}"
            )
            self.thread.start()
        else:
            logger.info(f"Starting to monitor {self.dir}")
            self._monitor(sleep)

    def _monitor(self, sleep: int):
        while self.free:
            if new_files := self._check():
                logger.info(f"{len(new_files)} new files found")
                self._file_queue.put(new_files)
            time.sleep(sleep)

    def stop(self):
        self.free = False

    def wait(self):
        self.thread.join()
