from __future__ import annotations

import logging
import pathlib
import time
from typing import Dict, List

from murfey.util import Processor

logger = logging.getLogger("murfey.util.monitor")


class Monitor(Processor):
    def __init__(self, directory: pathlib.Path, name: str = "monitor"):
        super().__init__(name=name)
        self.dir = directory
        self._timed_cache: Dict[pathlib.Path, float] = {}
        self.free: bool = True

    def _check(self) -> List[pathlib.Path]:
        new_files: Dict[pathlib.Path, float] = {
            f: f.stat().st_mtime
            for f in self.dir.glob("**/*")
            if not self._timed_cache.get(f) or self._timed_cache[f] != f.stat().st_mtime
        }
        if not new_files:
            return []
        self._timed_cache.update(new_files)
        return list(new_files.keys())

    def _process(self, sleep: int = 10, **kwargs):
        while self.free:
            self._queue_new_files()
            time.sleep(sleep)
        self._queue_new_files()

    def _queue_new_files(self):
        if new_files := self._check():
            logger.info(f"{len(new_files)} new files found")
            self._out.put(new_files)

    def stop(self):
        self.free = False

    def wait(self):
        super().wait()
        self._out.put([])
