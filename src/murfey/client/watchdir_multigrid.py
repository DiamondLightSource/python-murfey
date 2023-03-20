from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import List

import murfey.util

log = logging.getLogger("murfey.client.wathdir_multigrid")


class MultigridDirWatcher(murfey.util.Observer):
    def __init__(
        self,
        path: str | os.PathLike,
    ):
        super().__init__()
        self._basepath = Path(path)
        self._seen_dirs: List[Path] = []
        self._stopping = False
        self.thread = threading.Thread(
            name=f"MultigridDirWatcher {self._basepath}",
            target=self._process,
            daemon=True,
        )

    def start(self):
        if self.thread.is_alive():
            raise RuntimeError("DirWatcher already running")
        log.info(f"MultigridDirWatcher thread starting for {self}")
        self.thread.start()

    def _process(self):
        while not self._stopping:
            for d in self._basepath.glob("*"):
                if d.is_dir() and d not in self._seen_dirs:
                    self.notify(d)
                    self._seen_dirs.append(d)
            time.sleep(15)
