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
        machine_config: dict,
        skip_existing_processing: bool = False,
    ):
        super().__init__()
        self._basepath = Path(path)
        self._skip_existing_processing = skip_existing_processing
        self._seen_dirs: List[Path] = []
        self._stopping = False
        self._machine_config = machine_config
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

    def stop(self):
        log.debug("MultigridDirWatcher thread stop requested")
        self._stopping = True
        self._halt_thread = True
        if self.thread.is_alive():
            self.thread.join()
        log.debug("MultigridDirWatcher thread stop completed")

    def _process(self):
        first_loop = True
        while not self._stopping:
            for d in self._basepath.glob("*"):
                if d.name in self._machine_config["create_directories"].values():
                    if d.is_dir() and d not in self._seen_dirs:
                        self.notify(
                            d,
                            include_mid_path=False,
                            use_suggested_path=False,
                            analyse=False,
                        )
                        self._seen_dirs.append(d)
                else:
                    if d.is_dir() and d not in self._seen_dirs:
                        self.notify(
                            d,
                            extra_directory=f"metadata_{d.name}",
                            include_mid_path=False,
                            analyse=True,  # not (first_loop and self._skip_existing_processing),
                            limited=True,
                        )
                        self._seen_dirs.append(d)
                    processing_started = False
                    for d02 in (d.parent.parent / d.name).glob("Images-Disc*"):
                        if d02 not in self._seen_dirs:
                            # if skip exisiting processing is set then do not process for any
                            # data directories found on the first loop
                            # this allows you to avoid triggering processing again if murfey is restarted
                            self.notify(
                                d02,
                                include_mid_path=False,
                                remove_files=True,
                                analyse=not (
                                    first_loop and self._skip_existing_processing
                                ),
                            )
                            self._seen_dirs.append(d02)
                        processing_started = d02 in self._seen_dirs
                    if not processing_started:
                        d02 = d.parent.parent / d.name
                        if (
                            d02.is_dir()
                            and d02 not in self._seen_dirs
                            and list((d.parent.parent / d.name).iterdir())
                        ):
                            self.notify(
                                d02,
                                include_mid_path=False,
                                analyse=not (
                                    first_loop and self._skip_existing_processing
                                ),
                            )
                            self._seen_dirs.append(d02)

            if first_loop:
                first_loop = False
            time.sleep(15)
