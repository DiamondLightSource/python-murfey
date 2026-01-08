from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import List

from murfey.util.client import Observer

log = logging.getLogger("murfey.client.watchdir_multigrid")


class MultigridDirWatcher(Observer):
    def __init__(
        self,
        path: str | os.PathLike,
        machine_config: dict,
        skip_existing_processing: bool = False,
    ):
        super().__init__()
        self._basepath = Path(path)
        self._machine_config = machine_config
        self._seen_dirs: List[Path] = []
        self.thread = threading.Thread(
            name=f"MultigridDirWatcher {self._basepath}",
            target=self._process,
            daemon=True,
        )
        # Toggleable settings
        self._analyse = True
        self._skip_existing_processing = skip_existing_processing
        self._stopping = False

    def start(self):
        if self.thread.is_alive():
            raise RuntimeError("DirWatcher already running")
        log.info(f"MultigridDirWatcher thread starting for {self}")
        self.thread.start()

    def request_stop(self):
        self._stopping = True
        self._halt_thread = True

    def stop(self):
        log.debug("MultigridDirWatcher thread stop requested")
        self._stopping = True
        self._halt_thread = True
        if self.thread.is_alive():
            self.thread.join()
        log.debug("MultigridDirWatcher thread stop completed")

    def _handle_metadata(self, directory: Path, extra_directory: str):
        self.notify(
            directory,
            extra_directory=extra_directory,
            analyse=self._analyse,
            limited=True,
            tag="metadata",
        )
        self._seen_dirs.append(directory)

    def _handle_fractions(self, directory: Path, first_loop: bool):
        processing_started = False
        for d02 in directory.glob("Images-Disc*"):
            if d02 not in self._seen_dirs:
                # If 'skip_existing_processing' is set, do not process for
                # any data directories found on the first loop.
                # This allows you to avoid triggering processing again if Murfey is restarted
                self.notify(
                    d02,
                    remove_files=True,
                    analyse=(
                        not (first_loop and self._skip_existing_processing)
                        if self._analyse
                        else False
                    ),
                    tag="fractions",
                )
                self._seen_dirs.append(d02)
            processing_started = d02 in self._seen_dirs
        if not processing_started:
            if (
                directory.is_dir()
                and directory not in self._seen_dirs
                and list(directory.iterdir())
            ):
                self.notify(
                    directory,
                    analyse=(
                        not (first_loop and self._skip_existing_processing)
                        if self._analyse
                        else False
                    ),
                    tag="fractions",
                )
                self._seen_dirs.append(directory)

    def _process(self):
        first_loop = True
        while not self._stopping:
            for d in self._basepath.glob("*"):
                if d.name in self._machine_config["create_directories"]:
                    if d.is_dir() and d not in self._seen_dirs:
                        self.notify(
                            d,
                            use_suggested_path=False,
                            analyse=(
                                (
                                    d.name
                                    in self._machine_config[
                                        "analyse_created_directories"
                                    ]
                                )
                                if self._analyse
                                else False
                            ),
                            tag="atlas",
                        )
                        self._seen_dirs.append(d)
                else:
                    # hack for tomo multigrid metadata structure
                    sample_dirs = list(d.glob("Sample*"))
                    if d.is_dir() and len(sample_dirs):
                        for sample in sample_dirs:
                            if len(list(sample.glob("*.mdoc"))):
                                if sample not in self._seen_dirs:
                                    self._handle_metadata(
                                        sample,
                                        extra_directory=f"metadata_{sample.parent.name}_{sample.name}",
                                    )
                                self._handle_fractions(
                                    sample.parent.parent.parent
                                    / f"{sample.parent.name}_{sample.name}",
                                    first_loop,
                                )

                    else:
                        if d.is_dir() and d not in self._seen_dirs:
                            self._handle_metadata(
                                d, extra_directory=f"metadata_{d.name}"
                            )
                        self._handle_fractions(d.parent.parent / d.name, first_loop)

            if first_loop:
                first_loop = False
            time.sleep(15)

        self.notify(final=True)
