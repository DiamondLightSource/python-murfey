from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import NamedTuple, Optional

import murfey.util

log = logging.getLogger("murfey.client.watchdir")


class _FileInfo(NamedTuple):
    size: int
    modification_time: float
    settling_time: Optional[float] = None


class DirWatcher(murfey.util.Observer):
    def __init__(self, path: str | os.PathLike, settling_time: float = 60):
        super().__init__()
        self._basepath = os.fspath(path)
        self._lastscan: dict[str, _FileInfo] | None = None
        self._file_candidates: dict[str, _FileInfo] = {}
        self.settling_time = settling_time

    def __repr__(self) -> str:
        return f"<DirWatcher ({self._basepath})>"

    def scan(self):
        t_start = time.perf_counter()
        filelist = self._scan_directory()
        t_scan = time.perf_counter() - t_start
        log.info(f"Scan of {self._basepath} completed in {t_scan:.1f} seconds")
        scan_completion = time.time()
        if self._lastscan:
            # TODO: Decide what to do with initial scan
            for entry, entry_info in filelist.items():
                if entry_info != self._lastscan.get(entry):
                    self._file_candidates[entry] = entry_info._replace(
                        settling_time=scan_completion
                    )
        self._lastscan = filelist

        for x in sorted(self._file_candidates):
            if x not in filelist:
                log.debug(f"Previously seen file {x} has disappeared")
                del self._file_candidates[x]
                continue

            if (
                self._file_candidates[x].settling_time + self.settling_time
                < time.time()
            ):
                file_stat = os.stat(x)
                if (
                    file_stat.st_size == self._file_candidates[x].size
                    and file_stat.st_mtime <= self._file_candidates[x].modification_time
                    and file_stat.st_ctime <= self._file_candidates[x].modification_time
                ):
                    log.debug(f"File {x} is ready to be transferred")
                    self.notify(Path(x))
                    del self._file_candidates[x]
                    continue

            log.debug(f"File {x} is not yet ready for transfer")

    def _scan_directory(self, path: str = "") -> dict[str, _FileInfo]:
        result: dict[str, _FileInfo] = {}
        try:
            directory_contents = os.scandir(os.path.join(self._basepath, path))
        except FileNotFoundError:
            # Possible race condition here if the directory disappears before we had a chance to scan it.
            # If it is a sub directory then we can just ignore this case, but if it is our main directory
            # we should raise it to the caller.
            if path:
                return result
            raise
        for entry in directory_contents:
            entry_name = os.path.join(path, entry.name)
            if entry.is_dir():
                result.update(self._scan_directory(entry_name))
            else:
                try:
                    file_stat = entry.stat()
                except FileNotFoundError:
                    # Possible race condition here if the file disappears between the scandir and the stat call.
                    # In this case we can just ignore the file.
                    continue
                result[entry_name] = _FileInfo(
                    size=file_stat.st_size,
                    modification_time=max(file_stat.st_mtime, file_stat.st_ctime),
                )
        return result
