from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import NamedTuple, Optional

import murfey.util
from murfey.client.tui.status_bar import StatusBar

log = logging.getLogger("murfey.client.watchdir")


class _FileInfo(NamedTuple):
    size: int
    modification_time: float
    settling_time: Optional[float] = None


class DirWatcher(murfey.util.Observer):
    def __init__(
        self,
        path: str | os.PathLike,
        settling_time: float = 60,
        status_bar: StatusBar | None = None,
    ):
        super().__init__()
        self._basepath = os.fspath(path)
        self._lastscan: dict[str, _FileInfo] | None = {}
        self._file_candidates: dict[str, _FileInfo] = {}
        self._statusbar = status_bar
        self.settling_time = settling_time
        self._modification_overwrite: float | None = None
        self._init_time: float = time.time()

    def __repr__(self) -> str:
        return f"<DirWatcher ({self._basepath})>"

    def scan(self, modification_time: float | None = None, transfer_all: bool = False):
        try:
            filelist = self._scan_directory(
                modification_time=self._modification_overwrite or modification_time
            )
            scan_completion = time.time()

            for entry, entry_info in filelist.items():
                if self._lastscan is not None and entry_info != self._lastscan.get(
                    entry
                ):
                    self._file_candidates[entry] = entry_info._replace(
                        settling_time=scan_completion
                    )

            for x in sorted(
                self._file_candidates,
                key=lambda _x: self._file_candidates[_x].modification_time,
            ):
                if x not in filelist:
                    log.info(f"Previously seen file {x!r} has disappeared")
                    del self._file_candidates[x]
                    continue

                if (
                    self._file_candidates[x].settling_time + self.settling_time  # type: ignore
                    < time.time()
                ):
                    try:
                        file_stat = os.stat(x)
                        if (
                            file_stat.st_size == self._file_candidates[x].size
                            and file_stat.st_mtime
                            <= self._file_candidates[x].modification_time
                            and file_stat.st_ctime
                            <= self._file_candidates[x].modification_time
                        ):
                            if (
                                not modification_time
                                and not self._modification_overwrite
                                and not transfer_all
                            ):
                                if file_stat.st_mtime >= self._init_time:
                                    top_level_dir = (
                                        Path(self._basepath)
                                        / Path(x).relative_to(self._basepath).parts[0]
                                    )
                                    if top_level_dir.is_dir():
                                        # touch the changing directory so that when _modification_overwrite is set
                                        # we don't potentially catch old directories that aren't changing
                                        # this means it will only autodetect new directories from this point
                                        top_level_dir.touch(exist_ok=True)
                                        filelist.update(
                                            self._scan_directory(
                                                path=str(top_level_dir)
                                            )
                                        )
                                        self._modification_overwrite = max(
                                            top_level_dir.stat().st_mtime,
                                            top_level_dir.stat().st_ctime,
                                        )
                            else:
                                self._notify_for_transfer(x)
                            continue
                    except Exception as e:
                        log.error(f"Exception encountered: {e}", exc_info=True)
                        return

                if self._lastscan is not None and x not in self._lastscan:
                    log.debug(
                        f"Found file {Path(x).name!r} for potential future transfer"
                    )

            self._lastscan = filelist
        except Exception as e:
            log.error(f"Exception encountered: {e}")

    def _notify_for_transfer(self, file_candidate: str):
        log.debug(f"File {Path(file_candidate).name!r} is ready to be transferred")
        if self._statusbar:
            # log.info("Increasing number to be transferred")
            with self._statusbar.lock:
                self._statusbar.transferred = [
                    self._statusbar.transferred[0],
                    self._statusbar.transferred[1] + 1,
                ]

        if not Path(file_candidate).name.startswith("."):
            self.notify(Path(file_candidate))
        del self._file_candidates[file_candidate]

    def _scan_directory(
        self, path: str = "", modification_time: float | None = None
    ) -> dict[str, _FileInfo]:
        result: dict[str, _FileInfo] = {}
        try:
            directory_contents = os.scandir(os.path.join(self._basepath, path))
        except FileNotFoundError:
            # Possible race condition here if the directory disappears before
            # we had a chance to scan it. If it is a sub directory then we can
            # just ignore this case, but if it is our main directory we should
            # raise it to the caller.
            if path:
                return result
            raise
        for entry in directory_contents:
            entry_name = os.path.join(path, entry.name)
            if entry.is_dir() and (
                modification_time is None or entry.stat().st_ctime >= modification_time
            ):
                result.update(self._scan_directory(entry_name))
            else:
                # avoid textual log
                if "textual" in str(entry):
                    continue
                try:
                    file_stat = entry.stat()
                except FileNotFoundError:
                    # Possible race condition here if the file disappears
                    # between the scandir and the stat call.
                    # In this case we can just ignore the file.
                    continue
                if modification_time:
                    if max(file_stat.st_mtime, file_stat.st_ctime) >= modification_time:
                        result[str(Path(self._basepath) / entry_name)] = _FileInfo(
                            size=file_stat.st_size,
                            modification_time=max(
                                file_stat.st_mtime, file_stat.st_ctime
                            ),
                        )
                else:
                    result[str(Path(self._basepath) / entry_name)] = _FileInfo(
                        size=file_stat.st_size,
                        modification_time=max(file_stat.st_mtime, file_stat.st_ctime),
                    )
        return result
