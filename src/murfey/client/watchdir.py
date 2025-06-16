"""
Watches the specified directory, crawling through it recursively to identify potential
files for transfer, and notifies listening processes to perform the transfer on files
that are ready.
"""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
from pathlib import Path
from typing import List, NamedTuple, Optional

from murfey.client.tui.status_bar import StatusBar
from murfey.util.client import Observer

log = logging.getLogger("murfey.client.watchdir")


class _FileInfo(NamedTuple):
    size: int
    modification_time: float
    settling_time: Optional[float] = None


class DirWatcher(Observer):
    def __init__(
        self,
        path: str | os.PathLike,
        settling_time: float = 60,
        appearance_time: float | None = None,
        transfer_all: bool = True,
        status_bar: StatusBar | None = None,
    ):
        super().__init__()
        self._basepath = os.fspath(path)
        self._lastscan: dict[str, _FileInfo] | None = {}
        self._file_candidates: dict[str, _FileInfo] = {}
        self._statusbar = status_bar
        self.settling_time = settling_time
        self._appearance_time = appearance_time
        self._transfer_all = transfer_all
        self._modification_overwrite: float | None = None
        self._init_time: float = time.time()
        self.queue: queue.Queue[Path | None] = queue.Queue()
        self.thread = threading.Thread(
            name=f"DirWatcher {self._basepath}", target=self._process, daemon=True
        )
        self._stopping = False
        self._halt_thread = False

    def __repr__(self) -> str:
        return f"<DirWatcher ({self._basepath})>"

    def start(self):
        if self.thread.is_alive():
            raise RuntimeError("DirWatcher already running")
        if self._stopping:
            raise RuntimeError("DirWatcher has already stopped")
        log.info(f"DirWatcher thread starting for {self}")
        self.thread.start()

    def request_stop(self):
        self._stopping = True
        self._halt_thread = True

    def stop(self):
        log.debug("DirWatcher thread stop requested")
        self._stopping = True
        if self.thread.is_alive():
            self.queue.join()

        self._halt_thread = True
        if self.thread.is_alive():
            self.queue.put(None)
            self.thread.join()
        log.debug("DirWatcher thread stop completed")

    def _process(self):
        if self._appearance_time:
            if self._appearance_time > 0:
                modification_time: float | None = (
                    time.time() - self._appearance_time * 3600
                )
            else:
                modification_time = None
        else:
            modification_time = None
        while not self._stopping:
            self.scan(
                modification_time=modification_time, transfer_all=self._transfer_all
            )
            time.sleep(15)
        self.notify(final=True)

    def scan(self, modification_time: float | None = None, transfer_all: bool = False):
        """
        Scans the specified directory and its subdirectories recursively for files, and
        compiles of a list of files to send for transfer.
        """
        try:
            filelist = self._scan_directory(
                modification_time=self._modification_overwrite or modification_time
            )
            scan_completion = time.time()

            # Update the timestamps associated with the discovered files
            for entry, entry_info in filelist.items():
                if self._lastscan is not None and entry_info != self._lastscan.get(
                    entry
                ):
                    self._file_candidates[entry] = entry_info._replace(
                        settling_time=scan_completion
                    )

            # Create a list of files sroted based on their timestamps
            files_for_transfer = []
            time_ordered_file_candidates = sorted(
                self._file_candidates,
                key=lambda _x: self._file_candidates[_x].modification_time,
            )
            ordered_file_candidates: List[str] = []
            for x in time_ordered_file_candidates:
                if x.endswith(".mdoc"):
                    ordered_file_candidates.insert(0, x)
                else:
                    ordered_file_candidates.append(x)

            # Check if files are ready, and append them for transfer
            for x in ordered_file_candidates:
                if x not in filelist:
                    # Delete file from file candidates list if they're no longer there
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
                                        # Touch the changing directory so that we don't
                                        # potentially catch old directories that aren't
                                        # changing when _modification_overwrite is set.
                                        # This means it will only autodetect new
                                        # directories from this point.
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
                                if self._notify_for_transfer(x):
                                    files_for_transfer.append(Path(x))
                            continue
                    except Exception as e:
                        log.error(f"Exception encountered: {e}", exc_info=True)
                        return

                if self._lastscan is not None and x not in self._lastscan:
                    log.debug(
                        f"Found file {Path(x).name!r} for potential future transfer"
                    )

            # Notify secondary listening processes and add files to scan history
            self.notify(files_for_transfer, secondary=True)
            self._lastscan = filelist
        except Exception as e:
            log.error(f"Exception encountered: {e}")

    def _notify_for_transfer(self, file_candidate: str) -> bool:
        """
        Perform a Boolean check to see if a file is ready to be transferred, and
        removes it from the file candidates list.
        """
        log.debug(f"File {Path(file_candidate).name!r} is ready to be transferred")
        if self._statusbar:
            # log.info("Increasing number to be transferred")
            with self._statusbar.lock:
                self._statusbar.transferred = [
                    self._statusbar.transferred[0],
                    self._statusbar.transferred[1] + 1,
                ]

        # Check that it's not a hidden file, ".", "..", or still downloading
        transfer_check = not Path(file_candidate).name.startswith(".") and not Path(
            file_candidate
        ).name.endswith("downloading")

        # Notify primary listeners that file is ready, and delete it from candidates list
        if transfer_check:
            self.notify(Path(file_candidate))
        del self._file_candidates[file_candidate]
        return transfer_check

    def _scan_directory(
        self, path: str = "", modification_time: float | None = None
    ) -> dict[str, _FileInfo]:
        """
        Explores the specified directory and its subdirectories recursively to identify
        files for potential transfer, returning them as dictionary entries.
        """
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
                # Exclude textual log
                if "textual" in str(entry):
                    continue

                # Get file statistics and append file to dictionary
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
