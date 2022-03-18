from __future__ import annotations

import os
import time
from typing import NamedTuple, Optional

basepath = "/dls/m08/data/2022/cm31098-1/trash"


class FileInfo(NamedTuple):
    size: int
    modification_time: float
    settling_time: Optional[float] = None


class WatchedLocation:
    def __init__(self, path: str):
        self._basepath = path
        self._lastscan: dict[str, FileInfo] | None = None
        self._file_candidates: dict[str, FileInfo] = {}

    def scan(self):
        t = time.time()
        filelist = self.scan_directory()
        t1 = time.time() - t
        print(f"Scan completed in {t1} seconds")
        scan_completion = time.time()
        if self._lastscan:
            for entry, entry_info in self.difference_of_scans(
                self._lastscan, filelist
            ).items():
                self._file_candidates[entry] = entry_info._replace(
                    settling_time=scan_completion
                )
        self._lastscan = filelist

        for x in sorted(self._file_candidates):
            if x not in filelist:
                print(f"{x} has disappeared!")
                del self._file_candidates[x]
                continue

            if self._file_candidates[x].settling_time + 60 < time.time():
                file_stat = os.stat(x)
                if (
                    file_stat.st_size == self._file_candidates[x].size
                    and file_stat.st_mtime <= self._file_candidates[x].modification_time
                    and file_stat.st_ctime <= self._file_candidates[x].modification_time
                ):
                    print(f"{x} is ready to be transferred")
                    del self._file_candidates[x]
                    continue

            print(f"{x} must wait")

    def scan_directory(self, path: str = "") -> dict[str, FileInfo]:
        result = {}
        try:
            directory_contents = os.scandir(os.path.join(self._basepath, path))
        except FileNotFoundError:
            # Possible race condition here if the directory disappears before we had a chance to scan it.
            # If it is a sub directory then we can just ignore this case, but if it is our main directory
            # we should raise it to the caller.
            if path:
                return {}
            raise
        for entry in directory_contents:
            entry_name = os.path.join(path, entry.name)
            if entry.is_dir():
                result.update(self.scan_directory(entry_name))
            else:
                try:
                    file_stat = entry.stat()
                except FileNotFoundError:
                    # Possible race condition here if the file disappears between the scandir and the stat call.
                    # In this case we can just ignore the file.
                    continue
                result[entry_name] = FileInfo(
                    size=file_stat.st_size,
                    modification_time=max(file_stat.st_mtime, file_stat.st_ctime),
                )
        return result

    def difference_of_scans(self, old, new) -> dict[str, FileInfo]:
        return {entry: value for entry, value in new.items() if value != old.get(entry)}


w = WatchedLocation(basepath)

while True:
    w.scan()
    time.sleep(3)
