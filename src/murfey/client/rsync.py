from __future__ import annotations

import logging
import os
import queue
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, Union

import procrunner

from murfey.util import Observer, Processor
from murfey.util.file_monitor import Monitor

logger = logging.getLogger("murfey.util.rsync")

# $ rsync -riiv . --times --outbuf=line wra62962@ws133:/dls/tmp/wra62962/junk --dry-run
# sending incremental file list
# .d          ./
# .f          README.md
# <f.st...... large
# .f          tests/util/__pycache__/test_state.cpython-39-pytest-6.2.5.pyc
# <f+++++++++ tests/server/test_main.py
#
# sent 3,785 bytes  received 355 bytes  2,760.00 bytes/sec
# total size is 314,923,092  speedup is 76,068.38 (DRY RUN)


from enum import Enum
from typing import NamedTuple


class TransferResult(Enum):
    SUCCESS = 1
    FAILURE = 2


class RSyncerUpdate(NamedTuple):
    file_name: str
    file_size: int
    outcome: TransferResult
    transfer_total: int
    transfer_speed: float
    queue_size: int


class RSyncer(Observer):
    def __init__(
        self,
        basepath_local: Path,
        basepath_remote: Path,
        host: str,
    ):
        super().__init__()
        self._basepath = basepath_local
        self._remote = f"{host}:{basepath_remote}"
        self._files_transferred = 0
        self._bytes_transferred = 0

        self.queue = queue.Queue[Optional[str]]()
        self.thread = threading.Thread(
            name=f"RSync {self._basepath}:{self._remote}", target=self._process
        )
        self._stop = False

        self.subscribe(print)

    def __repr__(self) -> str:
        return f"<RSyncer {self._basepath} → {self._remote} ({self.status})"

    @property
    def status(self) -> str:
        if self._stop:
            if self.thread.is_alive():
                return "stopping"
            else:
                return "finished"
        else:
            if self.thread.is_alive():
                return "running"
            else:
                return "ready"

    def start(self):
        if self.thread.is_alive():
            raise RuntimeError("RSyncer already running")
        if self._stop:
            raise RuntimeError("RSyncer has already stopped")
        self.thread.start()

    def stop(self):
        print("Stopping")
        self._stop = True
        self.queue.put(None)
        if self.thread.is_alive():
            self.thread.join()

    def _process(self):
        print("Process start")
        files_to_transfer: List[str]
        while not self._stop:
            print("wait")
            first = self.queue.get()
            if not first:
                # allow leaving thread when 'None' is passed
                self.queue.task_done()
                continue

            files_to_transfer = [first]
            try:
                while True:
                    next_file = self.queue.get(block=True, timeout=0.1)
                    if next_file:
                        files_to_transfer.append(next_file)
                    else:
                        self.queue.task_done()
                        break
            except queue.Empty:
                pass

            self._transfer(files_to_transfer)

            for _ in files_to_transfer:
                self.queue.task_done()
        print("Process stop")

    def _transfer(self, files: List[str]):
        """
        Actually transfer files in an rsync subprocess
        """
        previously_transferred = self._files_transferred

        next_file: RSyncerUpdate | None = None
        errors: list[str] = []
        transfer_success: set[str] = set()

        def parse_stdout(line: str):
            nonlocal next_file

            if not line:
                return

            if chr(13) in line:
                # partial transfer
                #
                #           3,136 100%    1.50MB/s    0:00:00
                #           3,136 100%    1.50MB/s    0:00:00 (xfr#5, to-chk=109/115)
                xfer_line = line.split(chr(13))[-1]
                if xfer_line.endswith(" files to consider"):
                    return
                if "(xfr" not in xfer_line:
                    raise RuntimeError(
                        f"Unexpected line {xfer_line} {line.split(chr(13))}"
                    )
                transfer_success.add(next_file.file_name)
                size_bytes = int(xfer_line.split()[0].replace(",", ""))
                self.notify(next_file._replace(file_size=size_bytes))
                next_file = None
                return
            if line.startswith(("building file list", "created directory", "sending")):
                return
            if line.startswith("sent "):
                # sent 6,676 bytes  received 397 bytes  4,715.33 bytes/sec
                return
            if line.startswith("total "):
                # total size is 315,265,653  speedup is 44,573.12 (DRY RUN)
                return

            if line.startswith((".f", "<f")):
                # .d          ./
                # .f          README.md
                # .f          tests/util/__pycache__/test_state.cpython-39-pytest-6.2.5.pyc
                # No transfer happening
                assert next_file is None, f"Invalid state {line=}, {next_file=}"

                self._files_transferred += 1
                current_outstanding = self.queue.unfinished_tasks - (
                    self._files_transferred - previously_transferred
                )
                update = RSyncerUpdate(
                    file_name=line[12:],
                    file_size=0,
                    outcome=TransferResult.SUCCESS,
                    transfer_total=self._files_transferred - previously_transferred,
                    transfer_speed=0.0,
                    queue_size=current_outstanding,
                )
                if line[0] == ".":
                    # No transfer happening
                    transfer_success.add(update.file_name)
                    self.notify(update)
                else:
                    # This marks the start of a transfer, wait for the progress line
                    next_file = update
                return

            if line.startswith(("cd", ".d")):
                return

            print(line)

        def parse_stderr(line: str):
            nonlocal errors
            logger.error(line)
            errors.append(line)

        result = procrunner.run(
            [
                "rsync",
                "-iiv",
                "--times",
                "--progress",
                "--outbuf=line",
                "--files-from=-",
                ".",
                "wra62962@ws133:/dls/tmp/wra62962/junk",
                #               "--dry-run",
            ],
            callback_stdout=parse_stdout,
            callback_stderr=parse_stderr,
            working_directory=self._basepath,
            stdin=b"\n".join(os.fsencode(f) for f in files),
            print_stdout=False,
            print_stderr=False,
        )

        if errors:
            print("STDERR:")
            print("\n".join(errors))
        if result.returncode:
            print("Returncode:", result.returncode)

        from pprint import pprint

        missing_files = set(files) - transfer_success
        pprint(missing_files)

    def _parse_rsync_stdout(self, stdout: bytes):
        """
        Parse rsync stdout to collect information such as the paths of transferred
        files and the amount of data transferred.

        :param stdout: stdout of rsync process
        :type stdout: bytes
        """
        stringy_stdout = str(stdout)
        if stringy_stdout:
            if self._transferring:
                if stringy_stdout.startswith("sent"):
                    self._transferring = False
                    byte_info = stringy_stdout.split()
                    self.sent_bytes = int(
                        byte_info[byte_info.index("sent") + 1].replace(",", "")
                    )
                    self.received_bytes = int(
                        byte_info[byte_info.index("received") + 1].replace(",", "")
                    )
                    self.byte_rate = float(
                        byte_info[byte_info.index("bytes/sec") - 1].replace(",", "")
                    )
                elif len(stringy_stdout.split()) == 1:
                    if self._root and self._sub_structure:
                        self._notify(
                            self._finaldir / self._sub_structure / stringy_stdout
                        )
                        self._out.put(self._root / self._sub_structure / stringy_stdout)
                    else:
                        logger.warning(
                            f"root or substructure not set for transfer of {stringy_stdout}"
                        )
            else:
                if "total size" in stringy_stdout:
                    self.total_size = int(
                        stringy_stdout.replace("total size", "").split()[1]
                    )

    def _parse_rsync_stderr(self, stderr: bytes):
        """
        Parse rsync stderr to collect information on any files that failed to transfer.

        :param stderr: stderr of rsync process
        :type stderr: bytes
        """
        stringy_stderr = str(stderr)
        if stringy_stderr:
            if (
                stringy_stderr.startswith("rsync: link_stat")
                and "failed" in stringy_stderr
            ):
                failed_msg = stringy_stderr.split()
                self._failed_tmp.append(
                    failed_msg[failed_msg.index("failed:") - 1].replace('"', "")
                )


if __name__ == "__main__":
    x = RSyncer(
        "/dls/tmp/wra62962/directories/z2MvX0sf",
        basepath_remote="/remote",
        host="ws312",
    )
    print(x)
    x.start()
    print(x)
    with open("/dls/tmp/wra62962/directories/z2MvX0sf/filelist", "r") as fh:
        filelist = fh.read().split("\n")
    for f in filelist:
        x.queue.put(f)
    time.sleep(15)
    x.stop()
    print(x)
