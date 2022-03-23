from __future__ import annotations

import logging
import os
import queue
import threading
from enum import Enum
from pathlib import Path
from typing import NamedTuple, Optional
from urllib.parse import ParseResult

import procrunner

from murfey.util import Observer

logger = logging.getLogger("murfey.client.rsync")

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


class TransferResult(Enum):
    SUCCESS = 1
    FAILURE = 2


class RSyncerUpdate(NamedTuple):
    file_path: Path
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
        server_url: ParseResult,
    ):
        super().__init__()
        self._basepath = basepath_local.absolute()
        self._remote = f"{server_url.hostname}::{basepath_remote}"
        self._files_transferred = 0
        self._bytes_transferred = 0

        self.queue = queue.Queue[Optional[Path]]()
        self.thread = threading.Thread(
            name=f"RSync {self._basepath}:{self._remote}", target=self._process
        )
        self._stopping = False
        self._halt_thread = False

    def __repr__(self) -> str:
        return f"<RSyncer {self._basepath} → {self._remote} ({self.status})"

    @property
    def status(self) -> str:
        if self._stopping:
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
        if self._stopping:
            raise RuntimeError("RSyncer has already stopped")
        logger.info(f"RSync thread starting for {self}")
        self.thread.start()

    def stop(self):
        logger.info("RSync thread stop requested")
        self._stopping = True
        if self.thread.is_alive():
            # Wait for all ongoing transfers to complete
            self.queue.join()

        self._halt_thread = True
        if self.thread.is_alive():
            self.queue.put(None)
            self.thread.join()
        logger.info("RSync thread stop completed")

    def enqueue(self, filepath: Path):
        if not self._stopping:
            absolute_path = (self._basepath / filepath).resolve()
            self.queue.put(absolute_path)

    def _process(self):
        logger.info("RSync thread starting")
        files_to_transfer: list[Path]
        while not self._halt_thread:
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

            logger.info(f"Preparing to transfer {len(files_to_transfer)} files")
            try:
                self._transfer(files_to_transfer)
            except Exception as e:
                logger.error(f"Unhandled exception {e} in RSync thread", exc_info=True)

            logger.info(f"Completed transfer of {len(files_to_transfer)} files")
            for _ in files_to_transfer:
                self.queue.task_done()
            logger.debug(
                f"Queue status: {self.queue.qsize()} {self.queue.unfinished_tasks}"
            )

        logger.info("RSync thread finishing")

    def _transfer(self, files: list[Path]):
        """
        Actually transfer files in an rsync subprocess
        """
        previously_transferred = self._files_transferred

        next_file: RSyncerUpdate | None = None
        transfer_success: set[Path] = set()

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
                if xfer_line.endswith((" file to consider", " files to consider")):
                    return
                if "(xfr" not in xfer_line:
                    raise RuntimeError(
                        f"Unexpected line {xfer_line} {line.split(chr(13))}"
                    )
                assert (
                    next_file is not None
                ), f"Invalid state {xfer_line=}, {next_file=}"
                transfer_success.add(next_file.file_path)
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
                    file_path=Path(line[12:]),
                    file_size=0,
                    outcome=TransferResult.SUCCESS,
                    transfer_total=self._files_transferred - previously_transferred,
                    transfer_speed=0.0,
                    queue_size=current_outstanding,
                )
                if line[0] == ".":
                    # No transfer happening
                    transfer_success.add(update.file_path)
                    self.notify(update)
                else:
                    # This marks the start of a transfer, wait for the progress line
                    next_file = update
                return

            if line.startswith(("cd", ".d")):
                return

            print(line)

        def parse_stderr(line: str):
            logger.error(line)

        relative_filenames = []
        for f in files:
            try:
                relative_filenames.append(f.relative_to(self._basepath))
            except ValueError:
                raise ValueError(f"File '{f}' is outside of {self._basepath}") from None
        rsync_stdin = b"\n".join(os.fsencode(f) for f in relative_filenames)

        result = procrunner.run(
            [
                "rsync",
                "-iiv",
                "--times",
                "--progress",
                "--outbuf=line",
                "--files-from=-",
                ".",
                self._remote,
                "--dry-run",
            ],
            callback_stdout=parse_stdout,
            callback_stderr=parse_stderr,
            working_directory=self._basepath,
            stdin=rsync_stdin,
            print_stdout=False,
            print_stderr=False,
        )

        for f in set(relative_filenames) - transfer_success:
            self._files_transferred += 1
            current_outstanding = self.queue.unfinished_tasks - (
                self._files_transferred - previously_transferred
            )
            update = RSyncerUpdate(
                file_path=f,
                file_size=0,
                outcome=TransferResult.FAILURE,
                transfer_total=self._files_transferred,
                transfer_speed=0.0,
                queue_size=current_outstanding,
            )
            self.notify(update)

        logger.log(
            logging.WARNING if result.returncode else logging.INFO,
            f"rsync process finished with return code {result.returncode}",
        )

    def _parse_rsync_stdout(self, stdout: bytes):
        """
        Parse rsync stdout to collect information such as the paths of transferred
        files and the amount of data transferred.
        """
        stringy_stdout = str(stdout)
        if stringy_stdout:
            if stringy_stdout.startswith("sent"):
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
            if "total size" in stringy_stdout:
                self.total_size = int(
                    stringy_stdout.replace("total size", "").split()[1]
                )
