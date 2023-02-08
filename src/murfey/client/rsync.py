from __future__ import annotations

import logging
import os
import queue
import threading
import time
from enum import Enum
from pathlib import Path
from typing import NamedTuple
from urllib.parse import ParseResult

import procrunner

from murfey.client.tui.status_bar import StatusBar
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
    queue_size: int
    base_path: Path | None = None


class RSyncer(Observer):
    def __init__(
        self,
        basepath_local: Path,
        basepath_remote: Path,
        server_url: ParseResult,
        local: bool = False,
        status_bar: StatusBar | None = None,
        do_transfer: bool = True,
        remove_files: bool = False,
    ):
        super().__init__()
        self._basepath = basepath_local.absolute()
        self._do_transfer = do_transfer
        self._remove_files = remove_files
        if local:
            self._remote = str(basepath_remote)
        else:
            self._remote = f"{server_url.hostname}::{basepath_remote}"
        # For local tests you can use something along the lines of
        # self._remote = f"wra62962@ws133:/dls/tmp/wra62962/junk/{basepath_remote}"
        # to avoid having to set up an rsync daemon
        self._files_transferred = 0
        self._bytes_transferred = 0

        # self.queue = queue.Queue[Optional[Path]]()
        self.queue: queue.Queue[Path | None] = queue.Queue()
        self.thread = threading.Thread(
            name=f"RSync {self._basepath}:{self._remote}", target=self._process
        )
        self._stopping = False
        self._halt_thread = False
        self._statusbar = status_bar

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
        logger.debug("RSync thread stop requested")
        self._stopping = True
        if self.thread.is_alive():
            logger.info("Waiting for ongoing transfers to complete...")
            self.queue.join()

        self._halt_thread = True
        if self.thread.is_alive():
            self.queue.put(None)
            self.thread.join()
        logger.debug("RSync thread stop completed")

    def enqueue(self, file_path: Path):
        if not self._stopping:
            absolute_path = (self._basepath / file_path).resolve()
            self.queue.put(absolute_path)

    def _process(self):
        logger.info("RSync thread starting")
        files_to_transfer: list[Path]
        backoff = 0
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

            logger.info(
                f"Preparing to transfer {len(files_to_transfer)} files: {files_to_transfer}, {self}"
            )
            if self._do_transfer:
                try:
                    success = self._transfer(files_to_transfer)
                except Exception as e:
                    logger.error(
                        f"Unhandled exception {e} in RSync thread", exc_info=True
                    )
                    success = False
            else:
                success = self._fake_transfer(files_to_transfer)

            logger.info(f"Completed transfer of {len(files_to_transfer)} files")
            for _ in files_to_transfer:
                self.queue.task_done()
            logger.debug(
                f"{self.queue.unfinished_tasks} files remain in queue for processing"
            )

            if success:
                backoff = 0
            else:
                backoff = min(backoff * 2 + 1, 120)
                logger.info(f"Waiting {backoff} seconds before next rsync attempt")
                time.sleep(backoff)

        logger.info("RSync thread finished")

    def _fake_transfer(self, files: list[Path]) -> bool:
        previously_transferred = self._files_transferred

        relative_filenames = []
        for f in files:
            try:
                relative_filenames.append(f.relative_to(self._basepath))
            except ValueError:
                raise ValueError(f"File '{f}' is outside of {self._basepath}") from None

        for f in set(relative_filenames):
            self._files_transferred += 1
            update = RSyncerUpdate(
                file_path=f,
                file_size=0,
                outcome=TransferResult.SUCCESS,
                transfer_total=self._files_transferred - previously_transferred,
                queue_size=0,
                base_path=self._basepath,
            )
            self.notify(update)
        return True

    def _transfer(self, files: list[Path]) -> bool:
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
                if next_file is None:
                    logger.warning(f"Invalid state {xfer_line=}, {next_file=}")
                    return
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

            if line.startswith((".f", ">f", "<f")):
                # .d          ./
                # .f          README.md
                # .f          tests/util/__pycache__/test_state.cpython-39-pytest-6.2.5.pyc
                # No transfer happening
                if next_file is not None:
                    logger.warning(f"Invalid state {line=}, {next_file=}")
                    return

                self._files_transferred += 1
                if self._statusbar:
                    logger.debug("Incrementing number of transferred files")
                    with self._statusbar.lock:
                        self._statusbar.transferred = [
                            self._statusbar.transferred[0] + 1,
                            self._statusbar.transferred[1],
                        ]
                current_outstanding = self.queue.unfinished_tasks - (
                    self._files_transferred - previously_transferred
                )
                update = RSyncerUpdate(
                    file_path=Path(line[12:].replace(" ", "")),
                    file_size=0,
                    outcome=TransferResult.SUCCESS,
                    transfer_total=self._files_transferred - previously_transferred,
                    queue_size=current_outstanding,
                    base_path=self._basepath,
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

        def parse_stderr(line: str):
            logger.warning(line)

        relative_filenames = []
        for f in files:
            try:
                relative_filenames.append(f.relative_to(self._basepath))
            except ValueError:
                raise ValueError(f"File '{f}' is outside of {self._basepath}") from None
        rsync_stdin = b"\n".join(os.fsencode(f) for f in relative_filenames)
        rsync_cmd = [
            "rsync",
            "-iiv",
            "--times",
            "--progress",
            "--outbuf=line",
            "--files-from=-",
            "-o",  # preserve ownership
            "-p",  # preserve permissions
        ]
        if self._remove_files:
            rsync_cmd.append("--remove-source-files")
        rsync_cmd.extend([".", self._remote])

        result = procrunner.run(
            rsync_cmd,
            callback_stdout=parse_stdout,
            callback_stderr=parse_stderr,
            working_directory=str(self._basepath),
            stdin=rsync_stdin,
            print_stdout=False,
            print_stderr=False,
        )
        success = result.returncode == 0

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
                queue_size=current_outstanding,
                base_path=self._basepath,
            )
            self.notify(update)
            success = False

        logger.log(
            logging.WARNING if result.returncode else logging.DEBUG,
            f"rsync process finished with return code {result.returncode}",
        )
        return success
