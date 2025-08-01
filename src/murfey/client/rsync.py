"""
rsync is a Linux process that does fast, versatile remote (and local) file copying.
This module allows Murfey to use rsync to facilitate its file transfers, and to
parse the logs outputted by rsync in order confirm that the file has been correctly
transferred.
"""

from __future__ import annotations

import logging
import os
import queue
import subprocess
import threading
import time
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Awaitable, Callable, List, NamedTuple
from urllib.parse import ParseResult

from murfey.client.tui.status_bar import StatusBar
from murfey.util.client import Observer

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
        rsync_module: str,
        server_url: ParseResult,
        stop_callback: Callable = lambda *args, **kwargs: None,
        local: bool = False,
        status_bar: StatusBar | None = None,
        do_transfer: bool = True,
        remove_files: bool = False,
        required_substrings_for_removal: List[str] = [],
        notify: bool = True,
        end_time: datetime | None = None,
    ):
        super().__init__()
        self._basepath = basepath_local.absolute()
        self._basepath_remote = basepath_remote
        self._rsync_module = rsync_module
        self._do_transfer = do_transfer
        self._remove_files = remove_files
        self._required_substrings_for_removal = required_substrings_for_removal
        self._stop_callback = stop_callback
        self._local = local
        self._server_url = server_url
        self._notify = notify
        self._finalised = False
        self._end_time = end_time

        self._skipped_files: List[Path] = []

        # Set rsync destination
        if local:
            self._remote = str(basepath_remote)
        else:
            self._remote = (
                f"{server_url.hostname}::{self._rsync_module}/{basepath_remote}/"
            )
        logger.debug(f"rsync destination path set to {self._remote}")

        # For local tests you can use something along the lines of
        # self._remote = f"wra62962@ws133:/dls/tmp/wra62962/junk/{basepath_remote}"
        # to avoid having to set up an rsync daemon
        self._files_transferred = 0
        self._bytes_transferred = 0

        # self.queue = queue.Queue[Optional[Path]]()
        self.queue: queue.Queue[Path | None] = queue.Queue()
        self.thread = threading.Thread(
            name=f"RSync {self._basepath}:{self._remote}",
            target=self._process,
            daemon=True,
        )
        self._stopping = False
        self._halt_thread = False
        self._statusbar = status_bar

    def __repr__(self) -> str:
        return f"<RSyncer ({self._basepath} → {self._remote}) [{self.status}]"

    @classmethod
    def from_rsyncer(cls, rsyncer: RSyncer, **kwargs):
        kwarguments_from_rsyncer = {
            "local": rsyncer._local,
            "status_bar": rsyncer._statusbar,
            "do_transfer": rsyncer._do_transfer,
            "remove_files": rsyncer._remove_files,
            "notify": rsyncer._notify,
        }
        kwarguments_from_rsyncer.update(kwargs)
        assert isinstance(kwarguments_from_rsyncer["local"], bool)
        if kwarguments_from_rsyncer["status_bar"] is not None:
            assert isinstance(kwarguments_from_rsyncer["status_bar"], StatusBar)
        assert isinstance(kwarguments_from_rsyncer["do_transfer"], bool)
        assert isinstance(kwarguments_from_rsyncer["remove_files"], bool)
        assert isinstance(kwarguments_from_rsyncer["notify"], bool)
        return cls(
            rsyncer._basepath,
            rsyncer._basepath_remote,
            rsyncer._rsync_module,
            rsyncer._server_url,
            local=kwarguments_from_rsyncer["local"],
            status_bar=kwarguments_from_rsyncer["status_bar"],
            do_transfer=kwarguments_from_rsyncer["do_transfer"],
            remove_files=kwarguments_from_rsyncer["remove_files"],
            notify=kwarguments_from_rsyncer["notify"],
        )

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

    def notify(self, *args, secondary: bool = False, **kwargs) -> None:
        if self._notify:
            super().notify(*args, secondary=secondary, **kwargs)

    def start(self):
        if self.thread.is_alive():
            raise RuntimeError("RSyncer already running")
        if self._stopping:
            raise RuntimeError("RSyncer has already stopped")
        logger.info(f"RSync thread starting for {self}")
        self.thread.start()

    def restart(self):
        self.thread.join()
        self._halt_thread = False
        self.thread = threading.Thread(
            name=f"RSync {self._basepath}:{self._remote}",
            target=self._process,
            daemon=True,
        )
        self.start()

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

    def request_stop(self):
        self._stopping = True
        self._halt_thread = True

    def finalise(
        self,
        thread: bool = True,
        callback: Callable[..., Awaitable[None] | None] | None = None,
    ):
        self.stop()
        self._remove_files = True
        self._notify = False
        if thread:
            self.thread = threading.Thread(
                name=f"RSync finalisation {self._basepath}:{self._remote}",
                target=self._process,
                daemon=True,
            )
            for f in self._basepath.glob("**/*"):
                self.queue.put(f)
            self.stop()
        else:
            self._transfer(list(self._basepath.glob("**/*")))
        self._finalised = True
        if callback:
            callback()

    def enqueue(self, file_path: Path):
        if not self._stopping:
            absolute_path = self._basepath / file_path
            self.queue.put(absolute_path)

    def flush_skipped(self):
        for f in self._skipped_files:
            self.queue.put(f)
        self._skipped_files = []

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

            files_to_transfer = [first] if not first.name.startswith(".") else []
            stop = False
            try:
                num_files = 0
                while True:
                    if num_files > 100:
                        break
                    next_file = self.queue.get(block=True, timeout=0.1)
                    if not next_file:
                        stop = True
                        break
                    if not next_file.name.startswith("."):
                        files_to_transfer.append(next_file)
                        num_files += 1
            except queue.Empty:
                pass

            logger.info(f"Preparing to transfer {len(files_to_transfer)} files")
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

            if stop:
                self.queue.task_done()
                continue

        self._stop_callback(self._basepath, explicit_stop=self._stopping)
        logger.info("RSync thread finished")

    def _fake_transfer(self, files: list[Path]) -> bool:
        previously_transferred = self._files_transferred

        relative_filenames = []
        for f in files:
            try:
                relative_filenames.append(f.relative_to(self._basepath))
            except ValueError:
                raise ValueError(f"File '{f}' is outside of {self._basepath}") from None

        updates = []
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
            updates.append(update)
            time.sleep(0.01)
            self.notify([update], num_skipped_files=0, secondary=True)
        # self.notify(updates, secondary=True)

        return True

    def _transfer(self, infiles: list[Path]) -> bool:
        """
        Transfer files via an rsync sub-process, and parses the rsync stdout to verify
        the success of the transfer.
        """

        # Set up initial variables
        if self._end_time:
            files = [
                f
                for f in infiles
                if f.is_file() and f.stat().st_ctime < self._end_time.timestamp()
            ]
            self._skipped_files.extend(set(infiles).difference(set(files)))
            num_skipped_files = len(set(infiles).difference(set(files)))
        else:
            files = [f for f in infiles if f.is_file()]
            num_skipped_files = 0

        previously_transferred = self._files_transferred
        transfer_success: set[Path] = set()
        successful_updates: list[RSyncerUpdate] = []
        next_file: RSyncerUpdate | None = None

        def parse_stdout(line: str):
            """
            Reads the stdout from rsync in order to verify the status of transferred
            files and other things.
            """
            nonlocal next_file

            # Miscellaneous rsync stdout lines to skip
            if not line:
                return
            if line.startswith(("building file list", "created directory", "sending")):
                return
            if line.startswith("sent "):
                # sent 6,676 bytes  received 397 bytes  4,715.33 bytes/sec
                return
            if line.startswith("total "):
                # total size is 315,265,653  speedup is 44,573.12 (DRY RUN)
                return
            if line.startswith(("cd", ".d")):
                return

            # Lines with chr(13), \r, contain file transfer information
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
                successful_updates.append(next_file._replace(file_size=size_bytes))
                next_file = None
                return

            # Lines starting with "*f" contain file transfer information
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
                    with self._statusbar.lock:
                        self._statusbar.transferred = [
                            self._statusbar.transferred[0] + 1,
                            self._statusbar.transferred[1],
                        ]
                current_outstanding = self.queue.unfinished_tasks - (
                    self._files_transferred - previously_transferred
                )
                update = RSyncerUpdate(
                    file_path=Path(
                        line[12:].rstrip()
                    ),  # Remove trailing newlines, spaces, and carriage returns
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
                    successful_updates.append(update)
                else:
                    # This marks the start of a transfer, wait for the progress line
                    next_file = update
                return

        def parse_stderr(line: str):
            if line.strip():
                logger.warning(f"rsync stderr: {line!r}")

        # Generate list of relative filenames for this batch of transferred files
        #   Relative filenames will be safe to use on both Windows and Unix
        relative_filenames: List[Path] = []
        for f in files:
            try:
                relative_filenames.append(f.relative_to(self._basepath))
            except ValueError:
                raise ValueError(f"File '{f}' is outside of {self._basepath}") from None

        # Encode files to rsync as bytestring
        if self._remove_files:
            if self._required_substrings_for_removal:
                rsync_stdin_remove = b"\n".join(
                    os.fsencode(f)
                    for f in relative_filenames
                    if any(
                        substring in f.name
                        for substring in self._required_substrings_for_removal
                    )
                )
                rsync_stdin = b"\n".join(
                    os.fsencode(f)
                    for f in relative_filenames
                    if not any(
                        substring in f.name
                        for substring in self._required_substrings_for_removal
                    )
                )
            else:
                rsync_stdin_remove = b"\n".join(
                    os.fsencode(f) for f in relative_filenames
                )
                rsync_stdin = b""
        else:
            rsync_stdin_remove = b""
            rsync_stdin = b"\n".join(os.fsencode(f) for f in relative_filenames)

        # Create and run rsync subprocesses
        # rsync default settings
        rsync_cmd = [
            "rsync",
            "-iiv",
            "--times",
            "--progress",
            "--outbuf=line",
            "--files-from=-",  # '-' indicates reading from standard input
            # Needed as a pair to trigger permission modifications
            # Ref: https://serverfault.com/a/796341
            "-p",
            "--chmod=D0750,F0750",  # Use extended chmod format
        ]
        # Add file locations
        rsync_cmd.extend([".", self._remote])

        # Transfer files to destination
        result: subprocess.CompletedProcess[bytes] | None = None
        success = True
        if rsync_stdin:
            # Wrap rsync command in a bash command
            cmd = [
                "bash",
                "-c",
                # rsync command passed in as a single string
                " ".join(rsync_cmd),
            ]
            result = subprocess.run(
                cmd,
                cwd=self._basepath,  # As-is Path is fine
                capture_output=True,
                input=rsync_stdin,
            )
            # Parse outputs
            for line in result.stdout.decode("utf-8", "replace").split("\n"):
                parse_stdout(line)
            for line in result.stderr.decode("utf-8", "replace").split("\n"):
                parse_stderr(line)
            success = result.returncode == 0

        # Remove files from source
        if rsync_stdin_remove:
            # Insert file removal flag before locations
            rsync_cmd.insert(-2, "--remove-source-files")
            # Wrap rsync command in a bash command
            cmd = [
                "bash",
                "-c",
                # Pass rsync command as single string
                " ".join(rsync_cmd),
            ]
            result = subprocess.run(
                cmd,
                cwd=self._basepath,
                capture_output=True,
                input=rsync_stdin_remove,
            )
            # Parse outputs
            for line in result.stdout.decode("utf-8", "replace").split("\n"):
                parse_stdout(line)
            for line in result.stderr.decode("utf-8", "replace").split("\n"):
                parse_stderr(line)
            # Leave it as a failure if the previous rsync subprocess failed
            if success:
                success = result.returncode == 0

        self.notify(
            successful_updates, num_skipped_files=num_skipped_files, secondary=True
        )

        # Print out a summary message for each file transfer batch instead of individual messages
        # List out file paths as stored in memory to see if issue is due to file path mismatch
        if len(set(relative_filenames) - transfer_success) != 0:
            logger.debug(
                f"Files identified for transfer ({len(relative_filenames)}): {relative_filenames!r}"
            )
            logger.debug(
                f"Files successfully transferred ({len(transfer_success)}): {list(transfer_success)!r}"
            )

        # Compare files from rsync stdout to original list to verify transfer
        for f in set(relative_filenames) - transfer_success:
            # Mute individual file warnings; replace with summarised one above
            # logger.warning(f"Transfer of file {f.name!r} considered a failure")
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

        if result is None:
            # Only log this as an error if files were scheduled for transfer
            if files:
                logger.error(f"No rsync process ran for files: {files}")
        else:
            logger.log(
                logging.WARNING if result.returncode else logging.DEBUG,
                f"rsync process finished with return code {result.returncode}",
            )
        return success
