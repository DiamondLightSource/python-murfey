from __future__ import annotations

import logging
import queue
import threading
from pathlib import Path
from typing import Callable, Dict, List, Optional

import procrunner

from transferscript.utils.file_monitor import Monitor

logger = logging.getLogger("transferscript.utils.rsync")


class RsyncInstance:
    """
    Class for running an rsync process in a thread and collecting relevant information
    from its outputs.

    :param files: List of files to be transferred
    :param type: list of strings or pathlib.Path objects
    :param destination: Directory to copy files to
    :param destination: string or pathlib.Path object
    """

    def __init__(
        self,
        root: Path,
        files: List[Path],
        destination: Path,
    ):
        self.destination = destination
        self.root = root
        self.files = files
        self.total_files = len(files)
        self.transferred: List[Path] = []
        self._transferred_tmp: List[str] = []
        self.failed: List[Path] = []
        self._failed_tmp: List[str] = []
        self._transferring = False
        self.sent_bytes = 0
        self.received_bytes = 0
        self.byte_rate: float = 0
        self.total_size = 0
        self.thread: Optional[threading.Thread] = None
        self.runner_return: List[procrunner.ReturnObject] = []

    def __call__(self, in_thread: bool = False) -> Optional[threading.Thread]:
        self._transferring = True
        if in_thread:
            self.thread = threading.Thread(
                target=self.run_rsync,
                args=(
                    self.files,
                    self.destination,
                    self._parse_rsync_stdout,
                    self._parse_rsync_stderr,
                ),
            )
            self.thread.start()
            return self.thread
        else:
            self.run_rsync(
                self.files,
                self.destination,
                self._parse_rsync_stdout,
                self._parse_rsync_stderr,
            )
            return None

    def run_rsync(
        self,
        files: List[Path],
        destination: Path,
        callback_stdout: Callable,
        callback_stderr: Callable,
    ):
        """
        Run rsync -v on a list of files using procrunner.

        :param files: List of files to be transferred
        :type files: list of strigs or pathlib.Path objects
        :param destination: Directory that files are to be copied to.
        :type destination: string or pathlib.Path object
        :param callback_stdout: Method for parsing rsync's stdout
        :type callback_stdout: callable that takes a byte string as its only input
        :param callback_stderr: Method for parsing rsync's stderr
        :type callback_sterr: callable that takes a byte string as its only input
        """
        cmd: List[str] = ["rsync", "-v"]

        def _structure(p: Path) -> Path:
            return (p.relative_to(self.root)).parent

        divided_files: Dict[Path, List[Path]] = {}
        for f in files:
            s = _structure(f)
            try:
                divided_files[s].append(f)
            except KeyError:
                divided_files[s] = [f]
        for s in divided_files.keys():
            self._transferred_tmp = []
            self._failed_tmp = []
            cmd.extend(str(f) for f in divided_files[s])
            cmd.append(str(destination / s) + "/")
            runner = procrunner.run(
                cmd, callback_stdout=callback_stdout, callback_stderr=callback_stderr
            )
            self.runner_return.append(runner)
            self.transferred.extend(self.root / s / f for f in self._transferred_tmp)
            self.failed.extend(self.root / s / f for f in self._failed_tmp)

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
                    self.sent_bytes = int(byte_info[byte_info.index("sent") + 1])
                    self.received_bytes = int(
                        byte_info[byte_info.index("received") + 1]
                    )
                    self.byte_rate = float(byte_info[byte_info.index("bytes/sec") - 1])
                elif len(stringy_stdout.split()) == 1:
                    self._transferred_tmp.append(stringy_stdout)
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

    def wait(self):
        """
        Wait for rsync process of this instance to finish.
        """
        if self.thread:
            self.thread.join()

    def check(self) -> bool:
        """
        Print summary of rsync process.

        :return: True if the number of transferred files is equal to the number
        of files that was to be transferred and False otherwise.
        :rtype: bool
        """
        print("\n=====checking rsync instance=====")
        print(f"{len(self.transferred)} files transferred of {self.total_files}")
        print(f"total size {self.total_size} transferred")
        print(f"{self.sent_bytes} bytes sent and {self.received_bytes} received")
        print("=================================\n")
        return len(self.transferred) == self.total_files


class RsyncPipe:
    def __init__(self, monitor: Monitor, finaldir: Path):
        self.monitor = monitor
        self._finaldir = finaldir
        self._in_queue: queue.Queue = monitor._file_queue
        self.thread: Optional[threading.Thread] = None

    def process(self, retry: bool = True, in_thread: bool = False):
        if in_thread:
            self.thread = threading.Thread(
                target=self._process,
                args=(retry,),
                name=f"rsync -> {self._finaldir} thread",
            )
            self.thread.start()
        else:
            self._process(retry)

    def _process(self, retry: bool = True):
        if self.monitor.thread:
            while self.monitor.thread.is_alive():
                files_for_transfer = self._in_queue.get()
                if not files_for_transfer:
                    continue
                rsyncher = RsyncInstance(
                    self.monitor.dir, files_for_transfer, self._finaldir
                )
                rsyncher()
                rsyncher.wait()
                if rsyncher.failed:
                    for f in rsyncher.failed:
                        logger.error(f"Failed to transfer file {f}")
                    if retry:
                        # put the failed file transfers back into the queue
                        self._in_queue.put(rsyncher.failed)

    def wait(self):
        if self.thread:
            self.thread.join()
