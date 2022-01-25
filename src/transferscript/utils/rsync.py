from __future__ import annotations

import queue
import threading
from pathlib import Path
from typing import Callable, List, Optional, Union

import procrunner

from transferscript.utils.file_monitor import Monitor


class RsyncInstance:
    """
    Class for running an rsync process in a thread and collecting relevant information
    from its outputs.

    :param files: List of files to be transferred
    :param type: list of strings or pathlib.Path objects
    :param destination: Directory to copy files to
    :param destination: string or pathlib.Path object
    """

    def __init__(self, files: List[Union[str, Path]], destination: Union[str, Path]):
        self.destintation = destination
        self.files = files
        self.total_files = len(files)
        self.transferred: List[str] = []
        self.failed: List[str] = []
        self._transferring = False
        self.sent_bytes = 0
        self.received_bytes = 0
        self.byte_rate: float = 0
        self.total_size = 0
        self.thread = threading.Thread(
            target=self.run_rsync,
            args=(
                files,
                destination,
                self._parse_rsync_stdout,
                self._parse_rsync_stderr,
            ),
        )
        self.runner_return: Optional[procrunner.ReturnObject] = None

    def __call__(self) -> threading.Thread:
        self._transferring = True
        self.thread.start()
        return self.thread

    def run_rsync(
        self,
        files: List[Union[str, Path]],
        destination: Union[str, Path],
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
        cmd.extend(str(f) for f in files)
        cmd.append(str(destination))
        runner = procrunner.run(
            cmd, callback_stdout=callback_stdout, callback_stderr=callback_stderr
        )
        self.runner_return = runner

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
                else:
                    self.transferred.append(stringy_stdout)
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
                self.failed.append(
                    failed_msg[failed_msg.index("failed:") - 1].replace('"', "")
                )

    def wait(self):
        """
        Wait for rsync process of this instance to finish.
        """
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


class RsynchPipe:
    def __init__(self, monitor: Monitor, finaldir: Path):
        self.monitor = monitor
        self._finaldir = finaldir
        self._in_queue: queue.Queue = monitor._file_queue

    def process(self):
        while self.monitor.thread.is_alive():
            files_for_transfer = self._in_queue.get()
            rsyncher = RsyncInstance(files_for_transfer, self._finaldir)
            rsyncher()
            rsyncher.wait()
