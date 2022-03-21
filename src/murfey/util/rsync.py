from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, Union

import procrunner

from murfey.util import Processor
from murfey.util.file_monitor import Monitor

logger = logging.getLogger("murfey.util.rsync")


class RsyncPipe(Processor):
    def __init__(
        self,
        finaldir: Path,
        name: str = "rsync_pipe",
        root: Optional[Path] = None,
        notify: Optional[Callable[[Path], Optional[dict]]] = None,
        destination_structure: Optional[
            Callable[[Path, Path], Tuple[Path, str]]
        ] = None,
    ):
        super().__init__(name=name)
        self._finaldir = finaldir
        self.failed: List[Path] = []
        self._failed_tmp: List[str] = []
        self._transferring = False
        self.sent_bytes = 0
        self.received_bytes = 0
        self.byte_rate: float = 0
        self.total_size = 0
        self.runner_return: List[procrunner.ReturnObject] = []
        self._root = root
        self._sub_structure: Optional[Path] = None
        self._notify = notify or (lambda f: None)
        self._destination_structure = destination_structure

    def _process(self, retry: bool = True, **kwargs):
        if isinstance(self._previous, Monitor) and self._previous.thread:
            while self._previous.thread.is_alive():
                files_for_transfer = self._in.get()
                if not files_for_transfer:
                    continue
                self._run_rsync(self._previous.dir, files_for_transfer, retry=retry)

    def _run_rsync(
        self,
        root: Path,
        files: List[Path],
        retry: bool = True,
    ):
        """
        Run rsync -v on a list of files using procrunner.

        :param root: root path of files for transferring; structure below the root is preserved
        :type root: pathlib.Path object
        :param files: List of files to be transferred
        :type files: list of strigs or pathlib.Path objects
        :param destination: Directory that files are to be copied to.
        :type destination: string or pathlib.Path object
        :param retry: If True put failed files back into the queue to be consumed
        :type retry: bool
        """
        self._root = root

        def _structure(p: Path) -> Path:
            return (p.relative_to(root)).parent

        divided_files: Dict[Path, List[Path]] = {}
        for f in files:
            s = _structure(f)
            try:
                divided_files[s].append(f)
            except KeyError:
                divided_files[s] = [f]
        for s in divided_files.keys():
            if self._destination_structure:
                for f in divided_files[s]:
                    self._sub_structure, new_file_name = self._destination_structure(
                        s, f
                    )
                    self._single_rsync(
                        root,
                        self._sub_structure,
                        [f],
                        file_name=Path(new_file_name),
                        retry=retry,
                    )
            else:
                self._sub_structure = s
                self._single_rsync(root, s, divided_files[s], retry=retry)

    def _single_rsync(
        self,
        root: Path,
        sub_struct: Union[str, Path],
        sources: List[Path],
        file_name: Optional[Path] = None,
        retry: bool = True,
    ):
        cmd: List[str] = ["rsync", "-v"]
        self._failed_tmp = []
        cmd.extend(str(f) for f in sources)
        if file_name:
            cmd.append(str(self._finaldir / sub_struct / file_name))
        else:
            cmd.append(str(self._finaldir / sub_struct) + "/")
        self._transferring = True
        runner = procrunner.run(
            cmd,
            callback_stdout=self._parse_rsync_stdout,
            callback_stderr=self._parse_rsync_stderr,
        )
        self.runner_return.append(runner)
        self.failed.extend(root / sub_struct / f for f in self._failed_tmp)
        if retry:
            self._in.put(root / sub_struct / f for f in self._failed_tmp)

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
