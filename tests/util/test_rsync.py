from __future__ import annotations

from pathlib import Path
from typing import Tuple

from murfey.util.file_monitor import Monitor
from murfey.util.rsync import RsyncPipe


def test_a_simple_rsync_instance(tmp_path):
    (tmp_path / "from").mkdir()
    destination = tmp_path / "to"
    destination.mkdir()
    f01 = tmp_path / "from" / "file01.txt"
    f01.touch()
    rp = RsyncPipe(destination)
    rp._run_rsync(tmp_path / "from", [f01])
    assert rp._out.qsize() == 1
    transferred = rp._out.get()
    assert transferred == f01


def test_rsync_multiple_files(tmp_path):
    (tmp_path / "from").mkdir()
    destination = tmp_path / "to"
    destination.mkdir()
    f01 = tmp_path / "from" / "file01.txt"
    f01.touch()
    f02 = tmp_path / "from" / "file02.txt"
    f02.touch()
    rp = RsyncPipe(destination)
    rp._run_rsync(tmp_path / "from", [f01, f02])
    assert rp._out.qsize() == 2
    transferred = [rp._out.get()]
    transferred.append(rp._out.get())
    assert len(transferred) == 2
    assert set(transferred) == {f01, f02}


def test_rsync_a_nonexistant_file(tmp_path):
    (tmp_path / "from").mkdir()
    destination = tmp_path / "to"
    destination.mkdir()
    f01 = tmp_path / "from" / "file01.txt"
    f01.touch()
    f02 = tmp_path / "from" / "file02.txt"
    rp = RsyncPipe(destination)
    rp._run_rsync(tmp_path / "from", [f01, f02], retry=False)
    assert rp._out.qsize() == 1
    transferred = rp._out.get()
    assert transferred == f01
    assert len(rp.failed) == 1


def test_rsync_instance_on_nested_directory_structure(tmp_path):
    initial_dir = tmp_path / "from" / "nest"
    initial_dir.mkdir(parents=True)
    destination = tmp_path / "to"
    destination.mkdir()
    f01 = initial_dir / "file01.txt"
    f01.touch()
    rp = RsyncPipe(destination)
    rp._run_rsync(tmp_path / "from", [f01])
    assert rp._out.qsize() == 1
    transferred = rp._out.get()
    assert transferred == f01
    assert not len(rp.failed)
    assert (destination / "nest" / "file01.txt").exists()


def test_rsync_pipe_from_monitor(tmp_path):
    (tmp_path / "from").mkdir()
    destination = tmp_path / "to"
    destination.mkdir()
    f01 = tmp_path / "from" / "file01.txt"
    f01.touch()
    monitor = Monitor(tmp_path / "from")
    monitor.process(in_thread=True, sleep=0.1)
    rp = RsyncPipe(destination)
    monitor >> rp
    rp.process(in_thread=True)
    assert rp.thread
    monitor.stop()
    monitor.wait()
    rp.wait()
    assert (destination / "file01.txt").exists()


def test_rsync_with_additional_structure_without_changing_file_name(tmp_path):
    def _new_structure(s: Path, p: Path) -> Tuple[Path, str]:
        new_name = p.name
        new_dest = s / "extra"
        return new_dest, new_name

    (tmp_path / "from").mkdir()
    destination = tmp_path / "to"
    destination.mkdir()
    (destination / "extra").mkdir()
    f01 = tmp_path / "from" / "file01.txt"
    f01.touch()
    monitor = Monitor(tmp_path / "from")
    monitor.process(in_thread=True, sleep=0.1)
    rp = RsyncPipe(destination, destination_structure=_new_structure)
    monitor >> rp
    rp.process(in_thread=True)
    assert rp.thread
    monitor.stop()
    monitor.wait()
    rp.wait()
    assert (destination / "extra" / "file01.txt").exists()


def test_rsync_with_changed_file_name(tmp_path):
    def _new_structure(s: Path, p: Path) -> Tuple[Path, str]:
        new_name = p.name.replace("01", "05")
        return s, new_name

    (tmp_path / "from").mkdir()
    destination = tmp_path / "to"
    destination.mkdir()
    f01 = tmp_path / "from" / "file01.txt"
    f01.touch()
    monitor = Monitor(tmp_path / "from")
    monitor.process(in_thread=True, sleep=0.1)
    rp = RsyncPipe(destination, destination_structure=_new_structure)
    monitor >> rp
    rp.process(in_thread=True)
    assert rp.thread
    monitor.stop()
    monitor.wait()
    rp.wait()
    assert not (destination / "file01.txt").exists()
    assert (destination / "file05.txt").exists()
