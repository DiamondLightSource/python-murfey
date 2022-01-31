from __future__ import annotations

from transferscript.utils.file_monitor import Monitor
from transferscript.utils.rsync import RsyncInstance, RsyncPipe


def test_a_simple_rsync_instance(tmp_path):
    (tmp_path / "from").mkdir()
    destination = tmp_path / "to"
    destination.mkdir()
    f01 = tmp_path / "from" / "file01.txt"
    f01.touch()
    ri = RsyncInstance(tmp_path / "from", [f01], destination)
    ri()
    assert len(ri.transferred) == 1
    assert ri.transferred == [f01]


def test_a_simple_rsync_instance_in_thread(tmp_path):
    (tmp_path / "from").mkdir()
    destination = tmp_path / "to"
    destination.mkdir()
    f01 = tmp_path / "from" / "file01.txt"
    f01.touch()
    ri = RsyncInstance(tmp_path / "from", [f01], destination)
    ri(in_thread=True)
    ri.wait()
    assert len(ri.transferred) == 1
    assert ri.transferred == [f01]


def test_rsync_multiple_files(tmp_path):
    (tmp_path / "from").mkdir()
    destination = tmp_path / "to"
    destination.mkdir()
    f01 = tmp_path / "from" / "file01.txt"
    f01.touch()
    f02 = tmp_path / "from" / "file02.txt"
    f02.touch()
    ri = RsyncInstance(tmp_path / "from", [f01, f02], destination)
    ri()
    assert len(ri.transferred) == 2


def test_rsync_a_nonexistant_file(tmp_path):
    (tmp_path / "from").mkdir()
    destination = tmp_path / "to"
    destination.mkdir()
    f01 = tmp_path / "from" / "file01.txt"
    f01.touch()
    f02 = tmp_path / "from" / "file02.txt"
    ri = RsyncInstance(tmp_path / "from", [f01, f02], destination)
    ri()
    assert len(ri.transferred) == 1
    assert len(ri.failed) == 1


def test_rsync_instance_on_nested_directory_structure(tmp_path):
    initial_dir = tmp_path / "from" / "nest"
    initial_dir.mkdir(parents=True)
    destination = tmp_path / "to"
    destination.mkdir()
    f01 = initial_dir / "file01.txt"
    f01.touch()
    ri = RsyncInstance(tmp_path / "from", [f01], destination)
    ri()
    assert len(ri.transferred) == 1
    assert ri.transferred == [f01]
    assert not len(ri.failed)
    assert (destination / "nest" / "file01.txt").exists()


def test_rsync_pipe_from_monitor(tmp_path):
    (tmp_path / "from").mkdir()
    destination = tmp_path / "to"
    destination.mkdir()
    f01 = tmp_path / "from" / "file01.txt"
    f01.touch()
    monitor = Monitor(tmp_path / "from")
    monitor.monitor(in_thread=True, sleep=0.1)
    rp = RsyncPipe(monitor, destination)
    rp.process(in_thread=True)
    assert rp.thread
    monitor.stop()
    monitor.wait()
    rp.wait()
    assert (destination / "file01.txt").exists()
