from __future__ import annotations

from murfey.util.file_monitor import Monitor


def test_empty_directory_does_nothing(tmp_path):
    monitor = Monitor(tmp_path)
    monitor.process(in_thread=True, sleep=0.1)
    monitor.stop()
    monitor.wait()
    assert not monitor._timed_cache
    assert not monitor._out.get()
    assert monitor._out.empty()


def test_directory_with_a_file_finds_that_file(tmp_path):
    (tmp_path / "empty_file.txt").touch()
    monitor = Monitor(tmp_path)
    monitor.process(in_thread=True, sleep=0.1)
    monitor.stop()
    monitor.wait()
    assert len(monitor._timed_cache.keys()) == 1
    assert monitor._out.get() == [tmp_path / "empty_file.txt"]
    assert not monitor._out.get()
    assert monitor._out.empty()


def test_directory_with_an_added_file(tmp_path):
    (tmp_path / "empty_file.txt").touch()
    monitor = Monitor(tmp_path)
    monitor.process(in_thread=True, sleep=0.1)
    (tmp_path / "another_empty_file.txt").touch()
    monitor.stop()
    monitor.wait()
    assert len(monitor._timed_cache.keys()) == 2
