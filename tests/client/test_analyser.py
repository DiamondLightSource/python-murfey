from __future__ import annotations

from murfey.client.analyser import Analyser


def test_analyser_setup_and_stopping(tmp_path):
    analyser = Analyser(tmp_path)
    assert analyser.queue.empty()
    analyser.start()
    assert analyser.thread.is_alive()
    analyser.stop()
    assert analyser._halt_thread
    assert not analyser.thread.is_alive()


def test_analyser_tomo_determination(tmp_path):
    tomo_file = tmp_path / "Position_1_[30.0].tiff"
    analyser = Analyser(tmp_path)
    analyser.start()
    analyser.queue.put(tomo_file)
    analyser.stop()
    assert analyser._context._acquisition_software == "tomo"


def test_analyser_epu_determination(tmp_path):
    tomo_file = tmp_path / "FoilHole_12345_Data_6789.tiff"
    analyser = Analyser(tmp_path)
    analyser.start()
    analyser.queue.put(tomo_file)
    analyser.stop()
    assert analyser._context._acquisition_software == "epu"
