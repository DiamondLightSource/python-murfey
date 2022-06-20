from __future__ import annotations

from murfey.client.context import TomographyContext


def test_tomography_context_initialisation_for_tomo():
    context = TomographyContext("tomo")
    assert not context._last_transferred_file
    assert context._acquisition_software == "tomo"


def test_tomography_context_add_tomo_tilt(tmp_path):
    context = TomographyContext("tomo")
    context.post_transfer(tmp_path / "Position_1_[30.0].tiff")
    assert context._tilt_series == {"1": [tmp_path / "Position_1_[30.0].tiff"]}
    assert context._last_transferred_file == tmp_path / "Position_1_[30.0].tiff"
    context.post_transfer(tmp_path / "Position_1_[-30.0].tiff")
    assert not context._completed_tilt_series
    context.post_transfer(tmp_path / "Position_2_[30.0].tiff")
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["1"]


def test_tomography_context_add_tomo_tilt_out_of_order(tmp_path):
    context = TomographyContext("tomo")
    context.post_transfer(tmp_path / "Position_1_[30.0].tiff")
    assert context._tilt_series == {"1": [tmp_path / "Position_1_[30.0].tiff"]}
    assert context._last_transferred_file == tmp_path / "Position_1_[30.0].tiff"
    context.post_transfer(tmp_path / "Position_1_[-30.0].tiff")
    assert not context._completed_tilt_series
    context.post_transfer(tmp_path / "Position_2_[-30.0].tiff")
    assert len(context._tilt_series.values()) == 2
    assert not context._completed_tilt_series
    context.post_transfer(tmp_path / "Position_2_[30.0].tiff")
    assert len(context._tilt_series.values()) == 2
    assert not context._completed_tilt_series
    context.post_transfer(tmp_path / "Position_3_[-30.0].tiff")
    assert len(context._tilt_series.values()) == 3
    assert context._completed_tilt_series == ["1", "2"]
    context.post_transfer(tmp_path / "Position_3_[30.0].tiff")
    assert context._completed_tilt_series == ["1", "2"]


def test_tomography_context_add_tomo_tilt_delayed_tilt(tmp_path):
    context = TomographyContext("tomo")
    context.post_transfer(tmp_path / "Position_1_[30.0].tiff")
    assert context._tilt_series == {"1": [tmp_path / "Position_1_[30.0].tiff"]}
    assert context._last_transferred_file == tmp_path / "Position_1_[30.0].tiff"
    context.post_transfer(tmp_path / "Position_1_[-30.0].tiff")
    assert not context._completed_tilt_series
    context.post_transfer(tmp_path / "Position_2_[30.0].tiff")
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["1"]
    context.post_transfer(tmp_path / "Position_2_[-30.0].tiff")
    new_series = context.post_transfer(tmp_path / "Position_1_[60.0].tiff")
    assert context._completed_tilt_series == ["1"]
    assert new_series == ["1"]
