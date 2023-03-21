from __future__ import annotations

from murfey.client.context import TomographyContext


def test_tomography_context_initialisation_for_tomo(tmp_path):
    context = TomographyContext("tomo", tmp_path)
    assert not context._last_transferred_file
    assert context._acquisition_software == "tomo"


def test_tomography_context_add_tomo_tilt(tmp_path):
    context = TomographyContext("tomo", tmp_path)
    context.post_transfer(
        tmp_path / "Position_1_[30.0].tiff", role="detector", required_position_files=[]
    )
    assert context._tilt_series == {"Position_1": [tmp_path / "Position_1_[30.0].tiff"]}
    assert context._last_transferred_file == tmp_path / "Position_1_[30.0].tiff"
    context.post_transfer(
        tmp_path / "Position_1_[-30.0].tiff",
        role="detector",
        required_position_files=[],
    )
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_2_[30.0].tiff", role="detector", required_position_files=[]
    )
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["Position_1"]


def test_tomography_context_add_tomo_tilt_out_of_order(tmp_path):
    context = TomographyContext("tomo", tmp_path)
    context.post_transfer(
        tmp_path / "Position_1_[30.0].tiff", role="detector", required_position_files=[]
    )
    assert context._tilt_series == {"Position_1": [tmp_path / "Position_1_[30.0].tiff"]}
    assert context._last_transferred_file == tmp_path / "Position_1_[30.0].tiff"
    context.post_transfer(
        tmp_path / "Position_1_[-30.0].tiff",
        role="detector",
        required_position_files=[],
    )
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_2_[-30.0].tiff",
        role="detector",
        required_position_files=[],
    )
    assert len(context._tilt_series.values()) == 2
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_2_[30.0].tiff", role="detector", required_position_files=[]
    )
    assert len(context._tilt_series.values()) == 2
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_3_[-30.0].tiff",
        role="detector",
        required_position_files=[],
    )
    assert len(context._tilt_series.values()) == 3
    assert context._completed_tilt_series == ["Position_1", "Position_2"]
    context.post_transfer(
        tmp_path / "Position_3_[30.0].tiff", role="detector", required_position_files=[]
    )
    assert context._completed_tilt_series == ["Position_1", "Position_2", "Position_3"]


def test_tomography_context_add_tomo_tilt_delayed_tilt(tmp_path):
    context = TomographyContext("tomo", tmp_path)
    context.post_transfer(
        tmp_path / "Position_1_[30.0].tiff", role="detector", required_position_files=[]
    )
    assert context._tilt_series == {"Position_1": [tmp_path / "Position_1_[30.0].tiff"]}
    assert context._last_transferred_file == tmp_path / "Position_1_[30.0].tiff"
    context.post_transfer(
        tmp_path / "Position_1_[-30.0].tiff",
        role="detector",
        required_position_files=[],
    )
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_2_[30.0].tiff", role="detector", required_position_files=[]
    )
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["Position_1"]
    context.post_transfer(
        tmp_path / "Position_2_[-30.0].tiff",
        role="detector",
        required_position_files=[],
    )
    new_series = context.post_transfer(
        tmp_path / "Position_1_[60.0].tiff", role="detector", required_position_files=[]
    )
    assert context._completed_tilt_series == ["Position_2", "Position_1"]
    assert new_series == ["Position_1"]


def test_tomography_context_initialisation_for_serialem(tmp_path):
    context = TomographyContext("serialem", tmp_path)
    assert not context._last_transferred_file
    assert context._acquisition_software == "serialem"


def test_tomography_context_add_serialem_tilt(tmp_path):
    context = TomographyContext("serialem", tmp_path)
    context.post_transfer(tmp_path / "tomography_1_2_30.tiff", role="detector")
    assert context._tilt_series == {"1": [tmp_path / "tomography_1_2_30.tiff"]}
    assert context._last_transferred_file == tmp_path / "tomography_1_2_30.tiff"
    context.post_transfer(tmp_path / "tomography_1_2_-30.tiff", role="detector")
    assert context._tilt_series == {
        "1": [tmp_path / "tomography_1_2_30.tiff", tmp_path / "tomography_1_2_-30.tiff"]
    }
    assert not context._completed_tilt_series
    context.post_transfer(tmp_path / "tomography_2_2_30.tiff", role="detector")
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["1"]


def test_tomography_context_add_serialem_decimal_tilt(tmp_path):
    context = TomographyContext("serialem", tmp_path)
    context.post_transfer(tmp_path / "tomography_1_2_30.0.tiff", role="detector")
    assert context._tilt_series == {"1": [tmp_path / "tomography_1_2_30.0.tiff"]}
    assert context._last_transferred_file == tmp_path / "tomography_1_2_30.0.tiff"
    context.post_transfer(tmp_path / "tomography_1_2_-30.0.tiff", role="detector")
    assert context._tilt_series == {
        "1": [
            tmp_path / "tomography_1_2_30.0.tiff",
            tmp_path / "tomography_1_2_-30.0.tiff",
        ]
    }
    assert not context._completed_tilt_series
    context.post_transfer(tmp_path / "tomography_2_2_30.0.tiff", role="detector")
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["1"]
