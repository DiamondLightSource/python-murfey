from __future__ import annotations

from pathlib import Path

from murfey.client.context import TomographyContext


def test_tomography_context_initialisation_for_tomo():
    context = TomographyContext("tomo")
    assert not context._last_transferred_file
    assert context._acquisition_software == "tomo"


def test_tomography_context_add_tomo_tilt(tmp_path):
    context = TomographyContext("tomo")
    context.post_transfer(
        tmp_path / "Position_1_[30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert context._tilt_series == {
        "Position_1": [tmp_path / "Position_1_[30.0]_fractions.tiff"]
    }
    assert (
        context._last_transferred_file == tmp_path / "Position_1_[30.0]_fractions.tiff"
    )
    context.post_transfer(
        tmp_path / "Position_1_[-30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_2_[30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["Position_1"]


def test_tomography_context_add_tomo_tilt_out_of_order(tmp_path):
    context = TomographyContext("tomo")
    context.post_transfer(
        tmp_path / "Position_1_[30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert context._tilt_series == {
        "Position_1": [tmp_path / "Position_1_[30.0]_fractions.tiff"]
    }
    assert (
        context._last_transferred_file == tmp_path / "Position_1_[30.0]_fractions.tiff"
    )
    context.post_transfer(
        tmp_path / "Position_1_[-30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_2_[-30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert len(context._tilt_series.values()) == 2
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_2_[30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert len(context._tilt_series.values()) == 2
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_3_[-30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert len(context._tilt_series.values()) == 3
    assert context._completed_tilt_series == ["Position_1", "Position_2"]
    context.post_transfer(
        tmp_path / "Position_3_[30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert context._completed_tilt_series == ["Position_1", "Position_2", "Position_3"]


def test_tomography_context_add_tomo_tilt_delayed_tilt(tmp_path):
    context = TomographyContext("tomo")
    context.post_transfer(
        tmp_path / "Position_1_[30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert context._tilt_series == {
        "Position_1": [tmp_path / "Position_1_[30.0]_fractions.tiff"]
    }
    assert (
        context._last_transferred_file == tmp_path / "Position_1_[30.0]_fractions.tiff"
    )
    context.post_transfer(
        tmp_path / "Position_1_[-30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_2_[30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["Position_1"]
    context.post_transfer(
        tmp_path / "Position_2_[-30.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    new_series = context.post_transfer(
        tmp_path / "Position_1_[60.0]_fractions.tiff",
        role="detector",
        required_position_files=[],
    )
    assert context._completed_tilt_series == ["Position_2", "Position_1"]
    assert new_series == ["Position_1"]


def test_tomography_context_initialisation_for_serialem():
    context = TomographyContext("serialem")
    assert not context._last_transferred_file
    assert context._acquisition_software == "serialem"


def test_tomography_context_add_serialem_tilt(tmp_path):
    context = TomographyContext("serialem")
    context.post_transfer(tmp_path / "tomography_1_2_30.tiff", role="detector")
    assert context._tilt_series == {"1": [tmp_path / "tomography_1_2_30.tiff"]}
    assert context._last_transferred_file == tmp_path / "tomography_1_2_30.tiff"
    context.post_transfer(tmp_path / "tomography_1_2_-30.tiff", role="detector")
    assert context._tilt_series == {
        "1": [
            tmp_path / "tomography_1_2_30.tiff",
            tmp_path / "tomography_1_2_-30.tiff",
        ]
    }
    assert not context._completed_tilt_series
    context.post_transfer(tmp_path / "tomography_2_2_30.tiff", role="detector")
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["1"]


def test_tomography_context_add_serialem_decimal_tilt(tmp_path):
    context = TomographyContext("serialem")
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


def test_setting_tilt_series_size_and_completion_from_mdoc_parsing(tmp_path):
    context = TomographyContext("tomo")
    assert len(context._tilt_series_sizes) == 0
    context.post_transfer(
        Path(__file__).parent.parent / "util" / "test_1.mdoc", role="detector"
    )
    assert len(context._tilt_series_sizes) == 1
    assert context._tilt_series_sizes == {"test_1": 11}
    (tmp_path / "test_1.mdoc").touch()
    tilt = -50
    context.post_transfer(
        tmp_path / f"test_1_[{tilt:.1f}]_fractions.tiff", role="detector"
    )
    assert context._tilt_series == {
        "test_1": [tmp_path / f"test_1_[{tilt:.1f}]_fractions.tiff"]
    }
    for t in range(-40, 60, 10):
        assert not context._completed_tilt_series
        context.post_transfer(
            tmp_path / f"test_1_[{t:.1f}]_fractions.tiff", role="detector"
        )
    assert len(context._tilt_series["test_1"]) == 11
    assert context._completed_tilt_series == ["test_1"]
