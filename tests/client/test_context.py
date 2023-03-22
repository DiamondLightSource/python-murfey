from __future__ import annotations

from unittest.mock import patch
from urllib.parse import urlparse

from murfey.client.context import TomographyContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment


def test_tomography_context_initialisation_for_tomo(tmp_path):
    context = TomographyContext("tomo", tmp_path)
    assert not context._last_transferred_file
    assert context._acquisition_software == "tomo"


@patch("requests.get")
def test_tomography_context_add_tomo_tilt(mock_get, tmp_path):
    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        sources=[tmp_path],
        default_destinations={tmp_path: str(tmp_path)},
    )
    context = TomographyContext("tomo", tmp_path)
    context.post_transfer(
        tmp_path / "Position_1_[30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert context._tilt_series == {"Position_1": [tmp_path / "Position_1_[30.0].tiff"]}
    assert context._last_transferred_file == tmp_path / "Position_1_[30.0].tiff"
    context.post_transfer(
        tmp_path / "Position_1_[-30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_2_[30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["Position_1"]


@patch("requests.get")
def test_tomography_context_add_tomo_tilt_out_of_order(mock_get, tmp_path):
    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        sources=[tmp_path],
        default_destinations={tmp_path: str(tmp_path)},
    )
    context = TomographyContext("tomo", tmp_path)
    context.post_transfer(
        tmp_path / "Position_1_[30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert context._tilt_series == {"Position_1": [tmp_path / "Position_1_[30.0].tiff"]}
    assert context._last_transferred_file == tmp_path / "Position_1_[30.0].tiff"
    context.post_transfer(
        tmp_path / "Position_1_[-30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_2_[-30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert len(context._tilt_series.values()) == 2
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_2_[30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert len(context._tilt_series.values()) == 2
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_3_[-30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert len(context._tilt_series.values()) == 3
    assert context._completed_tilt_series == ["Position_1", "Position_2"]
    context.post_transfer(
        tmp_path / "Position_3_[30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert context._completed_tilt_series == ["Position_1", "Position_2", "Position_3"]


@patch("requests.get")
def test_tomography_context_add_tomo_tilt_delayed_tilt(mock_get, tmp_path):
    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        sources=[tmp_path],
        default_destinations={tmp_path: str(tmp_path)},
    )
    context = TomographyContext("tomo", tmp_path)
    context.post_transfer(
        tmp_path / "Position_1_[30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert context._tilt_series == {"Position_1": [tmp_path / "Position_1_[30.0].tiff"]}
    assert context._last_transferred_file == tmp_path / "Position_1_[30.0].tiff"
    context.post_transfer(
        tmp_path / "Position_1_[-30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "Position_2_[30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["Position_1"]
    context.post_transfer(
        tmp_path / "Position_2_[-30.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    new_series = context.post_transfer(
        tmp_path / "Position_1_[60.0].tiff",
        role="detector",
        required_position_files=[],
        environment=env,
    )
    assert context._completed_tilt_series == ["Position_2", "Position_1"]
    assert new_series == ["Position_1"]


def test_tomography_context_initialisation_for_serialem(tmp_path):
    context = TomographyContext("serialem", tmp_path)
    assert not context._last_transferred_file
    assert context._acquisition_software == "serialem"


@patch("requests.get")
def test_tomography_context_add_serialem_tilt(mock_get, tmp_path):
    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        sources=[tmp_path],
        default_destinations={tmp_path: str(tmp_path)},
    )
    context = TomographyContext("serialem", tmp_path)
    context.post_transfer(
        tmp_path / "tomography_1_2_30.tiff", role="detector", environment=env
    )
    assert context._tilt_series == {"1": [tmp_path / "tomography_1_2_30.tiff"]}
    assert context._last_transferred_file == tmp_path / "tomography_1_2_30.tiff"
    context.post_transfer(
        tmp_path / "tomography_1_2_-30.tiff", role="detector", environment=env
    )
    assert context._tilt_series == {
        "1": [tmp_path / "tomography_1_2_30.tiff", tmp_path / "tomography_1_2_-30.tiff"]
    }
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "tomography_2_2_30.tiff", role="detector", environment=env
    )
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["1"]


@patch("requests.get")
def test_tomography_context_add_serialem_decimal_tilt(mock_get, tmp_path):
    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        sources=[tmp_path],
        default_destinations={tmp_path: str(tmp_path)},
    )
    context = TomographyContext("serialem", tmp_path)
    context.post_transfer(
        tmp_path / "tomography_1_2_30.0.tiff", role="detector", environment=env
    )
    assert context._tilt_series == {"1": [tmp_path / "tomography_1_2_30.0.tiff"]}
    assert context._last_transferred_file == tmp_path / "tomography_1_2_30.0.tiff"
    context.post_transfer(
        tmp_path / "tomography_1_2_-30.0.tiff", role="detector", environment=env
    )
    assert context._tilt_series == {
        "1": [
            tmp_path / "tomography_1_2_30.0.tiff",
            tmp_path / "tomography_1_2_-30.0.tiff",
        ]
    }
    assert not context._completed_tilt_series
    context.post_transfer(
        tmp_path / "tomography_2_2_30.0.tiff", role="detector", environment=env
    )
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["1"]
