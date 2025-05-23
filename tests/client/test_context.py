from __future__ import annotations

from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlparse

from murfey.client.contexts.tomo import TomographyContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment


def test_tomography_context_initialisation_for_tomo(tmp_path):
    context = TomographyContext("tomo", tmp_path)
    assert not context._completed_tilt_series
    assert context._acquisition_software == "tomo"


@patch("requests.get")
@patch("requests.post")
def test_tomography_context_add_tomo_tilt(mock_post, mock_get, tmp_path):
    mock_post().status_code = 200

    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path],
        default_destinations={tmp_path: str(tmp_path)},
        instrument_name="",
        visit="test",
    )
    context = TomographyContext("tomo", tmp_path)
    (tmp_path / "Position_1_001_[30.0]_date_time_fractions.tiff").touch()
    context.post_transfer(
        tmp_path / "Position_1_001_[30.0]_date_time_fractions.tiff",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert context._tilt_series == {
        "Position_1": [tmp_path / "Position_1_001_[30.0]_date_time_fractions.tiff"]
    }
    (tmp_path / "Position_1_002_[-30.0]_date_time_fractions.tiff").touch()
    context.post_transfer(
        tmp_path / "Position_1_002_[-30.0]_date_time_fractions.tiff",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert not context._completed_tilt_series

    # Add Position_1.mdoc, which completes this position
    with open(tmp_path / "Position_1.mdoc", "w") as mdoc:
        mdoc.write("[ZValue = 0]\n[ZValue = 1]\n")
    context.post_transfer(
        tmp_path / "Position_1.mdoc",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert context._completed_tilt_series == ["Position_1"]

    # Start Position_2, this is not complete
    (tmp_path / "Position_2_002_[30.0]_date_time_fractions.tiff").touch()
    context.post_transfer(
        tmp_path / "Position_2_002_[30.0]_date_time_fractions.tiff",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert len(context._tilt_series.values()) == 2
    assert context._completed_tilt_series == ["Position_1"]


@patch("requests.get")
@patch("requests.post")
def test_tomography_context_add_tomo_tilt_out_of_order(mock_post, mock_get, tmp_path):
    mock_post().status_code = 200

    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path],
        default_destinations={tmp_path: str(tmp_path)},
        instrument_name="",
        visit="test",
    )
    context = TomographyContext("tomo", tmp_path)
    (tmp_path / "Position_1_001_[30.0]_date_time_fractions.tiff").touch()
    context.post_transfer(
        tmp_path / "Position_1_001_[30.0]_date_time_fractions.tiff",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert context._tilt_series == {
        "Position_1": [tmp_path / "Position_1_001_[30.0]_date_time_fractions.tiff"]
    }
    (tmp_path / "Position_1_002_[-30.0]_date_time_fractions.tiff").touch()
    context.post_transfer(
        tmp_path / "Position_1_002_[-30.0]_date_time_fractions.tiff",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert not context._completed_tilt_series
    (tmp_path / "Position_2_002_[-30.0]_date_time_fractions.tiff").touch()
    context.post_transfer(
        tmp_path / "Position_2_002_[-30.0]_date_time_fractions.tiff",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert len(context._tilt_series.values()) == 2
    assert not context._completed_tilt_series
    (tmp_path / "Position_2_001_[30.0]_date_time_fractions.tiff").touch()
    context.post_transfer(
        tmp_path / "Position_2_001_[30.0]_date_time_fractions.tiff",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert len(context._tilt_series.values()) == 2
    assert not context._completed_tilt_series
    (tmp_path / "Position_3_001_[30.0]_date_time_fractions.tiff").touch()
    (tmp_path / "Position_3_002_[-30.0]_date_time_fractions.tiff").touch()
    context.post_transfer(
        tmp_path / "Position_3_002_[-30.0]_date_time_fractions.tiff",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert len(context._tilt_series.values()) == 3
    assert not context._completed_tilt_series

    # Add Position_1.mdoc, which completes this position
    with open(tmp_path / "Position_1.mdoc", "w") as mdoc:
        mdoc.write("[ZValue = 0]\n[ZValue = 1]\n")
    context.post_transfer(
        tmp_path / "Position_1.mdoc",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert context._completed_tilt_series == ["Position_1"]

    # Add Position_2.mdoc, which completes this position
    with open(tmp_path / "Position_2.mdoc", "w") as mdoc:
        mdoc.write("[ZValue = 0]\n[ZValue = 1]\n")
    context.post_transfer(
        tmp_path / "Position_2.mdoc",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert context._completed_tilt_series == ["Position_1", "Position_2"]


@patch("requests.get")
@patch("requests.post")
def test_tomography_context_add_tomo_tilt_delayed_tilt(mock_post, mock_get, tmp_path):
    mock_post().status_code = 200

    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path],
        default_destinations={tmp_path: str(tmp_path)},
        instrument_name="",
        visit="test",
    )
    context = TomographyContext("tomo", tmp_path)
    (tmp_path / "Position_1_001_[30.0]_date_time_fractions.tiff").touch()
    context.post_transfer(
        tmp_path / "Position_1_001_[30.0]_date_time_fractions.tiff",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert context._tilt_series == {
        "Position_1": [tmp_path / "Position_1_001_[30.0]_date_time_fractions.tiff"]
    }
    (tmp_path / "Position_1_002_[-30.0]_date_time_fractions.tiff").touch()
    context.post_transfer(
        tmp_path / "Position_1_002_[-30.0]_date_time_fractions.tiff",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert not context._completed_tilt_series

    # Add Position_1.mdoc, with more tilts than have been seen so far
    with open(tmp_path / "Position_1.mdoc", "w") as mdoc:
        mdoc.write("[ZValue = 0]\n[ZValue = 1]\n[ZValue = 2]\n")
    context.post_transfer(
        tmp_path / "Position_1.mdoc",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert not context._completed_tilt_series

    # Now add the tilt which completes the series
    (tmp_path / "Position_1_003_[60.0]_data_time_fractions.tiff").touch()
    new_series = context.post_transfer(
        tmp_path / "Position_1_003_[60.0]_data_time_fractions.tiff",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert context._completed_tilt_series == ["Position_1"]
    assert new_series == ["Position_1"]


def test_tomography_context_initialisation_for_serialem(tmp_path):
    context = TomographyContext("serialem", tmp_path)
    assert not context._completed_tilt_series
    assert context._acquisition_software == "serialem"


@patch("requests.get")
@patch("requests.post")
def test_setting_tilt_series_size_and_completion_from_mdoc_parsing(
    mock_post, mock_get, tmp_path
):
    mock_post().status_code = 200

    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path],
        default_destinations={tmp_path: str(tmp_path)},
        instrument_name="",
        visit="test",
    )
    context = TomographyContext("tomo", tmp_path)
    assert len(context._tilt_series_sizes) == 0
    context.post_transfer(
        Path(__file__).parent.parent / "util" / "test_1.mdoc",
        environment=env,
        required_strings=["fractions"],
    )
    assert len(context._tilt_series_sizes) == 1
    assert context._tilt_series_sizes == {"test_1": 11}
    (tmp_path / "test_1.mdoc").touch()
    tilt = -50
    (tmp_path / f"test_1_001_[{tilt:.1f}]_data_time_fractions.tiff").touch()
    context.post_transfer(
        tmp_path / f"test_1_001_[{tilt:.1f}]_data_time_fractions.tiff",
        environment=env,
        required_strings=["fractions"],
    )
    assert context._tilt_series == {
        "test_1": [tmp_path / f"test_1_001_[{tilt:.1f}]_data_time_fractions.tiff"]
    }
    for i, t in enumerate(range(-40, 60, 10)):
        assert not context._completed_tilt_series
        (tmp_path / f"test_1_{i:03}_[{t:.1f}]_data_time_fractions.tiff").touch()
        context.post_transfer(
            tmp_path / f"test_1_{i:03}_[{t:.1f}]_data_time_fractions.tiff",
            environment=env,
            required_strings=["fractions"],
        )
    assert len(context._tilt_series["test_1"]) == 11
    assert context._completed_tilt_series == ["test_1"]
