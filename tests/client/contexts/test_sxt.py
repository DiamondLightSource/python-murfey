from unittest.mock import patch
from urllib.parse import urlparse

from murfey.client.contexts.sxt import SXTContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment


def test_sxt_context_initialisation(tmp_path):
    context = SXTContext("zeiss", tmp_path, "")
    assert context._acquisition_software == "zeiss"
    assert context._basepath == tmp_path


@patch("requests.post")
def test_sxt_context_xrm(mock_post, tmp_path):
    """Currently nothing happens with an xrm file"""
    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path],
        default_destinations={tmp_path: str(tmp_path)},
        instrument_name="",
        visit="test",
        murfey_session=1,
    )
    context = SXTContext("zeiss", tmp_path, "")
    return_value = context.post_transfer(
        tmp_path / "example.xrm",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert return_value
    mock_post.assert_not_called()


@patch("requests.post")
@patch("murfey.client.contexts.sxt.Inspector")
@patch("murfey.client.contexts.sxt.open_txrm")
@patch("murfey.client.contexts.sxt.read_stream")
def test_sxt_context_txrm(
    mock_read_stream, mock_open_txrm, mock_inspector, mock_post, tmp_path
):
    mock_post().status_code = 200
    mock_read_stream.side_effect = [
        [-55, -25, 5, 35, 65],  # Angles
        [0.01001],  # Pixel size
        [1024],  # Image Width
        [2048],  # Image Height
        [1.5],  # Exposure time
        [1000],  # Mag
        [5],  # Image count
    ]

    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path],
        default_destinations={tmp_path: str(tmp_path / "destination")},
        instrument_name="",
        visit="test",
        murfey_session=1,
    )
    context = SXTContext("zeiss", tmp_path, "")
    context.post_transfer(
        tmp_path / "example.txrm",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )

    mock_open_txrm.assert_called_once_with(
        tmp_path / "example.txrm", load_images=False, load_reference=False, strict=False
    )
    mock_inspector.assert_called_once()

    assert mock_post.call_count == 5
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/visits/test/sessions/1/register_data_collection_group",
        json={
            "experiment_type_id": 47,
            "tag": str(tmp_path),
        },
        headers={"Authorization": "Bearer "},
    )
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/visits/test/sessions/1/start_data_collection",
        json={
            "experiment_type": "sxt",
            "file_extension": ".txrm",
            "acquisition_software": "zeiss",
            "image_directory": f"{tmp_path}/destination",
            "data_collection_tag": "example",
            "source": str(tmp_path),
            "tag": "example",
            "pixel_size_on_image": "100.1",
            "image_size_x": 1024,
            "image_size_y": 2048,
            "magnification": 1000,
            "voltage": 0,
        },
        headers={"Authorization": "Bearer "},
    )
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/visits/test/sessions/1/register_processing_job",
        json={
            "tag": "example",
            "source": str(tmp_path),
            "recipe": "sxt-tomo-align",
            "experiment_type": "sxt",
        },
        headers={"Authorization": "Bearer "},
    )
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/sxt/visits/test/sessions/1/sxt_tilt_series",
        json={
            "session_id": 1,
            "tag": "example",
            "source": str(tmp_path),
            "pixel_size": 100.1,
            "tilt_offset": 5,
            "txrm": str(tmp_path / "destination/example.txrm"),
        },
        headers={"Authorization": "Bearer "},
    )
