from unittest.mock import patch
from urllib.parse import urlparse

import numpy as np

from murfey.client.contexts.sxt import SXTContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment


def test_sxt_context_initialisation(tmp_path):
    context = SXTContext("zeiss", tmp_path, {}, "")
    assert context._acquisition_software == "zeiss"
    assert context._machine_config == {}
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
    context = SXTContext("zeiss", tmp_path, {}, "")
    return_value = context.post_transfer(
        tmp_path / "example.xrm",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )
    assert return_value
    mock_post.assert_not_called()


@patch("requests.post")
@patch("murfey.client.contexts.sxt.OleFileIO")
def test_sxt_context_txrm(mock_ole_file, mock_post, tmp_path):
    mock_post().status_code = 200
    mock_ole_file().__enter__().exists.return_value = True
    # Motor position names
    mock_ole_file().__enter__().openstream().read.return_value = (
        "\x00Val1\x00\x00Energy\x00".encode()
    )
    # Metadata encoded arrays
    mock_ole_file().__enter__().openstream().getvalue.side_effect = [
        np.array([-55, -25, 5, 35, 65], dtype=np.float32).tobytes(),  # Angles
        np.array([0.01001], dtype=np.float32).tobytes(),  # Pixel size
        np.array([1024], dtype=np.int32).tobytes(),  # Image Width
        np.array([2048], dtype=np.int32).tobytes(),  # Image Height
        np.array([1.5], dtype=np.float32).tobytes(),  # Exposure time
        np.array([1000], dtype=np.float32).tobytes(),  # Mag
        np.array([200], dtype=np.int32).tobytes(),  # Image count
        np.array([0, 519, 2, 3], dtype=np.float32).tobytes(),  # Motor Pos (energy)
    ]

    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path / "cm12345-6/grid1"],
        default_destinations={
            f"{tmp_path}/cm12345-6/grid1": f"{tmp_path}/destination/cm12345-6/grid1"
        },
        instrument_name="",
        visit="cm12345-6",
        murfey_session=1,
    )
    context = SXTContext(
        "zeiss",
        tmp_path / "cm12345-6/grid1",
        {"recipes": {"aretomo": "sxt-aretomo"}},
        "",
    )
    context.post_transfer(
        tmp_path / "cm12345-6/grid1/example.txrm",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )

    mock_ole_file.assert_any_call(str(tmp_path / "cm12345-6/grid1/example.txrm"))
    assert mock_ole_file().__enter__().exists.call_count == 10
    assert mock_ole_file().__enter__().openstream.call_count == 11  # 9 + 2 above
    mock_ole_file().__enter__().exists.assert_any_call("ReferenceData/Image")
    for field_name in [
        "ImageInfo/Angles",
        "ImageInfo/PixelSize",
        "ImageInfo/ImageWidth",
        "ImageInfo/ImageHeight",
        "ImageInfo/ExpTimes",
        "ImageInfo/XrayMagnification",
        "ImageInfo/ImagesTaken",
        "PositionInfo/AxisNames",
        "PositionInfo/MotorPositions",
    ]:
        mock_ole_file().__enter__().exists.assert_any_call(field_name)
        mock_ole_file().__enter__().openstream.assert_any_call(field_name)

    assert mock_post.call_count == 5
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/visits/cm12345-6/sessions/1/register_data_collection_group",
        json={
            "experiment_type_id": 47,
            "tag": f"{tmp_path}/cm12345-6/grid1",
        },
        headers={"Authorization": "Bearer "},
    )
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/visits/cm12345-6/sessions/1/start_data_collection",
        json={
            "experiment_type": "sxt",
            "file_extension": ".txrm",
            "acquisition_software": "zeiss",
            "image_directory": f"{tmp_path}/destination/cm12345-6/grid1",
            "data_collection_tag": "example",
            "source": f"{tmp_path}/cm12345-6/grid1",
            "tag": "example",
            "pixel_size_on_image": str(100.1 * 1e-10),
            "image_size_x": 1024,
            "image_size_y": 2048,
            "magnification": 1000,
            "energy": 519,
            "voltage": 0,
            "axis_start": -55,
            "axis_end": 65,
            "tilt_series_length": 200,
        },
        headers={"Authorization": "Bearer "},
    )
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/visits/cm12345-6/sessions/1/register_processing_job",
        json={
            "tag": "example",
            "source": f"{tmp_path}/cm12345-6/grid1",
            "recipe": "sxt-aretomo",
            "experiment_type": "sxt",
        },
        headers={"Authorization": "Bearer "},
    )
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/sxt/visits/cm12345-6/sessions/1/sxt_tilt_series",
        json={
            "tag": "example",
            "source": f"{tmp_path}/cm12345-6/grid1",
            "pixel_size": 100.1,
            "tilt_offset": 5,
            "tilt_series_length": 200,
            "txrm": str(tmp_path / "destination/cm12345-6/grid1/example.txrm"),
            "xrm_reference": None,
        },
        headers={"Authorization": "Bearer "},
    )


@patch("requests.post")
@patch("murfey.client.contexts.sxt.OleFileIO")
def test_sxt_context_txrm_external_ref(mock_ole_file, mock_post, tmp_path):
    mock_post().status_code = 200
    exists_return = [False]  # False for reference, then True
    exists_return.extend([True for i in range(20)])
    mock_ole_file().__enter__().exists.side_effect = exists_return
    # Motor position names
    mock_ole_file().__enter__().openstream().read.return_value = (
        "\x00Val1\x00\x00Energy\x00".encode()
    )
    # Metadata encoded arrays
    mock_ole_file().__enter__().openstream().getvalue.side_effect = [
        np.array([-55, -25, 5, 35, 65], dtype=np.float32).tobytes(),  # Angles
        np.array([0.01001], dtype=np.float32).tobytes(),  # Pixel size
        np.array([1024], dtype=np.int32).tobytes(),  # Image Width
        np.array([2048], dtype=np.int32).tobytes(),  # Image Height
        np.array([1.5], dtype=np.float32).tobytes(),  # Exposure time
        np.array([1000], dtype=np.float32).tobytes(),  # Mag
        np.array([200], dtype=np.int32).tobytes(),  # Image count
        np.array([0, 519, 2, 3], dtype=np.float32).tobytes(),  # Motor Pos (energy)
        np.array([0], dtype=np.int32).tobytes(),  # Mosaic size
        np.array([0], dtype=np.int32).tobytes(),  # Mosaic size
    ]

    # xrm file as reference
    (tmp_path / "cm12345-6/grid1").mkdir(parents=True)
    (tmp_path / "cm12345-6/grid1/ref.xrm").touch()

    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path / "cm12345-6/grid1"],
        default_destinations={
            f"{tmp_path}/cm12345-6/grid1": f"{tmp_path}/destination/cm12345-6/grid1"
        },
        instrument_name="",
        visit="cm12345-6",
        murfey_session=1,
    )
    context = SXTContext(
        "zeiss",
        tmp_path / "cm12345-6/grid1",
        {"recipes": {"aretomo": "sxt-aretomo", "imod": "sxt-imod-patch-wbp"}},
        "",
    )
    context.post_transfer(
        tmp_path / "cm12345-6/grid1/example_-60to60@0.5.txrm",
        required_position_files=[],
        required_strings=["fractions"],
        environment=env,
    )

    mock_ole_file.assert_any_call(
        str(tmp_path / "cm12345-6/grid1/example_-60to60@0.5.txrm")
    )
    mock_ole_file.assert_any_call(str(tmp_path / "cm12345-6/grid1/ref.xrm"))

    assert mock_post.call_count == 6
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/visits/cm12345-6/sessions/1/register_data_collection_group",
        json={
            "experiment_type_id": 47,
            "tag": f"{tmp_path}/cm12345-6/grid1",
        },
        headers={"Authorization": "Bearer "},
    )
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/visits/cm12345-6/sessions/1/start_data_collection",
        json={
            "experiment_type": "sxt",
            "file_extension": ".txrm",
            "acquisition_software": "zeiss",
            "image_directory": f"{tmp_path}/destination/cm12345-6/grid1",
            "data_collection_tag": "example",
            "source": f"{tmp_path}/cm12345-6/grid1",
            "tag": "example",
            "pixel_size_on_image": str(100.1 * 1e-10),
            "image_size_x": 1024,
            "image_size_y": 2048,
            "magnification": 1000,
            "energy": 519,
            "voltage": 0,
            "axis_start": -55,
            "axis_end": 65,
            "tilt_series_length": 200,
        },
        headers={"Authorization": "Bearer "},
    )
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/visits/cm12345-6/sessions/1/register_processing_job",
        json={
            "tag": "example",
            "source": f"{tmp_path}/cm12345-6/grid1",
            "recipe": "sxt-aretomo",
            "experiment_type": "sxt",
        },
        headers={"Authorization": "Bearer "},
    )
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/visits/cm12345-6/sessions/1/register_processing_job",
        json={
            "tag": "example",
            "source": f"{tmp_path}/cm12345-6/grid1",
            "recipe": "sxt-imod-patch-wbp",
            "experiment_type": "sxt",
        },
        headers={"Authorization": "Bearer "},
    )
    mock_post.assert_any_call(
        "http://localhost:8000/workflow/sxt/visits/cm12345-6/sessions/1/sxt_tilt_series",
        json={
            "tag": "example",
            "source": f"{tmp_path}/cm12345-6/grid1",
            "pixel_size": 100.1,
            "tilt_offset": 5,
            "tilt_series_length": 200,
            "txrm": str(
                tmp_path / "destination/cm12345-6/grid1/example_-60to60@0.5.txrm"
            ),
            "xrm_reference": str(tmp_path / "destination/cm12345-6/grid1/ref.xrm"),
        },
        headers={"Authorization": "Bearer "},
    )
