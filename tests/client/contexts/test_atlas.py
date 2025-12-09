from unittest.mock import patch
from urllib.parse import urlparse

from murfey.client.contexts.atlas import AtlasContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment


def test_atlas_context_initialisation(tmp_path):
    context = AtlasContext("tomo", tmp_path, "token")
    assert context.name == "Atlas"
    assert context._acquisition_software == "tomo"
    assert context._basepath == tmp_path
    assert context._token == "token"


@patch("murfey.client.contexts.atlas.capture_post")
def test_atlas_context_mrc(mock_capture_post, tmp_path):
    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path / "cm12345-6"],
        default_destinations={
            tmp_path / "cm12345-6": f"{tmp_path}/destination/cm12345-6"
        },
        instrument_name="",
        visit="cm12345-6",
        murfey_session=1,
    )
    context = AtlasContext("tomo", tmp_path, "token")

    atlas_mrc = tmp_path / "cm12345-6/Supervisor_atlas/Sample2/Atlas/Atlas_1.mrc"
    atlas_mrc.parent.mkdir(parents=True)
    atlas_mrc.touch()

    context.post_transfer(
        atlas_mrc,
        environment=env,
    )
    mock_capture_post.assert_called_once_with(
        base_url="http://localhost:8000",
        router_name="session_control.spa_router",
        function_name="make_atlas_jpg",
        token="token",
        session_id=1,
        data={"path": f"{tmp_path}/destination/{atlas_mrc.relative_to(tmp_path)}"},
    )


@patch("murfey.client.contexts.atlas.capture_post")
def test_atlas_context_xml(mock_capture_post, tmp_path):
    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path / "cm12345-6"],
        default_destinations={
            tmp_path / "cm12345-6": f"{tmp_path}/destination/cm12345-6"
        },
        instrument_name="",
        visit="cm12345-6",
        murfey_session=1,
    )
    context = AtlasContext("tomo", tmp_path, "token")

    atlas_pixel_size = 4.6
    atlas_xml = tmp_path / "cm12345-6/Supervisor_atlas/Sample2/Atlas/Atlas_1.xml"
    atlas_xml.parent.mkdir(parents=True)
    with open(atlas_xml, "w") as new_xml:
        new_xml.write(
            f"<MicroscopeImage><SpatialScale><pixelSize><x><numericValue>{atlas_pixel_size}"
            "</numericValue></x></pixelSize></SpatialScale></MicroscopeImage>"
        )

    context.post_transfer(
        atlas_xml,
        environment=env,
    )
    dcg_data = {
        "experiment_type_id": 44,  # Atlas
        "tag": str(atlas_xml.parent),
        "atlas": f"{tmp_path}/destination/{atlas_xml.relative_to(tmp_path).with_suffix('.jpg')}",
        "sample": 2,
        "atlas_pixel_size": atlas_pixel_size * 7.8,
    }
    mock_capture_post.assert_called_once_with(
        base_url="http://localhost:8000",
        router_name="workflow.router",
        function_name="register_dc_group",
        token="token",
        visit_name="cm12345-6",
        session_id=1,
        data=dcg_data,
    )
