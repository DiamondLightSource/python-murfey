from unittest.mock import patch
from urllib.parse import urlparse

from murfey.client.contexts.atlas import AtlasContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment


def test_atlas_context_initialisation(tmp_path):
    context = AtlasContext("tomo", tmp_path, {}, "token")
    assert context.name == "Atlas"
    assert context._acquisition_software == "tomo"
    assert context._basepath == tmp_path
    assert context._machine_config == {}
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
        instrument_name="m01",
        visit="cm12345-6",
        murfey_session=1,
    )
    context = AtlasContext("tomo", tmp_path, {}, "token")

    atlas_mrc = tmp_path / "cm12345-6/Supervisor_atlas/Sample2/Atlas/Atlas_1.mrc"
    atlas_mrc.parent.mkdir(parents=True)
    atlas_mrc.touch()

    context.post_transfer(atlas_mrc, environment=env)
    mock_capture_post.assert_called_once_with(
        base_url="http://localhost:8000",
        router_name="session_control.spa_router",
        function_name="make_atlas_jpg",
        token="token",
        instrument_name="m01",
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
        instrument_name="m01",
        visit="cm12345-6",
        murfey_session=1,
    )
    context = AtlasContext("tomo", tmp_path, {}, "token")

    atlas_pixel_size = 4.6
    atlas_xml = tmp_path / "cm12345-6/Supervisor_atlas/Sample2/Atlas/Atlas_1.xml"
    atlas_xml.parent.mkdir(parents=True)
    with open(atlas_xml, "w") as new_xml:
        new_xml.write(
            f"<MicroscopeImage><SpatialScale><pixelSize><x><numericValue>{atlas_pixel_size}"
            "</numericValue></x></pixelSize></SpatialScale></MicroscopeImage>"
        )

    context.post_transfer(atlas_xml, environment=env)
    dcg_data = {
        "experiment_type_id": 44,  # Atlas
        "tag": str(atlas_xml.parent),
        "atlas": f"{tmp_path}/destination/{atlas_xml.relative_to(tmp_path).with_suffix('.jpg')}",
        "sample": 2,
        "atlas_pixel_size": atlas_pixel_size * 7.8,
        "create_smartem_grid": False,
        "acquisition_uuid": None,
    }
    mock_capture_post.assert_called_once_with(
        base_url="http://localhost:8000",
        router_name="workflow.router",
        function_name="register_dc_group",
        token="token",
        instrument_name="m01",
        visit_name="cm12345-6",
        session_id=1,
        data=dcg_data,
    )


@patch("murfey.client.contexts.atlas.capture_post")
def test_atlas_context_dm(mock_capture_post, tmp_path):
    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path / "cm12345-6"],
        default_destinations={
            tmp_path / "cm12345-6": f"{tmp_path}/destination/cm12345-6"
        },
        instrument_name="m01",
        visit="cm12345-6",
        murfey_session=1,
        acquisition_uuid="uuid1",
    )

    # Write sample dm file
    atlas_dm = tmp_path / "cm12345-6/Supervisor_atlas/Sample2/Atlas/Atlas.dm"
    atlas_dm.parent.mkdir(parents=True)
    grid_square_values = (
        "<value><b:PositionOnTheAtlas>"
        "<c:Center><d:x>1200</d:x><d:y>1500</d:y></c:Center>"
        "<c:Physical><d:x>2</d:x><d:y>3</d:y></c:Physical>"
        "<c:Size><d:width>130</d:width><d:height>560</d:height></c:Size>"
        "<c:Rotation>0.14</c:Rotation>"
        "</b:PositionOnTheAtlas></value>"
    )
    with open(atlas_dm, "w") as new_xml:
        new_xml.write(
            "<AtlasSessionXml><Atlas><TilesEfficient><_items>"
            # First tile with two grid squares
            "<TileXml><Nodes><KeyValuePairs><KeyValuePairOfintNodeXml1>"
            f"<key>101</key>{grid_square_values}"
            "</KeyValuePairOfintNodeXml1><KeyValuePairOfintNodeXml1>"
            f"<key>102</key>{grid_square_values}"
            "</KeyValuePairOfintNodeXml1></KeyValuePairs></Nodes></TileXml>"
            # Second tile with two grid squares
            "<TileXml><Nodes><KeyValuePairs><KeyValuePairOfintNodeXml1>"
            f"<key>103</key>{grid_square_values}"
            "</KeyValuePairOfintNodeXml1><KeyValuePairOfintNodeXml1>"
            f"<key>104</key>{grid_square_values}"
            "</KeyValuePairOfintNodeXml1></KeyValuePairs></Nodes></TileXml>"
            # Close all
            "</_items></TilesEfficient></Atlas></AtlasSessionXml>"
        )

    context = AtlasContext("tomo", tmp_path, {}, "token")
    context.post_transfer(atlas_dm, environment=env)

    assert mock_capture_post.call_count == 5
    mock_capture_post.assert_any_call(
        base_url="http://localhost:8000",
        router_name="session_control.spa_router",
        function_name="register_grid_square",
        token="token",
        instrument_name="m01",
        session_id=1,
        gsid=101,
        data={
            "tag": str(atlas_dm.parent),
            "x_location": 1200,
            "y_location": 1500,
            "x_stage_position": 2e9,
            "y_stage_position": 3e9,
            "width": 130,
            "height": 560,
            "angle": 0.14,
        },
    )
    mock_capture_post.assert_any_call(
        base_url="http://localhost:8000",
        router_name="session_control.spa_router",
        function_name="register_atlas",
        token="token",
        instrument_name="m01",
        session_id=1,
        data={
            "name": "cm12345-6-sample-2",
            "acquisition_uuid": "uuid1",
            "register_grid": True,
            "tag": str(atlas_dm.parent),
        },
    )
