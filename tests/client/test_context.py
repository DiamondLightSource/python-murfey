from unittest.mock import patch
from urllib.parse import urlparse

from murfey.client.context import ensure_dcg_exists
from murfey.client.instance_environment import MurfeyInstanceEnvironment


@patch("murfey.client.context.capture_post")
def test_ensure_dcg_exists_tomo(mock_capture_post, tmp_path):
    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path / "cm12345-6/metadata_folder"],
        default_destinations={
            tmp_path
            / "cm12345-6/metadata_folder": f"{tmp_path}/destination/cm12345-6/raw"
        },
        instrument_name="",
        visit="cm12345-6",
        murfey_session=1,
    )

    metadata_source = tmp_path / "cm12345-6/metadata_folder"
    metadata_source.mkdir(parents=True)
    with open(metadata_source / "Session.dm", "w") as dm_file:
        dm_file.write(
            "<TomographySession><AtlasId>"
            r"X:\cm12345-6\atlas\atlas_metadata\Sample6\Atlas\Atlas.dm"
            "</AtlasId></TomographySession>"
        )

    atlas_xml = tmp_path / "cm12345-6/atlas/atlas_metadata/Sample6/Atlas/Atlas_4.xml"
    atlas_xml.parent.mkdir(parents=True)
    with open(atlas_xml, "w") as xml_file:
        xml_file.write(
            "<MicroscopeImage><SpatialScale><pixelSize><x><numericValue>4.7"
            "</numericValue></x></pixelSize></SpatialScale></MicroscopeImage>"
        )

    ensure_dcg_exists(
        collection_type="tomo",
        metadata_source=metadata_source,
        environment=env,
        token="token",
    )

    dcg_data = {
        "experiment_type_id": 36,
        "tag": f"{tmp_path}/metadata_folder",
        "atlas": f"{tmp_path}/destination/{atlas_xml.relative_to(tmp_path).with_suffix('.jpg')}",
        "sample": 6,
        "atlas_pixel_size": 4.7 * 7.8,
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


@patch("murfey.client.context.capture_post")
def test_ensure_dcg_exists_spa(mock_capture_post, tmp_path):
    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path / "cm12345-6/metadata_folder"],
        default_destinations={
            tmp_path
            / "cm12345-6/metadata_folder": f"{tmp_path}/destination/cm12345-6/raw",
        },
        instrument_name="",
        visit="cm12345-6",
        murfey_session=1,
    )

    metadata_source = tmp_path / "cm12345-6/metadata_folder"
    metadata_source.mkdir(parents=True)
    with open(metadata_source / "EpuSession.dm", "w") as dm_file:
        dm_file.write(
            "<EpuSessionXml><Samples><_items><SampleXml><AtlasId z:Id='10'>"
            r"X:\cm12345-6\atlas\atlas_metadata\Sample6\Atlas\Atlas.dm"
            "</AtlasId></SampleXml><SampleXml></SampleXml></_items></Samples></EpuSessionXml>"
        )

    # Make data location
    (tmp_path / "metadata_folder/Images-Disc1").mkdir(parents=True)

    atlas_xml = tmp_path / "cm12345-6/atlas/atlas_metadata/Sample6/Atlas/Atlas_4.xml"
    atlas_xml.parent.mkdir(parents=True)
    with open(atlas_xml, "w") as xml_file:
        xml_file.write(
            "<MicroscopeImage><SpatialScale><pixelSize><x><numericValue>4.7"
            "</numericValue></x></pixelSize></SpatialScale></MicroscopeImage>"
        )

    ensure_dcg_exists(
        collection_type="spa",
        metadata_source=metadata_source,
        environment=env,
        token="token",
    )

    dcg_data = {
        "experiment_type_id": 37,
        "tag": f"{tmp_path}/metadata_folder/Images-Disc1",
        "atlas": f"{tmp_path}/destination/{atlas_xml.relative_to(tmp_path).with_suffix('.jpg')}",
        "sample": 6,
        "atlas_pixel_size": 4.7 * 7.8,
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


@patch("murfey.client.context.capture_post")
def test_ensure_dcg_exists_spa_missing_xml(mock_capture_post, tmp_path):
    env = MurfeyInstanceEnvironment(
        url=urlparse("http://localhost:8000"),
        client_id=0,
        sources=[tmp_path],
        default_destinations={tmp_path: str(tmp_path)},
        instrument_name="",
        visit="cm12345-6",
        murfey_session=1,
    )

    metadata_source = tmp_path / "cm12345-6/metadata_folder"
    ensure_dcg_exists(
        collection_type="spa",
        metadata_source=metadata_source,
        environment=env,
        token="token",
    )

    dcg_data = {
        "experiment_type_id": 37,
        "tag": f"{tmp_path}/metadata_folder",
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
