from unittest import mock
from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from murfey.util.db import Atlas
from murfey.util.models import AtlasParameters
from murfey.workflows.register_atlas_update import run


def test_run_insert(
    mocker: MockerFixture,
):
    # Set up mocks and the dummy message to be registered
    mock_transport_object = mocker.patch("murfey.server._transport_object")
    mock_transport_object.do_insert_atlas.return_value = {
        "success": True,
        "return_value": 1,
    }
    mock_murfey_db = MagicMock()
    mock_murfey_db.exec().one_or_none.return_value = None
    message = {
        "dcgid": 1,
        "register": "atlas_update",
        "session_id": 1,
        "atlas_type": "ROI",
        "atlas": "image",
        "atlas_pixel_size": 0.1,
        "sample": 2,
    }

    # Run the function and check the results and calls made
    result = run(message, mock_murfey_db)
    mock_transport_object.do_insert_atlas.assert_called_once_with(
        1,
        AtlasParameters(
            atlas_type=message["atlas_type"],
            image=message["atlas"],
            pixel_size=message["atlas_pixel_size"],
            cassette_slot=message["sample"],
        ),
    )
    mock_murfey_db.add.assert_called_once_with(
        Atlas(
            id=1,
            dcg_id=1,
            atlas_type=message["atlas_type"],
            atlas_image=message["atlas"],
            cassette_slot=message["sample"],
            pixel_size=message["atlas_pixel_size"],
        )
    )
    assert result == {"success": True}


def test_run_update(
    mocker: MockerFixture,
):
    # Set up mocks and the dummy message to be registered
    mock_transport_object = mocker.patch("murfey.server._transport_object")
    mock_transport_object.do_update_atlas.return_value = {
        "success": True,
        "return_value": 1,
    }
    mock_murfey_db = MagicMock()
    mock_murfey_db.exec().one_or_none.return_value = Atlas(
        id=1, dcg_id=1, atlas_type="ROI"
    )
    message = {
        "dcgid": 1,
        "register": "atlas_update",
        "session_id": 1,
        "atlas_type": "ROI",
        "atlas": mock.sentinel,
        "atlas_pixel_size": mock.sentinel,
        "sample": mock.sentinel,
    }

    # Run the function and check the results and calls made
    result = run(message, mock_murfey_db)
    mock_transport_object.do_update_atlas.assert_called_once_with(
        dcg_id=1,
        atlas_id=1,
        atlas_image=message["atlas"],
        pixel_size=message["atlas_pixel_size"],
        slot=message["sample"],
        collection_mode=message.get("collection_mode"),
        colour_flags=message.get("colour_flags", {}),
    )
    mock_murfey_db.add.assert_called_once_with(
        Atlas(
            id=1,
            dcg_id=1,
            atlas_type=message["atlas_type"],
            atlas_image=message["atlas"],
            cassette_slot=message["sample"],
            pixel_size=message["atlas_pixel_size"],
        )
    )
    assert result == {"success": True}
