from unittest import mock
from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from murfey.workflows.register_atlas_update import run


def test_run(
    mocker: MockerFixture,
):
    # Set up mocks and the dummy message to be registered
    mock_transport_object = mocker.patch(
        "murfey.workflows.register_atlas_update._transport_object"
    )
    mock_murfey_db = MagicMock()
    message = {
        "register": "atlas_update",
        "atlas_id": mock.sentinel,
        "atlas": mock.sentinel,
        "atlas_pixel_size": mock.sentinel,
        "sample": mock.sentinel,
    }

    # Run the function and check the results and calls made
    result = run(message, mock_murfey_db)
    mock_transport_object.do_update_atlas.assert_called_once_with(
        message["atlas_id"],
        message["atlas"],
        message["atlas_pixel_size"],
        message["sample"],
    )
    assert result == {"success": True}
