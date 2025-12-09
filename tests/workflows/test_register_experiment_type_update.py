from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.workflows.register_experiment_type_update import run

register_experiment_type_update_matrix = (0, 1, None)


@pytest.mark.parametrize("insert_dcg", register_experiment_type_update_matrix)
def test_run(
    mocker: MockerFixture,
    insert_dcg: int | None,
):
    # Mock the transport object functions
    mock_transport_object = mocker.patch(
        "murfey.workflows.register_experiment_type_update._transport_object"
    )
    mock_transport_object.do_update_data_collection_group.return_value = {
        "return_value": insert_dcg,
    }
    mock_ispyb = mocker.patch(
        "murfey.workflows.register_experiment_type_update.ISPyBDB"
    )
    mock_ispyb.DataCollectionGroup.return_value = "ispyb_dcg"

    # Mock the Murfey database
    mock_murfey_db = MagicMock()

    # Run the function and check the results and calls
    message = {
        "dcgid": 1,
        "experiment_type_id": 0,
    }
    result = run(message=message, murfey_db=mock_murfey_db)
    mock_ispyb.DataCollectionGroup.assert_called_once_with(
        dataCollectionGroupId=1, experimentTypeId=0
    )
    mock_transport_object.do_update_data_collection_group.assert_called_once_with(
        "ispyb_dcg"
    )
    if insert_dcg is not None:
        assert result == {"success": True}
    else:
        assert result == {"success": False, "requeue": True}
