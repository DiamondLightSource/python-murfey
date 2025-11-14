from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.workflows.register_data_collection_group import run
from tests.conftest import ExampleVisit

register_data_collection_group_params_matrix = (
    # ISPyB session ID | # DCG search result | # DCG insert result | # Atlas insert result
    (0, 0, 0, 0),
    (0, 0, 0, None),
    (0, 0, None, 0),
    (0, 0, None, None),
    (0, None, 0, 0),
    (0, None, 0, None),
    (0, None, None, 0),
    (0, None, None, None),
    (None, 0, 0, 0),
    (None, 0, 0, None),
    (None, 0, None, 0),
    (None, 0, None, None),
    (None, None, 0, 0),
    (None, None, 0, None),
    (None, None, None, 0),
    (None, None, None, None),
)


@pytest.mark.parametrize("test_params", register_data_collection_group_params_matrix)
def test_run(
    mocker: MockerFixture,
    test_params: tuple[int | None, int | None, int | None, int | None],
):
    # Unpack test params
    (ispyb_session_id, dcg_result, insert_dcg, insert_atlas) = test_params

    # Mock the transport object functions
    mock_transport_object = mocker.patch(
        "murfey.workflows.register_data_collection_group._transport_object"
    )
    mock_transport_object.do_insert_data_collection_group.return_value = {
        "return_value": insert_dcg,
    }
    mock_transport_object.do_insert_atlas.return_value = {"return_value": insert_atlas}

    # Mock the 'get_session_id' return value
    mock_get_session_id = mocker.patch(
        "murfey.workflows.register_data_collection_group.get_session_id"
    )
    mock_get_session_id.return_value = ispyb_session_id

    # Mock the Murfey database
    mock_murfey_db = MagicMock()
    mock_dcg = MagicMock()
    mock_dcg.id = dcg_result
    mock_murfey_db.exec.return_value.all.return_value = (
        [mock_dcg] if dcg_result is not None else []
    )

    # Run the function and check the results and calls
    message = {
        "microscope": "test",
        "proposal_code": ExampleVisit.proposal_code,
        "proposal_number": ExampleVisit.proposal_number,
        "visit_number": ExampleVisit.visit_number,
        "session_id": ExampleVisit.murfey_session_id,
        "tag": "some_text",
        "experiment_type_id": 0,
        "atlas": "some_file",
        "atlas_pixel_size": 1e-9,
        "sample": 0,
    }
    result = run(message=message, murfey_db=mock_murfey_db)
    if dcg_result is not None:
        assert result == {"success": True}
    else:
        if ispyb_session_id is not None:
            mock_transport_object.do_insert_data_collection_group.assert_called_once()
            mock_transport_object.do_insert_atlas.assert_called_once()
            if insert_dcg is not None:
                assert result == {"success": True}
            else:
                assert result == {"success": False, "requeue": True}
        else:
            assert result == {"success": False, "requeue": True}
