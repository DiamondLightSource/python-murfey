from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.workflows.register_data_collection import run
from tests.conftest import ExampleVisit

register_data_collection_params_matrix = (
    # ISPyB session ID return value | DCG search result | DC search result | Insert data collection
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


@pytest.mark.parametrize("test_params", register_data_collection_params_matrix)
def test_run(
    mocker: MockerFixture,
    test_params: tuple[int | None, int | None, int | None, int | None],
):
    # Unpack test params
    ispyb_session_id, dcg_result, dc_result, insert_data_collection = test_params

    # Set up mock objects
    # 'get_session_id'
    mock_get_session_id = mocker.patch(
        "murfey.workflows.register_data_collection.get_session_id"
    )
    mock_get_session_id.return_value = ispyb_session_id

    # Transport object inserts
    mock_transport_object = mocker.patch(
        "murfey.workflows.register_data_collection._transport_object"
    )
    mock_transport_object.do_insert_data_collection.return_value = {
        "return_value": insert_data_collection
    }

    # Murfey database
    mock_murfey_db = MagicMock()
    mock_dcg = MagicMock()
    mock_dcg.id = dcg_result

    mock_dc = MagicMock()
    mock_dc.id = dc_result
    mock_murfey_db.exec.return_value.all.side_effect = [
        # Sequence of mock database tables
        [mock_dcg] if dcg_result is not None else [],
        [mock_dc] if dc_result is not None else [],
    ]

    # Run the function and check results and calls
    message = {
        "session_id": 0,
        "microscope": "test_instrument",
        "proposal_code": ExampleVisit.proposal_code,
        "proposal_number": ExampleVisit.proposal_number,
        "visit_number": ExampleVisit.visit_number,
        "source": "some_path",
        "image_directory": "some_path",
        "tag": "some_string",
        "experiment_type": "SPA",
        "image_suffix": ".jpg",
        "voltage": 200,
        "pixel_size": 1e-9,
        "image_size_x": 2048,
        "image_size_y": 2048,
        "slit_width": 0.005,
        "magnification": 150000,
        "exposure_time": 30,
        "total_exposed_dose": 30,
        "c2aperture": 5,
        "phase_plate": 1,
    }
    result = run(message=message, murfey_db=mock_murfey_db)
    if dcg_result is None:
        assert result == {"success": False, "requeue": True}
    else:
        if dc_result is None:
            if ispyb_session_id is not None:
                mock_transport_object.do_insert_data_collection.assert_called_once()
                if insert_data_collection is not None:
                    assert result == {"success": True}
                else:
                    assert result == {"success": False, "requeue": True}
            else:
                mock_transport_object.do_insert_data_collection.assert_not_called()
                assert result == {"success": False, "requeue": True}
        else:
            assert result == {"success": True}
