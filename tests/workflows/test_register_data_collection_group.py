from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.util.db import DataCollectionGroup, ImagingSite
from murfey.workflows.register_data_collection_group import run
from tests.conftest import ExampleVisit


@pytest.mark.parametrize(
    "test_params",
    (
        # ISPyB session ID | # DCG search result | # DCG insert result | # Atlas insert result | # Atlas with location
        (0, 0, 0, 0, False),
        (0, 0, 0, None, False),
        (0, 0, None, 0, False),
        (0, 0, None, None, False),
        (0, None, 0, 0, True),
        (0, None, 0, None, False),
        (0, None, None, 0, False),
        (0, None, None, None, False),
        (None, 0, 0, 0, True),
        (None, 0, 0, None, False),
        (None, 0, None, 0, False),
        (None, 0, None, None, False),
        (None, None, 0, 0, False),
        (None, None, 0, None, False),
        (None, None, None, 0, False),
        (None, None, None, None, False),
    ),
)
def test_run(
    mocker: MockerFixture,
    test_params: tuple[int | None, int | None, int | None, int | None, bool],
):
    # Unpack test params
    (ispyb_session_id, dcg_result, insert_dcg, insert_atlas, atlas_location) = (
        test_params
    )

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
    if atlas_location:
        message["atlas_x_stage_position"] = 10
        message["atlas_y_stage_position"] = 20
        message["atlas_width"] = 200
        message["atlas_height"] = 400
    result = run(message=message, murfey_db=mock_murfey_db)
    if dcg_result is not None:
        assert result == {"success": True}
    else:
        if ispyb_session_id is not None:
            mock_transport_object.do_insert_data_collection_group.assert_called_once()
            if insert_dcg is not None:
                mock_transport_object.do_insert_atlas.assert_called_once()
                mock_murfey_db.add.assert_any_call(
                    DataCollectionGroup(
                        id=insert_dcg,
                        session_id=ExampleVisit.murfey_session_id,
                        tag="some_text",
                        smartem_grid_uuid=None,
                        atlas_id=insert_atlas,
                        atlas_pixel_size=1e-9,
                        atlas="some_file",
                        sample=0,
                    )
                )
                assert result == {"success": True}
            else:
                assert result == {"success": False, "requeue": True}
        else:
            mock_murfey_db.add.assert_any_call(
                DataCollectionGroup(
                    session_id=ExampleVisit.murfey_session_id,
                    tag="some_text",
                    smartem_grid_uuid=None,
                )
            )
            assert result == {"success": True}

        if atlas_location:
            mock_murfey_db.add.assert_any_call(
                ImagingSite(
                    dcg_id=insert_dcg,
                    session_id=ExampleVisit.murfey_session_id,
                    site_name="some_text",
                    data_type="atlas",
                    pos_x=10,
                    pos_y=20,
                    image_pixels_x=200,
                    image_pixels_y=400,
                    image_pixel_size=1e-9,
                )
            )
