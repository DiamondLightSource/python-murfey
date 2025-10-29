from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.workflows.register_processing_job import run

register_processing_job_params_matrix = [
    # ISPyB session present | DC search result | PJ search result | APP search result | Insert ISPyB job | Update processing status
    (v0, v1, v2, v3, v4, v5)
    for v0 in (0, None)
    for v1 in (0, None)
    for v2 in (0, None)
    for v3 in (0, None)
    for v4 in (0, None)
    for v5 in (0, None)
]


@pytest.mark.parametrize("test_params", register_processing_job_params_matrix)
def test_run(
    mocker: MockerFixture,
    test_params: tuple[
        int | None, int | None, int | None, int | None, int | None, int | None
    ],
):
    # Unpack test params
    ispyb_session, dc_result, pj_result, app_result, insert_job, update_status = (
        test_params
    )

    # Create mocks
    # Transport object functions
    mock_transport_object = mocker.patch(
        "murfey.workflows.register_processing_job._transport_object"
    )
    mock_transport_object.do_create_ispyb_job.return_value = {
        "return_value": insert_job
    }
    mock_transport_object.do_update_processing_status.return_value = {
        "return_value": update_status
    }

    # ISPyB session
    mock_ispyb_session = mocker.patch(
        "murfey.workflows.register_processing_job.ISPyBSession"
    )
    mock_ispyb_session.return_value = ispyb_session

    # Murfey database
    mock_murfey_dc = MagicMock()
    mock_murfey_dc.id = dc_result
    mock_murfey_pj = MagicMock()
    mock_murfey_pj.id = pj_result
    mock_murfey_app = MagicMock()
    mock_murfey_app.id = app_result

    # Set up side effects depending on route taken through the function
    db_call_order = [[[mock_murfey_dc]] if dc_result is not None else []]
    if dc_result is not None:
        db_call_order.append([mock_murfey_pj] if pj_result is not None else [])
    if pj_result is not None or insert_job is not None or ispyb_session is None:
        db_call_order.append([mock_murfey_app] if app_result is not None else [])
    mock_murfey_db = MagicMock()
    mock_murfey_db.exec.return_value.all.side_effect = db_call_order

    # Mock Prometheus object
    mock_prom = mocker.patch("murfey.workflows.register_processing_job.prom")

    # Run function and check results and calls
    message = {
        "session_id": 0,
        "source": "some_path",
        "tag": "some_tag",
        "recipe": "some_recipe",
        "parameters": {},
        "job_parameters": {"dummy": "dummy"},
    }
    result = run(message=message, murfey_db=mock_murfey_db)
    if dc_result is not None:
        if pj_result is not None:
            mock_prom.preprocessed_movies.labels.assert_called_once()
            if app_result is not None:
                assert {"success": True}
            else:
                if update_status is not None:
                    assert result == {"success": True}
                else:
                    if ispyb_session is not None:
                        assert result == {"success": False, "requeue": True}
                    else:
                        assert result == {"success": True}
        else:
            if ispyb_session is not None:
                mock_transport_object.do_create_ispyb_job.assert_called_once()
                if insert_job is not None:
                    if app_result is not None:
                        assert result == {"success": True}
                    else:
                        if update_status is not None:
                            assert result == {"success": True}
                        else:
                            assert result == {"success": False, "requeue": True}
                else:
                    assert result == {"success": False, "requeue": True}
            else:
                assert result == {"success": False, "requeue": True}
    else:
        assert result == {"success": False, "requeue": True}
