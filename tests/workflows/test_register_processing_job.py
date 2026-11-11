from unittest.mock import MagicMock
from urllib.parse import urlparse

import pytest
from pytest_mock import MockerFixture

from murfey.util.api import url_path_for
from murfey.workflows.register_processing_job import run
from tests.conftest import ExampleVisit

visit_name = f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}"
instrument_name = ExampleVisit.instrument_name


@pytest.mark.parametrize(
    "test_params",
    [
        # ISPyB session present | DC search result | PJ search result | APP search result | Insert ISPyB job | Update processing status
        (v0, v1, v2, v3, v4, v5)
        for v0 in (1, None)
        for v1 in (1, None)
        for v2 in (1, None)
        for v3 in (1, None)
        for v4 in (1, None)
        for v5 in (1, None)
    ],
)
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
    mock_transport_object = mocker.patch("murfey.server._transport_object")
    mock_transport_object.do_create_ispyb_job.return_value = {
        "return_value": insert_job
    }
    mock_transport_object.do_update_processing_status.return_value = {
        "return_value": update_status
    }

    # ISPyB session
    mocker.patch(
        "murfey.workflows.register_processing_job.ISPyBSession",
        return_value=ispyb_session,
    )

    # Mock the Murfey databse session call
    mock_murfey_db = MagicMock()
    mock_murfey_session = MagicMock(
        id=1, visit=visit_name, instrument_name=instrument_name
    )
    mock_murfey_db.exec.return_value.one.return_value = mock_murfey_session

    # Mock the Murfey database group call
    mock_murfey_dc = MagicMock(id=dc_result)
    mock_murfey_pj = MagicMock(id=pj_result)
    mock_murfey_app = MagicMock(id=app_result)

    # Set up side effects depending on route taken through the function
    db_call_order = [[[mock_murfey_dc]] if dc_result is not None else []]
    if dc_result is not None:
        db_call_order.append([mock_murfey_pj] if pj_result is not None else [])
    if pj_result is not None or insert_job is not None or ispyb_session is None:
        db_call_order.append([mock_murfey_app] if app_result is not None else [])
    mock_murfey_db.exec.return_value.all.side_effect = db_call_order

    # Mock the machine config
    murfey_url = "https://murfey.com"
    mock_machine_config = MagicMock(murfey_url=murfey_url)
    mocker.patch(
        "murfey.workflows.register_processing_job.get_machine_config",
        return_value={instrument_name: mock_machine_config},
    )
    # Mock the POST request
    mock_response = MagicMock(status_code=200)
    mock_post = mocker.patch(
        "murfey.workflows.register_processing_job.requests.post",
        return_value=mock_response,
    )

    # Run function and check results and calls
    message = {
        "session_id": 1,
        "source": "some_path",
        "tag": "some_tag",
        "recipe": "some_recipe",
        "parameters": {},
        "job_parameters": {"dummy": "dummy"},
    }
    result = run(message=message, murfey_db=mock_murfey_db)
    if dc_result is not None:
        if pj_result is not None:
            url_path = url_path_for(
                "prometheus.feedback_router",
                "increment_preprocessed_movies_prometheus",
                session_id=1,
            )
            murfey_url = urlparse(murfey_url).geturl()
            mock_post.assert_called_once_with(
                url=f"{murfey_url}{url_path}",
                json={
                    "instrument_name": instrument_name,
                    "visit_name": visit_name,
                    "rsync_source": message["tag"],
                },
            )
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
                assert result == {"success": True}
    else:
        assert result == {"success": False, "requeue": True}
