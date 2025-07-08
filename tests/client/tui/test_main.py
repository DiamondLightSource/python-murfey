from unittest import mock
from unittest.mock import Mock
from urllib.parse import urlparse

import pytest

from murfey.client.tui.main import _get_visit_list
from murfey.util.models import Visit

test_get_visit_list_params_matrix = (
    ("http://0.0.0.0:8000",),
    ("http://0.0.0.0:8000/api",),
    ("http://murfey_server",),
    ("http://murfey_server/api",),
    ("http://murfey_server.com",),
)


@pytest.mark.parametrize("test_params", test_get_visit_list_params_matrix)
@mock.patch("murfey.client.tui.main.requests")
def test_get_visit_list(
    mock_request,
    test_params: tuple[str],
    mock_client_configuration,
):
    # Unpack test params and set up other params
    (server_url,) = test_params
    instrument_name = mock_client_configuration["Murfey"]["instrument_name"]

    # Construct the expected request response
    example_visits = [
        {
            "start": "1999-09-09T09:00:00",
            "end": "1999-09-11T09:00:00",
            "session_id": 123456789,
            "name": "cm12345-0",
            "beamline": "murfey",
            "proposal_title": "Commissioning Session 1",
        },
        {
            "start": "1999-09-09T09:00:00",
            "end": "1999-09-11T09:00:00",
            "session_id": 246913578,
            "name": "cm23456-1",
            "beamline": "murfey",
            "proposal_title": "Cryo-cycle 1999",
        },
    ]
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = example_visits
    mock_request.get.return_value = mock_response

    # read_config() has to be patched using fixture, so has to be done in function
    with mock.patch("murfey.util.client.read_config", mock_client_configuration):
        visits = _get_visit_list(urlparse(server_url), instrument_name)

    # Check that request was sent with the correct URL
    expected_url = (
        f"{server_url}/session_control/instruments/{instrument_name}/visits_raw"
    )
    mock_request.get.assert_called_once_with(expected_url)

    # Check that expected outputs are correct (order-sensitive)
    for v, visit in enumerate(visits):
        assert (
            visit.model_dump() == Visit.model_validate(example_visits[v]).model_dump()
        )
