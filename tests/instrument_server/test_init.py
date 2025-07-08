from unittest import mock
from urllib.parse import urlparse

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import murfey
from murfey.instrument_server import check_for_updates
from murfey.server.api.bootstrap import bootstrap as bootstrap_router
from murfey.server.api.bootstrap import pypi as pypi_router
from murfey.util.api import url_path_for

# Set up a test router with only the essential endpoints
app = FastAPI()
for router in [pypi_router, bootstrap_router]:
    app.include_router(router)
client = TestClient(app)


check_for_updates_test_matrix = (
    # Downgrade, upgrade, or keep client version?
    ("downgrade",),
    ("upgrade",),
    ("keep",),
)


@pytest.mark.parametrize("test_params", check_for_updates_test_matrix)
def test_check_for_updates(
    test_params: tuple[str],
):

    # Unpack test params
    (handle_client_version,) = test_params

    with (
        mock.patch("murfey.instrument_server.urlparse") as mock_parse,
        mock.patch("murfey.client.update.requests.get") as mock_get,
    ):
        # Return the test client URL
        api_base = urlparse("http://testserver", allow_fragments=False)
        mock_parse.return_value = api_base
        check_for_updates()

        # Modify client version as needed
        current_version = murfey.__version__
        supported_client_version = murfey.__supported_client_version__

        # Check that a request was sent to the test_client with the correct URL
        proxy_path = api_base.path.rstrip("/")
        version_check_url = api_base._replace(
            path=f"{proxy_path}{url_path_for('bootstrap.version', 'get_version')}",
            query=f"client_version={current_version}",
        )
        mock_get.assert_any_call(version_check_url.geturl())

        # Construct the mock response
        mock_response = mock.Mock()
        mock_response.json.return_value = {
            "server": current_version,
            "oldest-supported-client": supported_client_version,
            "client-needs-update": True,
            "client-needs-downgrade": False,
        }
        mock_response.status_code = 200
        mock_get.return_value = mock_response

    pass
