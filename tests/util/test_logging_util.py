import time
from unittest import mock
from unittest.mock import MagicMock

import pytest
from fastapi import Response
from pytest_mock import MockerFixture

from murfey.util.logging import HTTPSHandler

https_handler_test_matrix = (
    # Num messages | Status code
    (10, 200),
    (10, 404),
)


@pytest.mark.parametrize("test_params", https_handler_test_matrix)
def test_https_handler(
    mocker: MockerFixture,
    mock_client_configuration,
    test_params: tuple[int, int],
):
    # Unpack test params
    num_messages, status_code = test_params

    # Mock the imported 'requests' module and the HTTPX response
    mock_response = MagicMock(spec=Response)
    mock_response.status_code = status_code
    mock_requests = mocker.patch("murfey.util.logging.requests")
    mock_requests.post.return_value = mock_response

    # Import logger and set up a logger object
    from logging import getLogger

    # Initialise the logger with URL from mock client config
    client_config = dict(mock_client_configuration["Murfey"])
    server_url = client_config["server"]
    https_handler = HTTPSHandler(
        endpoint_url=server_url,
        min_batch=5,
        max_batch=10,
        min_interval=0.5,
        max_interval=1.0,
        max_retry=1,
    )

    logger = getLogger("tests.util.test_logging")
    logger.setLevel(10)
    logger.addHandler(https_handler)
    for i in range(num_messages):
        # Test all the logging levels
        if i % 4 == 0:
            logger.debug("This is a debug log")
        if i % 4 == 1:
            logger.info("This is an info log")
        if i % 4 == 2:
            logger.warning("This is a warning log")
        if i % 4 == 3:
            logger.error("This is an error log")

    # Let it run in the background before checking for the expected calls
    time.sleep(1)
    mock_requests.post.assert_called_with(
        server_url,
        json=mock.ANY,
        timeout=5,
    )

    # Close the handler thread
    https_handler.close()
