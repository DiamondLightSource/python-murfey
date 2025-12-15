import json
import logging
import time
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_mock import MockerFixture

import murfey
from murfey.server.api.logging import forward_logs


@pytest.mark.asyncio
async def test_forward_logs(
    mocker: MockerFixture,
):
    # Create example log messages
    message_list = [
        json.dumps(
            {
                "name": f"murfey.{module_name}",
                "msg": "Starting Murfey server version {murfey.__version__}, listening on 0.0.0.0:8000",
                "args": [],
                "levelname": levelname,
                "levelno": levelno,
                "pathname": f"{murfey.__file__}/{module_name}/__init__.py",
                "filename": "__init__.py",
                "module": "__init__",
                "exc_info": None,
                "exc_text": None,
                "stack_info": None,
                "lineno": 76,
                "funcName": f"start_{module_name}",
                "created": time.time(),
                "msecs": 930.0,
                "relativeCreated": 1379.8329830169678,
                "thread": time.time_ns(),
                "threadName": "MainThread",
                "processName": "MainProcess",
                "process": time.time_ns(),
                "message": f"Starting Murfey server version {murfey.__version__}, listening on 0.0.0.0:8000",
                "type": "log",
            }
        )
        for module_name, levelname, levelno in (
            ("module_1", "DEBUG", logging.DEBUG),
            ("module_2", "INFO", logging.INFO),
            ("module_3", "WARNING", logging.WARNING),
            ("module_4", "ERROR", logging.ERROR),
        )
    ]

    # Create a mock request to pass to the function
    mock_request = MagicMock()
    mock_request.json = AsyncMock(return_value=message_list)

    # Mock the logging module
    mock_logging = mocker.patch("murfey.server.api.logging.logging")

    # Mock the 'getLogger()' and 'handle()' functions
    mock_logger = MagicMock()
    mock_logger.handle.return_value = None
    mock_logging.getLogger.return_value = mock_logger

    # Run the function and check that the results are as expected
    await forward_logs(mock_request)

    # Check that the correct logger name was called.
    for i, message in enumerate(message_list):
        # Process the message as in the actual function
        log_data: dict[str, Any] = json.loads(message)
        logger_name = log_data["name"]
        log_data.pop("msecs", None)
        log_data.pop("relativeCreated", None)
        client_timestamp = log_data.pop("created", 0)
        if client_timestamp:
            log_data["client_time"] = datetime.fromtimestamp(
                client_timestamp
            ).isoformat()
        log_data["client_host"] = None  # No host, as function is being tested directly

        # Check that messages are unpacked and handled in sequence
        mock_logging.getLogger.call_args_list[i][0][0] == logger_name
        mock_logger.handle.call_args_list[i][0][0] == logging.makeLogRecord(log_data)

    # Check that 'handle' was called for each message
    assert mock_logger.handle.call_count == len(message_list)
