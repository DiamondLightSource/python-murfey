import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from murfey.server.feedback import feedback_listen
from murfey.server.run import run


@pytest.mark.parametrize(
    "args",
    (  # Host | Port | Root path | Workers | Feedback | Feedback only | Temporary | Quiet | Verbose
        ("1.1.1.1", 5432, "/api", 4, True, False, False, False, 0),
        ("1.1.1.1", 5432, "/api", 4, False, True, False, False, 0),
        ("", None, "", None, False, True, False, False, 0),
        ("", None, "", None, True, False, False, False, 0),
        ("", None, "", None, False, True, False, True, 0),
        ("", None, "", None, True, False, False, False, 0),
        ("", None, "", None, False, True, False, False, 1),
        ("", None, "", None, True, False, False, False, 2),
        ("", None, "", None, True, False, False, False, 3),
    ),
)
def test_run(
    mocker: MockerFixture,
    tmp_path: Path,
    args: tuple[str, int | None, str, int | None, bool, bool, bool, bool, int],
):
    # Unpack test params
    (
        host,
        port,
        root_path,
        workers,
        feedback,
        feedback_only,
        temporary,
        quiet,
        verbosity,
    ) = args

    # Manually pass in argument variables
    sys.argv = [
        "murfey.server",
    ]
    if host:
        sys.argv.extend(["--host", host])
    if port is not None:
        sys.argv.extend(["--port", str(port)])
    if root_path:
        sys.argv.append(f"--root-path={root_path}")
    if workers:
        sys.argv.extend(["--workers", str(workers)])
    if feedback:
        sys.argv.append("--feedback")
    if feedback_only:
        sys.argv.append("--feedback-only")
    if temporary:
        sys.argv.append("--temporary")
    if quiet:
        sys.argv.append("--quiet")
    if verbosity:
        sys.argv.append(f"--{'v' * verbosity}")

    # Set up the mock security configuration
    rmq_creds = tmp_path / "configs" / "rmq-creds.yml"
    mocker.patch(
        "murfey.server.run.get_security_config",
        return_value=MagicMock(
            graylog_host="",
            graylog_port=12345,
            rabbitmq_credentials=rmq_creds,
            feedback_queue="murfey_feedback",
        ),
    )

    # Mock the PikaTransport object and its calls
    mock_pika_transport = MagicMock()
    mocker.patch("murfey.server.run.PikaTransport", mock_pika_transport)

    # Mock the Thread and Event classes
    mock_thread = MagicMock()
    mock_thread_class = mocker.patch(
        "murfey.server.run.Thread", return_value=mock_thread
    )
    mock_event = MagicMock()
    mock_event.is_set.return_value = False  # Don't go into the 'while' block
    mocker.patch("murfey.server.run.Event", return_value=mock_event)

    # Mock the Uvicorn Config and Server classes
    mock_config = MagicMock()
    mock_config_class = mocker.patch(
        "murfey.server.run.uvicorn.Config", return_value=mock_config
    )
    mock_server = MagicMock()
    mock_server_class = mocker.patch(
        "murfey.server.run.uvicorn.Server", return_value=mock_server
    )

    # Run the function and check that the expected calls were made
    run()

    # PikaTransport should have loaded the configuration file
    mock_pika_transport.return_value.load_configuration_file.assert_called_once_with(
        rmq_creds
    )

    # RabbitMQ thread should start if '--feedback' or '--feedback-only' are set
    if feedback or feedback_only:
        mock_thread_class.assert_called_once_with(target=feedback_listen, daemon=True)
        mock_thread.start.assert_called_once()
        if feedback_only:
            mock_event.set.assert_called_once()
        else:
            mock_event.set.assert_not_called()
    else:
        mock_thread_class.assert_not_called()
        mock_thread.start.assert_not_called()
        mock_event.set.assert_not_called()

    # Uvicorn server should start if '--feedback-only' is not set
    if not feedback_only:
        mock_config_class.assert_called_once_with(
            "murfey.server.main:app",
            host=host if host else "0.0.0.0",
            port=port if port is not None else 8000,
            log_config=None,
            ws_ping_interval=300,
            ws_ping_timeout=300,
            workers=workers if workers is not None else 2,
            root_path=root_path,
        )
        mock_server_class.assert_called_once_with(config=mock_config)
        mock_server.run.assert_called_once()
    else:
        mock_config_class.assert_not_called()
        mock_server_class.assert_not_called()
        mock_server.run.assert_not_called()
