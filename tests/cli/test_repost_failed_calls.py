import json
import os
import subprocess
import sys
from pathlib import Path
from queue import Empty
from unittest import mock

from murfey.cli import repost_failed_calls
from murfey.util.config import security_from_file
from murfey.util.db import Tilt


@mock.patch("murfey.cli.repost_failed_calls.PikaTransport")
@mock.patch("murfey.cli.repost_failed_calls.Queue")
def test_dlq_purge(mock_queue, mock_transport, tmp_path):
    """Test the dlq purging function.
    Currently doesn't test saving the message, as the subscribe is mocked out"""
    mock_queue().get.return_value = {"message": "dummy"}
    mock_queue().get.side_effect = [None, Empty]

    exported_messages = repost_failed_calls.dlq_purge(
        tmp_path / "DLQ", "dummy", tmp_path / "config_file"
    )

    # The transport should be connected to and subscribes to the queue
    mock_transport.assert_called_once()
    mock_transport().load_configuration_file.assert_called_with(
        tmp_path / "config_file"
    )
    mock_transport().connect.assert_called_once()
    mock_transport().subscribe.assert_called_with(
        "dlq.dummy",
        mock.ANY,
        acknowledgement=True,
    )
    mock_transport().disconnect.assert_called_once()

    # Should read from the queue
    mock_queue().get.assert_any_call(True, 0.1)

    # Ideally this test would return the message, but the partial isn't called yet
    assert exported_messages == []


@mock.patch("murfey.cli.repost_failed_calls.PikaTransport")
def test_handle_dlq_messages(mock_transport, tmp_path):
    """Reinject some example messages"""
    # Create two sample messages
    messages_paths_list: list[Path] = [tmp_path / "not_a_message"]
    messages_dict: dict[str, dict] = {
        "msg1": {
            "header": {
                "x-death": [{"queue": "queue_msg1"}],
                "message-id": 1,
                "routing_key": "dlq.queue_msg1",
                "redelivered": True,
                "exchange": "",
                "consumer_tag": "1",
                "delivery_mode": 2,
                "other_key": "value",
            },
            "message": {"parameters": "msg1"},
        },
        "msg2": {
            "header": {"x-death": [{"queue": "queue_msg2"}]},
            "message": {"content": "msg2"},
        },
    }
    for file_name, message in messages_dict.items():
        messages_paths_list.append(tmp_path / file_name)
        with open(tmp_path / file_name, "w") as msg_file:
            json.dump(message, msg_file)

    # Send the two messages, plus a file that is not a message
    repost_failed_calls.handle_dlq_messages(
        messages_path=messages_paths_list,
        rabbitmq_credentials=tmp_path / "config_file",
    )

    mock_transport.assert_called_once()
    mock_transport().load_configuration_file.assert_called_with(
        tmp_path / "config_file"
    )
    mock_transport().connect.assert_called_once()

    # Only two messages should have been sent, the rest are invalid so are skipped
    assert mock_transport().send.call_count == 2
    mock_transport().send.assert_any_call(
        "queue_msg1",
        {"parameters": "msg1"},
        headers={
            "x-death": "[{'queue': 'queue_msg1'}]",
            "other_key": "value",
            "dlq-reinjected": "True",
        },
    )
    mock_transport().send.assert_any_call(
        "queue_msg2",
        {"content": "msg2"},
        headers={"x-death": "[{'queue': 'queue_msg2'}]", "dlq-reinjected": "True"},
    )

    # Removal and waiting
    assert not (tmp_path / "msg1").is_file()
    assert not (tmp_path / "msg2").is_file()
    mock_transport().disconnect.assert_called_once()


def test_handle_failed_posts(tmp_path):
    """Test that the API is called with any failed client post messages"""
    # Create some sample messages
    messages_paths_list: list[Path] = []
    messages_dict: dict[str, dict] = {
        "msg1": {
            "message": {
                "router_name": "workflow.tomo_router",
                "function_name": "register_completed_tilt_series",
                "kwargs": {"visit_name": "cm12345-1", "session_id": 1},
                "data": {
                    "tags": ["tag"],
                    "source": "source",
                    "tilt_series_lengths": [10],
                },
            },
        },  # normal example
        "msg2": {
            "message": {
                "router_name": "workflow.tomo_router",
                "function_name": "register_tilt",
                "kwargs": {"visit_name": "cm12345-1", "session_id": 1},
                "data": {
                    "tilt_series_tag": "tag",
                    "source": "source",
                    "movie_path": "path",
                },
            },
        },  # async example
        "msg3": {
            "message": {
                "router_name": "workflow.tomo_router",
                "function_name": "register_completed_tilt_series",
                "data": {"tags": ["tag"]},
            }
        },
        "msg4": {
            "message": {"function_name": "dummy"},  # does not have a router
        },
        "msg5": {
            "message": {"router_name": "workflow"},  # does not have a function
        },
        "msg6": {
            "message": {
                "router_name": "workflow",
                "function_name": "dummy",
            },  # function does not exist
        },
    }
    for file_name, message in messages_dict.items():
        messages_paths_list.append(tmp_path / file_name)
        with open(tmp_path / file_name, "w") as msg_file:
            json.dump(message, msg_file)

    mock_db = mock.Mock()
    mock_exec_return = mock.Mock()
    mock_exec_return.all.return_value = []
    mock_db.exec.return_value = mock_exec_return
    repost_failed_calls.handle_failed_posts(messages_paths_list, mock_db)

    # Check the failed posts were resent
    assert mock_db.exec.call_count == 3
    assert mock_db.exec().one.call_count == 1
    assert mock_db.exec().all.call_count == 2
    assert mock_exec_return.one.call_count == 1
    assert mock_exec_return.all.call_count == 2
    assert mock_db.commit.call_count == 3
    mock_db.add.assert_called_once_with(
        Tilt(movie_path="path", tilt_series_id=mock.ANY, motion_corrected=False)
    )

    # Check only the failed post which was successfully reinjected got deleted
    assert not (tmp_path / "msg1").is_file()  # got resent
    assert not (tmp_path / "msg2").is_file()  # got resent
    assert (tmp_path / "msg3").is_file()  # failed reinjection
    assert (tmp_path / "msg4").is_file()  # does not have a router
    assert (tmp_path / "msg5").is_file()  # does not have a function
    assert (tmp_path / "msg6").is_file()  # function does not exist


@mock.patch("workflows.transport.pika_transport.PikaTransport")
@mock.patch("murfey.cli.repost_failed_calls.PikaTransport")
@mock.patch("murfey.cli.repost_failed_calls.dlq_purge")
@mock.patch("murfey.cli.repost_failed_calls.handle_failed_posts")
@mock.patch("murfey.cli.repost_failed_calls.handle_dlq_messages")
@mock.patch("murfey.cli.repost_failed_calls.url")
@mock.patch("murfey.cli.repost_failed_calls.create_engine")
@mock.patch("murfey.cli.repost_failed_calls.Session")
def test_run_repost_failed_calls(
    mock_db_session,
    mock_db_engine,
    mock_db_url,
    mock_reinject,
    mock_repost,
    mock_purge,
    mock_pika,
    mock_workflows,
    mock_security_configuration,
):
    mock_session = mock.MagicMock()

    mock_db_url.return_value = "db_url"
    mock_db_engine.return_value = "db_engine"
    mock_db_session.return_value = mock_session
    mock_purge.return_value = ["/path/to/msg1"]

    config_file = mock_security_configuration
    with open(config_file) as f:
        security_config_dict = json.load(f)

    sys.argv = [
        "murfey.repost_failed_calls",
        "--config",
        str(config_file),
        "--dir",
        "DLQ_dir",
    ]
    repost_failed_calls.run()

    security_config_class = security_from_file(config_file)
    mock_db_url.assert_called_with(security_config_class)
    mock_db_engine.assert_called_with("db_url")
    mock_db_session.assert_called_with("db_engine")

    mock_pika().load_configuration_file.assert_called_with(
        Path("/path/to/rabbitmq.yaml")
    )
    mock_workflows().connect.assert_called_once()

    mock_purge.assert_called_once_with(
        Path("DLQ_dir"),
        "murfey_feedback",
        Path(security_config_dict["rabbitmq_credentials"]),
    )
    mock_repost.assert_called_once_with(["/path/to/msg1"], mock_session.__enter__())
    mock_reinject.assert_called_once_with(
        ["/path/to/msg1"], Path(security_config_dict["rabbitmq_credentials"])
    )


def test_repost_failed_calls_exists(mock_security_configuration):
    """Test the CLI is made"""
    os.environ["MURFEY_SECURITY_CONFIGURATION"] = str(mock_security_configuration)
    result = subprocess.run(
        [
            "murfey.repost_failed_calls",
            "--help",
        ],
        capture_output=True,
    )
    assert not result.returncode

    # Find the first line of the help and strip out all the spaces and newlines
    stdout_as_string = result.stdout.decode("utf8", "replace")
    cleaned_help_line = (
        stdout_as_string.split("\n\n")[0].replace("\n", "").replace(" ", "")
    )
    assert cleaned_help_line == ("usage:murfey.repost_failed_calls[-h]-cCONFIG[-dDIR]")
