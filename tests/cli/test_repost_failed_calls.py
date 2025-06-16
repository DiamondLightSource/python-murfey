import json
import subprocess
import sys
from pathlib import Path
from queue import Empty
from unittest import mock

from murfey.cli import repost_failed_calls


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


@mock.patch("murfey.cli.repost_failed_calls.requests")
def test_handle_failed_posts(mock_requests, tmp_path):
    """Test that the API is called with any failed client post messages"""
    # Create some sample messages
    messages_paths_list: list[Path] = []
    messages_dict: dict[str, dict] = {
        "msg1": {
            "message": {"url": "sample/url", "json": {"content": "msg1"}},
        },
        "msg2": {
            "message": {"url": "sample/url", "json": {"content": "msg2"}},
        },
        "msg3": {
            "message": {"content": "msg3"},  # not a failed client post
        },
        "msg4": {
            "header": {"content": "msg3"},  # does not have a message
        },
    }
    for file_name, message in messages_dict.items():
        messages_paths_list.append(tmp_path / file_name)
        with open(tmp_path / file_name, "w") as msg_file:
            json.dump(message, msg_file)

    class Response:
        def __init__(self, status_code):
            self.status_code = status_code

    mock_requests.post.side_effect = [Response(200), Response(300)]

    repost_failed_calls.handle_failed_posts(messages_paths_list, "dummy_token")

    # Check the failed posts were resent
    assert mock_requests.post.call_count == 2
    mock_requests.post.assert_any_call(
        "sample/url",
        json={"content": "msg1"},
        headers={"Authorization": "Bearer dummy_token"},
    )
    mock_requests.post.assert_any_call(
        "sample/url",
        json={"content": "msg2"},
        headers={"Authorization": "Bearer dummy_token"},
    )

    # Check only the failed post which was successfully reinjected got deleted
    assert not (tmp_path / "msg1").is_file()  # got resent
    assert (tmp_path / "msg2").is_file()  # failed reinjection
    assert (tmp_path / "msg3").is_file()  # not a failed client post
    assert (tmp_path / "msg4").is_file()  # does not have a message


@mock.patch("murfey.cli.repost_failed_calls.dlq_purge")
@mock.patch("murfey.cli.repost_failed_calls.handle_failed_posts")
@mock.patch("murfey.cli.repost_failed_calls.handle_dlq_messages")
@mock.patch("murfey.cli.repost_failed_calls.jwt")
def test_run_repost_failed_calls(
    mock_jwt,
    mock_reinject,
    mock_repost,
    mock_purge,
    mock_security_configuration,
):
    mock_jwt.encode.return_value = "dummy_token"
    mock_purge.return_value = ["/path/to/msg1"]

    config_file = mock_security_configuration
    with open(config_file) as f:
        security_config = json.load(f)

    sys.argv = [
        "murfey.repost_failed_calls",
        "--config",
        str(config_file),
        "--username",
        "user",
        "--dir",
        "DLQ_dir",
    ]
    repost_failed_calls.run()

    mock_jwt.encode.assert_called_with(
        {"user": "user"},
        security_config["auth_key"],
        algorithm=security_config["auth_algorithm"],
    )

    mock_purge.assert_called_once_with(
        Path("DLQ_dir"),
        "murfey_feedback",
        Path(security_config["rabbitmq_credentials"]),
    )
    mock_repost.assert_called_once_with(["/path/to/msg1"], "dummy_token")
    mock_reinject.assert_called_once_with(
        ["/path/to/msg1"], Path(security_config["rabbitmq_credentials"])
    )


def test_repost_failed_calls_exists():
    """Test the CLI is made"""
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
    assert cleaned_help_line == (
        "usage:murfey.repost_failed_calls[-h]-cCONFIG-uUSERNAME[-dDIR]"
    )
