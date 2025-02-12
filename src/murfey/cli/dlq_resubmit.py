import argparse
import json
import os
import time
from datetime import datetime
from functools import partial
from pathlib import Path
from queue import Empty, Queue

import requests
from jose import jwt
from workflows.transport.pika_transport import PikaTransport

dlq_dump_path = Path("./DLQ")


def dlq_purge(queue: str, rabbitmq_credentials: Path) -> list[Path]:
    transport = PikaTransport()
    transport.load_configuration_file(rabbitmq_credentials)
    transport.connect()

    queue_to_purge = "dlq." + queue
    idlequeue: Queue = Queue()
    exported_messages = []

    def receive_dlq_message(header: dict, message: dict) -> None:
        idlequeue.put_nowait("start")
        header["x-death"][0]["time"] = datetime.timestamp(header["x-death"][0]["time"])

        timestamp = time.localtime(int(header["x-death"][0]["time"]))
        filepath = dlq_dump_path / time.strftime("%Y-%m-%d", timestamp)
        filepath.mkdir(parents=True, exist_ok=True)
        filename = filepath / (
            f"{queue}-"
            + time.strftime("%Y%m%d-%H%M%S", timestamp)
            + "-"
            + str(header["message-id"])
        )

        dlqmsg = {
            "exported": {
                "date": time.strftime("%Y-%m-%d"),
                "time": time.strftime("%H:%M:%S"),
            },
            "header": header,
            "message": message,
        }

        with filename.open("w") as fh:
            json.dump(dlqmsg, fh, indent=2, sort_keys=True)
        print(f"Message {header['message-id']} exported to {filename}")
        exported_messages.append(filename)
        transport.ack(header)
        idlequeue.put_nowait("done")

    print("Looking for DLQ messages in " + queue_to_purge)
    transport.subscribe(
        queue_to_purge,
        partial(receive_dlq_message),
        acknowledgement=True,
    )
    try:
        idlequeue.get(True, 3)
        while True:
            idlequeue.get(True, 0.1)
    except Empty:
        print("Done.")
    transport.disconnect()
    return exported_messages


def handle_dlq_messages(messages_path: list[Path], rabbitmq_credentials: Path):
    transport = PikaTransport()
    transport.load_configuration_file(rabbitmq_credentials)
    transport.connect()

    for f, dlqfile in enumerate(messages_path):
        if not Path(dlqfile).is_file():
            print(f"Ignoring missing file {dlqfile}")
            continue
        with open(dlqfile) as fh:
            dlqmsg = json.load(fh)
        print(f"Parsing message from {dlqfile}")
        if (
            not isinstance(dlqmsg, dict)
            or not dlqmsg.get("header")
            or not dlqmsg.get("message")
        ):
            print(f"File {dlqfile} is not a valid DLQ message.")
            continue

        header = dlqmsg["header"]
        header["dlq-reinjected"] = "True"

        drop_keys = {
            "message-id",
            "routing_key",
            "redelivered",
            "exchange",
            "consumer_tag",
            "delivery_mode",
        }
        clean_header = {k: str(v) for k, v in header.items() if k not in drop_keys}

        destination = header.get("x-death", [{}])[0].get("queue")
        transport.send(
            destination,
            dlqmsg["message"],
            headers=clean_header,
        )
        dlqfile.unlink()
        print(f"Done {dlqfile}\n")

    transport.disconnect()


def handle_failed_posts(messages_path: list[Path], token: str):
    """Deal with any messages that have been sent as failed client posts"""
    for json_file in messages_path:
        with open(json_file, "r") as json_data:
            message = json.load(json_data)

        if not message.get("message") or not message["message"].get("url"):
            print(f"{json_file} is not a failed client post")
            continue
        dest = message["message"]["url"]
        message_json = message["message"]["json"]

        response = requests.post(
            dest, json=message_json, headers={"Authorization": f"Bearer {token}"}
        )
        if response.status_code != 200:
            print(f"Failed to repost {json_file}")
        else:
            print(f"Reposted {json_file}")
            json_file.unlink()


def run():
    """
    Method of checking and purging murfey queues on rabbitmq
    Two types of messages are possible:
    - failed client posts which need reposting to the murfey server API
    - feedback messages that can be sent back to rabbitmq
    """
    parser = argparse.ArgumentParser(
        description="Purge and reinject failed murfey messages"
    )
    parser.add_argument(
        "-c",
        "--config",
        help="Security config file",
        required=False,
    )
    parser.add_argument(
        "-u",
        "--username",
        help="Token username",
        required=True,
    )
    args = parser.parse_args()

    # Set the environment variable then read it by importing the security config
    os.environ["MURFEY_SECURITY_CONFIGURATION"] = args.config
    from murfey.util.config import get_security_config

    security_config = get_security_config()

    # Get the token to post to the api with
    token = jwt.encode(
        {"user": args.username},
        security_config.auth_key,
        algorithm=security_config.auth_algorithm,
    )

    # Purge the queue and repost/reinject any messages found
    exported_messages = dlq_purge(
        security_config.feedback_queue, security_config.rabbitmq_credentials
    )
    handle_failed_posts(exported_messages, token)
    handle_dlq_messages(exported_messages, security_config.rabbitmq_credentials)

    # Clean up any created directories
    for date_directory in dlq_dump_path.glob("*"):
        try:
            date_directory.rmdir()
        except OSError:
            print(f"Cannot remove {date_directory} as it is not empty")
    try:
        dlq_dump_path.rmdir()
    except OSError:
        print(f"Cannot remove {dlq_dump_path} as it is not empty")
    print("Done")
