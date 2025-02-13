import argparse
import json
from datetime import datetime
from functools import partial
from pathlib import Path
from queue import Empty, Queue

import requests
from jose import jwt
from workflows.transport.pika_transport import PikaTransport

from murfey.util.config import security_from_file


def dlq_purge(
    dlq_dump_path: Path, queue: str, rabbitmq_credentials: Path
) -> list[Path]:
    transport = PikaTransport()
    transport.load_configuration_file(rabbitmq_credentials)
    transport.connect()

    queue_to_purge = f"dlq.{queue}"
    idlequeue: Queue = Queue()
    exported_messages = []

    def receive_dlq_message(header: dict, message: dict) -> None:
        idlequeue.put_nowait("start")
        header["x-death"][0]["time"] = datetime.timestamp(header["x-death"][0]["time"])
        filename = dlq_dump_path / f"{queue}-{header['message-id']}"
        dlqmsg = {"header": header, "message": message}
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
        while True:
            idlequeue.get(True, 0.1)
    except Empty:
        print("Done dlq purge")
    transport.disconnect()
    return exported_messages


def handle_dlq_messages(messages_path: list[Path], rabbitmq_credentials: Path):
    transport = PikaTransport()
    transport.load_configuration_file(rabbitmq_credentials)
    transport.connect()

    for f, dlqfile in enumerate(messages_path):
        if not dlqfile.is_file():
            continue
        with open(dlqfile) as fh:
            dlqmsg = json.load(fh)
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
        print(f"Reinjected {dlqfile}\n")

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
        required=True,
    )
    parser.add_argument(
        "-u",
        "--username",
        help="Token username",
        required=True,
    )
    parser.add_argument(
        "-d", "--dir", default="DLQ", help="Directory to export messages to"
    )
    args = parser.parse_args()

    # Read the security config file
    security_config = security_from_file(args.config)

    # Get the token to post to the api with
    token = jwt.encode(
        {"user": args.username},
        security_config.auth_key,
        algorithm=security_config.auth_algorithm,
    )

    # Purge the queue and repost/reinject any messages found
    dlq_dump_path = Path(args.dir)
    dlq_dump_path.mkdir(parents=True, exist_ok=True)
    exported_messages = dlq_purge(
        dlq_dump_path,
        security_config.feedback_queue,
        security_config.rabbitmq_credentials,
    )
    handle_failed_posts(exported_messages, token)
    handle_dlq_messages(exported_messages, security_config.rabbitmq_credentials)

    # Clean up any created directories
    try:
        dlq_dump_path.rmdir()
    except OSError:
        print(f"Cannot remove {dlq_dump_path} as it is not empty")
    print("Done")
