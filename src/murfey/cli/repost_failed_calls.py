import argparse
import asyncio
import json
from datetime import datetime
from functools import partial
from inspect import getfullargspec, iscoroutinefunction
from pathlib import Path
from queue import Empty, Queue

from sqlmodel import Session, create_engine
from workflows.transport.pika_transport import PikaTransport

from murfey.server.murfey_db import url
from murfey.server.run import _set_up_transport
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


def handle_failed_posts(messages_path: list[Path], murfey_db: Session):
    """Deal with any messages that have been sent as failed client posts"""
    # These imports need to happen after transport object is configured
    import murfey.server.api.auth
    import murfey.server.api.bootstrap
    import murfey.server.api.clem
    import murfey.server.api.display
    import murfey.server.api.file_io_frontend
    import murfey.server.api.file_io_instrument
    import murfey.server.api.hub
    import murfey.server.api.instrument
    import murfey.server.api.mag_table
    import murfey.server.api.processing_parameters
    import murfey.server.api.prometheus
    import murfey.server.api.session_control
    import murfey.server.api.session_info
    import murfey.server.api.websocket
    import murfey.server.api.workflow

    for json_file in messages_path:
        with open(json_file, "r") as json_data:
            message = json.load(json_data)
        router_name = message.get("message", {}).get("router_name", "")
        router_base = router_name.split(".")[0]
        function_name = message.get("message", {}).get("function_name", "")
        if not router_name or not function_name:
            print(
                f"Cannot repost {json_file} as it does not have a router or function name"
            )
            continue

        try:
            function_to_call = getattr(
                getattr(murfey.server.api, router_base), function_name
            )
        except AttributeError:
            print(f"Cannot repost {json_file} as {function_name} does not exist")
            continue
        expected_args = getfullargspec(function_to_call)

        call_kwargs = message.get("message", {}).get("kwargs", {})
        call_data = message.get("message", {}).get("data", {})
        function_call_dict = {}

        try:
            for call_arg in expected_args.args:
                call_arg_type = expected_args.annotations.get(call_arg, str)
                if call_arg in call_kwargs.keys():
                    function_call_dict[call_arg] = call_arg_type(call_kwargs[call_arg])
                elif call_arg == "db":
                    function_call_dict["db"] = murfey_db
                else:
                    print(call_data, call_arg_type, call_arg)
                    function_call_dict[call_arg] = call_arg_type(**call_data)
        except TypeError as e:
            print(f"Cannot repost {json_file} due to argument error: {e}")
            continue

        try:
            if iscoroutinefunction(function_to_call):
                asyncio.run(function_to_call(**function_call_dict))
            else:
                function_to_call(**function_call_dict)
            print(f"Reposted {json_file}")
            json_file.unlink()
        except Exception as e:
            print(f"Failed to post {json_file} to {function_name}: {e}")


def run():
    """
    Method of checking and purging murfey queues on rabbitmq
    Two types of messages are possible:
    - failed client posts which need reposting to the murfey server API
    - feedback messages that can be sent back to rabbitmq
    """
    parser = argparse.ArgumentParser(
        description=(
            "Purge and reinject failed murfey messages. "
            "Provide security configuration and set machine configuration."
        )
    )
    parser.add_argument(
        "-c",
        "--config",
        help="Security config file",
        required=True,
    )
    parser.add_argument(
        "-d", "--dir", default="DLQ", help="Directory to export messages to"
    )
    args = parser.parse_args()

    # Read the security config file
    security_config = security_from_file(args.config)

    # Configure the transport
    PikaTransport().load_configuration_file(security_config.rabbitmq_credentials)
    _set_up_transport("PikaTransport")

    # Now import transport object which was set up in the above step
    from murfey.server import _transport_object

    _transport_object.feedback_queue = security_config.feedback_queue

    # Purge the queue and repost/reinject any messages found
    dlq_dump_path = Path(args.dir)
    dlq_dump_path.mkdir(parents=True, exist_ok=True)
    exported_messages = dlq_purge(
        dlq_dump_path,
        security_config.feedback_queue,
        security_config.rabbitmq_credentials,
    )

    # Set up database and retry api calls
    _url = url(security_config)
    engine = create_engine(_url)
    with Session(engine) as murfey_db:
        handle_failed_posts(exported_messages, murfey_db)

    # Reinject all remaining messages to rabbitmq
    handle_dlq_messages(exported_messages, security_config.rabbitmq_credentials)

    # Clean up any created directories
    try:
        dlq_dump_path.rmdir()
    except OSError:
        print(f"Cannot remove {dlq_dump_path} as it is not empty")
    print("Done")
