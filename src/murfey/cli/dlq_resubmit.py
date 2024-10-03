import argparse
import json
import subprocess
from pathlib import Path

import requests


def handle_failed_posts(json_folder: Path, token: str):
    """Deal with any messages that have been sent as failed client posts"""
    for json_file in json_folder.glob("*"):
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


def handle_dlq_messages(json_folder: Path):
    """Reinjected to the queue"""
    for json_file in json_folder.glob("*"):
        reinject_result = subprocess.run(
            ["zocalo.dlq_reinject", "-e", "devrmq", str(json_file)],
            capture_output=True,
        )
        if reinject_result.returncode == 0:
            print(f"Reinjected {json_file}")
            json_file.unlink()
        else:
            print(f"Failed to reinject {json_file}")


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
        "--queue",
        help="Queue to check and purge",
        required=True,
    )
    parser.add_argument(
        "--token",
        help="Murfey token",
        required=True,
    )
    args = parser.parse_args()

    purge_result = subprocess.run(
        ["zocalo.dlq_purge", "-e", "devrmq", args.queue],
        capture_output=True,
    )
    if purge_result.returncode != 0:
        print(f"Failed to purge {args.queue}")
        return
    purge_stdout = purge_result.stdout.decode("utf8")
    export_directories = []
    if "exported" in purge_stdout:
        for line in purge_stdout.split("\n"):
            if line.strip().startswith("DLQ/"):
                dlq_dir = "DLQ/" + line.split("/")[1]
                if dlq_dir not in export_directories:
                    print(f"Found messages in {dlq_dir}")
                    export_directories.append(dlq_dir)

    if not export_directories:
        print("No exported messages found")
        return

    for json_dir in export_directories:
        handle_failed_posts(Path(json_dir), args.token)
        handle_dlq_messages(Path(json_dir))
    print("Done")


if __name__ == "__main__":
    run()
