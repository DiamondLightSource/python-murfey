from __future__ import annotations

import argparse
import configparser
import pathlib

import murfey.client.update


def run():
    config = read_config()
    known_server = config["Murfey"].get("server")

    parser = argparse.ArgumentParser(description="Start the Murfey client")
    # parser.add_argument("--visit", help="Name of visit", required=True)
    parser.add_argument(
        "--server", type=str, help="Murfey server to connect to", default=known_server
    )
    args = parser.parse_args()
    # print("Visit name: ", args.visit)
    # print(get_all_visits().text)
    # print(get_visit_info(args.visit).text)

    if not args.server:
        exit("Murfey server not set. Please run with --server")

    if args.server != known_server:
        print(f"Attempting to connect to new server {args.server}")
        # Verify the new given server is real
        try:
            server_response = murfey.client.update.check(args.server)
        except Exception as e:
            exit(f"Could not reach {args.server} - {e}")
        if not server_response:
            exit(f"Could not get a valid response from {args.server}")

        # If server is reachable then update the configuration
        config["Murfey"]["server"] = args.server
        write_config(config)
    if args.server:
        if murfey.client.update.check(args.server) is murfey.client.update.UPDATE.NONE:
            print(f"Murfey {murfey.__version__}")
        elif (
            murfey.client.update.check(args.server)
            is murfey.client.update.UPDATE.OPTIONAL
        ):
            print(f"Murfey {murfey.__version__} - an update is available")
        elif (
            murfey.client.update.check(args.server)
            is murfey.client.update.UPDATE.MANDATORY
        ):
            exit("This client is out of date and needs updating")


def read_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    try:
        with open(pathlib.Path.home() / ".murfey", "r") as configfile:
            config.read_file(configfile)
    except FileNotFoundError:
        pass
    if "Murfey" not in config:
        config["Murfey"] = {}
    return config


def write_config(config: configparser.ConfigParser):
    with open(pathlib.Path.home() / ".murfey", "w") as configfile:
        config.write(configfile)
