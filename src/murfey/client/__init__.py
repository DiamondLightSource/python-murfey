from __future__ import annotations

import argparse

from murfey.client.main import example_websocket_connection, post_file


def run():
    parser = argparse.ArgumentParser(description="Start the Murfey client")
    parser.add_argument("--visit", help="Name of visit", required=True)
    visit_name = parser.parse_args().visit
    example_websocket_connection(visit_name)
    post_file(visit_name)
    # args = parser.parse_args()
    # print("Visit name: ", args.visit)
    # print(get_all_visits().text)
    # print(get_visit_info(args.visit).text)
