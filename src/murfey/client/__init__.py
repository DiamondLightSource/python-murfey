from __future__ import annotations

import argparse


def run():
    parser = argparse.ArgumentParser(description="Start the Murfey client")
    parser.add_argument("--visit", help="Name of visit", required=True)
    # args = parser.parse_args()
    # print("Visit name: ", args.visit)
    # print(get_all_visits().text)
    # print(get_visit_info(args.visit).text)
