from __future__ import annotations

import argparse
import os

import requests


def run():
    parser = argparse.ArgumentParser(description="Start the Murfey client")
    parser.add_argument("--visit", help="Name of visit", required=True)
    args = parser.parse_args()
    print("Visit name: ", args.visit)
    print(get_all_visits().text)
    print(get_visit_info(args.visit).text)


def get_all_visits():
    bl = os.getenv("BEAMLINE")
    if bl:
        path = "http://127.0.0.1:8000/visits/" + bl
    else:
        raise RuntimeError("No BEAMLINE environment variable was specified")
    # uvicorn default host and port, specified in uvicorn.run in server/main.py
    r = requests.get(path)
    return r


def get_visit_info(visit_name: str):
    bl = os.getenv("BEAMLINE")
    if bl:
        path = "http://127.0.0.1:8000/visits/" + bl + "/" + visit_name
    else:
        raise RuntimeError("No BEAMLINE environment variable was specified")
    # uvicorn default host and port, specified in uvicorn.run in server/main.py
    r = requests.get(path)
    return r
