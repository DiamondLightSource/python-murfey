import argparse
import requests
import os

def run():
    parser = argparse.ArgumentParser(description="Start the transferscript client")
    parser.add_argument("--visit", help="Name of visit", required=True)
    args = parser.parse_args()
    print("Visit name: ", args.visit)
    print(get_all_visits().text)
    print(get_visit_info(args.visit).text)

def get_all_visits():
    path = 'http://127.0.0.1:8000/visits/' + os.getenv("BEAMLINE")
    # uvicorn default host and port, specified in uvicorn.run in server/main.py
    r = requests.get(path)
    return r

def get_visit_info(visit_name: str):
    path = 'http://127.0.0.1:8000/visits/' + os.getenv("MICROSCOPE") + '/' + visit_name
    # uvicorn default host and port, specified in uvicorn.run in server/main.py
    r = requests.get(path)
    return r
run()
