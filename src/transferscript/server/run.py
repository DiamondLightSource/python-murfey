import os
import argparse
import uvicorn
import pathlib

def run():
    parser = argparse.ArgumentParser(description="Start the transferscript server")
    parser.add_argument("--env_file", help="Path to environment file", default=pathlib.Path(__file__).parent / 'example_environment_file')
    args = parser.parse_args()
    uvicorn.run("server.main:app", host="127.0.0.1", port=8000, env_file=args.env_file)