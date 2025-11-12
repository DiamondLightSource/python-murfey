import contextlib
import io
import json
from argparse import ArgumentParser
from pathlib import Path

import yaml
from fastapi.openapi.utils import get_openapi

import murfey
from murfey.cli import LineWrapHelpFormatter, PrettierDumper


def run():
    # Set up argument parser
    parser = ArgumentParser(
        description=(
            "Generates an OpenAPI schema of the chosen FastAPI server "
            "and outputs it as either a JSON or YAML file"
        ),
        formatter_class=LineWrapHelpFormatter,
    )
    parser.add_argument(
        "--target",
        "-t",
        default="server",
        help=(
            "The target FastAPI server to construct the OpenAPI schema for. \n"
            "OPTIONS: instrument-server | server \n"
            "DEFAULT: server"
        ),
    )
    parser.add_argument(
        "--output",
        "-o",
        default="yaml",
        help=(
            "Set the output format of the OpenAPI schema. \n"
            "OPTIONS: json | yaml \n"
            "DEFAULT: yaml"
        ),
    )
    parser.add_argument(
        "--to-file",
        "-f",
        default="",
        help=(
            "Alternative file path and file name to save the schema as. "
            "Can be a relative or absolute path. \n"
            "By default, the schema will be saved to 'murfey/utils/', "
            "and it will have the name 'openapi.json' or 'openapi.yaml'."
        ),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Shows additional steps when setting ",
    )
    args = parser.parse_args()

    # Load the relevant FastAPI app
    target = str(args.target).lower()

    # Silence output during import; only return genuine errors
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
        if target == "server":
            from murfey.server.main import app
        elif target == "instrument-server":
            from murfey.instrument_server.main import app
        else:
            raise ValueError(
                "Unexpected value for target server. It must be one of "
                "'instrument-server' or 'server'"
            )
    if args.debug:
        print(f"Imported FastAPI app for {target}")

    if not app.openapi_schema:
        schema = get_openapi(
            title=app.title,
            version=app.version,
            openapi_version=app.openapi_version,
            description=app.description,
            routes=app.routes,
        )
        if args.debug:
            print(f"Constructed OpenAPI schema for {target}")
    else:
        schema = app.openapi_schema
        if args.debug:
            print(f"Loaded OpenAPI schema for {target}")

    output = str(args.output).lower()
    if output not in ("json", "yaml"):
        raise ValueError(
            "Invalid file format selected. Output must be either 'json' or 'yaml'"
        )
    murfey_dir = Path(murfey.__path__[0])
    save_path = (
        murfey_dir / "util" / f"openapi-{target}.{output}"
        if not args.to_file
        else Path(args.to_file)
    )
    with open(save_path, "w") as f:
        if output == "json":
            json.dump(schema, f, indent=2)
        else:
            yaml.dump(
                schema,
                f,
                Dumper=PrettierDumper,
                default_flow_style=False,
                sort_keys=False,
                indent=2,
            )
    print(f"OpenAPI schema saved to {save_path}")
    exit()


if __name__ == "__main__":
    run()
