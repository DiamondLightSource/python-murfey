from __future__ import annotations

import argparse
import configparser
import contextlib
import os
import pathlib
import subprocess
import sys
from urllib.parse import urlparse
from urllib.request import urlopen

# A script to simplify installing Murfey on a network-isolated machine.
# This could in theory be invoked by
#   python -m murfey.bootstrap
# but then you would already have murfey installed, so what is the point.
# More commonly this file will be run directly from a wheel with
#   python murfey.whl/murfey/bootstrap
# In this constellation you can not import any other files from the murfey
# package. If you absolutely have to do this then look at the pip package
# how this can be achieved. Also note that only standard library imports
# will be available at that installation stage.


def _download_to_file(url: str, outfile: str):
    """
    Downloads a single URL to a file.
    """
    with contextlib.closing(urlopen(url)) as socket:
        file_size = socket.info().get("Content-Length")
        if file_size:
            file_size = int(file_size)
        # There is no guarantee that the content-length header is set
        received = 0
        block_size = 8192
        # Allow for writing the file immediately so we can empty the buffer
        with open(outfile, mode="wb") as f:
            while True:
                block = socket.read(block_size)
                received += len(block)
                f.write(block)
                if not block:
                    break

    if file_size and file_size != received:
        raise OSError(
            f"Error downloading {url}: received {received} bytes instead of expected {file_size} bytes"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser("murfey.bootstrap")
    parser.add_argument("-?", action="help", help=argparse.SUPPRESS)
    parser.add_argument(
        type=str, dest="server", help="URL pointing to the murfey server"
    )
    args = parser.parse_args()

    # Validate the passed server address and construct a minimal base path string and
    # extract the host name for pip installation purposes
    try:
        murfey_url = urlparse(args.server)
    except Exception:
        exit(f"{args.server} is not a valid URL")
    murfey_base = f"{murfey_url.scheme}://{murfey_url.netloc}"
    murfey_hostname = murfey_url.netloc.split(":")[0]

    print(f"Python version: {sys.version_info.major}.{sys.version_info.minor}")
    if sys.hexversion < 0x3080000:
        exit(
            "Your python version is too old to support Murfey. "
            "You need at least Python 3.8"
        )

    print()
    print(f"1/3 -- Connecting to murfey server on {murfey_base}...")
    _download_to_file(f"{murfey_base}/bootstrap/pip.whl", "pip.whl")

    print()
    print("2/3 -- Bootstrapping pip")
    python = sys.executable
    result = subprocess.run(
        [
            python,
            "pip.whl/pip",
            "install",
            "--trusted-host",
            murfey_hostname,
            "-i",
            f"{murfey_base}/pypi",
            "pip",
        ]
    )
    if result.returncode:
        exit("Could not bootstrap pip")
    os.remove("pip.whl")

    print()
    print("3/3 -- Installing murfey client")
    result = subprocess.run(
        [
            python,
            "-mpip",
            "install",
            "--trusted-host",
            murfey_hostname,
            "-i",
            f"{murfey_base}/pypi",
            "murfey[client]",
        ]
    )
    if result.returncode:
        exit("Could not install murfey client")

    print()
    print("Installation completed.")
    config = configparser.ConfigParser()
    config["Murfey"] = {"Server": murfey_base}
    with open(pathlib.Path.home() / ".murfey", "w") as configfile:
        config.write(configfile)
