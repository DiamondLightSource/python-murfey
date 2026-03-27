import argparse
import logging
from urllib.parse import urlparse

logger = logging.getLogger("murfey.instrument_server")


class MurfeyServerUrl:
    url = ""


murfey_server_url = MurfeyServerUrl()


def check_for_updates():
    import murfey.client.update

    murfey_url = urlparse(murfey_server_url.url, allow_fragments=False)
    try:
        murfey.client.update.check(murfey_url)
    except Exception as e:
        print(f"Murfey update check failed with {e}")


def start_instrument_server(host: str, port: int):
    import uvicorn
    from rich.logging import RichHandler

    import murfey
    from murfey.util.api import url_path_for
    from murfey.util.logging import HTTPSHandler, LogFilter

    LogFilter.install()

    # Log everything from Murfey by default
    logging.getLogger("murfey").setLevel(logging.DEBUG)

    # Show only logs at INFO level and above in the console
    rich_handler = RichHandler(enable_link_path=False)
    rich_handler.setLevel(logging.INFO)
    logging.getLogger("murfey").addHandler(rich_handler)
    logging.getLogger("fastapi").addHandler(rich_handler)
    logging.getLogger("uvicorn").addHandler(rich_handler)

    # Construct URL for the HTTPS log handler
    logger_url = (
        f"{murfey_server_url.url}{url_path_for('api.logging.router', 'forward_logs')}"
    )

    # Forward DEBUG levels logs and above from Murfey to the backend
    murfey_https_handler = HTTPSHandler(endpoint_url=logger_url)
    murfey_https_handler.setLevel(logging.DEBUG)
    logging.getLogger("murfey").addHandler(murfey_https_handler)

    # Forward only INFO level logs and above for other packages
    other_https_handler = HTTPSHandler(endpoint_url=logger_url)
    other_https_handler.setLevel(logging.INFO)
    logging.getLogger("fastapi").addHandler(other_https_handler)
    logging.getLogger("uvicorn").addHandler(other_https_handler)

    logger.info(
        f"Starting Murfey server version {murfey.__version__}, listening on {host}:{port}"
    )
    config = uvicorn.Config(
        "murfey.instrument_server.main:app",
        host=host,
        port=port,
        log_config=None,
        ws_ping_interval=300,
        ws_ping_timeout=300,
    )

    logger.info("Starting instrument server")
    _running_server = uvicorn.Server(config=config)
    _running_server.run()
    logger.info("Instrument server shutting down")


def run():
    parser = argparse.ArgumentParser(description="Start the Murfey server")
    parser.add_argument(
        "--host",
        help="Listen for incoming connections on a specific interface (IP address or hostname)",
        type=str,
        default="0.0.0.0",
    )
    parser.add_argument(
        "--port",
        help="Listen for incoming TCP connections on this port (default: 8001)",
        type=int,
        default=8001,
    )
    parser.add_argument(
        "--server",
        help="URL path to backend murfey server",
        type=str,
        default="",
    )
    parser.add_argument(
        "--instrument",
        help="Name of this instrument in the murfey configuration",
        type=str,
    )
    args = parser.parse_args()

    if not args.server:
        from murfey.util.client import read_config

        client_config = dict(read_config()["Murfey"])
        murfey_server_url.url = client_config["server"].rstrip("/")
    else:
        murfey_server_url.url = args.server
    check_for_updates()
    start_instrument_server(args.host, args.port)
