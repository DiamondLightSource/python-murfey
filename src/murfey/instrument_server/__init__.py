import logging
from urllib.parse import urlparse

from murfey.util.client import read_config

logger = logging.getLogger("murfey.instrument_server")


def check_for_updates():
    import murfey.client.update

    murfey_url = urlparse(
        read_config().get("Murfey", "server", fallback=""), allow_fragments=False
    )
    try:
        murfey.client.update.check(murfey_url)
    except Exception as e:
        print(f"Murfey update check failed with {e}")


def start_instrument_server():
    import argparse

    import uvicorn
    from rich.logging import RichHandler

    import murfey
    import murfey.client.websocket
    from murfey.client.customlogging import CustomHandler
    from murfey.util import LogFilter

    parser = argparse.ArgumentParser(description="Start the Murfey server")
    parser.add_argument(
        "--host",
        help="Listen for incoming connections on a specific interface (IP address or hostname; default: all)",
        default="0.0.0.0",
    )
    parser.add_argument(
        "--port",
        help="Listen for incoming TCP connections on this port (default: 8001)",
        type=int,
        default=8001,
    )
    args = parser.parse_args()

    LogFilter.install()

    rich_handler = RichHandler(enable_link_path=False)
    logging.getLogger("murfey").setLevel(logging.INFO)
    logging.getLogger("murfey").addHandler(rich_handler)
    logging.getLogger("fastapi").addHandler(rich_handler)
    logging.getLogger("uvicorn").addHandler(rich_handler)

    ws = murfey.client.websocket.WSApp(
        server=read_config().get("Murfey", "server", fallback=""),
        register_client=False,
    )

    handler = CustomHandler(ws.send)
    logging.getLogger("murfey").addHandler(handler)
    logging.getLogger("fastapi").addHandler(handler)
    logging.getLogger("uvicorn").addHandler(handler)

    logger.info(
        f"Starting Murfey server version {murfey.__version__}, listening on {args.host}:{args.port}"
    )
    global _running_server
    config = uvicorn.Config(
        "murfey.instrument_server.main:app",
        host=args.host,
        port=args.port,
        log_config=None,
        ws_ping_interval=300,
        ws_ping_timeout=300,
    )

    logger.info("Starting instrument server")
    _running_server = uvicorn.Server(config=config)
    _running_server.run()
    logger.info("Instrument server shutting down")


def run():
    check_for_updates()
    start_instrument_server()
