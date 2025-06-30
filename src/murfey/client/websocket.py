from __future__ import annotations

import json
import logging
import queue
import threading
import time
import urllib.parse
import uuid
from typing import Optional

import websocket

from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.api import url_path_for

log = logging.getLogger("murfey.client.websocket")


class WSApp:
    environment: MurfeyInstanceEnvironment | None = None

    def __init__(
        self, *, server: str, id: int | str | None = None, register_client: bool = True
    ):
        self.id = str(uuid.uuid4()) if id is None else id
        log.info(f"Opening websocket connection for Client {self.id}")
        websocket.enableTrace(False)

        # Parse server URL and get proxy path used, if any
        url = urllib.parse.urlparse(server)._replace(
            scheme="wss" if server.startswith("https") else "ws"
        )
        proxy_path = url.path.rstrip("/")

        self._address = url.geturl()
        self._alive = True
        self._ready = False
        self._send_queue: queue.Queue[Optional[str]] = queue.Queue()
        self._receive_queue: queue.Queue[Optional[str]] = queue.Queue()

        # Construct the websocket URL
        # Prepend the proxy path to the new URL path
        # It will evaluate to "" if nothing's there, and starts with "/" if present
        ws_url = (
            url._replace(
                path=f"{proxy_path}{url_path_for('websocket.ws', 'websocket_endpoint', client_id=self.id)}"
            ).geturl()
            if register_client
            else url._replace(
                path=f"{proxy_path}{url_path_for('websocket.ws', 'websocket_connection_endpoint', client_id=self.id)}"
            ).geturl()
        )
        self._ws = websocket.WebSocketApp(
            ws_url,
            on_close=self.on_close,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=self.on_error,
        )
        self._ws_thread = threading.Thread(
            target=self._run_websocket_event_loop,
            daemon=True,
            name="websocket-connection",
        )
        self._ws_thread.start()
        self._feeder_thread = threading.Thread(
            target=self._send_queue_feeder, daemon=True, name="websocket-send-queue"
        )
        self._feeder_thread.start()
        self._receiver_thread = threading.Thread(
            target=self._receive_msgs, daemon=True, name="websocket-receive-queue"
        )
        self._receiver_thread.start()
        log.info("making wsapp")

    def __repr__(self):
        if self.alive:
            if self._ready:
                status = "connected"
            else:
                status = "connecting"
        else:
            status = "closed"
        return f"<WSApp host={self._address!r} {status=} sendqueue={self._send_queue.qsize()}>"

    @property
    def alive(self):
        return self._alive and self._ws_thread.is_alive()

    def _run_websocket_event_loop(self):
        backoff = 0
        while True:
            attempt_start = time.perf_counter()
            connection_failure = self._ws.run_forever(ping_interval=30, ping_timeout=10)
            if not connection_failure:
                break
            if (time.perf_counter() - attempt_start) < 5:
                # rapid connection cycling
                backoff = min(120, backoff * 2 + 1)
            else:
                backoff = 0
            time.sleep(backoff)
        log.info("Websocket connection closed")
        self._alive = False

    def _send_queue_feeder(self):
        log.debug("Websocket send-queue-feeder thread starting")
        while self.alive:
            element = self._send_queue.get()
            if element is None:
                self._send_queue.task_done()
                continue
            while not self._ready:
                time.sleep(0.3)
            try:
                self._ws.send(element)
            except Exception:
                log.error("Error sending message through websocket", exc_info=True)
            self._send_queue.task_done()
        log.debug("Websocket send-queue-feeder thread stopped")

    def _receive_msgs(self):
        while self.alive:
            element = self._receive_queue.get()
            if element is None:
                self._send_queue.task_done()
                continue
            while not self._ready:
                time.sleep(0.3)
            try:
                self._handle_msg(element)
            except json.decoder.JSONDecodeError:
                pass
            self._receive_queue.task_done()

    def close(self):
        log.info("Closing websocket connection")
        if self._feeder_thread.is_alive():
            self._send_queue.join()
        self._alive = False
        if self._feeder_thread.is_alive():
            self._send_queue.put(None)
            self._feeder_thread.join()
            self._receiver_thread.join()
        try:
            self._ws.close()
        except Exception:
            log.error("Error closing websocket connection", exc_info=True)

    def on_message(self, ws: websocket.WebSocketApp, message: str):
        self._receive_queue.put(message)

    def _handle_msg(self, message: str):
        data = json.loads(message)
        if data.get("message") == "state-update":
            self._register_id(data["attribute"], data["value"])
        elif data.get("message") == "state-update-partial":
            self._register_id_partial(data["attribute"], data["value"])

    def _register_id(self, attribute: str, value):
        if self.environment and hasattr(self.environment, attribute):
            setattr(self.environment, attribute, value)

    def _register_id_partial(self, attribute: str, value):
        if self.environment and hasattr(self.environment, attribute):
            if isinstance(value, dict):
                new_value = {**getattr(self.environment, attribute), **value}
                setattr(
                    self.environment,
                    attribute,
                    new_value,
                )

    def on_error(self, ws: websocket.WebSocketApp, error: websocket.WebSocketException):
        log.error(str(error))

    def on_close(self, ws: websocket.WebSocketApp, close_status_code, close_msg):
        self._ready = False
        if close_status_code or close_msg:
            log.debug(f"Websocket closed (code={close_status_code}, msg={close_msg})")
        else:
            log.debug("Websocket closed")

    def on_open(self, ws: websocket.WebSocketApp):
        log.info("Opened connection")
        self._ready = True

    def send(self, message: str):
        if self.alive:
            self._send_queue.put_nowait(message)
