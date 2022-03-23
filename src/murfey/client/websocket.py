from __future__ import annotations

import json
import logging
import queue
import random
import threading
import time
import urllib.parse
from typing import Optional

import websocket

log = logging.getLogger("murfey.client.websocket")


class WSApp:
    def __init__(self, *, server: str):
        id = random.randint(0, 100)
        log.info(f"Opening websocket connection for Client {id}")
        websocket.enableTrace(True)
        url = urllib.parse.urlparse(server)._replace(scheme="ws", path="")
        self._address = url.geturl()
        self._alive = True
        self._ready = False
        self._send_queue: queue.Queue[Optional[str]] = queue.Queue()
        self._ws = websocket.WebSocketApp(
            url._replace(path=f"/ws/test/{id}").geturl(),
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
        teardown = self._ws.run_forever()
        if teardown:
            log.error("Exception raised in websocket event loop")
        else:
            log.info("Websocket connection closed")
        self._alive = False

    def _send_queue_feeder(self):
        log.info("Websocket send-queue-feeder thread starting")
        while self.alive:
            element = self._send_queue.get()
            if element is None:
                self._send_queue.task_done()
                continue
            while not self._ready:
                time.sleep(0.3)
            self._ws.send(element)
            self._send_queue.task_done()
        log.info("Websocket send-queue-feeder thread stopped")

    def close(self):
        log.info("Closing websocket connection")
        if self._feeder_thread.is_alive():
            self._send_queue.join()
        self._alive = False
        if self._feeder_thread.is_alive():
            self._send_queue.put(None)
            self._feeder_thread.join()
        self._ws.close()

    def on_message(self, ws: websocket.WebSocketApp, message: str):
        log.info(f"Received message: {message!r}")
        try:
            data = json.loads(message)
            log.info(f"Interpreted data as {data!r}")
        except Exception:
            pass

    def on_error(self, ws: websocket.WebSocketApp, error: websocket.WebSocketException):
        log.error(error)

    def on_close(self, ws: websocket.WebSocketApp, close_status_code, close_msg):
        log.info(f"Connection closed due to {close_status_code}, {close_msg}")
        self._ws.close()

    def on_open(self, ws: websocket.WebSocketApp):
        log.info("Opened connection")
        self._ready = True

    def send(self, thing: str):
        if self.alive:
            self._send_queue.put_nowait(thing)
