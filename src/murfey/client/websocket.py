from __future__ import annotations

import json
import logging
import queue
import random
import threading
import time
import urllib.parse
from pathlib import Path
from typing import List, Optional

import websocket

log = logging.getLogger("murfey.client.websocket")

_lock = threading.RLock()


def registration_engine(env, attribute, old_value, value):
    if attribute == "data_collection_group_id":
        for l in env.listeners.get("data_collection_group_id", []):
            l(ws=env.websocket)
    elif attribute == "data_collection_ids":
        for l in env.listeners.get("data_collection_ids", []):
            if env.data_collection_ids:
                for k in set(old_value.keys()) ^ set(value.keys()):
                    log.info(f"calling callback with {k}, data collections exist")
                    l(k, ws=env.websocket)
            else:
                for k in value.keys():
                    l(k, ws=env.websocket)
    elif attribute == "autoproc_program_ids":
        for l in env.listeners.get("autoproc_program_ids", []):
            if env.autoproc_program_ids:
                for k in set(old_value.keys()) ^ set(value.keys()):
                    if value[k].get("em-tomo-preprocess"):
                        l(k, value[k]["em-tomo-preprocess"], ws=env.websocket)
            else:
                for k in value.keys():
                    if value[k].get("em-tomo-preprocess"):
                        l(k, value[k]["em-tomo-preprocess"], ws=env.websocket)
    elif attribute == "motion_corrected_movies":
        _url = f"{str(env.url.geturl())}/visits/{env.visit}/align"
        value = {Path(k): v for k, v in value.items()}
        for l in env.listeners.get("motion_corrected_movies", []):
            if old_value:
                for k in set(old_value.keys()) ^ set(
                    value.keys()
                ):  # k is a path (key), v[k] is the value matching the key
                    tilt = env.movie_tilt_pair[Path(k)]
                    file_tilt_list = []
                    for movie, angle in env.tilt_angles[tilt]:
                        if Path(movie) in value:  # values["motion_corrected_movies"]:
                            file_tilt_list.append(
                                [
                                    str(value[Path(movie)][0]),
                                    angle,
                                    str(value[Path(movie)][1]),
                                ]
                            )
                    l(
                        k,
                        value[Path(k)][0],
                        _url,
                        env.data_collection_ids[tilt],
                        env.processing_job_ids[tilt]["em-tomo-align"],
                        env.autoproc_program_ids[tilt]["em-tomo-align"],
                        value[Path(k)][1],
                        file_tilt_list,
                        env.tilt_offset,
                    )
            else:
                for k in value.keys():
                    try:
                        # possible race condition here where values accessing by [k] sometimes aren't ready when we
                        # try to access them - it throws a key error for a value which has just been set.
                        tilt = env.movie_tilt_pair[k]
                        file_tilt_list = []
                        for movie, angle in env.tilt_angles[tilt]:
                            file_tilt_list.append(
                                [
                                    str(value[Path(movie)][0]),
                                    angle,
                                    str(value[Path(movie)][1]),
                                ]
                            )  # or v(k)
                        l(
                            k,
                            value[k][0],
                            _url,
                            env.data_collection_ids[tilt],
                            env.processing_job_ids[tilt]["em-tomo-align"],
                            env.autoproc_program_ids[tilt]["em-tomo-align"],
                            value[k][1],
                            file_tilt_list,
                            env.tilt_offset,
                        )
                    except KeyError:
                        pass
                    except Exception as e:
                        log.warning(f"ERROR {e}")


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
        self.environment = None  # type: ignore
        self._paused = False
        self._msg_cache: List[str] = []

    def __repr__(self):
        if self.alive:
            if self._ready:
                status = "connected"
            else:
                status = "connecting"
        else:
            status = "closed"
        return f"<WSApp host={self._address!r} {status=} sendqueue={self._send_queue.qsize()}>"

    def pause(self):
        self._paused = True

    def resume(self):
        self._paused = False

    @property
    def alive(self):
        return self._alive and self._ws_thread.is_alive()

    def _run_websocket_event_loop(self):
        backoff = 0
        while True:
            attempt_start = time.perf_counter()
            connection_failure = self._ws.run_forever()
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
            self._ws.send(element)
            self._send_queue.task_done()
        log.debug("Websocket send-queue-feeder thread stopped")

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
        # log.info(f"Received message: {message!r}")
        try:
            if self._paused:
                with _lock:
                    self._msg_cache.append(message)
            else:
                self.clear_cache()
                if self._msg_cache:
                    for m in self._msg_cache:
                        self._handle_msg(m)
                self._handle_msg(message)
        except Exception:
            pass

    def _handle_msg(self, msg: str):
        data = json.loads(msg)
        if data.get("message") == "state-update":
            self._register_id(data["attribute"], data["value"])
        elif data.get("message") == "state-update-partial":
            self._register_id_partial(data["attribute"], data["value"])

    def _register_id(self, attribute: str, value):
        if self.environment and hasattr(self.environment, attribute):
            old_value = getattr(self.environment, attribute)
            setattr(self.environment, attribute, value)
            try:
                registration_engine(self.environment, attribute, old_value, value)
            except Exception as e:
                log.info(f"Exception encountered in registration: {e=}, {type(e)=}")
                raise

    def _register_id_partial(self, attribute: str, value):
        if self.environment and hasattr(self.environment, attribute):
            if isinstance(value, dict):
                new_value = {**getattr(self.environment, attribute), **value}
                old_value = getattr(self.environment, attribute)
                setattr(
                    self.environment,
                    attribute,
                    new_value,
                )
                try:
                    registration_engine(
                        self.environment, attribute, old_value, new_value
                    )
                except Exception as e:
                    log.info(f"Exception encountered in registration: {e=}, {type(e)=}")
                    raise

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

    def recv(self):
        try:
            msg = self._ws.sock.recv()
            self._handle_msg(msg)
        except websocket.WebSocketTimeoutException:
            return

    def clear_cache(self):
        with _lock:
            for m in self._msg_cache:
                self._handle_msg(m)
            self._msg_cache = []
