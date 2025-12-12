from __future__ import annotations

import json
import logging
import threading
import time
from queue import Empty, Queue

import requests


class CustomHandler(logging.Handler):
    def __init__(self, callback):
        """Set up a handler instance, record the callback function."""
        super().__init__()
        self._callback = callback

    def prepare(self, record):
        self.format(record)
        record_dict = record.__dict__
        record_dict["type"] = "log"
        try:
            return json.dumps(record_dict)
        except TypeError:
            return json.dumps({str(k): str(v) for k, v in record_dict.items()})

    def emit(self, record):
        try:
            self._callback(self.prepare(record))
        except Exception:
            self.handleError(record)


class HTTPSHandler(logging.Handler):
    """
    A log handler collects log messages and posts them in batches to the backend
    FastAPI server using HTTPS POST.
    """

    def __init__(
        self,
        endpoint_url: str,
        min_batch: int = 5,
        max_batch: int = 50,
        min_interval: float = 0.5,
        max_interval: float = 2.0,
        max_retry: int = 5,
        timeout: int = 3,
        token: str = "",
    ):
        super().__init__()
        self.endpoint_url = endpoint_url
        self.queue: Queue = Queue()
        self._stop_event = threading.Event()
        self.min_batch = min_batch
        self.max_batch = max_batch
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.max_retry = max_retry
        self.timeout = timeout
        self.token = token

        self.log_times: list[
            float
        ] = []  # Timestamps of recent logs for rate estimation
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()

    def emit(self, record: logging.LogRecord):
        """
        Formats the log and puts it on a queue for submission to the backend server
        """
        try:
            log_entry = self.format_record(record)
            self.queue.put(log_entry)
            self.log_times.append(time.time())
        except Exception:
            self.handleError(record)
        pass

    def format_record(self, record: logging.LogRecord):
        """
        Packages the log record as a JSON-formatted string
        """
        self.format(record)
        log_data = record.__dict__.copy()
        log_data["type"] = "log"
        return json.dumps(log_data)

    def _worker(self):
        """
        Worker function that sends batches of logs to the URL endpoint specified,
        with logic to adjust how frequently it should batch and send logs depending
        on rate of incoming traffic
        """

        batch: list[str] = []
        last_flush = time.time()

        while not self._stop_event.is_set():
            try:
                log_entry = self.queue.get(timeout=0.05)
                batch.append(log_entry)
            except Empty:
                pass

            # Calculate logging rate
            now = time.time()
            self.log_times = [
                t for t in self.log_times if now - t <= 1.0
            ]  # Number of logs in last second
            log_rate = len(self.log_times)

            # Adjust batch size and flush interval
            batch_size = min(max(self.min_batch, log_rate), self.max_batch)
            flush_interval = max(
                self.min_interval, min(self.max_interval, 1 / max(log_rate, 1))
            )

            # Flush if batch is ready
            if batch and (
                len(batch) >= batch_size or now - last_flush >= flush_interval
            ):
                self._send_batch(batch)
                batch = []
                last_flush = now

        # Flush remaining logs on shutdown
        if batch:
            self._send_batch(batch)

    def _send_batch(self, batch: list[str]):
        """
        Submits the list of stringified log records to the URL endpoint specified.
        """
        for attempt in range(1, self.max_retry + 1):
            try:
                response = requests.post(self.endpoint_url, json=batch, timeout=5)
                if response.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(2**attempt * 0.1)  # Exponential backoff

    def close(self):
        self._stop_event.set()
        self.thread.join()
        super().close()
