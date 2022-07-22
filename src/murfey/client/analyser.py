from __future__ import annotations

import logging
import queue
import threading
from pathlib import Path

from murfey.client.context import Context, SPAContext, TomographyContext
from murfey.client.rsync import RSyncerUpdate
from murfey.util import Observer

logger = logging.getLogger("murfey.client.analyser")


class Analyser(Observer):
    def __init__(self):
        super().__init__()
        self._experiment_type = ""
        self._acquisition_software = ""
        self._role = ""
        self._context: Context | None = None
        self._batch_store = {}

        self.queue = queue.Queue()
        self.thread = threading.Thread(name="Analyser", target=self._analyse)
        self._stopping = False
        self._halt_thread = False

    def _find_context(self, file_path: Path) -> bool:
        split_file_name = file_path.name.split("_")
        if split_file_name:
            if split_file_name[0] == "Position":
                self._context = TomographyContext("tomo")
                self._role = "detector"
                return True
            if split_file_name[0].startswith("FoilHole"):
                self._context = SPAContext("epu")
                self._role = "detector"
                return True
        return False

    def _analyse(self):
        logger.info("Analyser thread started")
        while not self._halt_thread:
            transferred_file = self.queue.get()
            logger.info(f"Analysing transferred file {transferred_file}")
            if not transferred_file:
                return
            if not self._experiment_type or not self._acquisition_software:
                found = self._find_context(transferred_file)
                if not found:
                    logger.warning(
                        f"Context not understood for {transferred_file}, stopping analysis"
                    )
                    self.stop()
                else:
                    self._context.post_first_transfer(transferred_file, role=self._role)
            else:
                self._context.post_transfer(transferred_file, role=self._role)

    def enqueue(self, update: RSyncerUpdate):
        if not self._stopping:
            file_path = Path(update.file_path)
            self.queue.put(file_path)

    def start(self):
        if self.thread.is_alive():
            raise RuntimeError("Analyser already running")
        if self._stopping:
            raise RuntimeError("Analyser has already stopped")
        logger.info(f"Analyser thread starting for {self}")
        self.thread.start()

    def stop(self):
        logger.debug("Analyser thread stop requested")
        self._stopping = True
        self._halt_thread = True
        if self.thread.is_alive():
            self.queue.put(None)
            self.thread.join()
        logger.debug("Analyser thread stop completed")
