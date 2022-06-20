from __future__ import annotations

import logging
import queue
import threading
from pathlib import Path
from typing import Optional

from murfey.client.context import Context, SPAContext, TomographyContext
from murfey.util import Observer

logger = logging.getLogger("murfey.client.analyser")


class Analyser(Observer):
    def __init__(self):
        super().__init__()
        self._experiment_type = ""
        self._acquisition_software = ""
        self._context: Context | None = None
        self._batch_store = {}

        self.queue = queue.Queue[Optional[Path]]()
        self.thread = threading.Thread(name="Analyser", target=self._analyse)
        self._stopping = False
        self._halt_thread = False

    def _find_context(self, file_path: Path) -> bool:
        split_file_name = file_path.name.split("_")
        if split_file_name:
            if split_file_name[0] == "Position":
                self._context = TomographyContext("tomo")
                return True
            if split_file_name[0].startswith("FoilHole"):
                self._context = SPAContext("epu")
                return True
        return False

    def _analyse(self):
        while not self._halt_thread:
            transferred_file = self.queue.get()
            if not self._experiment_type or not self._acquisition_software:
                found = self._find_context(transferred_file)
                if not found:
                    logger.warning(
                        f"Context not understood for {transferred_file}, stopping analysis"
                    )
                    self.stop()
                else:
                    self._context.post_first_transfer(transferred_file)
            else:
                self._context.post_transfer(transferred_file)

    def stop(self):
        logger.debug("Analyser thread stop requested")
        self._stopping = True
        self._halt_thread = True
        if self.thread.is_alive():
            self.queue.put(None)
            self.thread.join()
        logger.debug("Analyser thread stop completed")
