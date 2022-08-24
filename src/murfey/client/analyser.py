from __future__ import annotations

import logging
import queue
import threading
from pathlib import Path

from murfey.client.context import Context, SPAContext, TomographyContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util import Observer

logger = logging.getLogger("murfey.client.analyser")


class Analyser(Observer):
    def __init__(self, environment: MurfeyInstanceEnvironment | None = None):
        super().__init__()
        self._experiment_type = ""
        self._acquisition_software = ""
        self._role = ""
        self._extension: str = ""
        self._unseen_xml: list = []
        self._context: Context | None = None
        self._batch_store: dict = {}
        self._environment = environment

        self.queue: queue.Queue = queue.Queue()
        self.thread = threading.Thread(name="Analyser", target=self._analyse)
        self._stopping = False
        self._halt_thread = False

    def _find_extension(self, file_path: Path):
        if (
            file_path.suffix in (".mrc", ".tiff", ".tif", ".eer")
            and not self._extension
        ):
            logger.info(f"File extension determined: {file_path.suffix}")
            self._extension = file_path.suffix

    def _find_context(self, file_path: Path) -> bool:
        split_file_name = file_path.name.split("_")
        if split_file_name:
            if split_file_name[0] == "Position" or "[" in file_path.name:
                self._context = TomographyContext("tomo")
                if split_file_name[-1].startswith("Fractions"):
                    self._role = "detector"
                elif (
                    file_path.suffix == ".mdoc"
                    or file_path.with_suffix(".mdoc").is_file()
                ):
                    self._role = "microscope"
                return True
            if split_file_name[0].startswith("FoilHole"):
                self._context = SPAContext("epu")
                self._role = "detector"
                return True
            if file_path.suffix in (".mrc", ".tiff", ".tif", ".eer"):
                self._context = TomographyContext("serialem")
                if "Frames" in file_path.parts:
                    self._role = "detector"
                else:
                    self._role = "microscope"
                return True
        return False

    def _analyse(self):
        logger.info("Analyser thread started")
        while not self._halt_thread:
            transferred_file = self.queue.get()
            if not transferred_file:
                self._halt_thread = True
                continue
            if (
                not self._context
            ):  # self._experiment_type or not self._acquisition_software:
                if not self._extension:
                    self._find_extension(transferred_file)
                found = self._find_context(transferred_file)
                if not found:
                    # logger.warning(
                    #     f"Context not understood for {transferred_file}, stopping analysis"
                    # )
                    continue
                elif self._extension:
                    logger.info(f"Context found successfully: {self._role}")
                    self._context.post_first_transfer(
                        transferred_file, role=self._role, environment=self._environment
                    )
                    if self._role == "detector":
                        try:
                            dc_metadata = self._context.gather_metadata(
                                transferred_file.with_suffix(".mdoc")
                                if self._context._acquisition_software == "serialem"
                                else transferred_file.with_suffix(".xml")
                            )
                        except NotImplementedError:
                            dc_metadata = {}
                        if not dc_metadata:
                            self._unseen_xml.append(transferred_file)
                            # continue
                        else:
                            self._unseen_xml = []
                            self.notify({"allowed_responses": ["y", "n"]})
                            dc_metadata["tilt"] = transferred_file.name.split("_")[1]
                            dc_metadata["file_extension"] = self._extension
                            dc_metadata[
                                "acquisition_software"
                            ] = self._context._acquisition_software
                            self.notify({"form": dc_metadata})
            elif not self._extension or self._unseen_xml:
                self._find_extension(transferred_file)
                if self._extension:
                    logger.info(
                        f"Context found successfully: {self._role}, {transferred_file}"
                    )
                    self._context.post_first_transfer(
                        transferred_file, role=self._role, environment=self._environment
                    )
                    if self._role == "detector":
                        dc_metadata = self._context.gather_metadata(
                            transferred_file.with_suffix(".xml")
                        )
                        if not dc_metadata:
                            self._unseen_xml.append(transferred_file)
                        else:
                            self._unseen_xml = []
                            self.notify({"allowed_responses": ["y", "n"]})
                            dc_metadata["tilt"] = transferred_file.name.split("_")[1]
                            dc_metadata["file_extension"] = self._extension
                            dc_metadata[
                                "acquisition_software"
                            ] = self._context._acquisition_software
                            self.notify({"form": dc_metadata})
            else:
                _tilt_series = set(self._context._tilt_series.keys())
                self._context.post_transfer(
                    transferred_file, role=self._role, environment=self._environment
                )
                if (
                    len(self._context._tilt_series.keys()) > len(_tilt_series)
                    and self._role == "detector"
                ):
                    dc_metadata = self._context.gather_metadata(
                        transferred_file.with_suffix(".xml")
                    )
                    self.notify({"form": dc_metadata})

    def enqueue(self, file_path: Path):
        if not self._stopping:
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
        try:
            if self.thread.is_alive():
                self.queue.put(None)
                self.thread.join()
        except Exception as e:
            logger.debug(f"Exception encountered while stopping analyser: {e}")
        logger.debug("Analyser thread stop completed")
