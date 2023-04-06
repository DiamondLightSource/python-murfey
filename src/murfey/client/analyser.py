from __future__ import annotations

import logging
import queue
import threading
from pathlib import Path
from typing import Type

from murfey.client.context import Context, SPAContext, TomographyContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.rsync import RSyncerUpdate
from murfey.client.tui.forms import TUIFormValue
from murfey.util import Observer, get_machine_config
from murfey.util.models import DCParametersSPA, DCParametersTomo

logger = logging.getLogger("murfey.client.analyser")


class Analyser(Observer):
    def __init__(
        self,
        basepath_local: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        force_mdoc_metadata: bool = False,
    ):
        super().__init__()
        self._basepath = basepath_local.absolute()
        self._experiment_type = ""
        self._acquisition_software = ""
        self._role = ""
        self._extension: str = ""
        self._unseen_xml: list = []
        self._context: Context | None = None
        self._batch_store: dict = {}
        self._environment = environment
        self._force_mdoc_metadata = force_mdoc_metadata
        self.parameters_model: Type[DCParametersSPA] | Type[
            DCParametersTomo
        ] | None = None

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
        elif file_path.suffix in (".tiff", ".tif", ".eer"):
            logger.info(f"File extension re-evaluated: {file_path.suffix}")
            self._extension = file_path.suffix

    def _find_context(self, file_path: Path) -> bool:
        split_file_name = file_path.name.split("_")
        if split_file_name:
            if split_file_name[0].startswith("FoilHole"):
                if not self._context:
                    logger.info("Acquisition software: EPU")
                    self._context = SPAContext("epu", self._basepath)
                self.parameters_model = DCParametersSPA
                if not self._role:
                    self._role = "detector"
                return True
            if (
                split_file_name[0] == "Position"
                or "[" in file_path.name
                or "Fractions" in split_file_name[-1]
                or "fractions" in split_file_name[-1]
            ):
                if not self._context:
                    logger.info("Acquisition software: tomo")
                    self._context = TomographyContext("tomo", self._basepath)
                    self.parameters_model = DCParametersTomo
                if not self._role:
                    if (
                        "Fractions" in split_file_name[-1]
                        or "fractions" in split_file_name[-1]
                    ):
                        self._role = "detector"
                    elif (
                        file_path.suffix == ".mdoc"
                        or file_path.with_suffix(".mdoc").is_file()
                    ):
                        self._role = "microscope"
                    else:
                        self._role = "detector"
                return True
            if file_path.suffix in (".mrc", ".tiff", ".tif", ".eer"):
                if file_path.with_suffix(".jpg").is_file():
                    return False
                self._context = TomographyContext("serialem", self._basepath)
                self.parameters_model = DCParametersTomo
                if not self._role:
                    if "Frames" in file_path.parts:
                        self._role = "detector"
                    else:
                        self._role = "microscope"
                return True
        return False

    def _analyse(self):
        logger.info("Analyser thread started")
        mdoc_for_reading = None
        while not self._halt_thread:
            transferred_file = self.queue.get()
            logger.info(f"analysing file {transferred_file}")
            if not transferred_file:
                self._halt_thread = True
                continue
            dc_metadata = {}
            if (
                self._force_mdoc_metadata
                and transferred_file.suffix == ".mdoc"
                or mdoc_for_reading
            ):
                if self._context:
                    dc_metadata = self._context.gather_metadata(
                        mdoc_for_reading or transferred_file,
                        environment=self._environment,
                    )
                elif transferred_file.suffix == ".mdoc":
                    mdoc_for_reading = transferred_file
            if not self._context:
                self._find_extension(transferred_file)
                found = self._find_context(transferred_file)
                if not found:
                    # logger.warning(
                    #     f"Context not understood for {transferred_file}, stopping analysis"
                    # )
                    continue
                elif self._extension:
                    logger.info(f"Context found successfully: {self._role}")
                    try:
                        self._context.post_first_transfer(
                            transferred_file,
                            role=self._role,
                            environment=self._environment,
                        )
                    except Exception as e:
                        logger.info(f"exception encountered {e}")
                    if self._role == "detector":
                        if not dc_metadata:
                            try:
                                dc_metadata = self._context.gather_metadata(
                                    transferred_file.with_suffix(".mdoc")
                                    if self._context._acquisition_software == "serialem"
                                    else self._xml_file(transferred_file),
                                    environment=self._environment,
                                )
                            except NotImplementedError:
                                dc_metadata = {}
                        if not dc_metadata or not self._force_mdoc_metadata:
                            self._unseen_xml.append(transferred_file)
                        else:
                            self._unseen_xml = []
                            if dc_metadata.get("file_extension"):
                                self._extension = dc_metadata["file_extension"].data
                            else:
                                dc_metadata["file_extension"] = TUIFormValue(
                                    self._extension
                                )
                            dc_metadata["acquisition_software"] = TUIFormValue(
                                self._context._acquisition_software
                            )
                            self.notify({"form": dc_metadata})
            elif not self._extension or self._unseen_xml:
                self._find_extension(transferred_file)
                if self._extension:
                    logger.info(
                        f"Context found successfully: {self._role}, {transferred_file}"
                    )
                    try:
                        self._context.post_first_transfer(
                            transferred_file,
                            role=self._role,
                            environment=self._environment,
                        )
                    except Exception as e:
                        logger.info(f"exception encountered {e}")
                    if self._role == "detector":
                        if not dc_metadata:
                            dc_metadata = self._context.gather_metadata(
                                mdoc_for_reading or self._xml_file(transferred_file),
                                environment=self._environment,
                            )
                        if not dc_metadata or not self._force_mdoc_metadata:
                            self._unseen_xml.append(transferred_file)
                        if dc_metadata:
                            self._unseen_xml = []
                            if dc_metadata.get("file_extension"):
                                self._extension = dc_metadata["file_extension"].data
                            else:
                                dc_metadata["file_extension"] = TUIFormValue(
                                    self._extension
                                )
                            dc_metadata["acquisition_software"] = TUIFormValue(
                                self._context._acquisition_software
                            )
                            self.notify({"form": dc_metadata})
            elif isinstance(self._context, TomographyContext):
                _tilt_series = set(self._context._tilt_series.keys())
                self._context.post_transfer(
                    transferred_file, role=self._role, environment=self._environment
                )
                if (
                    len(self._context._tilt_series.keys()) > len(_tilt_series)
                    and self._role == "detector"
                ):
                    if not dc_metadata:
                        dc_metadata = self._context.gather_metadata(
                            self._xml_file(transferred_file),
                            environment=self._environment,
                        )
                    self.notify({"form": dc_metadata})

    def _xml_file(self, data_file: Path) -> Path:
        if (fxml := data_file.with_suffix(".xml")).is_file() or not self._environment:
            return fxml
        file_name = (
            f"{data_file.stem.replace('_fractions', '').replace('_Fractions', '')}.xml"
        )
        data_directories = get_machine_config(self._environment.url.geturl()).get(
            "data_directories", {}
        )
        for dd in data_directories.keys():
            if str(data_file).startswith(str(dd)):
                base_dir = dd
                mid_dir = data_file.relative_to(dd).parent
                break
        else:
            return data_file.with_suffix(".xml")
        return base_dir / self._environment.visit / mid_dir / file_name

    def enqueue(self, rsyncer: RSyncerUpdate):
        if not self._stopping:
            absolute_path = (self._basepath / rsyncer.file_path).resolve()
            self.queue.put(absolute_path)

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
