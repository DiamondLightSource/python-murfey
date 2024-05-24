from __future__ import annotations

import logging
import queue
import threading
from pathlib import Path
from typing import Type

from murfey.client.context import Context
from murfey.client.contexts.clem import CLEMContext
from murfey.client.contexts.spa import SPAContext, SPAModularContext
from murfey.client.contexts.spa_metadata import SPAMetadataContext
from murfey.client.contexts.tomo import TomographyContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.rsync import RSyncerUpdate, TransferResult
from murfey.client.tui.forms import FormDependency
from murfey.util import Observer, get_machine_config
from murfey.util.models import PreprocessingParametersTomo, ProcessingParametersSPA

logger = logging.getLogger("murfey.client.analyser")

spa_form_dependencies: dict = {
    "use_cryolo": FormDependency(
        dependencies={"estimate_particle_diameter": False}, trigger_value=False
    ),
    "estimate_particle_diameter": FormDependency(
        dependencies={
            "use_cryolo": True,
            "boxsize": "None",
            "small_boxsize": "None",
            "mask_diameter": "None",
            "particle_diameter": "None",
        },
        trigger_value=True,
    ),
}


class Analyser(Observer):
    def __init__(
        self,
        basepath_local: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        force_mdoc_metadata: bool = False,
        limited: bool = False,
    ):
        super().__init__()
        self._basepath = basepath_local.absolute()
        self._limited = limited
        self._experiment_type = ""
        self._acquisition_software = ""
        self._role = ""
        self._extension: str = ""
        self._unseen_xml: list = []
        self._context: Context | None = None
        self._batch_store: dict = {}
        self._environment = environment
        self._force_mdoc_metadata = force_mdoc_metadata
        self.parameters_model: (
            Type[ProcessingParametersSPA] | Type[PreprocessingParametersTomo] | None
        ) = None

        self.queue: queue.Queue = queue.Queue()
        self.thread = threading.Thread(name="Analyser", target=self._analyse)
        self._stopping = False
        self._halt_thread = False
        self._murfey_config = (
            get_machine_config(str(environment.url.geturl()), demo=environment.demo)
            if environment
            else {}
        )

    def _find_extension(self, file_path: Path):
        """
        Identifies the file extension and stores that information in the class.
        """
        if (
            required_substrings := self._murfey_config.get(
                "data_required_substrings", {}
            )
            .get(self._acquisition_software, {})
            .get(file_path.suffix)
        ):
            if not any(r in file_path.name for r in required_substrings):
                return []

        # Checks for MRC, TIFF, TIF, and EER files if no extension has been defined
        if (
            file_path.suffix in (".mrc", ".tiff", ".tif", ".eer")
            and not self._extension
        ):
            logger.info(f"File extension determined: {file_path.suffix}")
            self._extension = file_path.suffix
        # Check for TIFF, TIF, or EER if the file's already been assigned an extension
        elif (
            file_path.suffix in (".tiff", ".tif", ".eer")
            and self._extension != file_path.suffix
        ):
            logger.info(f"File extension re-evaluated: {file_path.suffix}")
            self._extension = file_path.suffix
        # Check for LIF files separately
        elif file_path.suffix == ".lif":
            self._extension = file_path.suffix

    def _find_context(self, file_path: Path) -> bool:
        """
        Using various conditionals, identifies what workflow the file is part of, and
        assigns the necessary context class to it for subsequent stages of processing
        """

        # CLEM workflow check
        # Look for LIF files
        if file_path.suffix == ".lif":
            self._role = "detector"
            self._context = CLEMContext("leica", self._basepath)
            return True

        split_file_name = file_path.name.split("_")
        if split_file_name:
            # Files starting with "FoilHole" belong to the SPA workflow
            if split_file_name[0].startswith("FoilHole"):
                if not self._context:
                    logger.info("Acquisition software: EPU")
                    if self._environment:
                        try:
                            cfg = get_machine_config(
                                str(self._environment.url.geturl()),
                                demo=self._environment.demo,
                            )
                        except Exception as e:
                            logger.warning(f"exception encountered: {e}")
                            cfg = {}
                    else:
                        cfg = {}
                    self._context = (
                        SPAModularContext("epu", self._basepath)
                        if cfg.get("modular_spa")
                        else SPAContext("epu", self._basepath)
                    )
                self.parameters_model = ProcessingParametersSPA
                # Assign it the detector attribute if not already present
                if not self._role:
                    self._role = "detector"
                return True

            # Files starting with "Position" belong to the standard tomography workflow
            if (
                split_file_name[0] == "Position"
                or "[" in file_path.name
                or "Fractions" in split_file_name[-1]
                or "fractions" in split_file_name[-1]
            ):
                if not self._context:
                    logger.info("Acquisition software: tomo")
                    self._context = TomographyContext("tomo", self._basepath)
                    self.parameters_model = PreprocessingParametersTomo
                # Assign role if not already present
                if not self._role:
                    # Fractions files attributed to the detector
                    if (
                        "Fractions" in split_file_name[-1]
                        or "fractions" in split_file_name[-1]
                    ):
                        self._role = "detector"
                    # MDOC files attributed to the microscope
                    elif (
                        file_path.suffix == ".mdoc"
                        or file_path.with_suffix(".mdoc").is_file()
                    ):
                        self._role = "microscope"
                    # Attribute all other files to the detector
                    else:
                        self._role = "detector"
                return True

            # Files with these suffixes belong to the serial EM tomography workflow
            if file_path.suffix in (".mrc", ".tiff", ".tif", ".eer"):
                # Ignore batch files and search maps
                if any(p in file_path.parts for p in ("Batch", "SearchMaps")):
                    return False
                # Ignore JPG files
                if file_path.with_suffix(".jpg").is_file():
                    return False
                # Ignore the averaged movies written out by the Falcon
                if (
                    len(
                        list(
                            file_path.parent.glob(
                                f"{file_path.name}*{file_path.suffix}"
                            )
                        )
                    )
                    > 1
                ):
                    return False
                self._context = TomographyContext("serialem", self._basepath)
                self.parameters_model = PreprocessingParametersTomo
                if not self._role:
                    if "Frames" in file_path.parts:
                        self._role = "detector"
                    else:
                        self._role = "microscope"
                return True
        return False

    def post_transfer(self, transferred_file: Path):
        try:
            if self._context:
                self._context.post_transfer(
                    transferred_file, role=self._role, environment=self._environment
                )
        except Exception as e:
            logger.error(f"An exception was encountered post transfer: {e}")

    def _analyse(self):
        logger.info("Analyser thread started")
        mdoc_for_reading = None
        while not self._halt_thread:
            transferred_file = self.queue.get()
            if not transferred_file:
                self._halt_thread = True
                continue
            if self._limited:
                if (
                    "Metadata" in transferred_file.parts
                    or transferred_file.name == "EpuSession.dm"
                    and not self._context
                ):
                    self._context = SPAMetadataContext("epu", self._basepath)
                self.post_transfer(transferred_file)
            else:
                dc_metadata = {}
                if (
                    self._force_mdoc_metadata
                    and transferred_file.suffix == ".mdoc"
                    or mdoc_for_reading
                ):
                    if self._context:
                        try:
                            dc_metadata = self._context.gather_metadata(
                                mdoc_for_reading or transferred_file,
                                environment=self._environment,
                            )
                        except KeyError as e:
                            logger.error(
                                f"Metadata gathering failed with a key error for key: {e.args[0]}"
                            )
                            raise e
                        if not dc_metadata:
                            mdoc_for_reading = None
                    elif transferred_file.suffix == ".mdoc":
                        mdoc_for_reading = transferred_file
                if not self._context:
                    self._find_extension(transferred_file)
                    found = self._find_context(transferred_file)
                    if not found:
                        # logger.warning(
                        #     f"Context not understood for {transferred_file}, stopping analysis"
                        # )
                        self.queue.task_done()
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
                            logger.warning(f"exception encountered {e}")
                        if self._role == "detector":
                            if not dc_metadata:
                                try:
                                    dc_metadata = self._context.gather_metadata(
                                        (
                                            transferred_file.with_suffix(".mdoc")
                                            if self._context._acquisition_software
                                            == "serialem"
                                            else self._xml_file(transferred_file)
                                        ),
                                        environment=self._environment,
                                    )
                                except NotImplementedError:
                                    dc_metadata = {}
                                except KeyError as e:
                                    logger.error(
                                        f"Metadata gathering failed with a key error for key: {e.args[0]}"
                                    )
                                    raise e
                            if not dc_metadata or not self._force_mdoc_metadata:
                                self._unseen_xml.append(transferred_file)
                            else:
                                self._unseen_xml = []
                                if dc_metadata.get("file_extension"):
                                    self._extension = dc_metadata["file_extension"]
                                else:
                                    dc_metadata["file_extension"] = self._extension
                                dc_metadata["acquisition_software"] = (
                                    self._context._acquisition_software
                                )
                                self.notify(
                                    {
                                        "form": dc_metadata,
                                        "dependencies": (
                                            spa_form_dependencies
                                            if isinstance(self._context, SPAContext)
                                            or isinstance(
                                                self._context, SPAModularContext
                                            )
                                            else {}
                                        ),
                                    }
                                )
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
                                try:
                                    dc_metadata = self._context.gather_metadata(
                                        mdoc_for_reading
                                        or self._xml_file(transferred_file),
                                        environment=self._environment,
                                    )
                                except KeyError as e:
                                    logger.error(
                                        f"Metadata gathering failed with a key error for key: {e.args[0]}"
                                    )
                                    raise e
                            if not dc_metadata or not self._force_mdoc_metadata:
                                mdoc_for_reading = None
                                self._unseen_xml.append(transferred_file)
                            if dc_metadata:
                                self._unseen_xml = []
                                if dc_metadata.get("file_extension"):
                                    self._extension = dc_metadata["file_extension"]
                                else:
                                    dc_metadata["file_extension"] = self._extension
                                dc_metadata["acquisition_software"] = (
                                    self._context._acquisition_software
                                )
                                self.notify(
                                    {
                                        "form": dc_metadata,
                                        "dependencies": (
                                            spa_form_dependencies
                                            if isinstance(self._context, SPAContext)
                                            or isinstance(
                                                self._context, SPAModularContext
                                            )
                                            else {}
                                        ),
                                    }
                                )
                elif isinstance(
                    self._context,
                    (
                        TomographyContext,
                        SPAModularContext,
                        SPAMetadataContext,
                        CLEMContext,
                    ),
                ):
                    self.post_transfer(transferred_file)
            self.queue.task_done()

    def _xml_file(self, data_file: Path) -> Path:
        if not self._environment:
            return data_file.with_suffix(".xml")
        file_name = f"{'_'.join(p for p in data_file.stem.split('_')[:-1])}.xml"
        data_directories = self._murfey_config.get("data_directories", {})
        for dd in data_directories.keys():
            if str(data_file).startswith(dd):
                base_dir = Path(dd)
                mid_dir = data_file.relative_to(dd).parent
                break
        else:
            return data_file.with_suffix(".xml")
        return base_dir / self._environment.visit / mid_dir / file_name

    def enqueue(self, rsyncer: RSyncerUpdate):
        if not self._stopping and rsyncer.outcome == TransferResult.SUCCESS:
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
