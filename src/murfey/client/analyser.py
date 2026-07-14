"""
Contains functions for analysing the various types of files hauled by Murfey, and
assigning to them the correct contexts (CLEM, SPA, tomography, etc.) for processing
on the server side.

Individual contexts can be found in murfey.client.contexts.
"""

from __future__ import annotations

import functools
import logging
import queue
import threading
from importlib.metadata import entry_points
from pathlib import Path
from typing import Type

from murfey.client.context import Context
from murfey.client.destinations import find_longest_data_directory
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.rsync import RSyncerUpdate, TransferResult
from murfey.util.client import Observer, get_machine_config_client
from murfey.util.mdoc import get_block
from murfey.util.models import ProcessingParametersSPA, ProcessingParametersTomo
from murfey.util.sim import SIM_DATA_SUFFIXES

logger = logging.getLogger("murfey.client.analyser")


# Load the Context entry points as a list upon initialisation
context_eps = list(entry_points(group="murfey.contexts"))


@functools.lru_cache(maxsize=1)
def _get_context(name: str):
    """
    Load the desired context from the configured list of entry points.
    Returns None if the entry point is not found
    """
    if context := [ep for ep in context_eps if ep.name == name]:
        return context[0]
    else:
        logger.warning(f"Could not find entry point for {name!r}")
        return None


class Analyser(Observer):
    def __init__(
        self,
        basepath_local: Path,
        token: str,
        environment: MurfeyInstanceEnvironment | None = None,
        force_mdoc_metadata: bool = False,
        limited: bool = False,
        serialem: bool = False,
    ):
        super().__init__()
        self._basepath = basepath_local.absolute()
        self._token = token
        self._environment = environment
        self._limited = limited
        self._experiment_type = ""
        self._acquisition_software = ""
        self._context: Context | None = None
        self.queue: queue.Queue = queue.Queue()
        self.thread = threading.Thread(
            name=f"Analyser {basepath_local}", target=self._analyse_in_thread
        )
        self._stopping = False
        self._halt_thread = False
        self._murfey_config = (
            get_machine_config_client(
                str(environment.url.geturl()),
                self._token,
                instrument_name=environment.instrument_name,
            )
            if environment
            else {}
        )

        # SPA & Tomo-specific attributes
        self._extension: str = ""
        self._processing_params_found: bool = (
            False  # Have the processing parameters been collected from the metadata?
        )
        # self._force_mdoc_metadata = force_mdoc_metadata  # Seems deprecated
        self._serialem = serialem
        self.parameters_model: (
            Type[ProcessingParametersSPA] | Type[ProcessingParametersTomo] | None
        ) = None

    def __repr__(self) -> str:
        return f"<Analyser ({self._basepath})>"

    def _find_extension(self, file_path: Path) -> bool:
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
                return False

        # Checks for MRC, TIFF, TIF, and EER files
        if file_path.suffix in (".mrc", ".tiff", ".tif", ".eer"):
            if not self._extension:
                logger.info(f"File extension determined: {file_path.suffix}")
                self._extension = file_path.suffix
            elif self._extension != file_path.suffix:
                logger.info(f"File extension re-evaluated: {file_path.suffix}")
                self._extension = file_path.suffix
            return True
        # If we see an .mdoc file first, use that to determine the file extensions
        elif file_path.suffix == ".mdoc":
            with open(file_path, "r") as md:
                md.seek(0)
                mdoc_data_block = get_block(md)
            if subframe_path := mdoc_data_block.get("SubFramePath"):
                self._extension = Path(subframe_path).suffix
                return True
        return False

    def _find_context(self, file_path: Path) -> bool:
        """
        Using various conditionals, identifies what workflow the file is part of, and
        assigns the correct context class to that batch of rsync files for subsequent
        stages of processing. Actions to take for individual files will be determined
        in the Context classes themselves.
        """
        logger.debug(f"Finding context using file {str(file_path)!r}")

        # -----------------------------------------------------------------------------
        # CLEM workflow checks
        # -----------------------------------------------------------------------------
        if (
            # Look for LIF and XLIF files
            file_path.suffix in (".lif", ".xlif")
            or (
                # TIFF files have "--Stage", "--Z", and/or "--C" in their file stem
                file_path.suffix in (".tiff", ".tif")
                and any(
                    pattern in file_path.stem for pattern in ("--Stage", "--Z", "--C")
                )
            )
        ):
            if (context := _get_context("CLEMContext")) is None:
                return False
            self._context = context.load()(
                "leica",
                self._basepath,
                self._murfey_config,
                self._token,
            )
            return True

        # -----------------------------------------------------------------------------
        # FIB workflow checks
        # -----------------------------------------------------------------------------
        # Determine if it's from AutoTEM
        if (
            # AutoTEM generates a "ProjectData.dat" file
            file_path.name == "ProjectData.dat"
            or (
                # Images are stored in ".../Sites/Lamella (N)/..."
                any(path.startswith("Lamella") for path in file_path.parts)
                and "Sites" in file_path.parts
            )
        ):
            if (context := _get_context("FIBContext")) is None:
                return False
            self._context = context.load()(
                "autotem",
                self._basepath,
                self._murfey_config,
                self._token,
            )
            return True

        # Determine if it's from Maps
        if (
            # Electron snapshot metadata in "EMproject.emxml"
            file_path.name == "EMproject.emxml"
            or (
                # Key images are stored in ".../LayersData/Layer/..."
                all(path in file_path.parts for path in ("LayersData", "Layer"))
            )
        ):
            if (context := _get_context("FIBContext")) is None:
                return False
            self._context = context.load()(
                "maps",
                self._basepath,
                self._murfey_config,
                self._token,
            )
            return True

        # Determine if it's from Meteor
        if (
            # Image metadata stored in "features.json" file
            file_path.name == "features.json" or ()
        ):
            if (context := _get_context("FIBContext")) is None:
                return False
            self._context = context.load()(
                "meteor",
                self._basepath,
                self._murfey_config,
                self._token,
            )
            return True

        # -----------------------------------------------------------------------------
        # SIM workflow checks
        # -----------------------------------------------------------------------------
        if (
            # CryoSIM raw data files have no extension, and end with specific suffixes
            not file_path.suffix and file_path.stem.endswith(SIM_DATA_SUFFIXES)
        ):
            if (context := _get_context("SIMContext")) is None:
                return False
            self._context = context.load()(
                "sim",
                self._basepath,
                self._murfey_config,
                self._token,
            )
            return True

        # -----------------------------------------------------------------------------
        # SXT workflow checks
        # -----------------------------------------------------------------------------
        if file_path.suffix in (".txrm", ".xrm"):
            if (context := _get_context("SXTContext")) is None:
                return False
            self._context = context.load()(
                "zeiss",
                self._basepath,
                self._murfey_config,
                self._token,
            )
            return True

        # -----------------------------------------------------------------------------
        # Tomography and SPA workflow checks
        # -----------------------------------------------------------------------------
        if "atlas" in file_path.parts:
            if (context := _get_context("AtlasContext")) is None:
                return False
            self._context = context.load()(
                "serialem" if self._serialem else "epu",
                self._basepath,
                self._murfey_config,
                self._token,
            )
            return True

        if (
            "Metadata" in file_path.parts
            or "gridsquares" in file_path.parts
            or file_path.name == "EpuSession.dm"
        ):
            if (context := _get_context("SPAMetadataContext")) is None:
                return False
            self._context = context.load()(
                "serialem" if self._serialem else "epu",
                self._basepath,
                self._murfey_config,
                self._token,
            )
            return True
        elif (
            "Batch" in file_path.parts
            or "SearchMaps" in file_path.parts
            or "Thumbnails" in file_path.parts
            or file_path.name == "Session.dm"
        ):
            if (context := _get_context("TomographyMetadataContext")) is None:
                return False
            self._context = context.load()(
                "tomo",
                self._basepath,
                self._murfey_config,
                self._token,
            )
            return True

        split_file_stem = file_path.stem.split("_")
        if split_file_stem:
            if split_file_stem[-1] == "gain":
                return False

            # Files starting with "FoilHole" belong to the SPA workflow
            if split_file_stem[0].startswith("FoilHole") and split_file_stem[-1] in [
                "Fractions",
                "fractions",
                "EER",
            ]:
                if not self._context:
                    logger.info("Acquisition software: EPU")
                    if (context := _get_context("SPAContext")) is None:
                        return False
                    self._context = context.load()(
                        "epu",
                        self._basepath,
                        self._murfey_config,
                        self._token,
                    )
                self.parameters_model = ProcessingParametersSPA
                return True

            # Files starting with "Position" belong to the standard tomography workflow
            # NOTE: not completely reliable, mdocs can be in tomography metadata as well
            if not self._serialem and (
                split_file_stem[0] == "Position"
                or "[" in file_path.name
                or split_file_stem[-1] in ["Fractions", "fractions", "EER"]
                or file_path.suffix == ".mdoc"
            ):
                if not self._context:
                    logger.info("Acquisition software: tomo")
                    if (context := _get_context("TomographyContext")) is None:
                        return False
                    self._context = context.load()(
                        "tomo",
                        self._basepath,
                        self._murfey_config,
                        self._token,
                    )
                    self.parameters_model = ProcessingParametersTomo
                return True
        return False

    def post_transfer(self, transferred_file: Path):
        try:
            if self._context:
                self._context.post_transfer(
                    transferred_file, environment=self._environment
                )
        except Exception as e:
            logger.error(
                f"An exception was encountered post transfer: {e}", exc_info=True
            )

    def _analyse_in_thread(self):
        """
        Class function that will be executed by the '_thread' attribute. It will
        execute a while-loop where it takes files off the queue and feeds them to
        the '_analyse' class function until '_halt_thread' is set to True.
        """
        logger.info("Analyser thread started")
        while not self._halt_thread:
            transferred_file = self.queue.get()
            transferred_file = (
                Path(transferred_file)
                if isinstance(transferred_file, str)
                else transferred_file
            )
            if not transferred_file:
                self._halt_thread = True
                continue
            self._analyse(transferred_file)
            self.queue.task_done()
        logger.debug("Analyser thread has stopped analysing incoming files")
        self.notify(final=True)

    def _analyse(self, transferred_file: Path):
        """
        Class function that is called by '_analyse_in_thread'. It will identify
        the Context class to use based on the files inspected, then run different
        processing logic based on the context that was established.
        """
        if self._limited:
            if (
                "Metadata" in transferred_file.parts
                or "gridsquares" in transferred_file.parts
                or transferred_file.name == "EpuSession.dm"
            ) and not self._context:
                if not (context := _get_context("SPAMetadataContext")):
                    return None
                self._context = context.load()(
                    "serialem" if self._serialem else "epu",
                    self._basepath,
                    self._murfey_config,
                    self._token,
                )
            elif (
                "Batch" in transferred_file.parts
                or "SearchMaps" in transferred_file.parts
                or transferred_file.name == "Session.dm"
            ) and not self._context:
                if not (context := _get_context("TomographyMetadataContext")):
                    return None
                self._context = context.load()(
                    "tomo",
                    self._basepath,
                    self._murfey_config,
                    self._token,
                )
            self.post_transfer(transferred_file)
        else:
            # Try and determine context, and notify once when context is found
            if self._context is None:
                # Exit early if the file can't be used to determine the context
                if not self._find_context(transferred_file):
                    logger.debug(f"Couldn't find context for {str(transferred_file)!r}")
                    return None
                logger.info(f"Context found successfully using {transferred_file}")

            # Extra if-block for MyPy to verify that the context is set by this point
            if self._context is None:
                logger.error("Failed to set context even after finding context")
                return None

            # Trigger processing and metadata parsing according to the context
            match self._context.name:
                case (
                    "AtlasContext"
                    | "CLEMContext"
                    | "FIBContext"
                    | "SIMContext"
                    | "SPAMetadataContext"
                    | "SXTContext"
                    | "TomographyMetadataContext"
                ):
                    logger.debug(
                        f"File {transferred_file.name!r} transferred with context {self._context.name}"
                    )
                    self.post_transfer(transferred_file)
                case "SPAContext":
                    logger.debug(
                        f"File {transferred_file.name!r} transferred with context {self._context.name}"
                    )
                    self.post_transfer(transferred_file)

                    # Find extension
                    if not self._extension:
                        if not self._find_extension(transferred_file):
                            logger.warning(f"No extension found for {transferred_file}")
                            return None
                        logger.info(
                            f"Extension found successfully for {transferred_file}"
                        )
                    if not self._processing_params_found:
                        # Try and gather the metadata from each file passing through
                        # Once gathered, set the attribute to True and don't repeat again
                        try:
                            dc_metadata = self._context.gather_metadata(
                                self._xml_file(transferred_file),
                                environment=self._environment,
                            )
                        except (KeyError, ValueError) as e:
                            logger.error(
                                f"Metadata gathering failed with the following error: {e}"
                            )
                            dc_metadata = None
                        if dc_metadata:
                            self._processing_params_found = True
                            if dc_metadata.get("file_extension"):
                                self._extension = dc_metadata["file_extension"]
                            else:
                                dc_metadata["file_extension"] = self._extension
                            dc_metadata["acquisition_software"] = (
                                self._context._acquisition_software
                            )
                            self.notify(dc_metadata)

                case "TomographyContext":
                    logger.debug(
                        f"File {transferred_file.name!r} transferred with context {self._context.name}"
                    )
                    self.post_transfer(transferred_file)

                    # Find extension
                    if not self._extension:
                        if not self._find_extension(transferred_file):
                            logger.warning(f"No extension found for {transferred_file}")
                            return None
                        logger.info(
                            f"Extension found successfully for {transferred_file}"
                        )
                    if (
                        not self._processing_params_found
                        and transferred_file.suffix == ".mdoc"
                    ):
                        # Try and gather the metadata from a passing .mdoc file
                        # When gathered, set the attribute to True and don't repeat again
                        try:
                            dc_metadata = self._context.gather_metadata(
                                transferred_file,
                                environment=self._environment,
                            )
                        except (KeyError, ValueError) as e:
                            logger.error(
                                f"Metadata gathering failed with the following error: {e}"
                            )
                            dc_metadata = None
                        if dc_metadata:
                            self._processing_params_found = True
                            if dc_metadata.get("file_extension"):
                                self._extension = dc_metadata["file_extension"]
                            else:
                                dc_metadata["file_extension"] = self._extension
                            dc_metadata["acquisition_software"] = (
                                self._context._acquisition_software
                            )
                            self.notify(dc_metadata)
                case _:
                    logger.warning(f"Unknown context provided: {str(self._context)}")
        return None

    def _xml_file(self, data_file: Path) -> Path:
        if not self._environment:
            return data_file.with_suffix(".xml")
        file_name = f"{'_'.join(p for p in data_file.stem.split('_')[:-1])}.xml"
        data_directories = self._murfey_config.get("data_directories", [])
        base_dir, mid_dir = find_longest_data_directory(data_file, data_directories)
        if not base_dir:
            return data_file.with_suffix(".xml")
        # Add the visit directory to the file path and return it
        # The file is moved from a location where the visit name is not part of its path
        return base_dir / self._environment.visit / (mid_dir or "") / file_name

    def enqueue(self, rsyncer: RSyncerUpdate):
        if not self._stopping and rsyncer.outcome == TransferResult.SUCCESS:
            absolute_path = (self._basepath / rsyncer.file_path).absolute()
            self.queue.put(absolute_path)

    def start(self):
        if self.thread.is_alive():
            raise RuntimeError("Analyser already running")
        if self._stopping:
            raise RuntimeError("Analyser has already stopped")
        logger.info(f"Analyser thread starting for {self}")
        self.thread.start()

    def request_stop(self):
        self._stopping = True
        self._halt_thread = True

    def is_safe_to_stop(self):
        """
        Checks that the analyser thread is safe to stop
        """
        return self._stopping and self._halt_thread and not self.queue.qsize()

    def stop(self):
        logger.debug("Analyser thread stop requested")
        self._stopping = True
        self._halt_thread = True
        try:
            if self.thread.is_alive():
                self.queue.put(None)
                self.thread.join()
        except Exception as e:
            logger.error(
                f"Exception encountered while stopping Analyser: {e}",
                exc_info=True,
            )
        logger.debug("Analyser thread stop completed")
