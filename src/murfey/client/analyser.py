"""
Contains functions for analysing the various types of files hauled by Murfey, and
assigning to them the correct contexts (CLEM, SPA, tomography, etc.) for processing
on the server side.

Individual contexts can be found in murfey.client.contexts.
"""

from __future__ import annotations

import logging
import queue
import threading
from pathlib import Path
from typing import Type

from murfey.client.context import Context
from murfey.client.contexts.clem import CLEMContext
from murfey.client.contexts.spa import SPAModularContext
from murfey.client.contexts.spa_metadata import SPAMetadataContext
from murfey.client.contexts.tomo import TomographyContext
from murfey.client.contexts.tomo_metadata import TomographyMetadataContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.rsync import RSyncerUpdate, TransferResult
from murfey.util.client import Observer, get_machine_config_client
from murfey.util.mdoc import get_block
from murfey.util.models import ProcessingParametersSPA, ProcessingParametersTomo

logger = logging.getLogger("murfey.client.analyser")


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
        self._extension: str = ""
        self._unseen_xml: list = []
        self._context: Context | None = None
        self._batch_store: dict = {}
        self._environment = environment
        self._force_mdoc_metadata = force_mdoc_metadata
        self.parameters_model: (
            Type[ProcessingParametersSPA] | Type[ProcessingParametersTomo] | None
        ) = None

        self.queue: queue.Queue = queue.Queue()
        self.thread = threading.Thread(name="Analyser", target=self._analyse)
        self._stopping = False
        self._halt_thread = False
        self._murfey_config = (
            get_machine_config_client(
                str(environment.url.geturl()),
                instrument_name=environment.instrument_name,
                demo=environment.demo,
            )
            if environment
            else {}
        )

    def __repr__(self) -> str:
        return f"<Analyser ({self._basepath})>"

    def _find_extension(self, file_path: Path) -> bool:
        """
        Identifies the file extension and stores that information in the class.
        """
        if "atlas" in file_path.parts:
            self._extension = file_path.suffix
            return True

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
        # Check for LIF files separately
        elif file_path.suffix == ".lif":
            self._extension = file_path.suffix
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
        if "atlas" in file_path.parts:
            self._context = SPAMetadataContext("epu", self._basepath)
            return True

        # CLEM workflow checks
        # Look for LIF and XLIF files
        if file_path.suffix in (".lif", ".xlif"):
            self._context = CLEMContext("leica", self._basepath)
            return True
        # Look for TIFF files associated with CLEM workflow
        # Leica's autosave mode seems to name the TIFFs in the format
        # PostionXX--ZXX-CXX.tif
        if (
            "--" in file_path.name
            and file_path.suffix in (".tiff", ".tif")
            and self._environment
        ):
            created_directories = set(
                get_machine_config_client(
                    str(self._environment.url.geturl()),
                    instrument_name=self._environment.instrument_name,
                    demo=self._environment.demo,
                ).get("analyse_created_directories", [])
            )
            if created_directories.intersection(set(file_path.parts)):
                self._context = CLEMContext("leica", self._basepath)
                return True

        # Tomography and SPA workflow checks
        split_file_name = file_path.name.split("_")
        if split_file_name:
            # Files starting with "FoilHole" belong to the SPA workflow
            if split_file_name[0].startswith("FoilHole"):
                if not self._context:
                    logger.info("Acquisition software: EPU")
                    self._context = SPAModularContext("epu", self._basepath)
                self.parameters_model = ProcessingParametersSPA
                return True

            # Files starting with "Position" belong to the standard tomography workflow
            if (
                split_file_name[0] == "Position"
                or "[" in file_path.name
                or "Fractions" in split_file_name[-1]
                or "fractions" in split_file_name[-1]
                or "EER" in split_file_name[-1]
            ):
                if not self._context:
                    logger.info("Acquisition software: tomo")
                    self._context = TomographyContext("tomo", self._basepath)
                    self.parameters_model = ProcessingParametersTomo
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

    def _analyse(self):
        logger.info("Analyser thread started")
        mdoc_for_reading = None
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
            if self._limited:
                if (
                    "Metadata" in transferred_file.parts
                    or transferred_file.name == "EpuSession.dm"
                    and not self._context
                ):
                    self._context = SPAMetadataContext("epu", self._basepath)
                elif (
                    "Batch" in transferred_file.parts
                    or "SearchMaps" in transferred_file.parts
                    or transferred_file.name == "Session.dm"
                    and not self._context
                ):
                    self._context = TomographyMetadataContext("tomo", self._basepath)
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
                    valid_extension = self._find_extension(transferred_file)
                    if not valid_extension:
                        logger.error(f"No extension found for {transferred_file}")
                        continue
                    found = self._find_context(transferred_file)
                    if not found:
                        logger.debug(
                            f"Couldn't find context for {str(transferred_file)!r}"
                        )
                        self.queue.task_done()
                        continue
                    elif self._extension:
                        logger.info(
                            f"Context found successfully for {transferred_file}"
                        )
                        try:
                            self._context.post_first_transfer(
                                transferred_file,
                                environment=self._environment,
                            )
                        except Exception as e:
                            logger.error(f"Exception encountered: {e}")
                        if "atlas" not in transferred_file.parts:
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
                                except ValueError as e:
                                    logger.error(
                                        f"Metadata gathering failed with a value error: {e}"
                                    )
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
                                    }
                                )

                # If a file with a CLEM context is identified, immediately post it
                elif isinstance(self._context, CLEMContext):
                    logger.debug(
                        f"File {transferred_file.name!r} will be processed as part of CLEM workflow"
                    )
                    self.post_transfer(transferred_file)

                # Handle files with tomography and SPA context differently
                elif not self._extension or self._unseen_xml:
                    valid_extension = self._find_extension(transferred_file)
                    if not valid_extension:
                        logger.error(f"No extension found for {transferred_file}")
                        continue
                    if self._extension:
                        logger.info(
                            f"Extension found successfully for {transferred_file}"
                        )
                        try:
                            self._context.post_first_transfer(
                                transferred_file,
                                environment=self._environment,
                            )
                        except Exception as e:
                            logger.error(f"Exception encountered: {e}")
                        if "atlas" not in transferred_file.parts:
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
                                    }
                                )
                elif isinstance(
                    self._context,
                    (
                        SPAModularContext,
                        SPAMetadataContext,
                        TomographyContext,
                        TomographyMetadataContext,
                    ),
                ):
                    context = str(self._context).split(" ")[0].split(".")[-1]
                    logger.debug(
                        f"Transferring file {str(transferred_file)} with context {context!r}"
                    )
                    self.post_transfer(transferred_file)
            self.queue.task_done()
        self.notify(final=True)

    def _xml_file(self, data_file: Path) -> Path:
        if not self._environment:
            return data_file.with_suffix(".xml")
        file_name = f"{'_'.join(p for p in data_file.stem.split('_')[:-1])}.xml"
        data_directories = self._murfey_config.get("data_directories", [])
        for dd in data_directories:
            if str(data_file).startswith(dd):
                base_dir = Path(dd).absolute()
                mid_dir = data_file.relative_to(base_dir).parent
                break
        else:
            return data_file.with_suffix(".xml")
        return base_dir / self._environment.visit / mid_dir / file_name

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

    def stop(self):
        logger.debug("Analyser thread stop requested")
        self._stopping = True
        self._halt_thread = True
        try:
            if self.thread.is_alive():
                self.queue.put(None)
                self.thread.join()
        except Exception as e:
            logger.error(f"Exception encountered while stopping analyser: {e}")
        logger.debug("Analyser thread stop completed")
