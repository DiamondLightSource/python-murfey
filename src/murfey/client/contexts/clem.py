"""
Provides instructions to the server side on how different file types associated with
the CLEM workflow should be processed.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List, Optional
from urllib.parse import quote
from xml.etree import ElementTree as ET

from defusedxml.ElementTree import parse

from murfey.client.context import Context
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util import capture_post, get_machine_config_client

# Create logger object
logger = logging.getLogger("murfey.client.contexts.clem")


def _file_transferred_to(
    environment: MurfeyInstanceEnvironment, source: Path, file_path: Path
) -> Optional[Path]:
    """
    Returns the Path of the transferred file on the DLS file system.
    """
    machine_config = get_machine_config_client(
        str(environment.url.geturl()),
        instrument_name=environment.instrument_name,
        demo=environment.demo,
    )
    # rsync basepath and modules are set in the microscope's configuration YAML file
    return (
        Path(machine_config.get("rsync_basepath", ""))
        / (
            machine_config.get("rsync_module", "data") or "data"
        )  # Add "data" if it wasn't set
        / str(datetime.now().year)
        / source.name
        / file_path.relative_to(source)
    )


def _get_source(
    file_path: Path, environment: MurfeyInstanceEnvironment
) -> Optional[Path]:
    """
    Returns the Path of the file on the client PC.
    """
    for s in environment.sources:
        if file_path.is_relative_to(s):
            return s
    return None


def _get_image_elements(root: ET.Element) -> List[ET.Element]:
    """
    Searches the XML metadata recursively to find the nodes tagged as "Element" that
    have image-related tags. Some LIF datasets have layers of nested elements, so a
    recursive approach is needed to avoid certain datasets breaking it.
    """

    # Nested function which generates list of elements
    def _find_elements_recursively(
        node: ET.Element,
    ) -> Generator[ET.Element, None, None]:

        # Find items labelled "Element" under current node
        elem_list = node.findall("./Children/Element")
        if len(elem_list) < 1:  # Try alternative path for top-level of XML tree
            elem_list = node.findall("./Element")

        # Recursively search for items tagged as Element under child branches
        for elem in elem_list:
            yield elem
            new_node = elem  # New starting point for the search
            new_elem_list = _find_elements_recursively(new_node)  # Call self
            for new_elem in new_elem_list:
                yield new_elem

    # Get initial list of elements
    elem_list = list(_find_elements_recursively(root))

    # Keep only the element nodes that have image-related tags
    elem_list = [elem for elem in elem_list if elem.find("./Data/Image")]

    return elem_list


class CLEMContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path):
        super().__init__("CLEM", acquisition_software)
        self._basepath = basepath
        # CLEM contexts for "auto-save" acquisition mode
        self._tiff_series: Dict[str, List[str]] = {}  # {Series name : TIFF path list}
        self._series_metadata: Dict[str, str] = {}  # {Series name : Metadata file path}
        self._files_in_series: Dict[str, int] = {}  # {Series name : Total TIFFs}

    def post_transfer(
        self,
        transferred_file: Path,
        role: str = "",
        environment: Optional[MurfeyInstanceEnvironment] = None,
        **kwargs,
    ) -> bool:

        super().post_transfer(
            transferred_file, role=role, environment=environment, **kwargs
        )

        # Process files generated by "auto-save" acquisition mode
        # These include TIF/TIFF and XLIF files
        if transferred_file.suffix in (".tif", ".tiff", ".xlif"):
            logger.debug(f"File extension {transferred_file.suffix!r} detected")

            # Type checking to satisfy MyPy
            if not environment:
                logger.warning("No environment passed in")
                return False

            # Location of the file on the client PC
            source = _get_source(transferred_file, environment)
            # Type checking to satisfy MyPy
            if not source:
                logger.warning(f"No source found for file {transferred_file}")
                return False

            # Get the file Path at the destination
            destination_file = _file_transferred_to(
                environment=environment,
                source=source,
                file_path=transferred_file,
            )
            if not destination_file:
                logger.warning(
                    f"File {transferred_file.name!r} not found on the storage system"
                )
                return False

            # Skip processing of binned "_pmd" image series
            if "_pmd_" in transferred_file.stem:
                logger.debug(
                    f"File {transferred_file.name!r} belongs to the '_pmd_' series of binned images; skipping processing"
                )
                return True

            # Process TIF/TIFF files
            if transferred_file.suffix in (".tif", ".tiff"):

                # Files should be named "PositionX--ZXX--CXX.tif" by default
                # If Position is repeated, it will add an additional --00X to the end
                if len(transferred_file.stem.split("--")) not in [3, 4]:
                    logger.warning(
                        f"File {transferred_file.name!r} is likely not part of the CLEM workflow"
                    )
                    return False

                logger.debug(
                    f"File {transferred_file.name!r} is part of a TIFF image series"
                )

                # Create a unique name for the series
                # For standard file name
                if len(transferred_file.stem.split("--")) == 3:
                    series_name = "/".join(
                        [
                            *destination_file.parent.parts[
                                -2:
                            ],  # Upper 2 parent directories
                            destination_file.stem.split("--")[0],
                        ]
                    )
                # When this a repeated position
                elif len(transferred_file.stem.split("--")) == 4:
                    series_name = "/".join(
                        [
                            *destination_file.parent.parts[
                                -2:
                            ],  # Upper 2 parent directories
                            "--".join(
                                destination_file.stem.split("--")[i] for i in [0, -1]
                            ),
                        ]
                    )
                else:
                    logger.error(
                        f"Series name could not be generated from file {transferred_file.name!r}"
                    )
                    return False
                logger.debug(
                    f"File {transferred_file.name!r} given the series name {series_name!r}"
                )

                # Create key-value pairs containing empty list if not already present
                if series_name not in self._tiff_series.keys():
                    self._tiff_series[series_name] = []
                # Append information to list
                self._tiff_series[series_name].append(str(destination_file))
                logger.debug(
                    f"Created TIFF file dictionary entries for {series_name!r}"
                )

                # Register the TIFF file in the database
                post_result = self.register_tiff_file(destination_file, environment)
                if post_result is False:
                    return False

            # Process XLIF files
            if transferred_file.suffix == ".xlif":

                # Skip processing of "_histo" histogram XLIF files
                if transferred_file.stem.endswith("_histo"):
                    logger.debug(
                        f"File {transferred_file.name!r} contains histogram metadata; skipping processing"
                    )
                    return True

                # Skip processing of "IOManagerConfiguation.xlif" files (yes, the typo IS part of the file name)
                if "IOManagerConfiguation" in transferred_file.stem:
                    logger.debug(
                        f"File {transferred_file.name!r} is a Leica configuration file; skipping processing"
                    )
                    return True

                logger.debug(
                    f"File {transferred_file.name!r} contains metadata for an image series"
                )

                # Create series name for XLIF file
                # XLIF files don't have the "--ZXX--CXX" additions in the file name
                # But they have "/Metadata/" as the immediate parent
                series_name = "/".join(
                    [*destination_file.parent.parent.parts[-2:], destination_file.stem]
                )  # The previous 2 parent directories should be unique enough
                logger.debug(
                    f"File {transferred_file.name!r} given the series name {series_name!r}"
                )

                # Extract metadata to get the expected size of the series
                metadata = parse(transferred_file).getroot()
                metadata = _get_image_elements(metadata)[0]

                # Get channel and dimension information
                channels = metadata.findall(
                    "Data/Image/ImageDescription/Channels/ChannelDescription"
                )
                dimensions = metadata.findall(
                    "Data/Image/ImageDescription/Dimensions/DimensionDescription"
                )

                # Calculate expected number of files for this series
                num_channels = len(channels)
                num_frames = (
                    int(dimensions[2].attrib["NumberOfElements"])
                    if len(dimensions) > 2
                    else 1
                )
                num_files = num_channels * num_frames
                logger.debug(
                    f"Expected number of files in {series_name!r}: {num_files}"
                )

                # Update dictionary entries
                self._files_in_series[series_name] = num_files
                self._series_metadata[series_name] = str(destination_file)
                logger.debug(f"Created dictionary entries for {series_name!r} metadata")

                # A new copy of the metadata file is created in 'processed', so no need
                # to register this instance of it

            # Post message if all files for the associated series have been collected
            # .get(series_name, 0) returns 0 if no associated key is found
            if not len(self._tiff_series.get(series_name, [])):
                logger.debug(f"TIFF series {series_name!r} not yet loaded")
                return True
            elif self._files_in_series.get(series_name, 0) == 0:
                logger.debug(
                    f"Metadata for TIFF series {series_name!r} not yet processed"
                )
                return True
            elif len(
                self._tiff_series.get(series_name, [])
            ) == self._files_in_series.get(series_name, 0):
                logger.debug(
                    f"Collected expected number of TIFF files for series {series_name!r}; posting job to server"
                )

                # Post the message and log any errors that arise
                tiff_dataset = {
                    "series_name": series_name,
                    "tiff_files": self._tiff_series[series_name],
                    "series_metadata": self._series_metadata[series_name],
                }
                post_result = self.process_tiff_series(tiff_dataset, environment)
                if post_result is False:
                    return False

            else:
                logger.debug(f"TIFF series {series_name!r} is still being processed")

        # Process LIF files
        if transferred_file.suffix == ".lif":
            # Type checking to satisfy MyPy
            if not environment:
                logger.warning("No environment passed in")
                return False

            # Location of the file on the client PC
            source = _get_source(transferred_file, environment)
            # Type checking to satisfy MyPy
            if not source:
                logger.warning(f"No source found for file {transferred_file}")
                return False

            logger.debug(
                f"File {transferred_file.name!r} is a valid LIF file; starting processing"
            )

            # Get the Path at the destination
            destination_file = _file_transferred_to(
                environment=environment,
                source=source,
                file_path=transferred_file,
            )
            if not destination_file:
                logger.warning(
                    f"File {transferred_file.name!r} not found on the storage system"
                )
                return False

            # Post URL to register LIF file in database
            post_result = self.register_lif_file(destination_file, environment)
            if post_result is False:
                return False
            logger.info(f"Registered {destination_file.name!r} in the database")

            # Post URL to trigger job and convert LIF file into image stacks
            post_result = self.process_lif_file(destination_file, environment)
            if post_result is False:
                return False
            logger.info(f"Started preprocessing of {destination_file.name!r}")

        # Function has completed as expected
        return True

    def register_lif_file(
        self,
        lif_file: Path,
        environment: MurfeyInstanceEnvironment,
    ):
        """
        Constructs the URL and dictionary to be posted to the server, which will then
        register the LIF file in the database correctly as part of the CLEM workflow.
        """
        try:
            # Construct URL to post to post the request to
            url = f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/clem/lif_files?lif_file={quote(str(lif_file), safe='')}"
            # Validate
            if not url:
                logger.error(
                    "URL could not be constructed from the environment and file path"
                )
                return False

            # Send the message
            capture_post(url)
            return True

        except Exception as e:
            logger.error(
                f"Error encountered when registering the LIF file in the database: {e}"
            )
            return False

    def process_lif_file(
        self,
        lif_file: Path,
        environment: MurfeyInstanceEnvironment,
    ):
        """
        Constructs the URL and dictionary to be posted to the server, which will then
        trigger the preprocessing of the LIF file.
        """

        try:
            # Construct the URL to post the request to
            url = f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/lif_to_stack?lif_file={quote(str(lif_file), safe='')}"
            # Validate
            if not url:
                logger.error(
                    "URL could not be constructed from the environment and file path"
                )
                return False

            # Send the message
            capture_post(url)
            return True

        except Exception as e:
            logger.error(f"Error encountered processing LIF file: {e}")
            return False

    def register_tiff_file(
        self,
        tiff_file: Path,
        environment: MurfeyInstanceEnvironment,
    ):
        """
        Constructs the URL and dictionary to be posted to the server, which will then
        register the TIFF file in the database correctly as part of the CLEM workflow.
        """

        try:
            url = f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/clem/tiff_files?tiff_file={quote(str(tiff_file), safe='')}"
            if not url:
                logger.error(
                    "URL could not be constructed from the environment and file path"
                )
                return False

            # Send the message
            capture_post(url)
            return True

        except Exception as e:
            logger.error(
                f"Error encountered when registering the TIFF file in the database: {e}"
            )
            return False

    def process_tiff_series(
        self,
        tiff_dataset: dict,
        environment: MurfeyInstanceEnvironment,
    ):
        """
        Constructs the URL and dictionary to be posted to the server, which will then
        trigger the preprocessing of this instance of a TIFF series.
        """

        try:
            # Construct URL for Murfey server to communicate with
            url = f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/tiff_to_stack"
            if not url:
                logger.error(
                    "URL could not be constructed from the environment and file path"
                )
                return False

            # Send the message
            capture_post(url, json=tiff_dataset)
            return True

        except Exception as e:
            logger.error(f"Error encountered processing the TIFF series: {e}")
            return False
