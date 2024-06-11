"""
Add a CLEM class context (refer to src/murfey/client/contexts/fib.py)

Provide a post transfer function to pass on:
- File path on the DLS file system
- Session ID (src/murfey/client/instance_environment.py)
"""

# import requests
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from defusedxml.ElementTree import parse

from murfey.client.context import Context
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util import capture_post, get_machine_config, sanitise
from murfey.util.clem.xml import get_image_elements

# Create logger object
logger = logging.getLogger("murfey.client.contexts.clem")


def _file_transferred_to(
    environment: MurfeyInstanceEnvironment, source: Path, file_path: Path
) -> Optional[Path]:
    """
    Returns the Path of the transferred file on the DLS file system.
    """
    machine_config = get_machine_config(
        str(environment.url.geturl()), demo=environment.demo
    )
    return (
        Path(machine_config.get("rsync_basepath", ""))
        / machine_config.get("rsync_module", "data")
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


# WORK IN PROGRESS
class CLEMContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path):
        super().__init__("CLEM", acquisition_software)
        self._basepath = basepath
        # CLEM contexts for "auto-save" acquisition mode
        self._tiff_series: Dict[str, List[str]] = {}  # {Series name : TIFF path list}
        self._tiff_timestamps: Dict[str, List[float]] = {}  # {Series name : Timestamps}
        self._tiff_sizes: Dict[str, List[int]] = {}  # {Series name : File sizes}
        self._series_metadata: Dict[str, str] = {}  # {Series name : Metadata file path}
        self._metadata_timestamp: Dict[str, float] = {}  # {Series name : Timestamp}
        self._metadata_size: Dict[str, int] = {}  # {Series name : File size}
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
            # Type checking to satisfy MyPy
            if not environment:
                logger.warning("No environment passed in")
                return True

            # Location of the file on the client PC
            source = _get_source(transferred_file, environment)
            # Type checking to satisfy MyPy
            if not source:
                logger.warning(f"No source found for file {transferred_file}")
                return True

            # Get the Path on the DLS file system
            file_path = _file_transferred_to(
                environment=environment,
                source=source,
                file_path=transferred_file,
            )
            if not file_path:
                logger.warning(
                    f"File associated with {sanitise(str(transferred_file))} not found on the storage system"
                )
                return False

            # Process TIF/TIFF files
            if transferred_file.suffix in (".tif", ".tiff"):
                # Files should be named "PositionX--ZXX--CXX.tif" by default
                if not len(transferred_file.stem.split("--")) == 3:
                    logger.warning(
                        "This TIFF file is likely not part of the CLEM workflow"
                    )
                    return False

                # Get series name from file name
                series_name = "/".join(
                    [*file_path.parent.parts[-2:], file_path.stem.split("--")[0]]
                )  # The previous 2 parent directories should be unique enough

                # Create key-value pairs containing empty list if not already present
                if series_name not in self._tiff_series.keys():
                    self._tiff_series[series_name] = []
                if series_name not in self._tiff_sizes.keys():
                    self._tiff_sizes[series_name] = []
                if series_name not in self._tiff_timestamps.keys():
                    self._tiff_timestamps[series_name] = []
                # Append information to list
                self._tiff_series[series_name].append(str(file_path))
                self._tiff_sizes[series_name].append(transferred_file.stat().st_size)
                self._tiff_timestamps[series_name].append(
                    transferred_file.stat().st_ctime
                )

            # Process XLIF files
            if transferred_file.suffix == ".xlif":

                # XLIF files don't have the "--ZXX--CXX" additions in the file name
                # But they have "/Metadata/" as the immediate parent
                series_name = "/".join(
                    [*file_path.parent.parent.parts[-2:], file_path.stem]
                )  # The previous 2 parent directories should be unique enough

                # Extract metadata to get the expected size of the series
                metadata = parse(file_path).getroot()
                metadata = get_image_elements(metadata)[0]

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

                # Update dictionary entries
                self._files_in_series[series_name] = num_files
                self._series_metadata[series_name] = str(file_path)
                self._metadata_size[series_name] = transferred_file.stat().st_size
                self._metadata_timestamp[series_name] = transferred_file.stat().st_ctime

            # Post message if all files for the associated series have been collected
            if (
                len(self._tiff_series[series_name])
                == self._files_in_series[series_name]
            ):

                # Construct URL for Murfey server to communicate with
                url = f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/tiff_to_stack"
                if not url:
                    logger.warning("No URL found for the environment")
                    return True

                # Post the message and log any errors that arise
                capture_post(
                    url,
                    json={
                        "series_name": series_name,
                        "tiff_files": self._tiff_series[series_name],
                        "tiff_sizes": self._tiff_sizes[series_name],
                        "tiff_timestamps": self._tiff_timestamps[series_name],
                        "series_metadata": self._series_metadata[series_name],
                        "metadata_size": self._metadata_size[series_name],
                        "metadata_timestamp": self._metadata_timestamp[series_name],
                        "description": "",
                    },
                )

        # Process LIF files
        if transferred_file.suffix == ".lif":
            # Type checking to satisfy MyPy
            if not environment:
                logger.warning("No environment passed in")
                return True

            # Location of the file on the client PC
            source = _get_source(transferred_file, environment)
            # Type checking to satisfy MyPy
            if not source:
                logger.warning(f"No source found for file {transferred_file}")
                return True

            # Construct the URL for the Murfey server to communicate with
            url = f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/lif_to_tiff"
            # Type checking to satisfy MyPy
            if not url:
                logger.warning("No URL found for the environment")
                return True

            # Get the Path on the DLS file system
            file_path = _file_transferred_to(
                environment=environment,
                source=source,
                file_path=transferred_file,
            )

            # Post the message and logs it if there's an error
            capture_post(
                url,
                json={
                    "name": str(file_path),
                    "size": transferred_file.stat().st_size,  # File size, in bytes
                    "timestamp": transferred_file.stat().st_ctime,  # For Unix systems, shows last metadata change
                    "description": "",
                },
            )
        return True
