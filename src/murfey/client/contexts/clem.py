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
from typing import Optional

from murfey.client.context import Context
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util import capture_post, get_machine_config

# Create logger object
logger = logging.getLogger("murfey.client.contexts.clem")


def _file_transferred_to(
    environment: MurfeyInstanceEnvironment, source: Path, file_path: Path
) -> Optional[Path]:
    """
    Returns the Path of the transferred file on the DLS file system
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
    Returns the Path of the file on the client PC
    """
    for s in environment.sources:
        if file_path.is_relative_to(s):
            return s
    return None


# WORK IN PROGRESS
# Will need to add context for TIFF files associated with CLEM
class CLEMContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path):
        super().__init__("CLEM", acquisition_software)
        self._basepath = basepath
        # Add additional CLEM contexts here
        self._tiff_positions: dict = {}
        self._position_sizes: dict = {}

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

        # Process XLIF files
        if transferred_file.suffix == ".xlif":
            # Type checking to satisfy MyPy
            if not environment:
                logger.warning("No environment passed in")
                return True

            # parse xlif to get position name and size

            # self._position_sizes[position] = size
            # check if position is complete by looking at self._tiff_positions[position]
            # if complete API call (post)

        # Process TIF files that are part of the CLEM workflow
        if transferred_file.suffix == ".tif":
            # Type checking to satisfy MyPy
            if not environment:
                logger.warning("No environment passed in")
                return True

            # work out position name from file name
            # self._tiff_positions[position].append(transferred_file)
            # check of len(self._tiff_positions[position]) == self._position_sizes.get(position, 0)

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
                logger.warning("No url found for the environment")
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
