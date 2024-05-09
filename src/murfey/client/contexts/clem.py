"""
Add a CLEM class context (refer to src/murfey/client/contexts/fib.py)

Provide a post transfer function to pass on:
- File path on the DLS file system
- Session ID (src/murfey/client/instance_environment.py)
"""

# import requests
import logging
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
    machine_config = get_machine_config(
        str(environment.url.geturl()), demo=environment.demo
    )
    if environment.visit in environment.default_destinations[source]:
        return (
            Path(machine_config.get("rsync_basepath", ""))
            / Path(environment.default_destinations[source])
            / file_path.relative_to(source)
        )
    return (
        Path(machine_config.get("rsync_basepath", ""))
        / Path(environment.default_destinations[source])
        / environment.visit
        / file_path.relative_to(source)
    )


def _get_source(
    file_path: Path, environment: MurfeyInstanceEnvironment
) -> Optional[Path]:
    for s in environment.sources:
        if file_path.is_relative_to(s):
            return s
    return None


class CLEMContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path):
        super().__init__("CLEM", acquisition_software)
        self._basepath = basepath

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
        # Check if file is a LIF file
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

            # Get the file size and timestamp from transferred_file
            # Client PC cannot see file_path; that is for server PC

            # Post the message and logs it if there's an error
            capture_post(
                url,
                json={
                    "name": str(file_path),
                    "size": transferred_file.stat().st_size,
                    "timestamp": transferred_file.stat().st_ctime,
                    "description": "",
                },
            )
        return True
