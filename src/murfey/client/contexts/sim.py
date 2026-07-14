import logging
from pathlib import Path

from murfey.client.context import Context
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.client import capture_post

logger = logging.getLogger("murfey.client.contexts.sim")


def _get_source(file_path: Path, environment: MurfeyInstanceEnvironment) -> Path | None:
    """
    Returns the Path of the file on the client PC.
    """
    for s in environment.sources:
        if file_path.is_relative_to(s):
            return s
    return None


def _file_transferred_to(
    environment: MurfeyInstanceEnvironment,
    source: Path,
    file_path: Path,
    rsync_basepath: Path,
) -> Path | None:
    """
    Returns the Path of the transferred file on the DLS file system.
    """
    # Construct destination path
    base_destination = rsync_basepath / Path(environment.default_destinations[source])
    # Add visit number to the path if it's not present in default destination
    if environment.visit not in environment.default_destinations[source]:
        base_destination = base_destination / environment.visit
    destination = base_destination / file_path.relative_to(source)
    return destination


class SIMContext(Context):
    def __init__(
        self,
        acquisition_software: str,
        basepath: Path,
        machine_config: dict,
        token: str,
    ):
        super().__init__("SIMContext", acquisition_software, token)
        self._basepath = basepath
        self._machine_config = machine_config

    def post_transfer(
        self,
        transferred_file: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ):
        super().post_transfer(transferred_file, environment=environment, **kwargs)
        if environment is None:
            logger.warning("No environment passed in")
            return None

        # Look for raw data files
        # These have no extensions, and end with one of the listed suffixes
        if not transferred_file.suffix and transferred_file.stem.endswith(
            (
                # Fluorescent SIM raw data files end as follows
                "_BR",
                "_BFR",
                "_GR",
                "_GFR",
                "_BR_FL",
                "_BFR_FL",
                "_GR_FL",
                "_GFR_FL",
            )
        ):
            source = _get_source(transferred_file, environment)
            if source is None:
                logger.warning(f"No source found for file {transferred_file}")
                return None
            destination_file = _file_transferred_to(
                environment=environment,
                source=source,
                file_path=transferred_file,
                rsync_basepath=Path(self._machine_config.get("rsync_basepath", "")),
            )
            if destination_file is None:
                logger.warning(
                    f"Could not find destination file path for {transferred_file.name!r}"
                )
                return None
            # Submit fluorescent raw data files for processing
            capture_post(
                base_url=str(environment.url.geturl()),
                router_name="workflow_sim.router",
                function_name="request_sim_processing",
                token=self._token,
                instrument_name=environment.instrument_name,
                data={
                    "file": f"{destination_file}",
                },
                # Endpoint kwargs
                session_id=environment.murfey_session,
            )
            return None

        return None
