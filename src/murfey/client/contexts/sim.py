import logging
from pathlib import Path

from murfey.client.context import Context, _file_transferred_to, _get_source
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.client import capture_post

logger = logging.getLogger("murfey.client.contexts.sim")


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

            # Submit fluorescent raw data files for processing
            logger.info(f"Requesting processing for {transferred_file.name!r}")
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
