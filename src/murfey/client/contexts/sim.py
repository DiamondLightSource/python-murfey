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
        self._atlas_files: dict[str, dict[str, str]] = {}

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

        # Look for atlas files
        if (
            "mosaic" in transferred_file.stem
            and transferred_file.suffix in (".mrc", ".txt")
        ) or ("markers" in transferred_file.stem and transferred_file.suffix == ".txt"):
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
            # Add to dictionary
            atlas_parent = str(destination_file.parent)
            if "mosaic" in transferred_file.stem and transferred_file.suffix == ".mrc":
                file_type = "mosaic_mrc"
            elif (
                "mosaic" in transferred_file.stem and transferred_file.suffix == ".txt"
            ):
                file_type = "mosaic_txt"
            elif (
                "markers" in transferred_file.stem and transferred_file.suffix == ".txt"
            ):
                file_type = "markers_txt"
            if not self._atlas_files.get(atlas_parent):
                self._atlas_files[atlas_parent] = {}
            self._atlas_files[atlas_parent][file_type] = str(destination_file)

            # Only trigger processing once the 'mosaic.txt' and 'mosaic.mrc' files are available
            # Processing will be re-triggered if the optional 'markers.txt' file is detected
            if all(
                file in self._atlas_files[atlas_parent].keys()
                for file in ("mosaic_mrc", "mosaic_txt")
            ):
                logger.info(
                    f"Submitting processing request for atlas files found in {atlas_parent}"
                )
                capture_post(
                    base_url=str(environment.url.geturl()),
                    router_name="workflow_sim.router",
                    function_name="request_sim_atlas_stitching",
                    token=self._token,
                    instrument_name=environment.instrument_name,
                    data=self._atlas_files[atlas_parent],
                    # Endpoint kwargs
                    session_id=environment.murfey_session,
                )
            return None

        # Look for raw data files
        # These have no extensions, and end with "_FL"
        elif not transferred_file.suffix and transferred_file.stem.endswith("_FL"):
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
                function_name="request_sim_reconstruction",
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
