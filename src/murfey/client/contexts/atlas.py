import logging
from pathlib import Path
from typing import Optional

from murfey.client.context import Context
from murfey.client.contexts.spa import _get_source
from murfey.client.contexts.spa_metadata import _atlas_destination
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.client import capture_post

logger = logging.getLogger("murfey.client.contexts.atlas")


class AtlasContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path, token: str):
        super().__init__("Atlas", acquisition_software, token)
        self._basepath = basepath

    def post_transfer(
        self,
        transferred_file: Path,
        environment: Optional[MurfeyInstanceEnvironment] = None,
        **kwargs,
    ):
        super().post_transfer(
            transferred_file=transferred_file,
            environment=environment,
            **kwargs,
        )

        if (
            environment
            and "Atlas_" in transferred_file.stem
            and transferred_file.suffix == ".mrc"
        ):
            source = _get_source(transferred_file, environment)
            if source:
                transferred_atlas_name = _atlas_destination(
                    environment, source, transferred_file, self._token
                ) / transferred_file.relative_to(source.parent)
                capture_post(
                    base_url=str(environment.url.geturl()),
                    router_name="session_control.spa_router",
                    function_name="make_atlas_jpg",
                    token=self._token,
                    session_id=environment.murfey_session,
                    data={"path": str(transferred_atlas_name)},
                )
                logger.info(
                    f"Submitted request to create JPG image of atlas {str(transferred_atlas_name)!r}"
                )
