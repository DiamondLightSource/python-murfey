import logging
from pathlib import Path
from typing import Optional

import requests

from murfey.client.context import Context
from murfey.client.contexts.spa import _get_source
from murfey.client.contexts.spa_metadata import _atlas_destination
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.api import url_path_for
from murfey.util.client import authorised_requests, capture_post

logger = logging.getLogger("murfey.client.contexts.atlas")

requests.get, requests.post, requests.put, requests.delete = authorised_requests()


class AtlasContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path):
        super().__init__("Atlas", acquisition_software)
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
                    environment, source, transferred_file
                ) / transferred_file.relative_to(source.parent)
                capture_post(
                    f"{str(environment.url.geturl())}{url_path_for('session_control.spa_router', 'make_atlas_jpg', session_id=environment.murfey_session)}",
                    json={"path": str(transferred_atlas_name)},
                )
                logger.info(
                    f"Submitted request to create JPG image of atlas {str(transferred_atlas_name)!r}"
                )
