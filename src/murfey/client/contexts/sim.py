import logging
from pathlib import Path

from murfey.client.context import Context
from murfey.client.instance_environment import MurfeyInstanceEnvironment

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
        return None
