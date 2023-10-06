from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, NamedTuple

from murfey.client.instance_environment import MurfeyInstanceEnvironment

logger = logging.getLogger("murfey.client.context")


class FutureRequest(NamedTuple):
    url: str
    message: Dict[str, Any]


class ProcessingParameter(NamedTuple):
    name: str
    label: str
    default: Any = None


def detect_acquisition_software(dir_for_transfer: Path) -> str:
    glob = dir_for_transfer.glob("*")
    for f in glob:
        if f.name.startswith("EPU") or f.name.startswith("GridSquare"):
            return "epu"
        if f.name.startswith("Position") or f.suffix == ".mdoc":
            return "tomo"
    return ""


class Context:
    user_params: List[ProcessingParameter] = []
    metadata_params: List[ProcessingParameter] = []

    def __init__(self, acquisition_software: str):
        self._acquisition_software = acquisition_software

    def post_transfer(self, transferred_file: Path, role: str = "", **kwargs):
        raise NotImplementedError(
            f"post_transfer hook must be declared in derived class to be used: {self}"
        )

    def post_first_transfer(self, transferred_file: Path, role: str = "", **kwargs):
        self.post_transfer(transferred_file, role=role, **kwargs)

    def gather_metadata(
        self, metadata_file: Path, environment: MurfeyInstanceEnvironment | None = None
    ):
        raise NotImplementedError(
            f"gather_metadata must be declared in derived class to be used: {self}"
        )
