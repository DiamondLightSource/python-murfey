from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Union

import yaml
from pydantic import BaseModel


class MachineConfig(BaseModel):
    name: str
    acquisition_software: List[str]
    calibrations: Dict[str, Union[dict, float]]
    data_directory: Path


def from_file(config_file_path: Path) -> MachineConfig:
    with open(config_file_path, "r") as config_stream:
        config = yaml.safe_load(config_stream)
    return MachineConfig(**config)
