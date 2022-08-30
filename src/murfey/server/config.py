from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Union

import yaml
from pydantic import BaseModel


class MachineConfig(BaseModel):
    acquisition_software: List[str]
    calibrations: Dict[str, Union[dict, float]]
    data_directories: List[Path]
    gain_reference_directory: Optional[Path] = None


def from_file(config_file_path: Path, microscope: str) -> MachineConfig:
    with open(config_file_path, "r") as config_stream:
        config = yaml.safe_load(config_stream)
    return MachineConfig(**config.get(microscope, {}))
