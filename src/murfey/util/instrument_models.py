from pathlib import Path
from typing import Dict, List

from pydantic import BaseModel

from murfey.util.config import MachineConfig


class MultigridWatcherSpec(BaseModel):
    source: Path
    configuration: MachineConfig
    label: str
    visit: str
    instrument_name: str
    skip_existing_processing: bool = False
    destination_overrides: Dict[Path, str] = {}
    rsync_restarts: List[str] = []
