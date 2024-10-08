from pathlib import Path

from pydantic import BaseModel

from murfey.util.config import MachineConfig


class MultigridWatcherSpec(BaseModel):
    source: Path
    configuration: MachineConfig
    label: str
    visit: str
    instrument_name: str
    skip_existing_processing: bool = False
