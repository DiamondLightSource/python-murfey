from pathlib import Path

from pydantic import BaseModel

from murfey.server.config import MachineConfig


class MultigridWatcherSpec(BaseModel):
    source: Path
    configuration: MachineConfig
    label: str
    visit: str
    skip_existing_processing: bool = False