from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel


class MultigridWatcherSpec(BaseModel):
    source: Path
    label: str
    visit: str
    instrument_name: str
    skip_existing_processing: bool = False
    destination_overrides: Dict[Path, str] = {}
    rsync_restarts: List[str] = []
    visit_end_time: Optional[datetime] = None
