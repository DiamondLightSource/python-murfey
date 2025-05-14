from functools import lru_cache
from pathlib import Path
from typing import Optional

from murfey.util.config import MachineConfig, from_file, settings


@lru_cache(maxsize=5)
def get_machine_config_for_instrument(instrument_name: str) -> Optional[MachineConfig]:
    if settings.murfey_machine_configuration:
        return from_file(Path(settings.murfey_machine_configuration), instrument_name)[
            instrument_name
        ]
    return None
