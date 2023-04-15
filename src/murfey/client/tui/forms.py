from __future__ import annotations

from typing import Dict, NamedTuple


class FormDependency(NamedTuple):
    dependencies: Dict[str, bool | str]
    trigger_value: bool = True
