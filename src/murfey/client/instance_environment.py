from __future__ import annotations

from pathlib import Path
from typing import NamedTuple
from urllib.parse import ParseResult

from murfey.client.watchdir import DirWatcher


class MurfeyInstanceEnvironment(NamedTuple):
    murfey_url: ParseResult
    source: Path | None = None
    default_destination: str = ""
    watcher: DirWatcher | None = None
    demo: bool = False
