from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

logger = logging.getLogger("murfey.workflows.clem.register_align_and_merge_results")


class AlignAndMergeResult(BaseModel):
    series_name: str
    image_stacks: list[Path]
    align_self: Optional[str] = None
    flatten: Optional[str] = "mean"
    align_across: Optional[str] = None
    composite_image: Path


def register_align_and_merge_result():
    return True
