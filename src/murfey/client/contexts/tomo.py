from __future__ import annotations

from pathlib import Path
from typing import Callable, NamedTuple


class TiltInfoExtraction(NamedTuple):
    series: Callable[[Path], str]
    angle: Callable[[Path], str]
    tag: Callable[[Path], str]


def _get_tilt_series_v5_8(p: Path) -> str:
    return p.name.split("_")[1]


def _get_tilt_angle_v5_8(p: Path) -> str:
    return p.name.split("[")[1].split("]")[0]


def _get_tilt_tag_v5_8(p: Path) -> str:
    return p.name.split("_")[0]


tomo_tilt_info = {
    "5.8": TiltInfoExtraction(
        _get_tilt_series_v5_8, _get_tilt_angle_v5_8, _get_tilt_tag_v5_8
    )
}
