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


def _get_slice_index_v5_10(tag: str) -> int:
    slice_index = 0
    for i, ch in enumerate(tag[::-1]):
        if not ch.isnumeric():
            slice_index = -i
            break
    if not slice_index:
        raise ValueError(
            f"The file tag {tag} does not end in numeric characters or is entirely numeric: cannot parse"
        )
    return slice_index


def _get_tilt_series_v5_10(p: Path) -> str:
    tag = p.name.split("_")[0]
    slice_index = _get_slice_index_v5_10(tag)
    return tag[slice_index:]


def _get_tilt_angle_v5_10(p: Path) -> str:
    tag = p.name.split("_")[0]
    slice_index = _get_slice_index_v5_10(tag)
    return tag[:slice_index]


def _get_tilt_tag_v5_10(p: Path) -> str:
    _split = p.name.split("_")[2].split(".")
    return ".".join(_split[:-1])


tomo_tilt_info = {
    "5.8": TiltInfoExtraction(
        _get_tilt_series_v5_8, _get_tilt_angle_v5_8, _get_tilt_tag_v5_8
    ),
    "5.10": TiltInfoExtraction(
        _get_tilt_series_v5_10, _get_tilt_angle_v5_10, _get_tilt_tag_v5_10
    ),
}
