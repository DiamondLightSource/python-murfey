from __future__ import annotations

from pathlib import Path
from typing import Callable, List, NamedTuple


class TiltInfoExtraction(NamedTuple):
    series: Callable[[Path], str]
    angle: Callable[[Path], str]
    tag: Callable[[Path], str]


def _get_tilt_series_v5_7(p: Path) -> str:
    return p.name.split("_")[1]


def _get_tilt_angle_v5_7(p: Path) -> str:
    return p.name.split("[")[1].split("]")[0]


def _get_tilt_tag_v5_7(p: Path) -> str:
    return p.name.split("_")[0]


def _get_slice_index_v5_11(tag: str) -> int:
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


def _get_tilt_series_v5_11(p: Path) -> str:
    tag = p.name.split("_")[0]
    slice_index = _get_slice_index_v5_11(tag)
    return tag[slice_index:]


def _get_tilt_tag_v5_11(p: Path) -> str:
    tag = p.name.split("_")[0]
    slice_index = _get_slice_index_v5_11(tag)
    return tag[:slice_index]


def _get_tilt_angle_v5_11(p: Path) -> str:
    _split = p.name.split("_")[2].split(".")
    return ".".join(_split[:-1])


def _find_angle_index(split_name: List[str]) -> int:
    for i, part in enumerate(split_name):
        if "." in part:
            return i
    return 0


def _get_tilt_series_v5_12(p: Path) -> str:
    split_name = p.name.split("_")
    angle_idx = _find_angle_index(split_name)
    if split_name[angle_idx - 2].isnumeric():
        return split_name[angle_idx - 2]
    return "0"


def _get_tilt_angle_v5_12(p: Path) -> str:
    split_name = p.name.split("_")
    angle_idx = _find_angle_index(split_name)
    return split_name[angle_idx]


def _get_tilt_tag_v5_12(p: Path) -> str:
    split_name = p.name.split("_")
    angle_idx = _find_angle_index(split_name)
    if split_name[angle_idx - 2].isnumeric():
        return "_".join(split_name[: angle_idx - 2])
    return "_".join(split_name[: angle_idx - 1])


tomo_tilt_info = {
    "5.7": TiltInfoExtraction(
        _get_tilt_series_v5_7, _get_tilt_angle_v5_7, _get_tilt_tag_v5_7
    ),
    "5.11": TiltInfoExtraction(
        _get_tilt_series_v5_11, _get_tilt_angle_v5_11, _get_tilt_tag_v5_11
    ),
    "5.12": TiltInfoExtraction(
        _get_tilt_series_v5_12,
        _get_tilt_angle_v5_12,
        _get_tilt_tag_v5_12,
    ),
}
