"""
General functinos specific to the FIB workflow
"""

import math
from pathlib import Path


def number_from_name(name: str) -> int:
    """
    In the AutoTEM and Maps workflows for the FIB, the sites and images are
    auto-incremented with parenthesised numbers (e.g. "Lamella (2)"), with
    the first site/image typically not having a number.

    For sites set up and acquired manually, they will be saved in folders
    labelled "Site #1", "Site #2", etc.

    This function extracts the number from the file name, and returns 1 if
    no such number is found.
    """
    # Ensure only the stem is extracted for parsing
    stem = Path(name).stem
    # Handle naming pattern for sites acquired without autoTEM
    if "#" in stem:
        return int(stem.rpartition("#")[-1])
    # Handle naming pattern for sites acquired with autoTEM
    if "(" in stem and stem.endswith(")"):
        return int(stem[stem.rfind("(") + 1 : -1])
    # Names without '()' or '#' should return 1
    return 1


def get_slot_number(
    x: float | None = None,
    y: float | None = None,
    rotation: float | None = None,
    rotation_offset: float = -75,
):
    if x is not None and y is not None and rotation is not None:
        # Rotate the xy-coordinates to the -75 degrees frame
        theta = math.radians(rotation - rotation_offset)
        sin = math.sin(theta)
        cos = math.cos(theta)
        x_rot = (x * cos) - (y * sin)
        return 1 if x_rot < 0 else 2
    return None
