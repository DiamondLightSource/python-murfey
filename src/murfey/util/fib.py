"""
General functinos specific to the FIB workflow
"""

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
