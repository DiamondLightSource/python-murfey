"""
Functions to call for processing LIF and TIFF files generated as part of the cryo-CLEM
workflow.
"""

import logging
from pathlib import Path

from murfey.util.clem.lif import _convert_lif_to_tiff
from murfey.util.clem.tiff import _convert_tiff_to_stack

# Create logger object to output messages with
logger = logging.getLogger("murfey.util.clem")


def convert_lif_to_tiff(
    file: Path,
    root_folder: str,  # Name of the folder to treat as the root folder for LIF files
    number_of_processes: int = 1,  # Number of processing threads to run
):
    """
    Wrapper for the actual function in lif.py
    """
    result = _convert_lif_to_tiff(
        file,
        root_folder,
        number_of_processes,
    )
    if result:
        return True
    else:
        return False


def convert_tiff_to_stack(
    search_dir: Path,  # Directory to search for TIFF files in
    root_folder: str,  # Name of the folder to treat as the root folder for TIFF files
    number_of_processes: int = 1,  # Number of processing threads to run
):
    """
    Wrapper for the actual function in tiff.py
    """
    result = _convert_tiff_to_stack(
        search_dir,
        root_folder,
        number_of_processes,
    )
    if result:
        return True
    else:
        return False
