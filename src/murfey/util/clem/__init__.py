"""
Functions to call for processing LIF and TIFF files generated as part of the cryo-CLEM
workflow.
"""

import logging
from pathlib import Path
from typing import List, Optional

from murfey.util.clem import lif, tiff

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
    result = lif.convert_lif_to_tiff(
        file,
        root_folder,
        number_of_processes,
    )
    if result:
        return True
    else:
        return False


def convert_tiff_to_stack(
    tiff_list: List[Path],  # List of TIFFs from a single series
    root_folder: str,  # Name of the folder to treat as the root folder for TIFF files
    metadata_file: Optional[Path] = None,  # Option to manually provide metadata file
):
    """
    Wrapper for the actual function in tiff.py
    """
    result = tiff.convert_tiff_to_stack(
        tiff_list,
        root_folder,
        metadata_file,
    )
    if result:
        return True
    else:
        return False
