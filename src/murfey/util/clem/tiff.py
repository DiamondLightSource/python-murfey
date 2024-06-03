"""
Supporting functions to create image stacks from the TIFF files generated by the Leica
light microscope's auto-save acquisition mode.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import numpy as np
from defusedxml import ElementTree as ET
from PIL import Image

from murfey.util import sanitise
from murfey.util.clem.images import process_img_stk, write_to_tiff
from murfey.util.clem.xml import get_axis_resolution, get_image_elements

# Create logger object to output messages with
logger = logging.getLogger("murfey.util.clem.tiff")


def process_tiff_files(
    tiff_list: List[Path],
    metadata_file: Path,
    save_dir: Path,
):
    """
    Opens the TIFF files as NumPy arrays and stacks them.
    """

    # Validate metadata
    if not metadata_file.parents[-2] == tiff_list[0].parents[-2]:
        logger.error("The base paths of the metadata and TIFF files do not match")
        return False

    # Load relevant metadata
    elem_list = get_image_elements(ET.parse(metadata_file).getroot())
    metadata = elem_list[0]

    # Get name of image series
    img_name = metadata.attrib["Name"]
    logger.info(f"Processing {img_name}")

    # Create save directory for image metadata
    metadata_dir = save_dir / "metadata"
    if not metadata_dir.exists():
        metadata_dir.mkdir(parents=True)
        logger.info(f"Created metadata directory at {metadata_dir}")
    else:
        logger.info(f"{metadata_dir} already exists")

    # Save image metadata
    img_xml_file = metadata_dir / (img_name.replace(" ", "_") + ".xml")
    metadata_tree = ET.ElementTree(metadata)
    ET.indent(metadata_tree, "  ")
    metadata_tree.write(img_xml_file, encoding="utf-8")
    logger.info(f"Metadata for image stack saved to {img_xml_file}")

    # Load channels
    channels = metadata.findall(
        "Data/Image/ImageDescription/Channels/ChannelDescription"
    )
    colors = [channels[c].attrib["LUTName"].lower() for c in range(len(channels))]

    # Get x, y, and z resolution (pixels per um)
    dimensions = metadata.findall(
        "Data/Image/ImageDescription/Dimensions/DimensionDescription"
    )
    x_res = get_axis_resolution(dimensions[0])
    y_res = get_axis_resolution(dimensions[1])

    # Process z-axis if it exists
    z_res = get_axis_resolution(dimensions[2]) if len(dimensions) > 2 else float(0)

    # Load timestamps (might be useful in the future)
    # timestamps = elem.find("Data/Image/TimeStampList")

    # Generate slice labels for later
    num_frames = (
        int(dimensions[2].attrib["NumberOfElements"]) if len(dimensions) > 2 else 1
    )
    image_labels = [f"{f}" for f in range(num_frames)]

    # Process channels as individual TIFFs
    for c in range(len(colors)):

        # Get color
        color = colors[c]
        logger.info(f"Processing {color} channel")

        # Get bit depth
        bit_depth = int(channels[c].attrib["Resolution"])

        # Find TIFFs from relevant channel and series
        tiff_sublist = [
            f
            for f in tiff_list
            if (f"C{str(c).zfill(2)}" in f.stem or f"C{str(c).zfill(3)}" in f.stem)
            and (img_name in f.stem)
        ]
        tiff_sublist.sort()  # Increasing order of Z

        # Load image stack
        logger.info("Loading image stack")
        for t in range(len(tiff_sublist)):
            img = Image.open(tiff_sublist[t])
            if t == 0:
                arr = np.array([img])  # Store as 3D array
            else:
                arr = np.append(arr, [img], axis=0)
        logger.info(
            f"{img_name} {color} array has the dimensions {np.shape(arr)} \n"
            f"Min value: {np.min(arr)} \n"
            f"Max value: {np.max(arr)} \n"
        )

        # Rescale intensity values for fluorescent channels
        rescale = (
            True
            if any(
                color in key
                for key in [
                    "blue",
                    "cyan",
                    "green",
                    "magenta",
                    "red",
                    "yellow",
                ]
            )
            else False
        )

        # Process the image stack
        arr = process_img_stk(
            array=arr,
            initial_bit_depth=bit_depth,
            target_bit_depth=8,
            rescale=rescale,
        )

        # Save as a greyscale TIFF
        arr = write_to_tiff(
            array=arr,
            save_dir=save_dir,
            file_name=color,
            x_res=x_res,
            y_res=y_res,
            z_res=z_res,
            units="micron",
            axes="ZYX",
            image_labels=image_labels,
        )

    return True


def convert_tiff_to_stack(
    tiff_list: List[Path],  # List of files associated with this series
    root_folder: str,  # Name of the folder to treat as the root folder
    metadata_file: Optional[Path] = None,  # Option to manually provide metadata file
):
    """
    Main function for coordinating the processing of the TIFF files generated by the
    Leica LM's "auto-save" feature. Takes a list of TIFF files for a distinct image
    series and converts them into a TIFF image stack with the key metadata embedded.
    Stacks are saved in a folder called "processed", which preserves the directory
    structure of the raw files.

    The file structure when using "auto-save" differs slightly from that when saving
    as a single .LIF file:

    ___ session_id
        |__ images
        |   |__ position_1
        |   |   |__ metadata
        |   |   |   |__ position1.xlif     <-- Actually an XML file
        |   |   |__ position1--Z00--C00.tiff
        |   |   |__ position1--Z00--C01.tiff
        |   |       ... Images for each channel and slice are saved individually
        |   |__ position_2
        |       ...
        |__ processed
            |__ position_1
                |__ metadata
                |   |__ position_1.xlif
                |__ gray.tiff
                |__ red.tiff
                ... Mimics "images" folder structure
    """

    # Set variables and shorter names for use within function
    new_root_folder = "processed"

    # Use the names of the TIFF files to get the unique path to it
    # Remove the "--Z##--C##.tiff" end of the file path strings
    path = list({Path(str(f).split("--", 1)[0]) for f in tiff_list})[0]

    # Extract key variables
    parent_dir = path.parent  # File path not including partial file name
    series_name = path.stem  # Last item is part of file name

    logger.info(f"Processing {series_name} TIFF files")

    # Create processed directory
    path_parts = list(path.parts)
    counter = 0
    for p in range(len(path_parts)):
        part = path_parts[p]
        # Remove leading "/" in Unix systems for subsequent rejoining
        if part == "/":
            path_parts[p] = ""
        # Remove spaces to prevent subsequent Murfey errors
        if " " in part:
            path_parts[p] = part.replace(" ", "_")
        # Rename designated root folder to "processed"
        if (
            part.lower() == root_folder.lower() and counter < 1
        ):  # Remove case-sensitivity
            path_parts[p] = new_root_folder
            counter += 0  # Do for first instance only
        # Remove last level in path if same as previous one (redundancy)
        if p == len(path_parts) - 1:
            if part.replace(" ", "_") == path_parts[p - 1].replace(" ", "_"):
                path_parts.pop(p)
    # Check that "processed" has been inserted into file path
    if new_root_folder not in path_parts:
        logger.error(
            f"Subpath {sanitise(root_folder)} was not found in file path "
            f"{sanitise(str(parent_dir))}"
        )
        return None
    # Make directory for processed files
    processed_dir = Path("/".join(path_parts))  # Images
    if not processed_dir.exists():
        processed_dir.mkdir(parents=True)
        logger.info(f"Created {processed_dir}")
    else:
        logger.info(f"{str(processed_dir)} already exists")

    # Get associated XML file
    if not metadata_file:  # Search for it using relative paths if not provided
        xml_file = parent_dir / "Metadata" / (series_name + ".xlif")
        if xml_file.exists():
            logger.info(f"Metadata file found at {xml_file}")
        else:
            logger.error(f"No metadata file found at {xml_file}")
            return None
    else:
        xml_file = metadata_file

    result = process_tiff_files(tiff_list, xml_file, processed_dir)

    if result:
        return True
    else:
        return None
