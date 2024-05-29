"""
Contains functions to process the TIFF files generated by the Leica light microscope's
auto-save feature, writing them into image stacks according to their colour channels.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
from pathlib import Path
from typing import List
from xml.etree import ElementTree as ET

import numpy as np
from PIL import Image
from tifffile import imwrite

from murfey.util import sanitise
from murfey.util.clem import (
    change_bit_depth,
    get_axis_resolution,
    get_image_elements,
    rescale_across_channel,
    rescale_to_bit_depth,
)

# Create logger object to output messages with
logger = logging.getLogger("murfey.util.clem.tiff")


def process_tiff_files(
    tiff_list: List[Path],
    metadata_file: Path,
    save_dir: Path,
):
    """
    Opens the TIFF files provided as NumPy arrays and stacks them.
    """

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

    # Load dimensions and get x, y, and z resolution
    dimensions = metadata.findall(
        "Data/Image/ImageDescription/Dimensions/DimensionDescription"
    )
    x_res = get_axis_resolution(dimensions[0])
    y_res = get_axis_resolution(dimensions[1])

    # Process z-axis differently
    z_res = get_axis_resolution(dimensions[2]) if len(dimensions) > 2 else float(0)
    z_size = 1 / z_res if len(dimensions) > 2 else float(0)

    # Load timestamps
    # Might be useful in the future
    # timestamps = elem.find("Data/Image/TimeStampList")

    num_frames = (
        int(dimensions[2].attrib["NumberOfElements"]) if len(dimensions) > 2 else 1
    )
    image_labels = [f"{f}" for f in range(num_frames)]

    # Process channels as individual TIFFs
    for c in range(len(colors)):

        # Get color
        color = colors[c]
        logger.info(f"Processing {color} channel")

        # Find TIFFs from relevant channel and series
        tiff_sublist = [
            f
            for f in tiff_list
            if (f"C{str(c).zfill(2)}" in f.stem or f"C{str(c).zfill(3)}" in f.stem)
            and (img_name in f.stem)
        ]
        tiff_sublist.sort()  # Increasing order of Z

        # Load image stack
        for t in range(len(tiff_sublist)):
            img = Image.open(tiff_sublist[t])
            if t == 0:
                arr = np.array([img])  # Store as 3D array
            else:
                arr = np.append(arr, [img], axis=0)
            pass

        # Get bit depth
        bit_depth = int(channels[c].attrib["Resolution"])
        if not any(bit_depth == b for b in [8, 16, 32, 64]):
            logger.info(
                f"{bit_depth}-bit is not supported by NumPy; converting to 16-bit"
            )
            arr = (
                rescale_to_bit_depth(
                    array=arr, initial_bit_depth=bit_depth, target_bit_depth=16
                )
                if np.max(arr) > 0
                else change_bit_depth(array=arr, target_bit_depth=16)
            )
            bit_depth = 16  # Overwrite

        # Rescale intensity values for fluorescent channels
        # Currently pre-emptively converting for all coloured ones
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
        ):
            logger.info(f"Rescaling {color} channel across channel depth")
            arr = (
                rescale_across_channel(
                    array=arr,
                    bit_depth=bit_depth,
                    percentile_range=(0.5, 99.5),
                    round_to=16,
                )
                if np.max(arr) > 0
                else arr
            )

        # Convert to 8-bit
        if not bit_depth == 8:
            logger.info("Converting to 8-bit image")
            bit_depth_new = 8
            arr = (
                rescale_to_bit_depth(
                    array=arr,
                    initial_bit_depth=bit_depth,
                    target_bit_depth=bit_depth_new,
                )
                if np.max(arr) > 0
                else change_bit_depth(
                    array=arr,
                    target_bit_depth=bit_depth_new,
                )
            )
        else:
            logger.info("Image is already 8-bit")

        # Save as a gresycale TIFF
        save_name = save_dir / (color + ".tiff")
        logger.info(f"Save {color} image as {save_name}")
        imwrite(
            save_name,
            arr,
            imagej=True,  # ImageJ comptaible
            photometric="minisblack",  # Grayscale image from black to white
            shape=np.shape(arr),
            dtype=arr.dtype,
            resolution=(x_res * 10**6 / 10**6, y_res * 10**6 / 10**6),
            metadata={
                "spacing": z_size,
                "unit": "micron",
                "axes": "ZYX",
                "Labels": image_labels,
            },
        )

    return True


def convert_tiff_to_stack(
    search_dir: Path,
    root_folder: str,  # Name of the folder to treat as the root folder
    number_of_processes: int = 1,  # Number of processing threads to run
):
    """
    Main function for coordinating the processing of the TIFF files generated by the
    Leica LM's "auto-save" feature.

    The file structure when using "auto-save" differs slightly from that when saving
    as a single .LIF file:

    ___ session_id
        |__ images
        |   |__ position_1
        |       |__ metadata
        |       |   |__ image1.xlif     <-- Actually an XML file
        |       |__ image1--Z00--C00.tiff
        |       |__ image1--Z00--C01.tiff
        |           ... Images for each channel and slice are saved individually
        |   |__ position_2
        |       ...
        |__ processed
            |__ Mimics "images" folder structure
    """

    # Set variables and shorter names for use within function
    new_root_folder = "processed"
    num_procs = number_of_processes

    # Use the location of TIFF files and their names to identify unique datasets
    logger.info("Scanning for file paths with desired TIFF files")
    # TIFF file names start with "Position..." by default
    valid_tiffs = list(search_dir.glob("**/Position*.tif"))
    # Remove the "--Z##--C##.tiff" end of the file path strings
    unique_paths = list({Path(str(f).split("--", 1)[0]) for f in valid_tiffs})
    unique_paths.sort()  # Sort by path alphabetically
    logger.info(f"Found {len(unique_paths)} unique paths")

    # Collect arguments to use for parallel processing
    pool_args = []  # Empty list for parallel processing arguments
    for u in range(len(unique_paths)):

        path = unique_paths[u]
        logger.info(f"Processing files in {str(path)}")

        # Extract key variables
        raw_dir = path.parent  # File path not including partial file name
        series_name = path.stem  # Last item is part of file name

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
                f"{sanitise(str(raw_dir))}"
            )
            return None

        # Make directory for processed files
        processed_dir = Path("/".join(path_parts))  # Images
        if not processed_dir.exists():
            processed_dir.mkdir(parents=True)
            logger.info(f"Created {processed_dir}")
        else:
            logger.info(f"{str(processed_dir)} already exists")

        # Get associated list of TIFFs
        tiff_list = list(raw_dir.glob("**/Position*.tif"))
        if len(tiff_list) > 0:
            logger.info(f"TIFFs found at {raw_dir}: {len(tiff_list)}")
        else:
            logger.error(f"No TIFFs found at {raw_dir}")
            return None

        # Get associated XML file
        xml_file = raw_dir / "Metadata" / (series_name + ".xlif")
        if xml_file.exists():
            logger.info(f"Metadata file found at {xml_file}")
        else:
            logger.error(f"No metadata file found at {xml_file}")
            return None

        # Add list of arguments to main list
        pool_args.append(
            # Arguments need to be pickle-able; no complex objects
            [  # Follow order of args in the function
                tiff_list,
                xml_file,
                processed_dir,
            ]
        )

    # Parallel process image stacks
    with mp.Pool(processes=num_procs) as pool:
        result = pool.starmap(process_tiff_files, pool_args)

    return result
