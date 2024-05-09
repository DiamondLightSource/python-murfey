"""
Contains functions that help with reading LIF files and converting them into TIFF files
as part of the cryo-CLEM workflow.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Generator, List, Optional, Tuple
from xml.etree import ElementTree as ET

# import matplotlib.pyplot as plt
import numpy as np
from readlif.reader import LifFile
from tifffile import imwrite

from murfey.server.api import sanitise

# Create logger object to output messages with
logger = logging.getLogger("murfey.util.lif")


def get_xml_metadata(
    file: LifFile,
    save_xml: Optional[Path] = None,
) -> ET.Element:
    """
    Extracts and returns the file metadata as a formatted XML tree, and optionally
    saves it as an XML file to the specified file path.
    """

    # Use readlif function to get XML metadata
    xml_root: ET.Element = file.xml_root  # This one for navigating
    xml_tree = ET.ElementTree(xml_root)  # This one for saving

    # Skip saving the metadata if save_xml not provided
    if save_xml:
        xml_file = str(save_xml)  # Convert Path to string
        ET.indent(xml_tree, "  ")  # Format with proper indentation
        xml_tree.write(xml_file, encoding="utf-8")  # Save
        logger.info(f"File metadata saved to {xml_file}")

    return xml_root


def get_image_elements(root: ET.Element) -> List[ET.Element]:
    """
    Searches the XML metadata recursively to find the nodes tagged as "Element" that
    have image-related tags. Some LIF datasets have layers of nested elements, so a
    recursive approach is needed to avoid certain datasets breaking it.
    """

    # Nested function which generates list of elements
    def _find_elements_recursively(
        node: ET.Element,
    ) -> Generator[ET.Element, None, None]:

        # Find items labelled "Element" under current node
        elem_list = node.findall("./Children/Element")
        if len(elem_list) < 1:  # Try alternative path for top-level of XML tree
            elem_list = node.findall("./Element")

        # Recursively search for items tagged as Element under child branches
        for elem in elem_list:
            yield elem
            new_node = elem  # New starting point for the search
            new_elem_list = _find_elements_recursively(new_node)  # Call self
            for new_elem in new_elem_list:
                yield new_elem

    # Get initial list of elements
    elem_list = list(_find_elements_recursively(root))

    # Keep only the element nodes that have image-related tags
    elem_list = [elem for elem in elem_list if elem.find("./Data/Image")]

    return elem_list


def raise_bit_depth_error(bit_depth: int):
    """
    Raises an exception if the bit depth value provided is not one that NumPy can
    handle.
    """

    raise Exception(
        "The channel bit depth provided is not compatible with Numpy. "
        "Only 8, 16, 32, and 64-bit channel depths are allowed. "
        f"Current bit depth: {bit_depth}"
    )


def change_bit_depth(
    array: np.ndarray,
    bit_depth: int,
) -> np.ndarray:
    """
    Change the bit depth of the array without changing the values (barring rounding).
    """

    # Use shorter terms
    arr = array

    # NumPy defaults to float64; revert back to unsigned int
    if bit_depth == 8:
        arr = arr.astype(np.uint8)
    elif bit_depth == 16:
        arr = arr.astype(np.uint16)
    elif bit_depth == 32:
        arr = arr.astype(np.uint32)
    elif bit_depth == 64:
        arr = arr.astype(np.uint64)
    else:
        raise_bit_depth_error(bit_depth)
    return arr


def rescale_across_channel(
    array: np.ndarray,
    bit_depth: int,
    percentile_range: Optional[Tuple[float, float]],  # Lower and upper percentiles
    round_to: Optional[int] = 16,  # Round bounds to reasonably granular power of 2
) -> np.ndarray:
    """
    Checks the range of pixel values occupied by the data, then rescales it across the
    channel's bit depth.
    """

    # Check that bit depth is valid before processing even begins
    if not any(bit_depth == b for b in [8, 16, 32, 64]):
        raise_bit_depth_error(bit_depth)
    else:
        pass  # Proceed to rest of function

    # Use shorter variable names
    arr = array

    # Check if percentiles are provided
    if not percentile_range:
        logger.warning("No percentile range provided. Returning original array.")
    else:
        # Use shorter variables
        p_lo = percentile_range[0]
        p_up = percentile_range[1]

        # Calculate lower and upper bounds
        b_lo = np.floor(np.percentile(arr, p_lo) / round_to) * round_to
        b_up = (np.ceil(np.percentile(arr, p_up) / round_to) * round_to) - 1

        # Rescale across channel bit depth
        arr[arr < b_lo] = b_lo  # Overwrite lower outliers
        arr[arr > b_up] = b_up  # Overwrite upper outliers
        arr = arr - b_lo  # Shift lower bound to zero
        arr = (arr / (b_up - b_lo)) * (
            2**bit_depth - 1
        )  # Ensure data points don't exceed bit depth (max bit is 2**n - 1)

        # This step probably not needed
        # Overwrite values that exceed current channel bit depth
        # arr[arr >= (2**bit_depth - 1)] = (
        #     2**bit_depth - 1
        # )

        # Change bit depth back to initial one
        arr = change_bit_depth(arr, bit_depth)

    return arr


def rescale_to_bit_depth(
    array: np.ndarray,
    initial_bit_depth: int,
    target_bit_depth: int,
) -> Tuple[np.ndarray, int]:
    """
    Rescales the pixel values of the array to fit within the desired channel bit depth.
    Returns the array and the target bit depth as a tuple.
    """

    # Use shorter names for variables
    arr = array
    bit_init = initial_bit_depth
    bit_final = target_bit_depth

    # Check that target bit depth is allowed
    if not any(bit_final == b for b in [8, 16, 32, 64]):
        raise_bit_depth_error(bit_final)

    # Rescale (DIVIDE BEFORE MULTIPLY)
    arr = (arr / (2**bit_init - 1)) * (2**bit_final - 1)

    # This step probably not needed anymore
    # Overwrite pixels that exceed channel bit depth
    # arr[arr >= (2**bit_final - 1)] = 2**bit_final - 1

    # Change to correct unsigned integer type
    arr = change_bit_depth(arr, bit_final)

    return arr, bit_final


def convert_lif_to_tiff(file: Path):
    """
    Takes a LIF file, extracts its metadata as an XML tree, then parses through the
    sub-images stored inside it, saving each channel in the sub-image as a separate
    image stack. It uses information stored in the XML metadata to name the individual
    image stacks.

    FOLDER STRUCTURE
    ================
    Here is the folder structure of a typical DLS eBIC experiment session, with the
    folders created as part of the workflow shown as well.

    parent_folder   <- Session ID
    |_ processing   <- ARCHIVED BY DLS; Not used
    |_ spool        <- NOT ARCHIVED BY DLS; For confidential work
    |_ tmp          <- DELETED BY DLS AFTER A WHILE; Intermediate files
    |_ xml          <- ARCHIVED BY DLS; Not used
    |_ raw          <- ARCHIVED BY DLS; Raw data stored here; can have multiple raws
    |  |_ lifs
    |  |_ metadata  <- Created by us; Save raw XML metadata file here
    |_ processed    <- ARCHIVED BY DLS; Created by us
    |  |_ raw_n     <- Following the structure of the raw folders
    |     |_ lif_file_names <- Folders for data from the same LIF file
    |        |_ sub_image   <- Folders for individual sub-images
    |           |_ tiffs    <- Save channels as individual image stacks
    |           |_ metadata <- Individual XML files saved here (not yet implemented)
    """

    # Set up parent directories
    raw_dir = file.parent  # Raw data location
    root_dir = raw_dir.parent  # Session ID folder

    # Path to new directories
    # Save processed data here
    process_dir = root_dir / "processed" / file.stem
    # Save raw XML metadata here
    raw_xml_dir = raw_dir / "metadata"

    # Create new folders if not already present
    for folder in [process_dir, raw_xml_dir]:
        if not folder.exists():
            folder.mkdir(parents=True)
            logger.info(f"Created {folder}")
        else:
            logger.info(f"{folder} already exists")

    # Load LIF file as a LifFile class
    logger.info(f"Loading {sanitise(file.name)}")
    lif_file = LifFile(str(file))  # Stack of scenes
    scene_list = list(lif_file.get_iter_image())  # List of scene names

    # Save original metadata as XML tree
    logger.info("Extracting image metadata")
    xml_root = get_xml_metadata(
        file=lif_file,
        save_xml=raw_xml_dir.joinpath(file.stem + ".xml"),
    )

    # Recursively generate list of metadata-containing elements
    elem_list = get_image_elements(xml_root)

    # Check that elements match number of images
    if not len(elem_list) == len(scene_list):
        raise Exception(
            "Error matching metadata list to list of sub-images. \n"
            # Show what went wrong
            f"Metadata entries: {len(elem_list)} \n"
            f"Sub-images: {len(scene_list)}"
        )
    else:
        pass  # Carry on

    # Iterate through scenes
    logger.info("Examining sub-images")
    for i in range(len(scene_list)):

        # Load image
        img = lif_file.get_image(i)  # Set sub-image

        # Get name of sub-image
        elem = elem_list[i]  # Select corresponding element
        img_name = elem.attrib["Name"]  # Get sub-image name
        logger.info(f"Examining {img_name}")

        # Load relevant metadata (channels, dimensions, timestamps etc.)
        channels = elem.findall(
            "Data/Image/ImageDescription/Channels/ChannelDescription"
        )
        # Might be useful in the future
        # timestamps = elem.find("Data/Image/TimeStampList")
        # dimensions = elem.findall(
        #     "Data/Image/ImageDescription/Dimensions/DimensionDescription"
        # )

        # Create save dirs for TIFF files and their metadata
        img_dir = process_dir / img_name
        img_xml_dir = img_dir / "metadata"
        for folder in [img_dir, img_xml_dir]:
            if not folder.exists():
                folder.mkdir(parents=True)
                logger.info(f"Created {folder}")
            else:
                logger.info(f"{folder} already exists")

        # Parijat wants the images in 8-bit; scale down from 16-bit
        # Save channels as individual TIFFs
        for c in range(len(list(img.get_iter_c()))):
            # Get color
            color = channels[c].attrib["LUTName"]
            logger.info(f"Examining the {color.lower()} channel")

            # Extract image data to array
            logger.info("Loading image stack")
            arr: np.ndarray = []  # Array to store frames in
            # Iterate over slices
            for z in range(len(list(img.get_iter_z()))):
                frame = img.get_frame(z=z, t=0, c=c)  # PIL object; array-like
                arr.append(frame)
            arr = np.array(arr)  # Make independent copy of this array

            # Initial rescaling if bit depth not 8, 16, 32, or 64-bit
            bit_depth = img.bit_depth[c]  # Initial bit depth
            if not any(bit_depth == b for b in [8, 16, 32, 64]):
                logger.info("Bit depth non-standard, converting to 16-bit")
                arr, bit_depth = rescale_to_bit_depth(
                    array=arr, initial_bit_depth=bit_depth, target_bit_depth=16
                )
            else:
                pass

            # Rescale intensity values for fluorescent channels
            if any(
                color.lower() in key for key in ["red", "green"]
            ):  # Eliminate case-sensitivity
                logger.info(f"Rescaling {color.lower()} channel across channel depth")
                arr = rescale_across_channel(
                    array=arr,
                    bit_depth=bit_depth,
                    percentile_range=(0.5, 99.5),
                    round_to=16,
                )

            # Convert to 8-bit
            logger.info("Converting to 8-bit image")
            arr, bit_depth = rescale_to_bit_depth(
                arr, initial_bit_depth=bit_depth, target_bit_depth=8
            )

            # Get x, y, and z scales
            # Get resolution (pixels per um)
            x_res = img.scale[0]
            y_res = img.scale[1]

            # Might be used in future versions
            # Get pixel size (um per pixel)
            # x_scale = 1 / x_res
            # y_scale = 1 / y_res

            # Check that depth axis exists
            if not img.scale[2]:
                z_res: float = 0
                z_scale: float = 0  # Avoid divide by zero errors
            else:
                z_res = img.scale[2]  # Pixels per um
                z_scale = 1 / z_res  # um per pixel

            # Generate slice labels
            image_labels = [f"{f}" for f in range(len(list(img.get_iter_z())))]

            # Save as a greyscale TIFF
            save_name = img_dir.joinpath(color + ".tiff")
            logger.info(f"Saving {color.lower()} image as {save_name}")
            imwrite(
                save_name,
                arr,
                imagej=True,  # ImageJ comppatible
                photometric="minisblack",  # Grayscale image
                shape=np.shape(arr),
                dtype=arr.dtype,
                resolution=(x_res * 10**6 / 10**6, y_res * 10**6 / 10**6),
                metadata={
                    "spacing": z_scale,
                    "unit": "micron",
                    "axes": "ZYX",
                    "Labels": image_labels,
                },
            )
    return None
