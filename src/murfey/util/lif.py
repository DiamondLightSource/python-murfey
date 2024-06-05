"""
Contains functions that help with reading LIF files and converting them into TIFF files
as part of the cryo-CLEM workflow.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
from pathlib import Path
from typing import Generator, List, Optional, Tuple
from xml.etree import ElementTree as ET

import numpy as np
from readlif.reader import LifFile
from tifffile import imwrite

from murfey.util import sanitise

# Create logger object to output messages with
logger = logging.getLogger("murfey.util.lif")


def get_xml_metadata(
    file: LifFile,
    save_xml: Optional[Path] = None,
) -> ET.Element:
    """
    Extracts and returns the file metadata as a formatted XML tree. Provides option
    to save it as an XML file to the specified file path
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


def raise_BitDepthError(bit_depth: int):
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
    target_bit_depth: int,
) -> np.ndarray:
    """
    Change the bit depth of the array without changing the values (barring rounding).
    """

    # Use shorter terms in function
    arr = array
    bit_depth = target_bit_depth

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
        raise_BitDepthError(bit_depth)
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
        raise_BitDepthError(bit_depth)

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

        # Change bit depth back to initial one
        arr = change_bit_depth(array=arr, target_bit_depth=bit_depth)

    return arr


def rescale_to_bit_depth(
    array: np.ndarray,
    initial_bit_depth: int,
    target_bit_depth: int,
) -> np.ndarray:
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
        raise_BitDepthError(bit_final)

    # Rescale (DIVIDE BEFORE MULTIPLY)
    arr = (arr / (2**bit_init - 1)) * (2**bit_final - 1)

    # Change to correct unsigned integer type
    arr = change_bit_depth(array=arr, target_bit_depth=bit_final)

    return arr


def process_image_stack(
    file: Path,
    scene_num: int,
    metadata: ET.Element,
    save_dir: Path,
):
    """
    Takes the LIF file and its corresponding metadata and loads the relevant sub-stack,
    with each channel as its own array. Rescales their intensity values to utilise the
    whole channel, scales them down to 8-bit, then saves each each array as a separate
    TIFF image stack.
    """

    # Load LIF file
    file_name = file.stem.replace(" ", "_")
    image = LifFile(str(file)).get_image(scene_num)

    # Get name of sub-image
    img_name = metadata.attrib["Name"].replace(" ", "_")  # Remove spaces
    logger.info(f"Processing {file_name}-{img_name}")

    # Create save dirs for TIFF files and their metadata
    img_dir = save_dir / img_name
    img_xml_dir = img_dir / "metadata"
    for folder in [img_dir, img_xml_dir]:
        if not folder.exists():
            folder.mkdir(parents=True)
            logger.info(f"Created {folder}")
        else:
            logger.info(f"{folder} already exists")

    # Save image stack XML metadata (all channels together)
    img_xml_file = img_xml_dir / (img_name + ".xml")
    metadata_tree = ET.ElementTree(metadata)
    ET.indent(metadata_tree, "  ")
    metadata_tree.write(img_xml_file, encoding="utf-8")
    logger.info(f"Image stack metadata saved to {img_xml_file}")

    # Load channels
    channel_elem = metadata.findall(
        "Data/Image/ImageDescription/Channels/ChannelDescription"
    )
    channels: list = [
        channel_elem[c].attrib["LUTName"].lower() for c in range(len(channel_elem))
    ]

    # Load timestamps and dimensions
    # Might be useful in the future
    # timestamps = elem.find("Data/Image/TimeStampList")
    # dimensions = elem.findall(
    #     "Data/Image/ImageDescription/Dimensions/DimensionDescription"
    # )

    # Generate slice labels for later
    num_frames = image.dims.z
    image_labels = [f"{f}" for f in range(num_frames)]

    # Get x, y, and z scales
    # Get resolution (pixels per um)
    x_res = image.scale[0]
    y_res = image.scale[1]

    # Get pixel size (um per pixel)
    # Might be useful in the future
    # x_scale = 1 / x_res
    # y_scale = 1 / y_res

    # Check that depth axis exists
    z_res: float = image.scale[2] if num_frames > 1 else float(0)  # Pixels per um
    z_scale: float = 1 / z_res if num_frames > 1 else float(0)  # um per pixel

    # Process channels as individual TIFFs
    for c in range(len(channels)):

        # Get color
        color = channels[c]
        logger.info(f"Processing {color} channel")

        # Load image stack to array
        logger.info("Loading image stack")
        for z in range(num_frames):
            frame = image.get_frame(z=z, t=0, c=c)  # PIL object; array-like
            if z == 0:
                arr = np.array([frame])
            else:
                arr = np.append(arr, [frame], axis=0)
        logger.info(
            f"{file_name}-{img_name}-{color} has the dimensions {np.shape(arr)} \n"
            f"Min value: {np.min(arr)} \n"
            f"Max value: {np.max(arr)} \n"
        )

        # Initial rescaling if bit depth not 8, 16, 32, or 64-bit
        bit_depth = image.bit_depth[c]  # Initial bit depth
        if not any(bit_depth == b for b in [8, 16, 32, 64]):
            logger.info(f"{bit_depth}-bit is non-standard; converting to 16-bit")
            arr = (
                rescale_to_bit_depth(
                    array=arr, initial_bit_depth=bit_depth, target_bit_depth=16
                )
                if np.max(arr) > 0
                else change_bit_depth(
                    array=arr,
                    target_bit_depth=16,
                )
            )
            bit_depth = 16  # Overwrite

        # Rescale intensity values for fluorescent channels
        # Currently pre-emptively converting for all coloured ones
        if any(
            color in key
            for key in [
                "blue",  # Not tested
                "cyan",  # Not tested
                "green",
                "magenta",  # Not tested
                "red",
                "yellow",  # Not tested
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

        # Save as a greyscale TIFF
        save_name = img_dir.joinpath(color + ".tiff")
        logger.info(f"Saving {color} image as {save_name}")
        imwrite(
            save_name,
            arr,
            imagej=True,  # ImageJ compatible
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

    return True


def convert_lif_to_tiff(
    file: Path,
    root_folder: str,  # Name of the folder under which all raw LIF files are stored
    number_of_processes: int = 1,  # For parallel processing
):
    """
    Takes a LIF file, extracts its metadata as an XML tree, then parses through the
    sub-images stored inside it, saving each channel in the sub-image as a separate
    image stack. It uses information stored in the XML metadata to name the individual
    image stacks.

    FOLDER STRUCTURE:
    Here is the folder structure of a typical DLS eBIC experiment session, with the
    folders created as part of the workflow shown as well.

    parent_folder   <- Session ID
    |_ processing   <- Created by DLS; will be archived; not used
    |_ spool        <- Created by DLS; for confidential work; not used
    |_ tmp          <- Created by DLS; for intermediate files; not used
    |_ xml          <- Created by DLS; not used
    |_ images       <- Created by us; raw data stored here
    |  |_ sample_name   <- Folders for samples
    |     |_ lif files  <- LIF files of specific sample
    |     |_ metadata   <- Created by us; Save raw XML metadata file here
    |_ processed    <- ARCHIVED BY DLS; Created by us
    |  |_ sample_name
    |     |_ lif_file_names     <- Folders for data from the same LIF file
    |        |_ sub_image       <- Folders for individual sub-images
    |           |_ tiffs        <- Save channels as individual image stacks
    |           |_ metadata     <- Individual XML files saved here (not yet implemented)
    """

    # Validate processor count input
    num_procs = number_of_processes  # Use shorter phrase in script
    if num_procs < 1:
        logger.warning("Processor count set to zero or less; resetting to 1")
        num_procs = 1

    # Folder for processed files with same structure as old one
    file_name = file.stem.replace(" ", "_")  # Replace spaces
    path_parts = list(file.parts)
    new_root_folder = "processed"
    # Rewrite string in-place
    for p in range(len(path_parts)):
        part = path_parts[p]
        # Omit initial "/" in Linux file systems for subsequent rejoining
        if part == "/":
            path_parts[p] = ""
        # Rename designated raw folder to "processed"
        if part.lower() == root_folder.lower():  # Remove case-sensitivity
            path_parts[p] = new_root_folder
            break  # Do for first instance only
    # If specified folder not found by end of string, log as error
    if new_root_folder not in path_parts:
        logger.error(
            f"Subpath {sanitise(root_folder)} was not found in image path "
            f"{sanitise(str(file))}"
        )
        return None
    processed_dir = Path("/".join(path_parts)).parent / file_name

    # Folder for raw XML metadata
    raw_xml_dir = file.parent / "metadata"

    # Create folders if not already present
    for folder in [processed_dir, raw_xml_dir]:
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
        save_xml=raw_xml_dir.joinpath(file_name + ".xml"),
    )

    # Recursively generate list of metadata-containing elements
    elem_list = get_image_elements(xml_root)

    # Check that elements match number of images
    if not len(elem_list) == len(scene_list):
        raise IndexError(
            "Error matching metadata list to list of sub-images. \n"
            # Show what went wrong
            f"Metadata entries: {len(elem_list)} \n"
            f"Sub-images: {len(scene_list)}"
        )

    # Iterate through scenes
    logger.info("Examining sub-images")

    # Set up multiprocessing arguments
    pool_args = []
    for i in range(len(scene_list)):
        pool_args.append(
            # Arguments need to be pickle-able; no complex objects allowed
            [  # Follow order of args in the function
                file,  # Reload as LifFile object in the process
                i,
                elem_list[i],  # Corresponding metadata
                processed_dir,
            ]
        )

    # Parallel process image stacks
    with mp.Pool(processes=num_procs) as pool:
        result = pool.starmap(process_image_stack, pool_args)

    return result
