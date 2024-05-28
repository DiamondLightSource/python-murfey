"""
Contains functions that help with reading LIF files and converting them into TIFF files
as part of the cryo-CLEM workflow.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
from pathlib import Path
from xml.etree import ElementTree as ET

import numpy as np
from readlif.reader import LifFile
from tifffile import imwrite

from murfey.util import sanitise
from murfey.util.clem import (
    change_bit_depth,
    get_image_elements,
    get_xml_metadata,
    rescale_across_channel,
    rescale_to_bit_depth,
)

# Create logger object to output messages with
logger = logging.getLogger("murfey.util.clem.lif")


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
    root_folder: str,  # Name of the folder to treat as the root folder for LIF files
    number_of_processes: int = 1,  # Number of processing threads to run
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
    counter = 0
    for p in range(len(path_parts)):
        part = path_parts[p]
        # Omit initial "/" in Linux file systems for subsequent rejoining
        if part == "/":
            path_parts[p] = ""
        # Rename designated raw folder to "processed"
        if (
            part.lower() == root_folder.lower() and counter < 1
        ):  # Remove case-sensitivity
            path_parts[p] = new_root_folder
            counter += 1  # Do for first instance only
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
