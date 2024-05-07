"""
Contains functions that help with reading .lif files and converting them into other
useful file formats (e.g. .tiff files) as part of the cryo-CLEM workflow
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Generator, List, Optional, Tuple
from xml.etree import ElementTree as ET

# import matplotlib.pyplot as plt
import numpy as np
from readlif.reader import LifFile
from tifffile import imwrite


def get_xml_metadata(
    file: LifFile,
    save_xml: Optional[Path] = None,
) -> ET.Element:
    """
    Extracts and returns the file metadata as a formatted XML tree, and optionally
    saves it as an XML file to the specified file path.
    """
    xml_root = file.xml_root  # This one for navigating
    xml_tree = ET.ElementTree(xml_root)  # This one for saving

    if not save_xml:
        pass  # Skip saving the metadata
    else:
        xml_file = str(save_xml)
        # Save XML metadata for LIF file as one single unit
        ET.indent(xml_tree, "  ")  # Tidy it up
        xml_tree.write(xml_file, encoding="utf-8")
        print(f"File metadata saved to {xml_file}")
        pass

    return xml_root


def get_image_elements(root: ET.Element) -> List[ET.Element]:
    """
    Searches through the XML metadata recursively to find the nodes tagged as "Element"
    that have image-related tags. Some LIF datasets have layers of nested elements.
    """

    # Nested function which generates list of elements with
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
            new_node = elem
            new_elem_list = _find_elements_recursively(new_node)
            for new_elem in new_elem_list:
                yield new_elem

    # Get initial list of elements
    elem_list = list(_find_elements_recursively(root))

    # Keep only the ones that have image-related tags
    elem_list = [elem for elem in elem_list if elem.find("./Data/Image")]

    return elem_list


def raise_bit_depth_error(bit_depth: int):
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
    Checks the range of values occupied by the data, then rescales it across the
    channel's bit depth.
    """
    # Check that bit depth is valid before processing even begins
    if not any(bit_depth == b for b in [8, 16, 32, 64]):
        raise_bit_depth_error(bit_depth)
    else:
        pass  # Proceed to rest of function

    # Use shorter variable names
    arr = array
    if not percentile_range:
        print("No percentile range provided. Returning original array.")
        pass
    else:
        # Use shorter variables
        p_lo = percentile_range[0]
        p_up = percentile_range[1]

        # Calculate lower and upper bounds
        b_lo = np.floor(np.percentile(arr, p_lo) / round_to) * round_to  # Lower bound
        b_up = np.ceil(np.percentile(arr, p_up) / round_to) * round_to  # Upper bound

        # Rescale across channel bit depth
        arr[arr < b_lo] = b_lo  # Overwrite lower outliers
        arr[arr > b_up] = b_up  # Overwrite upper outliers
        arr = arr - b_lo  # Shift lower bound to zero
        arr = (arr / (b_up - b_lo)) * (
            2**bit_depth - 1
        )  # Ensure data points don't exceed bit depth

        # This step probably not needed
        # arr[arr >= (2**bit_depth - 1)] = (
        #     2**bit_depth - 1
        # )  # Ensure data points don't exceed bit depth

        # Change bit depth back to initial one
        arr = change_bit_depth(arr, bit_depth)

    return arr


def rescale_to_bit_depth(
    array: np.ndarray,
    initial_bit_depth: int,
    target_bit_depth: int,
) -> Tuple[np.ndarray, int]:

    # Use shorter names for variables
    arr = array
    bit_init = initial_bit_depth
    bit_final = target_bit_depth

    # Check that allowed target bit depth is given
    if not any(bit_final == b for b in [8, 16, 32, 64]):
        raise_bit_depth_error(bit_final)
    else:
        pass  # Continue with rest of function

    # Rescale (DIVIDE BEFORE MULTIPLY)
    arr = (arr / (2**bit_init - 1)) * (2**bit_final - 1)  # Avoid exceeding bit depth

    # This step probably not needed
    # arr[arr >= (2**bit_final - 1)] = 2**bit_final - 1  # Overwrite pixels that exceed channel bit depth

    # Change to correct unsigned integer type
    arr = change_bit_depth(arr, bit_final)

    return arr, bit_final


def convert_lif_to_tiff(file: Path):
    """
    DLS eBIC experiment sessions have the following folder structure:
    parent          <- Session ID
    |_ processing   <- ARCHIVED BY DLS; Not used
    |_ spool        <- NOT ARCHIVED BY DLS; For work proprietary work
    |_ tmp          <- DELETED; Intermediate files
    |_ xml          <- ARCHIVED BY DLS; Not used
    |_ raw          <- ARCHIVED BY DLS; Raw data stored here
    |  |_ metadata  <- Create this and save raw XML metadata file here
    |_ processed    <- ARCHIVED BY DLS; Created by us
    |  |_ raw_n     <- Following the structure of the raw folders
    |     |_ lif_file       <- Following LIF file name
    |        |_ sub_image   <- Folders for individual sub-images
    |           |_ tiffs    <- Save by channel
    |           |_ metadata
    |              |_ xml_files     <- Individual XML files
    """
    # Set up directories
    raw_dir = file.parent  # Raw data location
    root_dir = raw_dir.parent  # Session ID folder

    # Create new directories
    process_dir = root_dir.joinpath("processed").joinpath(
        file.stem
    )  # Store processing here
    raw_xml_dir = raw_dir.joinpath("metadata")  # Raw metadata

    # Create new folders if not already present
    for folder in [process_dir, raw_xml_dir]:
        if not folder.exists():
            os.makedirs(str(folder))
            print(f"Created {folder}")
        else:
            print(f"{folder} already exists")
            pass

    # Load LIF file as a LifFile class
    print(f"Loading {file}")
    lf = LifFile(str(file))  # Stack of scenes
    print("Done")
    scene_list = list(lf.get_iter_image())  # List of scene names

    # Save original metadata as XML tree
    print("Extracting image metadata")
    xml_root = get_xml_metadata(
        file=lf,
        save_xml=raw_xml_dir.joinpath(file.stem + ".xml"),
    )
    print("Done")

    # Recursively generate element list of metadata
    elem_list = get_image_elements(xml_root)

    # Check that elements match number of images
    if not len(elem_list) == len(scene_list):
        raise Exception(
            "Error matching metadata list to list of sub-images. \n"
            f"Metadata entries: {len(elem_list)} \n"
            f"Sub-images: {len(scene_list)}"
        )
    else:
        pass  # Carry on

    # Iterate through scenes
    print("Examining sub-images")
    for i in range(len(scene_list)):
        # Load image
        img = lf.get_image(i)  # Set sub-image

        # Load relevant metadata (name, dimensions, channels, timestamps)
        elem = elem_list[i]  # Select corresponding element
        img_name = elem.attrib["Name"]  # Get sub-image name
        print(f"Examining {img_name}")

        # Split by channel
        channels = elem.findall(
            "Data/Image/ImageDescription/Channels/ChannelDescription"
        )
        # Might be useful in the future
        # timestamps = elem.find("Data/Image/TimeStampList")

        # Might be useful in the future
        # Common to all images
        # dimensions = elem.findall(
        #     "Data/Image/ImageDescription/Dimensions/DimensionDescription"
        # )

        # Create save dirs for TIFF files and their metadata
        save_dir = process_dir.joinpath(img_name)
        xml_dir = save_dir.joinpath("metadata")
        for folder in [save_dir, xml_dir]:
            if not folder.exists():
                os.makedirs(str(folder))
                print(f"Created {folder}")
            else:
                print(f"{folder} already exists")
                pass

        # Parijat wants the images in 8-bit; scale down from 16-bit
        # Save channels as individual TIFFs
        for c in range(len(list(img.get_iter_c()))):
            # Get color
            color = channels[c].attrib["LUTName"]
            print(f"Examining the {color.lower()} channel")

            # Extract image data to array
            print("Loading image stack")
            arr: np.ndarray = []  # Array to store frames in
            # Iterate over slices
            for z in range(len(list(img.get_iter_z()))):
                frame = img.get_frame(z=z, t=0, c=c)  # PIL object; array-like
                arr.append(frame)
            arr = np.array(arr)  # Make independent copy of this array
            print("Done")

            # Initial rescaling if bit depth not 8, 16, 32, or 64-bit
            bit_depth = img.bit_depth[c]  # Initial bit depth
            if not any(bit_depth == b for b in [8, 16, 32, 64]):
                print("Bit depth non-standard, converting to 16-bit")
                arr, bit_depth = rescale_to_bit_depth(
                    array=arr, initial_bit_depth=bit_depth, target_bit_depth=16
                )
            else:
                pass

            # Rescale intensity values for fluorescent channels
            if any(
                color.lower() in key for key in ["red", "green"]
            ):  # Eliminate case-sensitivity
                print(f"Rescaling {color.lower()} channel across channel depth")
                arr = rescale_across_channel(
                    array=arr,
                    bit_depth=bit_depth,
                    percentile_range=(0.5, 99.5),
                    round_to=16,
                )
                print("Done")

            # Convert to 8-bit
            print("Converting to 8-bit image")
            arr, bit_depth = rescale_to_bit_depth(
                arr, initial_bit_depth=bit_depth, target_bit_depth=8
            )
            print("Done")

            # Get x, y, and z scales
            # Get resolution (pixels per distance)
            x_res = img.scale[0]  # Pixels per um
            y_res = img.scale[1]  # Pixels per um

            # Might be used in future versions of formula
            # x_scale = 1 / x_res  # um per pixel
            # y_scale = 1 / y_res  # um per pixel

            # Check that depth axis exists
            if not img.scale[2]:
                z_res: float = 0
                z_scale: float = 0  # Avoid divide by zero errors
            else:
                z_res = img.scale[2]
                z_scale = 1 / z_res

            # Generate slice labels
            image_labels = [f"{f}" for f in range(len(list(img.get_iter_z())))]

            # Save as a greyscale TIFF
            tiff_name = save_dir.joinpath(color + ".tiff")
            print(f"Saving {color.lower()} image as {tiff_name}")
            imwrite(
                tiff_name,
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
            print("Done")
    return None
