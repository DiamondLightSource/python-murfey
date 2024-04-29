"""
Contains functions that help with reading .lif files and converting them into other
useful file formats (e.g. .tiff files) as part of the cryo-CLEM workflow
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

import numpy as np
import matplotlib.pyplot as plt
from readlif.reader import LifFile
from tifffile import TiffFile

def get_xml_metadata(file: LifFile,
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
        xml_tree.write(xml_file, encoding="utf-8")
        pass

    return xml_root


def lif_to_tiff(file: Path):
    # Set up directories
    raw_dir = file.parent  # Raw data location
    root_dir = raw_dir.parent  # Session ID folder

    # Create new directories
    process_dir = root_dir.joinpath("processed").joinpath(file.stem)  # Store processing here
    raw_xml_dir = raw_dir.joinpath("metadata")  # Raw metadata

    # DLS eBIC experiment sessions have the following folder structure:
    # parent        <- Session ID
    # |_ processing <- ARCHIVED; Not used
    # |_ spool      <- NOT ARCHIVED; For work proprietary work
    # |_ tmp        <- DELETED; Intermediate files
    # |_ xml        <- ARCHIVED; Not used
    # |_ raw        <- ARCHIVED; Raw data stored here
    # |  |_ metadata    <- Create this and save raw XML metadata file here
    # |_ processed  <- ARCHIVED; Created by us
    # |  |_ raw_n   <- Following the structure of the raw folders
    # |     |_ lif_file     <- Following LIF file name
    # |        |_ sub_image <- Folders for individual sub-images
    # |           |_ tiffs      <- Save by channel
    # |           |_ metadata
    # |              |_ xml_files   <- Individual XML files

    # Create new folders if not already present
    for folder in [process_dir, raw_xml_dir]:
        if not folder.exists():
            os.makedirs(str(folder))
        else:
            pass

    # Load LIF file as a LifFile class
    lf = LifFile(str(file))  # Stack of scenes
    scene_list = list(lf.get_iter_image())  # List of scene names

    # Save original metadata as XML tree
    xml_root = get_xml_metadata(file=lf,
                                save_xml=raw_xml_dir.joinpath(file.stem + ".xml"),
                                )
    # Get metadata for individual datasets as a list
    elem_list = xml_root.findall("Element/Children/Element")

    # Iterate through scenes
    for i in range(len(scene_list)):
        # Load image
        img = lf.get_image(i)  # Set sub-image

        # Load relevant metadata (name, dimensions, channels, timestamps)
        elem = elem_list[i]  # Select corresponding element
        img_name = elem.attrib["Name"]  # Get sub-image name
        # Common to all images
        dimensions = elem.findall("Data/Image/ImageDescription/Dimensions/DimensionDescription")
        # Split by channel
        channels = elem.findall("Data/Image/ImageDescription/Channels/ChannelDescription")
        timestamps = elem.find("Data/Image/TimeStampList")

        # Create save dirs for TIFF files and their metadata
        save_dir = process_dir.joinpath(img_name)
        xml_dir = save_dir.joinpath("metadata")
        for folder in [save_dir, xml_dir]:
            if not save_dir.exists():
                os.makedirs(save_dir)

        # Parijat wants the images in 8-bit; scale down from 16-bit
        # Save channels as individual TIFFs
        for c in range(len(list(img.get_iter_c()))):
            # Extract image data
            arr = []  # Array to store frames in
            bit_depth = img.bit_depth[c]  # Initial bit depth
            # Iterate over slices
            for z in range(len(list(img.get_iter_z()))):
                frame = img.get_frame(z=z, t=0, c=c)  # PIL object; array-like
                arr.append(frame)
            arr = np.asarray(arr)

            # Get color
            color = channels[c].attrib["LUTName"]
            # Rescale intensity values for fluorescent channels
            if any(color.lower() in ["red", "green"]):  # Eliminate case-sensitivity
                # Remove outliers
                p_lo = 0.5   # Lower percentile
                p_up = 99.5  # Upper percentile
                round_to = 16  # Round bounds to reasonably granular power of 2
                bounds = [
                    np.floor(np.percentile(arr, p_lo) / round_to) * round_to,
                    np.ceil(np.percentile(arr, p_up) / round_to) * round_to,
                ]
                # Rescale
                arr[arr < bounds[0]] = bounds[0]  # Set lower outliers to floor
                arr[arr > bounds[1]] = bounds[1]  # Set upper outliers to ceiling
                arr = arr - bounds[0]  # Set lower bound to zero
                arr = arr * (2**bit_depth) / np.diff(bounds, n=1)
                arr = arr.astype(np.uint16)  # Set to unsigned 16-bit


    return None
