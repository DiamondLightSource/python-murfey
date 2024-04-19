"""
Contains functions that help with reading .lif files and converting them into other useful file formats (e.g. .tiff files) as part of the cryo-CLEM workflow
"""

from __future__ import annotations

from pathlib import Path

import bioformats as bf
import javabridge as jb
import numpy as np
import time

# import tifffile as tif
from matplotlib import pyplot as plt
from readlif.reader import LifFile as lif
#from xml.dom import minidom
import xml.etree.ElementTree as ET


def use_readlif(file: Path):
    """
    Inspection of the contents and structure of a .lif file using readlif Python package
    """
    # Load file as a LifFile object
    file = lif(file)

    # Inspect contents
    # Number of sub-files
    num_imgs = len(list(file.get_iter_image()))
    print(f"There are {num_imgs} sub-files in this document")

    # Get list of images
    img_list = list(file.get_iter_image())
    [print(i) for i in img_list]  # Check properties of files

    # Inspect single image
    img = img_list[0]
    print(f"Selected image {0} to inspect")

    depth = len(list(img.get_iter_z()))
    print(f"The image is {depth} frames deep")

    time = len(list(img.get_iter_t()))  # Time axis
    print(f"The time axis is {time} slice(s) long")

    mosaic = len(list(img.get_iter_m()))  # Number of tiles in image
    print(f"The mosaic image axis is {mosaic} slice(s) long")

    channel = len(list(img.get_iter_c()))
    print(f"The number of channels is {channel}")
    # Channel order
    # 0 - G
    # 1 - R
    # 2 - B/W
    # LIF files are not in RGB format!
    # Better to treat them as individual image stacks

    bit_depth = img.bit_depth  # Bit depth for each channel in image
    print(f"The bit depth of each channel is {bit_depth}")

    # Try opening image for viewing
    for m in range(mosaic):
        for t in range(time):
            for z in range(depth):  # Iterate through slices
                if not z == 0:
                    continue  # Inspect only one slice

                # Set up figure for plotting histograms
                fig, axs = plt.subplots(
                    nrows=channel,
                    ncols=1,
                    sharex="all",
                    sharey="all",  # Use common scale for all plots
                    squeeze=False,  # To allow for future scalability
                )

                frame = []  # Empty array to store RGB channels

                for c in range(channel):  # Iterate through image channels
                    # Get subplot axes
                    ax = axs[c][0]  # Row, then col

                    # Inspect single channel from single frame
                    frame_c = img.get_frame(z=z, c=c, t=t, m=m)
                    frame_c = np.uint16(np.array(frame_c))

                    # Append to RGB array
                    frame.append(frame_c)

                    # Statistical analysis of pixel distribution
                    print(f"Plotting histogram for channel {c}")
                    bit_size = 2 ** bit_depth[c]  # Number of bits for channel
                    ax.hist(
                        np.ravel(frame_c),  # Convert to 1D array for plotting
                        bins=64,  # 2**n makes for easy bit binning
                        range=(0, bit_size - 1),  # Numbering starts from 0
                    )
                    ax.set_title(label=f"Channel {c}", fontsize="small", pad=0.1)
                # Close histogram (if needed)
                plt.close()

    return None


def use_bioformats(file: Path):
    # Start Java virtual machine
    jb.start_vm(class_path=bf.JARS,
                run_headless=True)

    # Load file metadata
    print(f"Loading {file.stem + file.suffix} metadata")
    metadata = bf.OMEXML(bf.get_omexml_metadata(path=str(file)))  # get_omexml_metadata returns it as a string
    print("Successfully opened metadata")
    print(f"Metadata is {type(metadata)}")

    # Inspect metadata
    # print(f"Namespaces contains {metadata.ns}")
    # print(f"Root node contains {metadata.root_node}")
    # print(f"Metadata contains {[item for item in metadata.dom.iter()]}")  # Iter is a LONG list of objects

    # Get number of images in file metadata
    num_imgs = metadata.get_image_count()
    print(f"This file contains {num_imgs} images")
    # Iterate through metadata for each image
    for n in range(num_imgs):
        try:
            md = metadata
            print(f"Plates: {md.plates}")
            print(f"Structured annotations: {md.structured_annotations}")
            im = metadata.image(n)
            print(f"ID: {im.ID}")  # Equivalent to get_ID()
            print(f"Name: {im.Name}")  # Equivalent to get_Name()
            print(f"Acquisition date: {im.AcquisitionDate}")  # Equivalent to get_AcquisitionDate()
            print(f"Number of ROI refs: {im.get_roiref_count()}")
            num_roiref = im.get_roiref_count()
            for r in range(num_roiref):
                print(f"ROI ref number {r}: {im.roiref(r)}")
            px = im.Pixels
            print(f"Pixel ID: {px.ID}")
            print(f"Pixel dimension order: {px.DimensionOrder}")
            print(f"Pixel type: {px.PixelType}")

            print(f"Number of channels: {px.SizeC}")
            num_chan = px.SizeC
            print(f"Number of timepoints: {px.SizeT}")
            print(f"Number of x-pixels: {px.SizeX}")
            print(f"Number of y-pixels: {px.SizeY}")
            print(f"Number of slices: {px.SizeZ}")
            for c in range(num_chan):
                ch = px.Channel(c)
                print(f"Channel ID: {ch.ID}")
                print(f"Channel name: {ch.Name}")
                print(f"Channel samples per pixel: {ch.SamplesPerPixel}")
        except:
            print(f"Request failed for image number {n}")
    
    # Inspect generated metadata
    # file_name = metadata.image().Name
    # print(f"File name is {file_name}")

    # Identify channels
    # num_channels = metadata.image().Pixels.channel_count  # Get number of channels
    # channel_list = [metadata.image().Pixels.Channel(i).Name for i in range(num_channels)]
    # print(f"There are {num_channels} channels in this file with the names:")
    # [print(c) for c in channel_list]

    # End Java virtual machine
    jb.kill_vm()  # Javabridge cannot restart in VS Code Interactive Window

    return None


def get_xml(file: Path):
    # Start Java virtual machine
    jb.start_vm(class_path=bf.JARS,
                run_headless=True)

    # Get OME-XML string from file
    xml_string = bf.get_omexml_metadata(path=str(file))
    print("Loaded OME-XML metadata from file")

    # Write to file
    txt_file = file.parent.joinpath(file.stem + ".txt")
    if txt_file.exists():
        pass
        print("Text file already exists")
    else:
        tree = ET.ElementTree(ET.fromstring(xml_string))
        root = tree.getroot()
        xml_formatted = ET.tostring(root, encoding="utf8").decode("utf8")
        # print(xml_formatted)
        with open(txt_file, "w") as log_file:
            log_file.writelines(xml_formatted)
        log_file.close()
        print("Wrote OME-XML metadata to text file")

    # End Java virtual machine
    jb.kill_vm()

    return xml_string


def use_xml(file: Path):
    # Convert OME-XML metadata
    xml_string = get_xml(file=file)
    tree = ET.ElementTree(ET.fromstring(xml_string))
    # Navigate to XML root
    root = tree.getroot()
    print("Created ElementTree successfully")
    
    # Inspect element
    # print(f"Root tag: {root.tag}")
    # print(f"Root attribute: {root.attrib}")
    print(f"There are {len(root)} items under root")
    # [print(root[i].attrib) for i in range(len(root))]  # Top-level attributes: Present
    # [print(root[i].tag) for i in range(len(root))]  # Top-level tags: Present
    # [print(root[i].text) for i in range(len(root))]  # Top-level text: None
    
    # Inspect one level down
    branch0 = root[0]
    branch1 = root[int((len(root)-1)/2)]
    branch2 = root[-1]
    
    print(f"Examining {branch0.attrib}")
    print(f"There are {len(branch0)} items under this branch")
    [print(# child.tag,  # Tag is just the website
        child.attrib,  # Attributes present
        # child.text  # No text
        ) for child in branch0]
    print("\n")

    print(f"Examining {branch1.attrib}")
    print(f"There are {len(branch1)} items under this branch")
    [print(# child.tag,  # Tag is just the website
        child.attrib,  # Attributes present
        # child.text  # No text
        ) for child in branch1]
    print("\n")

    print(f"Examining {branch2.attrib}")
    print(f"There are {len(branch2)} items under this branch")
    print("\n")
    return tree


# Run only if opened as a file
if __name__ == "__main__":
    # Define test directory to extract data from
    test_repo = Path(
        "/dls/ebic/data/staff-scratch/tieneupin/projects/murfey-clem/test-data/nt26538-160/raw"
    )  # Create as Path object
    print(f"Test repository is {test_repo}")

    file_ext = ".lif"  # Look only for .lif files

    # Search in test repo
    file_list = list(test_repo.glob("*.lif"))  # Convert to list object
    file_list.sort()  # Sort in alphabetical order
    [
        print(f"{f.stem + f.suffix} | Path object? {isinstance(f, Path)}")
        for f in file_list
    ]  # Print individually to check

    file = file_list[0]  # Select one file to work with
    
    # Open and examine files
    # use_readlif(file=file)
    # use_bioformats(file=file)
    use_xml(file=file)
