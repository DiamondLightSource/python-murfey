"""
LIF image handling functions using bioio (with readlif plugin) as a base.
"""

import time as tm  # Prevent ambiguity with time as-defined in functions below
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
from bioio import BioImage
from matplotlib import pyplot as plt
from readlif.reader import LifFile as lif


def inspect_with_readlif(file: Path, save_dir: Path, show_plots: bool = False):
    """
    Inspection of the contents and structure of a .lif file using readlif Python package
    """

    # Load file as a LifFile object
    liffile = lif(file)

    # Inspect file metadata (if possible)
    print("Attempt to explore file metadata")

    # Extract root
    header = liffile.xml_header
    print(f"Metadata is a {type(header)}")
    if isinstance(header, str):
        print("Converting string to an XML element tree")
        xml_string = ET.canonicalize(header)
        tree = ET.ElementTree(ET.fromstring(xml_string))
        print(f"Metadata is now a {type(tree)}")
    elif isinstance(header, ET.Element):
        print("Converting XML element to element tree")
        tree = ET.ElementTree(header)
    elif isinstance(header, ET.ElementTree):
        print("No action needed")
        pass

    # Format XML tree
    ET.indent(tree, space="  ")
    print("Formatted XML tree")

    # Write to XML file, if it doesn't exist yet
    xml_file = save_dir.joinpath(file.stem + ".xml")
    if not xml_file.exists():
        tree.write(xml_file, encoding="utf-8")
        print(f"Wrote XML metadata to {xml_file}")
        pass
    else:
        print("XML file already exists")

    # Inspect contents
    # Number of sub-files
    num_imgs = len(list(liffile.get_iter_image()))
    print(f"There are {num_imgs} scenes in this document")

    # Get list of images
    img_list = list(liffile.get_iter_image())
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
    # LIF files are not in RGB format!
    # Channel colour defined in metadata under LUT key
    # Better to treat them as individual image stacks?

    bit_depth = img.bit_depth  # Bit depth for each channel in image
    print(f"The bit depth of each channel is {bit_depth}")

    # Try opening image for viewing
    for m in range(mosaic):
        for t in range(time):
            for z in range(depth):  # Iterate through slices
                if not z == 0:
                    continue  # Inspect only one slice

                # Set up figure for plotting histograms
                if not show_plots:
                    pass
                else:
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
                    # plt.close()

    return None


def inspect_with_bioio(file: Path):
    # Load image as BioImage object
    imgs = BioImage(str(file))
    print(f"Successfully loaded {file.stem + file.suffix}")

    # Use available commands to explore file
    num_scenes = len(imgs.scenes)
    print(
        f"There are {num_scenes} scenes in this file"
    )  # Image defaults to one of those scenes when examined

    for i in range(num_scenes):
        img_id = imgs.scenes[i]
        imgs.set_scene(i)  # Changes metadata references in-place
        img = imgs
        print(f"Now examining {img_id}")

        print(f"Image file has the shape {img.shape}")
        print(f"Image dimensions are in the order {img.dims.order}")

        metadata = (
            img.metadata
        )  # returns the metadata object for this file format (XML, JSON, etc.)
        channels = (
            img.channel_names
        )  # returns a list of string channel names found in the metadata
        size_x = (
            img.physical_pixel_sizes.X
        )  # returns the X dimension pixel size as found in the metadata
        size_y = (
            img.physical_pixel_sizes.Y
        )  # returns the Y dimension pixel size as found in the metadata
        size_z = (
            img.physical_pixel_sizes.Z
        )  # returns the Z dimension pixel size as found in the metadata

        print(
            f"Metadata contains {metadata} \n",
            f"Channels contains {channels} \n",
            f"Size X is {size_x} \n",  # Units of um?
            f"Size Y is {size_y} \n",  # Units of um?
            f"Size Z is {size_z} \n",  #
        )

    return None


def read_lif_file(file: Path):
    """
    Placeholder function

    Extract image data from LIF files as numpy array.
    """
    return None


# Run only if opened as a file
if __name__ == "__main__":

    # Start the stopwatch
    time_start = tm.time_ns()

    # Define test directory to extract data from
    test_data = Path(
        "/dls/ebic/data/staff-scratch/tieneupin/projects/murfey-clem/test-data/nt26538-160/raw"
    )  # Create as Path object
    print(f"Test repository is {test_data}")

    # Get file paths
    parent = test_data.parent
    save_dir = parent.joinpath("processing")  # Save under "processing"

    print("Saving files to processing")

    file_ext = ".lif"  # Look only for .lif files

    # Get list of files
    file_list = list(
        test_data.glob("*" + file_ext)
    )  # Search via glob and convert to list object
    file_list.sort()  # Sort in alphabetical order

    for f in range(len(file_list)):
        if not f == 1:  # Select one file to work with
            continue

        file = file_list[f]

        # Examine data
        # inspect_with_bioio(file)
        inspect_with_readlif(file=file, save_dir=save_dir, show_plots=False)

    # Stop the stopwatch
    time_stop = tm.time_ns()
    # Report time taken
    time_diff = time_stop - time_start  # In ns
    print(f"Time to completion was {round(time_diff * 10**-9, 2)} s")
