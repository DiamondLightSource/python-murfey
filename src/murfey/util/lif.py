"""
Contains functions that help with reading .lif files and converting them into other
useful file formats (e.g. .tiff files) as part of the cryo-CLEM workflow
"""

from __future__ import annotations

import time as tm  # Prevent ambiguity with time as-defined in functions below
import xml.etree.ElementTree as ET
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Callable

import bioformats as bf
import javabridge as jb
import numpy as np

# import tifffile as tif
from matplotlib import pyplot as plt
from readlif.reader import LifFile as lif


def _init_logger():
    """
    This is so that Javabridge doesn't spill out a lot of DEBUG/WARNING messages during
    runtime.
    Copied from: https://github.com/pskeshu/microscoper/blob/master/microscoper/io.py#L141-L162

    Valid logging options: TRACE, DEBUG, INFO, WARN, ERROR, OFF, ALL
    Taken from: https://logback.qos.ch/manual/architecture.html
    """

    rootLoggerName = jb.get_static_field(
        "org/slf4j/Logger", "ROOT_LOGGER_NAME", "Ljava/lang/String;"
    )

    rootLogger = jb.static_call(
        "org/slf4j/LoggerFactory",
        "getLogger",
        "(Ljava/lang/String;)Lorg/slf4j/Logger;",
        rootLoggerName,
    )

    logLevel = jb.get_static_field(
        "ch/qos/logback/classic/Level",
        "ERROR",  # Show only error messages or worse
        "Lch/qos/logback/classic/Level;",
    )

    jb.call(rootLogger, "setLevel", "(Lch/qos/logback/classic/Level;)V", logLevel)


def _run_as_separate_process(
    function: Callable, args=list  # List of arguments the function takes, IN ORDER
):
    """
    Run the function as its own separate process. Currently used for handling functions
    that make use of Java virtual machines, which cannot be started again after they
    have been stopped in a Python instance.
    """
    # Create a queue object to pass to the process
    queue: Queue = Queue()

    # Run functions that need JVM instances as a separate process
    p = Process(
        target=function, args=(*args, queue)  # Process takes arguments as a tuple
    )
    p.start()

    # Extract the result from the function
    results = queue.get()
    p.join()

    return results


def _get_xml_string(file: Path, queue: Queue):  # multiprocessing queue
    # Start Java virtual machine
    jb.start_vm(class_path=bf.JARS, run_headless=True)
    _init_logger()

    # Get OME-XML string from file
    xml_string = bf.get_omexml_metadata(path=str(file))
    print("Loaded OME-XML metadata from file")

    # Kill virtual machine
    jb.kill_vm()

    # Add result to queue
    queue.put(xml_string)
    return xml_string


def get_xml_string(file: Path):
    xml_string = _run_as_separate_process(function=_get_xml_string, args=[file])
    return xml_string


def write_to_raw_xml(xml_string: str):
    # Write raw xml to file
    xml_file = file.parent.joinpath(file.stem + ".xml")
    if xml_file.exists():
        print("XML file already exists")
        pass
    else:
        with open(xml_file, mode="w", encoding="utf-8") as log_file:
            log_file.writelines(xml_string)
        log_file.close()
        print("Wrote raw OME-XML metadata to XML file")

    return xml_file


def convert_to_xml_tree(xml_string: str):
    # Convert to ElementTree
    tree = ET.ElementTree(ET.fromstring(xml_string))
    print("Created ElementTree successfully")

    # Add indent to XML file
    ET.indent(tree, space="\t", level=0)

    return tree


def write_to_pretty_xml(xml_tree: ET.ElementTree, file: Path):
    # Write out metadata contents in a formatted structure
    xml_file = file.parent.joinpath(file.stem + ".xml")
    if xml_file.exists():
        print("XML file already exists")
        pass
    else:
        xml_tree.write(xml_file, encoding="utf-8")
        print("Wrote formatted OME-XML metadata to XML file")

    return xml_file


def extract_xml_metadata(file: Path):
    # Convert OME-XML metadata
    xml_string = get_xml_string(file=file)
    xml_tree = convert_to_xml_tree(xml_string=xml_string)
    write_to_pretty_xml(xml_tree=xml_tree, file=file)

    return xml_tree


def inspect_lif_file(file: Path):
    """
    Inspection of the contents and structure of a .lif file using readlif Python package
    """
    # Load file as a LifFile object
    liffile = lif(file)

    # Inspect contents
    # Number of sub-files
    num_imgs = len(list(liffile.get_iter_image()))
    print(f"There are {num_imgs} sub-files in this document")

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


def read_lif_file(file: Path):
    return None


# Run only if opened as a file
if __name__ == "__main__":

    # Start the stopwatch
    time_start = tm.time_ns()

    # Define test directory to extract data from
    test_repo = Path(
        "/dls/ebic/data/staff-scratch/tieneupin/projects/murfey-clem/test-data/nt26538-160/raw"
    )  # Create as Path object
    print(f"Test repository is {test_repo}")

    file_ext = ".lif"  # Look only for .lif files

    # Search in test repo
    file_list = list(test_repo.glob("*.lif"))  # Convert to list object
    file_list.sort()  # Sort in alphabetical order

    for f in range(len(file_list)):
        # if not f == 0:  # Select one file to work with
        #     continue

        file = file_list[f]

        # Extract data
        xml_tree = extract_xml_metadata(file=file)  # Get and save metadata
        lif_file = read_lif_file(file=file)  # Get image stacks

    # Stop the stopwatch
    time_stop = tm.time_ns()
    # Report time taken
    time_diff = time_stop - time_start  # In ns
    print(f"Time to completion was {round(time_diff * 10**-9, 2)} s")
