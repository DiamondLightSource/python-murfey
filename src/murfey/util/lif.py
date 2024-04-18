"""
Contains functions that help with reading .lif files and converting them into other useful file formats (e.g. .tiff files) as part of the cryo-CLEM workflow
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import tifffile as tif
from matplotlib import pyplot as plt
from readlif.reader import LifFile as lif


def open_lif_file(
        file: Path
        ):
    # Open file
    print(f"Opening file {file}...")
    file = lif(file)  # Open with readlif
    print(f"Successfully opened as a {type(file)}")

    return file

def inspect_lif_file(
        file: lif
        ):
    # Inspect contents
    # Number of sub-files
    num_imgs = len(list(file.get_iter_image()))
    print(f"There are {num_imgs} sub-files in this document")
    
    # Get list of images
    img_list = [f for f in file.get_iter_image()]
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

    bit_depth = img.bit_depth  # Bit depth for each channel in image
    print(f"The bit depth of each channel is {bit_depth}")

    # Try opening image for viewing
    for m in range(mosaic):
        for t in range(time):
            for z in range(depth):  # Iterate through slices
                if not z == 0: continue  # Inspect only one slice
                
                # Set up figure for plotting histograms
                fig, axs  = plt.subplots(nrows=channel, ncols=1,
                                         sharex="all",
                                         sharey="all",
                                         squeeze=False)
                print(f"Shape of axs: {np.shape(axs)}")  # Inspect shape of axs object
                
                frame = []  # Empty array to store RGB channels
                
                for c in range(channel):  # Iterate through image channels
                    # Get subplot axes
                    ax = axs[c][0]

                    # Inspect single channel from single frame
                    frame_c = img.get_frame(z=z, c=c, t=t, m=m)
                    frame_c = np.uint16(np.array(frame_c))
                    
                    # Append to RGB array
                    frame.append(frame_c)

                    # Statistical analysis of pixel distribution
                    print(f"Plotting histogram for channel {c}")
                    bit_size = 2 ** bit_depth[c]  # Number of bits for channel
                    ax.hist(np.ravel(frame), bins=32,
                            range=(0, bit_size - 1))
                    ax.set_title(label=f"Channel {c}",
                                 fontsize="small",
                                 pad=0.1)
                # Close histogram (if needed)
                # plt.close()

                # Convert list of arrays to RGB array
                frame = np.dstack(frame)  # Adds another dimension at back of array

                # Check that frames appended correctly
                print(f"Image slice currently has the shape {np.shape(frame)}")

                # Plot image to display
                plt.figure()
                plt.imshow(frame
                           #vmin=0,
                           #vmax=40000
                           )
                # Close image (if needed)
                # plt.close()

    return None

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
    [
        print(f"{f.stem + f.suffix} | Is it a Path object? {isinstance(f, Path)}")
        for f in file_list
    ]  # Print individually to check

    file = file_list[0]  # Select one file to work with
    print(f"Opening {file}")
    file = open_lif_file(file=file)
    file = inspect_lif_file(file=file)
