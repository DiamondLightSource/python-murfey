"""
Array manipulation and image processing functions for the images acquired via the Leica
light microscope.
"""

import logging
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
from tifffile import imwrite

# Create logger object to output messages with
logger = logging.getLogger("murfey.util.clem.images")


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


def process_img_stk(
    array: np.ndarray,
    initial_bit_depth: int,
    target_bit_depth: int = 8,
    rescale: bool = False,
) -> np.ndarray:
    """
    Processes the NumPy array, rescaling intensities and converting to the desired bit
    depth as needed.
    """

    # Use shorter aliases in function
    arr = array
    bdi = initial_bit_depth
    bdt = target_bit_depth

    if not any(bdi == b for b in [8, 16, 32, 64]):
        logger.info(f"{bdi}-bit is not supported by NumPy; converting to 16-bit")
        arr = (
            rescale_to_bit_depth(array=arr, initial_bit_depth=bdi, target_bit_depth=16)
            if np.max(arr) > 0
            else change_bit_depth(
                array=arr,
                target_bit_depth=16,
            )
        )
        bdi = 16  # Overwrite

    # Rescale intensity values for fluorescent channels
    if rescale is True:
        logger.info("Rescaling channel across its bit depth")
        arr = (
            rescale_across_channel(
                array=arr,
                bit_depth=bdi,
                percentile_range=(0.5, 99.5),
                round_to=16,
            )
            if np.max(arr) > 0
            else arr
        )

    # Convert to desired bit depth
    if not bdi == bdt:
        logger.info(f"Converting to {bdt}-bit image")
        arr = (
            rescale_to_bit_depth(
                array=arr,
                initial_bit_depth=bdi,
                target_bit_depth=bdt,
            )
            if np.max(arr) > 0
            else change_bit_depth(
                array=arr,
                target_bit_depth=bdt,
            )
        )
    else:
        logger.info(f"Image is already {bdt}-bit")

    return arr


def write_to_tiff(
    array: np.ndarray,
    save_dir: Path,
    file_name: str,
    # Resolution in pixels per unit length
    x_res: float,
    y_res: float,
    z_res: float,
    units: str,
    axes: str,
    image_labels: List[str],
):
    # Use shorter aliases and calculate what is needed
    arr = array
    z_size = (1 / z_res) if z_res > 0 else float(0)

    # Save as a greyscale TIFF
    save_name = save_dir.joinpath(file_name + ".tiff")
    logger.info(f"Saving {file_name} image as {save_name}")
    imwrite(
        save_name,
        arr,
        imagej=True,  # ImageJ compatible
        photometric="minisblack",  # Grayscale image
        shape=np.shape(arr),
        dtype=arr.dtype,
        resolution=(x_res * 10**6 / 10**6, y_res * 10**6 / 10**6),
        metadata={
            "spacing": z_size,
            "unit": units,
            "axes": axes,
            "Labels": image_labels,
        },
    )

    return arr
