"""
Building block functions to help with processing the LM images generated as part of the
cryo-CLEM workflow.
"""

import logging
from typing import Generator, List, Optional, Tuple
from xml.etree import ElementTree as ET

import numpy as np

# Create logger object to output messages with
logger = logging.getLogger("murfey.util.clem.functions")


def _get_image_elements(root: ET.Element) -> List[ET.Element]:
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


def _get_axis_resolution(element: ET.Element) -> float:
    """
    Calculates the resolution (pixels per unit length) for the x-, y-, and z-axes.
    Follows "readlif" convention of subtracting 1 from the number of frames/pixels
    to maintain consistency with its output.
    """
    # Use shortened variables
    elem = element

    # Verify
    if elem.tag != "DimensionDescription" and elem.attrib["Unit"] != "m":
        logger.error("This element does not have dimensional information")
        raise ValueError("This element does not have dimensional information")

    # Calculate
    length = (
        float(elem.attrib["Length"]) - float(elem.attrib["Origin"])
    ) * 10**6  # Convert to um
    pixels = int(elem.attrib["NumberOfElements"])
    resolution = (pixels - 1) / length  # Pixels per um

    return resolution


def _raise_BitDepthError(bit_depth: int):
    """
    Raises an exception if the bit depth value provided is not one that NumPy can
    handle.
    """

    raise Exception(
        "The channel bit depth provided is not compatible with Numpy. "
        "Only 8, 16, 32, and 64-bit channel depths are allowed. "
        f"Current bit depth: {bit_depth}"
    )


def _change_bit_depth(
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
        _raise_BitDepthError(bit_depth)
    return arr


def _rescale_across_channel(
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
        _raise_BitDepthError(bit_depth)

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
        arr = _change_bit_depth(array=arr, target_bit_depth=bit_depth)

    return arr


def _rescale_to_bit_depth(
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
        _raise_BitDepthError(bit_final)

    # Rescale (DIVIDE BEFORE MULTIPLY)
    arr = (arr / (2**bit_init - 1)) * (2**bit_final - 1)

    # Change to correct unsigned integer type
    arr = _change_bit_depth(array=arr, target_bit_depth=bit_final)

    return arr
