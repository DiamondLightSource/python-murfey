"""
Functions to handle file types that can be read in as XML Element objects.
These include, but are not limited to:
    1.  XML (self-explanatory)
    2.  XLIF (used when reconstructing image stacks from TIFFs)
    3.  XLEF
    4.  XLCF
"""

import logging
from pathlib import Path
from typing import Generator, List, Optional
from xml.etree import ElementTree as ET

from readlif.reader import LifFile

from murfey.util import sanitise

# Create logger object to output messages with
logger = logging.getLogger("murfey.util.clem.xml")


def get_lif_xml_metadata(
    file: LifFile,
    save_xml: Optional[Path] = None,
) -> ET.Element:
    """
    Extracts and returns the metadata from the LIF file as a formatted XML Element.
    It can be optionally saved as an XML file to the specified file path.
    """

    # Use readlif function to get XML metadata
    xml_root: ET.Element = file.xml_root  # This one for navigating
    xml_tree = ET.ElementTree(xml_root)  # This one for saving

    # Skip saving the metadata if save_xml not provided
    if save_xml:
        xml_file = str(save_xml)  # Convert Path to string
        ET.indent(xml_tree, "  ")  # Format with proper indentation
        xml_tree.write(xml_file, encoding="utf-8")  # Save
        logger.info(f"File metadata saved to {sanitise(xml_file)}")

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


def get_axis_resolution(element: ET.Element) -> float:
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
