import re


def number_from_name(name: str) -> int:
    """
    In the AutoTEM and Maps workflows for the FIB, the sites and images are
    auto-incremented with parenthesised numbers (e.g. "Lamella (2)"), with
    the first site/image typically not having a number.

    This function extracts the number from the file name, and returns 1 if
    no such number is found.
    """
    return (
        int(match.group(1))
        if (match := re.search(r"^[\w\s]+\((\d+)\)$", name)) is not None
        else 1
    )
