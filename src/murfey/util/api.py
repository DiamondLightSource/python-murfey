"""
Utility functions to help with URL path lookups using function names for our FastAPI
servers. This makes reference to the route_manifest.yaml file that is also saved in
this directory. This routes_manifest.yaml file should be regenerated whenver changes
are made to the API endpoints. This can be done using the 'generate_route_manifest'
CLI function.
"""

from __future__ import annotations

import re
from functools import lru_cache
from logging import getLogger
from pathlib import Path
from typing import Any

import yaml

import murfey.util

logger = getLogger("murfey.util.api")

route_manifest_file = Path(murfey.util.__path__[0]) / "route_manifest.yaml"


@lru_cache(maxsize=1)  # Load the manifest once and reuse
def load_route_manifest(
    file: Path = route_manifest_file,
):
    with open(file, "r") as f:
        return yaml.safe_load(f)


def find_unique_index(
    pattern: str,
    candidates: list[str],
    exact: bool = False,  # Allows partial matches
) -> int:
    """
    Finds the index of a unique entry in a list.
    """
    counter = 0
    matches = []
    index = 0
    for i, candidate in enumerate(candidates):
        if (not exact and pattern in candidate) or (exact and pattern == candidate):
            counter += 1
            matches.append(candidate)
            index = i
    if counter == 0:
        message = f"No match found for {pattern!r}"
        logger.error(message)
        raise KeyError(message)
    if counter > 1:
        message = f"Ambiguous match for {pattern!r}: {matches}"
        logger.error(message)
        raise KeyError(message)
    return index


def render_path(path_template: str, kwargs: dict) -> str:
    """
    Replace all FastAPI-style {param[:converter]} path parameters with corresponding
    values from kwargs.
    """

    pattern = re.compile(r"{([^}]+)}")  # Look for all path params

    def replace(match):
        raw_str = match.group(1)
        param_name = raw_str.split(":")[0]  # Ignore :converter in the field
        if param_name not in kwargs:
            message = (
                f"Error constructing URL for {path_template!r}; "
                f"missing path parameter {param_name!r}"
            )
            logger.error(message)
            raise KeyError(message)
        return str(kwargs[param_name])

    return pattern.sub(replace, path_template)


def url_path_for(
    router_name: str,  # With logic for partial matches
    function_name: str,  # With logic for partial matches
    **kwargs,  # Takes any path param and matches it against curly bracket contents
):
    """
    Utility function that takes the function name and API router name, along with all
    necessary path parameters, retrieves the matching URL path template from the route
    manifest, and returns a correctly populated instance of the URL path.
    """
    # Use 'Any' first and slowly reveal types as it is unpacked
    route_manifest: dict[str, list[Any]] = load_route_manifest()

    # Load the routes in the desired router
    routers = list(route_manifest.keys())
    routes: list[dict[str, Any]] = route_manifest[
        routers[find_unique_index(router_name, routers, exact=False)]
    ]

    # Search router for the function
    route_info = routes[
        find_unique_index(function_name, [r["function"] for r in routes], exact=True)
    ]

    # Unpack the dictionary
    route_path: str = route_info["path"]
    path_params: list[dict[str, str]] = route_info["path_params"]

    # Validate the stored path params against the ones provided
    if path_params:
        for path_param in path_params:
            param_name = path_param["name"]
            param_type = path_param["type"]
            if param_name not in kwargs.keys():
                message = (
                    f"Error validating parameters for {function_name!r}; "
                    f"path parameter {param_name!r} was not provided"
                )
                logger.error(message)
                raise KeyError(message)
            # Skip complicated type resolution for now
            if param_type.startswith("typing."):
                continue
            elif type(kwargs[param_name]).__name__ not in param_type:
                message = (
                    f"Error validating parameters for {function_name!r}; "
                    f"{param_name!r} must be {param_type!r}, "
                    f"received {type(kwargs[param_name]).__name__!r}"
                )
                logger.error(message)
                raise TypeError(message)

    # Render and return the path
    return render_path(route_path, kwargs)


if __name__ == "__main__":
    # Run test on some existing routes
    url_path = url_path_for(
        "workflow.tomo_router",
        "register_tilt",
        visit_name="nt15587-15",
        session_id=2,
    )
    print(url_path)
