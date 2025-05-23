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
from pathlib import Path
from typing import Any

import yaml

import murfey.util

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
) -> int:
    """
    Finds the index of a unique entry in a list.
    """
    counter = 0
    matches = []
    index = 0
    for i, candidate in enumerate(candidates):
        if pattern in candidate:
            counter += 1
            matches.append(candidate)
            index = i
    if counter == 0:
        raise KeyError(f"No match found for {pattern!r}")
    if counter > 1:
        raise KeyError(f"Ambiguous match for {pattern!r}: {matches}")
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
            raise KeyError(f"Missing path parameter: {param_name}")
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
        routers[find_unique_index(router_name, routers)]
    ]

    # Search router for the function
    route_info = routes[
        find_unique_index(function_name, [r["function"] for r in routes])
    ]

    # Unpack the dictionary
    route_path: str = route_info["path"]
    path_params: list[dict[str, str]] = route_info["path_params"]

    # Validate the kwargs provided
    for param, value in kwargs.items():
        # Check if the name is not a match
        if param not in [p["name"] for p in path_params] and path_params:
            raise KeyError(f"Unknown path parameter provided: {param}")
        for path_param in path_params:
            if (
                path_param["name"] == param
                and type(value).__name__ != path_param["type"]
            ):
                raise TypeError(
                    f"'{param}' must be {path_param['type']!r}; "
                    f"received {type(value).__name__!r}"
                )

    # Render and return the path
    return render_path(route_path, kwargs)


if __name__ == "__main__":
    # Run test on some existing routes
    url_path = url_path_for(
        "api.router",
        "register_processing_parameters",
        session_id=3,
    )
    print(url_path)
