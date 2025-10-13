"""
CLI to generate a manifest of the FastAPI router paths present in both the instrument
server and backend server to enable lookup of the URLs based on function name.
"""

import contextlib
import importlib
import inspect
import io
import pkgutil
from argparse import ArgumentParser
from pathlib import Path
from types import ModuleType
from typing import Any

import yaml
from fastapi import APIRouter

import murfey


class PrettierDumper(yaml.Dumper):
    """
    Custom YAML Dumper class that sets `indentless` to False. This generates a YAML
    file that is then compliant with Prettier's formatting style
    """

    def increase_indent(self, flow=False, indentless=False):
        # Force 'indentless=False' so list items align with Prettier
        return super(PrettierDumper, self).increase_indent(flow, indentless=False)


def prettier_str_representer(dumper, data):
    """
    Helper function to format strings according to Prettier's standards:
    - No quoting unless it can be misinterpreted as another data type
    - When quoting, use double quotes unless string already contains double quotes
    """

    def is_implicitly_resolved(value: str) -> bool:
        for (
            first_char,
            resolvers,
        ) in yaml.resolver.Resolver.yaml_implicit_resolvers.items():
            if first_char is None or (value and value[0] in first_char):
                for resolver in resolvers:
                    if len(resolver) == 3:
                        _, regexp, _ = resolver
                    else:
                        _, regexp = resolver
                    if regexp.match(value):
                        return True
        return False

    # If no quoting is needed, use default plain style
    if not is_implicitly_resolved(data):
        return dumper.represent_scalar("tag:yaml.org,2002:str", data)

    # If the string already contains double quotes, fall back to single quotes
    if '"' in data and "'" not in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="'")

    # Otherwise, prefer double quotes
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='"')


PrettierDumper.add_representer(str, prettier_str_representer)


def find_routers(name: str) -> dict[str, APIRouter]:
    def _extract_routers_from_module(module: ModuleType):
        routers = {}
        for name, obj in inspect.getmembers(module):
            if isinstance(obj, APIRouter):
                module_path = module.__name__
                key = f"{module_path}.{name}"
                routers[key] = obj
        return routers

    routers = {}

    # Silence output during import and only return messages if imports fail
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
        # Import the module or package
        try:
            root = importlib.import_module(name)
        except Exception as e:
            captured_logs = buffer.getvalue().strip()
            message = f"Cannot import '{name}': {e}"
            if captured_logs:
                message += f"\n--- Captured output ---\n{captured_logs}"
            raise ImportError(message) from e

        # If it's a package, walk through submodules and extract routers from each
        if hasattr(root, "__path__"):
            module_list = pkgutil.walk_packages(root.__path__, prefix=name + ".")
            for _, module_name, _ in module_list:
                try:
                    module = importlib.import_module(module_name)
                except Exception as e:
                    captured_logs = buffer.getvalue().strip()
                    message = f"Cannot import '{name}': {e}"
                    if captured_logs:
                        message += f"\n--- Captured output ---\n{captured_logs}"
                    raise ImportError(message) from e
                routers.update(_extract_routers_from_module(module))

        # Extract directly from single module
        else:
            routers.update(_extract_routers_from_module(root))

    return routers


def get_route_manifest(routers: dict[str, APIRouter]):
    manifest = {}

    for router_name, router in routers.items():
        routes = []
        for route in router.routes:
            path_params = []
            for param in route.dependant.path_params:
                param_type = param.type_ if param.type_ is not None else Any
                param_info = {
                    "name": param.name if hasattr(param, "name") else "",
                    "type": (
                        param_type.__name__
                        if hasattr(param_type, "__name__")
                        else str(param_type)
                    ),
                }
                path_params.append(param_info)
            for route_dependency in route.dependant.dependencies:
                for param in route_dependency.path_params:
                    param_type = param.type_ if param.type_ is not None else Any
                    param_info = {
                        "name": param.name if hasattr(param, "name") else "",
                        "type": (
                            param_type.__name__
                            if hasattr(param_type, "__name__")
                            else str(param_type)
                        ),
                    }
                    path_params.append(param_info)
            route_info = {
                "path": route.path if hasattr(route, "path") else "",
                "function": route.name if hasattr(route, "name") else "",
                "path_params": path_params,
                "methods": list(route.methods) if hasattr(route, "methods") else [],
            }
            routes.append(route_info)
        manifest[router_name] = routes
    return manifest


def run():
    # Set up additional args
    parser = ArgumentParser()
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help=("Outputs the modules being inspected when creating the route manifest"),
    )
    args = parser.parse_args()

    # Find routers
    print("Finding routers...")
    routers = {
        **find_routers("murfey.instrument_server.api"),
        **find_routers("murfey.server.api"),
    }
    # Generate the manifest
    print("Extracting route information")
    manifest = get_route_manifest(routers)

    # Verify
    if args.debug:
        for router_name, routes in manifest.items():
            print(f"Routes found in {router_name!r}")
            for route in routes:
                for key, value in route.items():
                    print(f"\t{key}: {value}")
                print()

    # Save the manifest
    murfey_dir = Path(murfey.__path__[0])
    manifest_file = murfey_dir / "util" / "route_manifest.yaml"
    with open(manifest_file, "w") as file:
        yaml.dump(
            manifest,
            file,
            Dumper=PrettierDumper,
            default_flow_style=False,
            sort_keys=False,
            indent=2,
        )
    print(
        "Route manifest for instrument and backend servers saved to "
        f"{str(manifest_file)!r}"
    )
    exit()


if __name__ == "__main__":
    run()
