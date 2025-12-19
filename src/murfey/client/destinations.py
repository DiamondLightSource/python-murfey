from __future__ import annotations

import logging
from pathlib import Path

from murfey.client.instance_environment import (
    MurfeyInstanceEnvironment,
    global_env_lock,
)
from murfey.util import secure_path
from murfey.util.client import capture_get, capture_post

logger = logging.getLogger("murfey.client.destinations")


def find_longest_data_directory(
    match_path: Path, data_directories: list[str] | list[Path]
):
    """
    Determine the longest path in the data_directories list
    which the match path is relative to
    """
    base_dir: Path | None = None
    mid_dir: Path | None = None
    for dd in data_directories:
        dd_base = str(Path(dd).absolute())
        if str(match_path).startswith(str(dd)) and len(dd_base) > len(str(base_dir)):
            base_dir = Path(dd_base)
            mid_dir = match_path.absolute().relative_to(Path(base_dir)).parent
    return base_dir, mid_dir


def determine_default_destination(
    visit: str,
    source: Path,
    destination: str,
    environment: MurfeyInstanceEnvironment,
    token: str,
    touch: bool = False,
    extra_directory: str = "",
    use_suggested_path: bool = True,
) -> str:
    if not destination or not visit:
        raise ValueError(f"No destination ({destination}) or visit ({visit}) supplied")
    machine_data = capture_get(
        base_url=str(environment.url.geturl()),
        router_name="session_control.router",
        function_name="machine_info_by_instrument",
        token=token,
        instrument_name=environment.instrument_name,
    ).json()
    base_path, mid_path = find_longest_data_directory(
        source, machine_data.get("data_directories", [])
    )
    if not base_path:
        raise ValueError(f"No data directory found for {source}")
    if source.absolute() == base_path.absolute():
        raise ValueError(
            f"Source is the same as the base path {secure_path(source.absolute())}"
        )

    _default = f"{destination}/{visit}/{source.name}"
    if use_suggested_path:
        with global_env_lock:
            if source.name == "Images-Disc1":
                source_name = source.parent.name
            elif source.name.startswith("Sample"):
                source_name = f"{source.parent.name}_{source.name}"
            else:
                source_name = source.name
            if environment.destination_registry.get(source_name):
                _default = environment.destination_registry[source_name]
            else:
                suggested_path_response = capture_post(
                    base_url=str(environment.url.geturl()),
                    router_name="file_io_instrument.router",
                    function_name="suggest_path",
                    token=token,
                    visit_name=visit,
                    session_id=environment.murfey_session,
                    data={
                        "base_path": f"{destination}/{visit}/raw",
                        "touch": touch,
                        "extra_directory": extra_directory,
                    },
                )
                if suggested_path_response is None:
                    raise RuntimeError("Murfey server is unreachable")
                _default = suggested_path_response.json().get("suggested_path")
                environment.destination_registry[source_name] = _default
    return (
        _default + f"/{extra_directory}"
        if not _default.endswith("/")
        else _default + f"{extra_directory}"
    )
