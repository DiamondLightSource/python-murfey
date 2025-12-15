from __future__ import annotations

import logging
from pathlib import Path

from murfey.client.instance_environment import (
    MurfeyInstanceEnvironment,
    global_env_lock,
)
from murfey.util.client import capture_get, capture_post

logger = logging.getLogger("murfey.client.destinations")


def determine_default_destination(
    visit: str,
    source: Path,
    destination: str,
    environment: MurfeyInstanceEnvironment,
    token: str,
    touch: bool = False,
    extra_directory: str = "",
    include_mid_path: bool = True,
    use_suggested_path: bool = True,
) -> str:
    machine_data = capture_get(
        base_url=str(environment.url.geturl()),
        router_name="session_control.router",
        function_name="machine_info_by_instrument",
        token=token,
        instrument_name=environment.instrument_name,
    ).json()
    _default = f"{destination}/{visit}"
    for data_dir in machine_data.get("data_directories", []):
        if source.absolute() == Path(data_dir).absolute():
            _default = f"{destination}/{visit}"
            break
        else:
            mid_path = source.absolute().relative_to(Path(data_dir).absolute())
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
                                "base_path": f"{destination}/{visit}/{mid_path.parent if include_mid_path else ''}/raw",
                                "touch": touch,
                                "extra_directory": extra_directory,
                            },
                        )
                        if suggested_path_response is None:
                            raise RuntimeError("Murfey server is unreachable")
                        _default = suggested_path_response.json().get("suggested_path")
                        environment.destination_registry[source_name] = _default
            else:
                _default = f"{destination}/{visit}/{mid_path if include_mid_path else source.name}"
            break
    return (
        _default + f"/{extra_directory}"
        if not _default.endswith("/")
        else _default + f"{extra_directory}"
    )
