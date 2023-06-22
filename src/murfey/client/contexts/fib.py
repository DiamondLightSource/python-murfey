from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, NamedTuple

import requests

from murfey.client.context import Context
from murfey.client.instance_environment import MurfeyInstanceEnvironment

logger = logging.getLogger("murfey.client.contexts.fib")


class Lamella(NamedTuple):
    name: str
    number: int
    file: Path
    timestamp: float
    angle: float = 0


class FIBContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path):
        super().__init__(acquisition_software)
        self._basepath = basepath
        self._lamellae: Dict[int, List[Lamella]] = {}

    def post_transfer(
        self,
        transferred_file: Path,
        role: str = "",
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ):
        if self._acquisition_software == "autotem":
            parts = transferred_file.parts
            if "DCImages" in parts and transferred_file.suffix == ".png":
                lamella_name = parts[parts.index("Sites") + 1]
                lamella_number = int(
                    lamella_name.strip()
                    .replace("Lamella", "")
                    .replace("(", "")
                    .replace(")", "")
                    or 1
                )
                time_from_name = transferred_file.name.split("-")[:6]
                timestamp = datetime.timestamp(
                    datetime(
                        year=int(time_from_name[0]),
                        month=int(time_from_name[1]),
                        day=int(time_from_name[2]),
                        hour=int(time_from_name[3]),
                        minute=int(time_from_name[4]),
                        second=int(time_from_name[5]),
                    )
                )
                if not self._lamellae.get(lamella_number):
                    self._lamellae[lamella_number] = [
                        Lamella(
                            name=lamella_name,
                            number=lamella_number,
                            timestamp=timestamp,
                            file=transferred_file,
                        )
                    ]
                else:
                    self._lamellae[lamella_number].append(
                        Lamella(
                            name=lamella_name,
                            number=lamella_number,
                            timestamp=timestamp,
                            file=transferred_file,
                        )
                    )
                gif_list = [
                    l.file
                    for l in sorted(
                        self._lamellae[lamella_number], key=lambda x: x.timestamp
                    )
                ]
                if environment:
                    raw_directory = Path(
                        environment.default_destinations[self._basepath]
                    ).name
                    # post gif list to gif making API call
                    requests.post(
                        f"{str(environment.url.geturl())}/visits/{datetime.now().year}/{environment.visit}/make_milling_gif",
                        json={
                            "lamella_number": lamella_number,
                            "images": gif_list,
                            "raw_directory": raw_directory,
                        },
                    )
