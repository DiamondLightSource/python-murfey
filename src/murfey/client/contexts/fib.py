from __future__ import annotations

import logging
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

import xmltodict

from murfey.client.context import Context
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.client import capture_post

logger = logging.getLogger("murfey.client.contexts.fib")

lock = threading.Lock()


class Lamella(NamedTuple):
    name: str
    number: int
    angle: float | None = None


class MillingProgress(NamedTuple):
    file: Path
    timestamp: float


def _number_from_name(name: str) -> int:
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


def _get_source(file_path: Path, environment: MurfeyInstanceEnvironment) -> Path | None:
    """
    Returns the Path of the file on the client PC.
    """
    for s in environment.sources:
        if file_path.is_relative_to(s):
            return s
    return None


def _file_transferred_to(
    environment: MurfeyInstanceEnvironment,
    source: Path,
    file_path: Path,
    rsync_basepath: Path,
) -> Path | None:
    """
    Returns the Path of the transferred file on the DLS file system.
    """
    # Construct destination path
    base_destination = rsync_basepath / Path(environment.default_destinations[source])
    # Add visit number to the path if it's not present in default destination
    if environment.visit not in environment.default_destinations[source]:
        base_destination = base_destination / environment.visit
    destination = base_destination / file_path.relative_to(source)
    return destination


class FIBContext(Context):
    def __init__(
        self,
        acquisition_software: str,
        basepath: Path,
        machine_config: dict,
        token: str,
    ):
        super().__init__("FIB", acquisition_software, token)
        self._basepath = basepath
        self._machine_config = machine_config
        self._milling: dict[int, list[MillingProgress]] = {}
        self._lamellae: dict[int, Lamella] = {}

    def post_transfer(
        self,
        transferred_file: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ):
        super().post_transfer(transferred_file, environment=environment, **kwargs)
        if environment is None:
            logger.warning("No environment passed in")
            return

        # -----------------------------------------------------------------------------
        # AutoTEM
        # -----------------------------------------------------------------------------
        if self._acquisition_software == "autotem":
            parts = transferred_file.parts
            if "DCImages" in parts and transferred_file.suffix == ".png":
                lamella_name = parts[parts.index("Sites") + 1]
                lamella_number = _number_from_name(lamella_name)
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
                    self._lamellae[lamella_number] = Lamella(
                        name=lamella_name,
                        number=lamella_number,
                    )
                if not (source := _get_source(transferred_file, environment)):
                    logger.warning(f"No source found for file {transferred_file}")
                    return
                if not (
                    destination_file := _file_transferred_to(
                        environment=environment,
                        source=source,
                        file_path=transferred_file,
                        rsync_basepath=Path(
                            self._machine_config.get("rsync_basepath", "")
                        ),
                    )
                ):
                    logger.warning(
                        f"File {transferred_file.name!r} not found on storage system"
                    )
                    return
                if not self._milling.get(lamella_number):
                    self._milling[lamella_number] = [
                        MillingProgress(
                            timestamp=timestamp,
                            file=destination_file,
                        )
                    ]
                else:
                    self._milling[lamella_number].append(
                        MillingProgress(
                            timestamp=timestamp,
                            file=destination_file,
                        )
                    )
                gif_list = [
                    l.file
                    for l in sorted(
                        self._milling[lamella_number], key=lambda x: x.timestamp
                    )
                ]
                raw_directory = Path(
                    environment.default_destinations[self._basepath]
                ).name
                # Submit job to backend to construct a GIF
                capture_post(
                    base_url=str(environment.url.geturl()),
                    router_name="workflow.correlative_router",
                    function_name="make_gif",
                    token=self._token,
                    instrument_name=environment.instrument_name,
                    year=datetime.now().year,
                    visit_name=environment.visit,
                    session_id=environment.murfey_session,
                    data={
                        "lamella_number": lamella_number,
                        "images": [str(file) for file in gif_list],
                        "raw_directory": raw_directory,
                    },
                )
            elif transferred_file.name == "ProjectData.dat":
                with open(transferred_file, "r") as dat:
                    try:
                        for_parsing = dat.read()
                    except Exception:
                        logger.warning(f"Failed to parse file {transferred_file}")
                        return
                    metadata = xmltodict.parse(for_parsing)
                sites = metadata["AutoTEM"]["Project"]["Sites"]["Site"]
                for site in sites:
                    number = _number_from_name(site["Name"])
                    milling_angle = site["Workflow"]["Recipe"][0]["Activites"][
                        "MillingAngleActivity"
                    ].get("MillingAngle")
                    if self._lamellae.get(number) and milling_angle:
                        self._lamellae[number]._replace(
                            angle=float(milling_angle.split(" ")[0])
                        )
        # -----------------------------------------------------------------------------
        # Maps
        # -----------------------------------------------------------------------------
        elif self._acquisition_software == "maps":
            if (
                # Electron snapshot images are grid atlases
                "Electron Snapshot" in transferred_file.name
                and transferred_file.suffix in (".tif", ".tiff")
            ):
                if not (source := _get_source(transferred_file, environment)):
                    logger.warning(f"No source found for file {transferred_file}")
                    return
                if not (
                    destination_file := _file_transferred_to(
                        environment=environment,
                        source=source,
                        file_path=transferred_file,
                        rsync_basepath=Path(
                            self._machine_config.get("rsync_basepath", "")
                        ),
                    )
                ):
                    logger.warning(
                        f"File {transferred_file.name!r} not found on storage system"
                    )
                    return

                # Register image in database
                self._register_atlas(destination_file, environment)
                return

        # -----------------------------------------------------------------------------
        # Meteor
        # -----------------------------------------------------------------------------
        elif self._acquisition_software == "meteor":
            pass

    def _register_atlas(self, file: Path, environment: MurfeyInstanceEnvironment):
        """
        Constructs the URL and dictionary to be posted to the server, which then triggers
        the processing of the electron snapshot image.
        """

        try:
            capture_post(
                base_url=str(environment.url.geturl()),
                router_name="workflow_fib.router",
                function_name="register_fib_atlas",
                token=self._token,
                instrument_name=environment.instrument_name,
                data={"file": str(file)},
                session_id=environment.murfey_session,
            )
            logger.info(f"Registering atlas image {file.name!r}")
            return True
        except Exception as e:
            logger.error(f"Error encountered registering atlas image {file.name}:\n{e}")
            return False
