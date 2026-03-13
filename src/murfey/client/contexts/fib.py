from __future__ import annotations

import logging
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional
from xml.etree import ElementTree as ET

import xmltodict

from murfey.client.context import Context
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.client import capture_post

logger = logging.getLogger("murfey.client.contexts.fib")

lock = threading.Lock()


class Lamella(NamedTuple):
    name: str
    number: int
    angle: Optional[float] = None


class MillingProgress(NamedTuple):
    file: Path
    timestamp: float


class ElectronSnapshotMetadata(NamedTuple):
    slot_num: int | None  # Which slot in the FIB-SEM it is from
    image_num: int
    image_dir: str  # Partial path from EMproject.emxml parent to the image
    status: str
    x_len: float | None
    y_len: float | None
    z_len: float | None
    x_center: float | None
    y_center: float | None
    z_center: float | None
    extent: tuple[float, float, float, float] | None
    rotation_angle: float | None


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
        if (match := re.search(r"\(([\d+])\)", name)) is not None
        else 1
    )


def _parse_electron_snapshot_metadata(xml_file: Path):
    metadata_dict = {}
    root = ET.parse(xml_file).getroot()
    datasets = root.findall(".//Datasets/Dataset")
    for dataset in datasets:
        # Extract all string-based values
        name, image_dir, status = [
            node.text
            if ((node := dataset.find(node_path)) is not None and node.text is not None)
            else ""
            for node_path in (
                ".//Name",
                ".//FinalImages",
                ".//Status",
            )
        ]

        # Extract all float values
        cx, cy, cz, x_len, y_len, z_len, rotation_angle = [
            float(node.text)
            if ((node := dataset.find(node_path)) is not None and node.text is not None)
            else None
            for node_path in (
                ".//BoxCenter/CenterX",
                ".//BoxCenter/CenterY",
                ".//BoxCenter/CenterZ",
                ".//BoxSize/SizeX",
                ".//BoxSize/SizeY",
                ".//BoxSize/SizeZ",
                ".//RotationAngle",
            )
        ]

        # Calculate the extent of the image
        extent = None
        if (
            cx is not None
            and cy is not None
            and x_len is not None
            and y_len is not None
        ):
            extent = (
                x_len - (cx / 2),
                x_len + (cx / 2),
                y_len - (cy / 2),
                y_len - (cy / 2),
            )

        # Append metadata for current site to dict
        metadata_dict[name] = ElectronSnapshotMetadata(
            slot_num=None if cx is None else (1 if cx < 0 else 2),
            image_num=_number_from_name(name),
            status=status,
            image_dir=image_dir,
            x_len=x_len,
            y_len=y_len,
            z_len=z_len,
            x_center=cx,
            y_center=cy,
            z_center=cz,
            extent=extent,
            rotation_angle=rotation_angle,
        )
    return metadata_dict


class FIBContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path, token: str):
        super().__init__("FIB", acquisition_software, token)
        self._basepath = basepath
        self._milling: Dict[int, List[MillingProgress]] = {}
        self._lamellae: Dict[int, Lamella] = {}
        self._electron_snapshots: Dict[str, Path] = {}
        self._electron_snapshot_metadata: Dict[str, ElectronSnapshotMetadata] = {}
        self._electron_snapshots_submitted: set[str] = set()

    def post_transfer(
        self,
        transferred_file: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ):
        super().post_transfer(transferred_file, environment=environment, **kwargs)
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
                if not self._milling.get(lamella_number):
                    self._milling[lamella_number] = [
                        MillingProgress(
                            timestamp=timestamp,
                            file=transferred_file,
                        )
                    ]
                else:
                    self._milling[lamella_number].append(
                        MillingProgress(
                            timestamp=timestamp,
                            file=transferred_file,
                        )
                    )
                gif_list = [
                    l.file
                    for l in sorted(
                        self._milling[lamella_number], key=lambda x: x.timestamp
                    )
                ]
                if environment:
                    raw_directory = Path(
                        environment.default_destinations[self._basepath]
                    ).name
                    # post gif list to gif making API call
                    capture_post(
                        base_url=str(environment.url.geturl()),
                        router_name="workflow.correlative_router",
                        function_name="make_gif",
                        token=self._token,
                        year=datetime.now().year,
                        visit_name=environment.visit,
                        session_id=environment.murfey_session,
                        data={
                            "lamella_number": lamella_number,
                            "images": gif_list,
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
            # Electron snapshot metadata file
            if transferred_file.name == "EMproject.emxml":
                # Extract all "Electron Snapshot" metadata and store it
                self._electron_snapshot_metadata = _parse_electron_snapshot_metadata(
                    transferred_file
                )
                # If dataset hasn't been transferred, register it
                for dataset_name in list(self._electron_snapshot_metadata.keys()):
                    if dataset_name not in self._electron_snapshots_submitted:
                        if dataset_name in self._electron_snapshots:
                            logger.info(f"Registering {dataset_name!r}")

                            ## Workflow to trigger goes here

                            # Clear old entry after triggering workflow
                            self._electron_snapshots_submitted.add(dataset_name)
                            with lock:
                                self._electron_snapshots.pop(dataset_name, None)
                                self._electron_snapshot_metadata.pop(dataset_name, None)
                        else:
                            logger.debug(f"Waiting for image for {dataset_name}")
            # Electron snapshot image
            elif (
                "Electron Snapshot" in transferred_file.name
                and transferred_file.suffix in (".tif", ".tiff")
            ):
                # Store file in Context memory
                dataset_name = transferred_file.stem
                self._electron_snapshots[dataset_name] = transferred_file

                if dataset_name not in self._electron_snapshots_submitted:
                    # If the metadata and image are both present, register dataset
                    if dataset_name in list(self._electron_snapshot_metadata.keys()):
                        logger.info(f"Registering {dataset_name!r}")

                        ## Workflow to trigger goes here

                        # Clear old entry after triggering workflow
                        self._electron_snapshots_submitted.add(dataset_name)
                        with lock:
                            self._electron_snapshots.pop(dataset_name, None)
                            self._electron_snapshot_metadata.pop(dataset_name, None)
                    else:
                        logger.debug(f"Waiting for metadata for {dataset_name}")
        # -----------------------------------------------------------------------------
        # Meteor
        # -----------------------------------------------------------------------------
        elif self._acquisition_software == "meteor":
            pass
