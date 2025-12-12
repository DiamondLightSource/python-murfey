from __future__ import annotations

import logging
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, List, NamedTuple

import xmltodict

from murfey.client.instance_environment import MurfeyInstanceEnvironment, SampleInfo
from murfey.util.client import capture_post, get_machine_config_client

logger = logging.getLogger("murfey.client.context")


def _atlas_destination(
    environment: MurfeyInstanceEnvironment, source: Path, token: str
) -> Path:
    machine_config = get_machine_config_client(
        str(environment.url.geturl()),
        token,
        instrument_name=environment.instrument_name,
        demo=environment.demo,
    )
    for i, destination_part in enumerate(
        Path(environment.default_destinations[source]).parts
    ):
        if destination_part == environment.visit:
            return Path(machine_config.get("rsync_basepath", "")) / "/".join(
                Path(environment.default_destinations[source]).parent.parts[: i + 1]
            )
    return (
        Path(machine_config.get("rsync_basepath", ""))
        / Path(environment.default_destinations[source]).parent
        / environment.visit
    )


def ensure_dcg_exists(
    collection_type: str,
    metadata_source: Path,
    environment: MurfeyInstanceEnvironment,
    token: str,
) -> str | None:
    """Create  a data collection group"""
    if collection_type == "tomo":
        experiment_type_id = 36
        session_file = metadata_source / "Session.dm"
    elif collection_type == "spa":
        experiment_type_id = 37
        session_file = metadata_source / "EpuSession.dm"
        for h in entry_points(group="murfey.hooks"):
            try:
                if h.name == "get_epu_session_metadata":
                    h.load()(
                        destination_dir=session_file.parent,
                        environment=environment,
                        token=token,
                    )
            except Exception as e:
                logger.warning(f"Get EPU session hook failed: {e}")
    else:
        logger.error(f"Unknown collection type {collection_type}")
        return None

    if not session_file.is_file():
        logger.warning(f"Cannot find session file {str(session_file)}")
        dcg_tag = (
            str(metadata_source).replace(f"/{environment.visit}", "").replace("//", "/")
        )
        dcg_data = {
            "experiment_type_id": experiment_type_id,
            "tag": dcg_tag,
        }
    else:
        with open(session_file, "r") as session_xml:
            session_data = xmltodict.parse(session_xml.read())

        if collection_type == "tomo":
            windows_path = session_data["TomographySession"]["AtlasId"]
        else:
            windows_path = session_data["EpuSessionXml"]["Samples"]["_items"][
                "SampleXml"
            ][0]["AtlasId"]["#text"]

        logger.info(f"Windows path to atlas metadata found: {windows_path}")
        if not windows_path:
            logger.warning("No atlas metadata path found")
            return None
        visit_index = windows_path.split("\\").index(environment.visit)
        partial_path = "/".join(windows_path.split("\\")[visit_index + 1 :])
        logger.info("Partial Linux path successfully constructed from Windows path")

        source_visit_dir = metadata_source.parent
        logger.info(
            f"Looking for atlas XML file in metadata directory {str((source_visit_dir / partial_path).parent)}"
        )
        atlas_xml_path = list(
            (source_visit_dir / partial_path).parent.glob("Atlas_*.xml")
        )[0]
        logger.info(f"Atlas XML path {str(atlas_xml_path)} found")
        with open(atlas_xml_path, "rb") as atlas_xml:
            atlas_xml_data = xmltodict.parse(atlas_xml)
            atlas_original_pixel_size = float(
                atlas_xml_data["MicroscopeImage"]["SpatialScale"]["pixelSize"]["x"][
                    "numericValue"
                ]
            )
        # need to calculate the pixel size of the downscaled image
        atlas_pixel_size = atlas_original_pixel_size * 7.8
        logger.info(f"Atlas image pixel size determined to be {atlas_pixel_size}")

        for p in partial_path.split("/"):
            if p.startswith("Sample"):
                sample = int(p.replace("Sample", ""))
                break
        else:
            logger.warning(f"Sample could not be identified for {metadata_source}")
            return None
        environment.samples[metadata_source] = SampleInfo(
            atlas=Path(partial_path), sample=sample
        )

        dcg_search_dir = (
            str(metadata_source).replace(f"/{environment.visit}", "").replace("//", "/")
        )
        if collection_type == "tomo":
            dcg_tag = dcg_search_dir
        else:
            dcg_images_dirs = sorted(
                Path(dcg_search_dir).glob("Images-Disc*"),
                key=lambda x: x.stat().st_ctime,
            )
            if not dcg_images_dirs:
                logger.warning(f"Cannot find Images-Disc* in {dcg_search_dir}")
                return None
            dcg_tag = str(dcg_images_dirs[-1])

        dcg_data = {
            "experiment_type_id": experiment_type_id,
            "tag": dcg_tag,
            "atlas": str(
                _atlas_destination(environment, metadata_source, token)
                / environment.samples[metadata_source].atlas.parent
                / atlas_xml_path.with_suffix(".jpg").name
            ).replace("//", "/"),
            "sample": environment.samples[metadata_source].sample,
            "atlas_pixel_size": atlas_pixel_size,
        }
    capture_post(
        base_url=str(environment.url.geturl()),
        router_name="workflow.router",
        function_name="register_dc_group",
        token=token,
        visit_name=environment.visit,
        session_id=environment.murfey_session,
        data=dcg_data,
    )
    return dcg_tag


class ProcessingParameter(NamedTuple):
    name: str
    label: str
    default: Any = None


def detect_acquisition_software(dir_for_transfer: Path) -> str:
    glob = dir_for_transfer.glob("*")
    for f in glob:
        if f.name.startswith("EPU") or f.name.startswith("GridSquare"):
            return "epu"
        if f.name.startswith("Position") or f.suffix == ".mdoc":
            return "tomo"
    return ""


class Context:
    user_params: List[ProcessingParameter] = []
    metadata_params: List[ProcessingParameter] = []

    def __init__(self, name: str, acquisition_software: str, token: str):
        self._acquisition_software = acquisition_software
        self._token = token
        self.name = name
        self.data_collection_parameters: dict = {}

    def post_transfer(
        self,
        transferred_file: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ):
        # Search external packages for additional hooks to include in Murfey
        for h in entry_points(group="murfey.post_transfer_hooks"):
            if h.name == self.name:
                h.load()(transferred_file, environment=environment, **kwargs)

    def post_first_transfer(
        self,
        transferred_file: Path,
        environment: MurfeyInstanceEnvironment | None = None,
        **kwargs,
    ):
        self.post_transfer(transferred_file, environment=environment, **kwargs)

    def gather_metadata(
        self, metadata_file: Path, environment: MurfeyInstanceEnvironment | None = None
    ):
        raise NotImplementedError(
            f"gather_metadata must be declared in derived class to be used: {self}"
        )
