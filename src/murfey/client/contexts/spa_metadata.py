import logging
from pathlib import Path
from typing import Dict, Optional

import requests
import xmltodict

from murfey.client.context import Context
from murfey.client.contexts.spa import _file_transferred_to, _get_source
from murfey.client.instance_environment import MurfeyInstanceEnvironment, SampleInfo
from murfey.util.api import url_path_for
from murfey.util.client import (
    authorised_requests,
    capture_post,
    get_machine_config_client,
)
from murfey.util.spa_metadata import (
    FoilHoleInfo,
    get_grid_square_atlas_positions,
    grid_square_data,
)

logger = logging.getLogger("murfey.client.contexts.spa_metadata")

requests.get, requests.post, requests.put, requests.delete = authorised_requests()


def _foil_hole_positions(xml_path: Path, grid_square: int) -> Dict[str, FoilHoleInfo]:
    with open(xml_path, "r") as xml:
        for_parsing = xml.read()
        data = xmltodict.parse(for_parsing)
    data = data["GridSquareXml"]
    if "TargetLocationsEfficient" in data["TargetLocations"].keys():
        # Grids with regular foil holes
        serialization_array = data["TargetLocations"]["TargetLocationsEfficient"][
            "a:m_serializationArray"
        ]
    elif "TargetLocations" in data["TargetLocations"].keys():
        # Lacey grids
        serialization_array = data["TargetLocations"]["TargetLocations"][
            "a:m_serializationArray"
        ]
    else:
        logger.warning(f"Target locations not found for {str(xml_path)}")
        return {}
    required_key = ""
    for key in serialization_array.keys():
        if key.startswith("b:KeyValuePairOfintTargetLocation"):
            required_key = key
            break
    if not required_key:
        logger.info(f"Required key not found for {str(xml_path)}")
        return {}
    foil_holes = {}
    for fh_block in serialization_array[required_key]:
        if fh_block["b:value"]["IsNearGridBar"] == "false":
            image_paths = list(
                (xml_path.parent.parent).glob(
                    f"Images-Disc*/GridSquare_{grid_square}/FoilHoles/FoilHole_{fh_block['b:key']}_*.jpg"
                )
            )
            image_paths.sort(key=lambda x: x.stat().st_ctime)
            image_path: str = str(image_paths[-1]) if image_paths else ""
            pix_loc = fh_block["b:value"]["PixelCenter"]
            stage = fh_block["b:value"]["StagePosition"]
            diameter = fh_block["b:value"]["PixelWidthHeight"]["c:width"]
            foil_holes[fh_block["b:key"]] = FoilHoleInfo(
                id=int(fh_block["b:key"]),
                grid_square_id=grid_square,
                x_location=int(float(pix_loc["c:x"])),
                y_location=int(float(pix_loc["c:y"])),
                x_stage_position=float(stage["c:X"]),
                y_stage_position=float(stage["c:Y"]),
                image=str(image_path),
                diameter=int(float(diameter)),
            )
    return foil_holes


def _atlas_destination(
    environment: MurfeyInstanceEnvironment, source: Path, file_path: Path
) -> Path:
    machine_config = get_machine_config_client(
        str(environment.url.geturl()),
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


class SPAMetadataContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path):
        super().__init__("SPA_metadata", acquisition_software)
        self._basepath = basepath

    def post_transfer(
        self,
        transferred_file: Path,
        environment: Optional[MurfeyInstanceEnvironment] = None,
        **kwargs,
    ):
        super().post_transfer(
            transferred_file=transferred_file,
            environment=environment,
            **kwargs,
        )

        if transferred_file.name == "EpuSession.dm" and environment:
            logger.info("EPU session metadata found")
            with open(transferred_file, "r") as epu_xml:
                data = xmltodict.parse(epu_xml.read())
            windows_path = data["EpuSessionXml"]["Samples"]["_items"]["SampleXml"][0][
                "AtlasId"
            ]["#text"]
            logger.info(f"Windows path to atlas metadata found: {windows_path}")
            visit_index = windows_path.split("\\").index(environment.visit)
            partial_path = "/".join(windows_path.split("\\")[visit_index + 1 :])
            logger.info("Partial Linux path successfully constructed from Windows path")

            source = _get_source(transferred_file, environment)
            if not source:
                logger.warning(
                    f"Source could not be indentified for {str(transferred_file)}"
                )
                return

            source_visit_dir = source.parent

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
                logger.warning(f"Sample could not be identified for {transferred_file}")
                return
            if source:
                environment.samples[source] = SampleInfo(
                    atlas=Path(partial_path), sample=sample
                )
                url = f"{str(environment.url.geturl())}{url_path_for('workflow.router', 'register_dc_group', visit_name=environment.visit, session_id=environment.murfey_session)}"
                dcg_search_dir = "/".join(
                    p for p in transferred_file.parent.parts if p != environment.visit
                )
                dcg_search_dir = (
                    dcg_search_dir[1:]
                    if dcg_search_dir.startswith("//")
                    else dcg_search_dir
                )
                dcg_images_dirs = sorted(
                    Path(dcg_search_dir).glob("Images-Disc*"),
                    key=lambda x: x.stat().st_ctime,
                )
                if not dcg_images_dirs:
                    logger.warning(f"Cannot find Images-Disc* in {dcg_search_dir}")
                    return
                dcg_tag = str(dcg_images_dirs[-1])
                dcg_data = {
                    "experiment_type": "single particle",
                    "experiment_type_id": 37,
                    "tag": dcg_tag,
                    "atlas": str(
                        _atlas_destination(environment, source, transferred_file)
                        / environment.samples[source].atlas.parent
                        / atlas_xml_path.with_suffix(".jpg").name
                    ),
                    "sample": environment.samples[source].sample,
                    "atlas_pixel_size": atlas_pixel_size,
                }
                capture_post(url, json=dcg_data)
                gs_pix_positions = get_grid_square_atlas_positions(
                    source_visit_dir / partial_path
                )
                for gs, pos_data in gs_pix_positions.items():
                    if pos_data:
                        capture_post(
                            f"{str(environment.url.geturl())}{url_path_for('session_control.spa_router', 'register_grid_square', session_id=environment.murfey_session, gsid=int(gs))}",
                            json={
                                "tag": dcg_tag,
                                "x_location": pos_data[0],
                                "y_location": pos_data[1],
                                "x_stage_position": pos_data[2],
                                "y_stage_position": pos_data[3],
                                "width": pos_data[4],
                                "height": pos_data[5],
                                "angle": pos_data[6],
                            },
                        )

        elif (
            transferred_file.suffix == ".dm"
            and transferred_file.name.startswith("GridSquare")
            and environment
        ):
            # Make sure we have a data collection group before trying to register grid square
            url = f"{str(environment.url.geturl())}{url_path_for('workflow.router', 'register_dc_group', visit_name=environment.visit, session_id=environment.murfey_session)}"
            dcg_search_dir = "/".join(
                p
                for p in transferred_file.parent.parent.parts
                if p != environment.visit
            )
            dcg_search_dir = (
                dcg_search_dir[1:]
                if dcg_search_dir.startswith("//")
                else dcg_search_dir
            )
            dcg_images_dirs = sorted(
                Path(dcg_search_dir).glob("Images-Disc*"),
                key=lambda x: x.stat().st_ctime,
            )
            if not dcg_images_dirs:
                logger.warning(f"Cannot find Images-Disc* in {dcg_search_dir}")
                return
            dcg_tag = str(dcg_images_dirs[-1])
            dcg_data = {
                "experiment_type": "single particle",
                "experiment_type_id": 37,
                "tag": dcg_tag,
            }
            capture_post(url, json=dcg_data)

            gs_name = int(transferred_file.stem.split("_")[1])
            logger.info(
                f"Collecting foil hole positions for {str(transferred_file)} and grid square {gs_name}"
            )
            fh_positions = _foil_hole_positions(transferred_file, gs_name)
            source = _get_source(transferred_file, environment=environment)
            if source is None:
                return None
            visitless_source_search_dir = str(source).replace(
                f"/{environment.visit}", ""
            )
            visitless_source_images_dirs = sorted(
                Path(visitless_source_search_dir).glob("Images-Disc*"),
                key=lambda x: x.stat().st_ctime,
            )
            if not visitless_source_images_dirs:
                logger.warning(
                    f"Cannot find Images-Disc* in {visitless_source_search_dir}"
                )
                return
            visitless_source = str(visitless_source_images_dirs[-1])

            if fh_positions:
                gs_url = f"{str(environment.url.geturl())}{url_path_for('session_control.spa_router', 'register_grid_square', session_id=environment.murfey_session, gsid=gs_name)}"
                gs_info = grid_square_data(
                    transferred_file,
                    gs_name,
                )
                image_path = (
                    _file_transferred_to(environment, source, Path(gs_info.image))
                    if gs_info.image
                    else ""
                )
                capture_post(
                    gs_url,
                    json={
                        "tag": visitless_source,
                        "readout_area_x": gs_info.readout_area_x,
                        "readout_area_y": gs_info.readout_area_y,
                        "thumbnail_size_x": gs_info.thumbnail_size_x,
                        "thumbnail_size_y": gs_info.thumbnail_size_y,
                        "pixel_size": gs_info.pixel_size,
                        "image": str(image_path),
                    },
                )

            for fh, fh_data in fh_positions.items():
                capture_post(
                    f"{str(environment.url.geturl())}{url_path_for('session_control.spa_router', 'register_foil_hole', session_id=environment.murfey_session, gs_name=gs_name)}",
                    json={
                        "name": fh,
                        "x_location": fh_data.x_location,
                        "y_location": fh_data.y_location,
                        "x_stage_position": fh_data.x_stage_position,
                        "y_stage_position": fh_data.y_stage_position,
                        "readout_area_x": fh_data.readout_area_x,
                        "readout_area_y": fh_data.readout_area_y,
                        "thumbnail_size_x": fh_data.thumbnail_size_x,
                        "thumbnail_size_y": fh_data.thumbnail_size_y,
                        "pixel_size": fh_data.pixel_size,
                        "diameter": fh_data.diameter,
                        "tag": visitless_source,
                        "image": fh_data.image,
                    },
                )
