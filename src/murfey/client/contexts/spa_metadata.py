import logging
from pathlib import Path
from typing import Dict, Optional

import xmltodict

from murfey.client.context import Context, ensure_dcg_exists
from murfey.client.contexts.spa import _file_transferred_to, _get_source
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.client import capture_post
from murfey.util.spa_metadata import (
    FoilHoleInfo,
    get_grid_square_atlas_positions,
    grid_square_data,
)

logger = logging.getLogger("murfey.client.contexts.spa_metadata")


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


class SPAMetadataContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path, token: str):
        super().__init__("SPA_metadata", acquisition_software, token)
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
                    f"Source could not be identified for {str(transferred_file)}"
                )
                return

            if source:
                dcg_tag = ensure_dcg_exists(
                    collection_type="spa",
                    metadata_source=source,
                    environment=environment,
                    token=self._token,
                )
                gs_pix_positions = get_grid_square_atlas_positions(
                    source.parent / partial_path
                )
                for gs, pos_data in gs_pix_positions.items():
                    if pos_data:
                        capture_post(
                            base_url=str(environment.url.geturl()),
                            router_name="session_control.spa_router",
                            function_name="register_grid_square",
                            token=self._token,
                            session_id=environment.murfey_session,
                            gsid=int(gs),
                            data={
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
            source = _get_source(transferred_file, environment=environment)
            if source is None:
                return None
            ensure_dcg_exists(
                collection_type="spa",
                metadata_source=source,
                environment=environment,
                token=self._token,
            )

            gs_name = int(transferred_file.stem.split("_")[1])
            logger.info(
                f"Collecting foil hole positions for {str(transferred_file)} and grid square {gs_name}"
            )
            fh_positions = _foil_hole_positions(transferred_file, gs_name)
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
                gs_info = grid_square_data(
                    transferred_file,
                    gs_name,
                )
                image_path = (
                    _file_transferred_to(
                        environment, source, Path(gs_info.image), self._token
                    )
                    if gs_info.image
                    else ""
                )
                capture_post(
                    base_url=str(environment.url.geturl()),
                    router_name="session_control.spa_router",
                    function_name="register_grid_square",
                    token=self._token,
                    session_id=environment.murfey_session,
                    gsid=gs_name,
                    data={
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
                    base_url=str(environment.url.geturl()),
                    router_name="session_control.spa_router",
                    function_name="register_foil_hole",
                    token=self._token,
                    session_id=environment.murfey_session,
                    gs_name=gs_name,
                    data={
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
