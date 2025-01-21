import logging
from pathlib import Path
from typing import Dict, NamedTuple, Optional

import requests
import xmltodict

from murfey.client.context import Context
from murfey.client.contexts.spa import _get_grid_square_atlas_positions, _get_source
from murfey.client.instance_environment import MurfeyInstanceEnvironment, SampleInfo
from murfey.util import authorised_requests, capture_post, get_machine_config_client

logger = logging.getLogger("murfey.client.contexts.spa_metadata")

requests.get, requests.post, requests.put, requests.delete = authorised_requests()


class FoilHole(NamedTuple):
    x_location: int
    y_location: int
    diameter: int
    x_stage_position: Optional[float] = None
    y_stage_position: Optional[float] = None
    readout_area_x: Optional[int] = None
    readout_area_y: Optional[int] = None
    thumbnail_size_x: Optional[int] = None
    thumbnail_size_y: Optional[int] = None
    pixel_size: Optional[float] = None
    image: str = ""


def _foil_hole_positions(xml_path: Path, grid_square: int) -> Dict[str, FoilHole]:
    with open(xml_path, "r") as xml:
        for_parsing = xml.read()
        data = xmltodict.parse(for_parsing)
    data = data["GridSquareXml"]
    readout_area = data["MicroscopeImage"]["microscopeData"]["acquisition"]["camera"][
        "ReadoutArea"
    ]
    pixel_size = data["MicroscopeImage"]["SpatialScale"]["pixelSize"]["x"][
        "numericValue"
    ]
    full_size = (int(readout_area["a:width"]), int(readout_area["a:height"]))
    serialization_array = data["TargetLocations"]["TargetLocationsEfficient"][
        "a:m_serializationArray"
    ]
    required_key = ""
    for key in serialization_array.keys():
        if key.startswith("b:KeyValuePairOfintTargetLocation"):
            required_key = key
            break
    if not required_key:
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
            stage = fh_block["b:value"]["PixelCenter"]
            stage = fh_block["b:value"]["StagePosition"]
            diameter = fh_block["b:value"]["PixelWidthHeight"]["c:width"]
            foil_holes[fh_block["b:key"]] = FoilHole(
                x_location=int(float(stage["c:x"])),
                y_location=int(float(stage["c:y"])),
                x_stage_position=float(stage["c:X"]),
                y_stage_position=float(stage["c:Y"]),
                readout_area_x=full_size[0] if image_path else None,
                readout_area_y=full_size[1] if image_path else None,
                thumbnail_size_x=None,
                thumbnail_size_y=None,
                pixel_size=float(pixel_size) if image_path else None,
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
    if environment.visit in environment.default_destinations[source]:
        return (
            Path(machine_config.get("rsync_basepath", ""))
            / Path(environment.default_destinations[source]).parent
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
            visit_index = windows_path.split("\\").index(environment.visit)
            partial_path = "/".join(windows_path.split("\\")[visit_index + 1 :])
            visitless_path = Path(
                str(transferred_file).replace(f"/{environment.visit}", "")
            )
            visit_index_of_transferred_file = transferred_file.parts.index(
                environment.visit
            )
            atlas_xml_path = list(
                (
                    Path(
                        "/".join(
                            transferred_file.parts[
                                : visit_index_of_transferred_file + 1
                            ]
                        )
                    )
                    / partial_path
                ).parent.glob("Atlas_*.xml")
            )[0]
            with open(atlas_xml_path, "rb") as atlas_xml:
                atlas_xml_data = xmltodict.parse(atlas_xml)
                atlas_original_pixel_size = atlas_xml_data["MicroscopeImage"][
                    "SpatialScale"
                ]["pixelSize"]["x"]["numericValue"]

            # need to calculate the pixel size of the downscaled image
            atlas_pixel_size = atlas_original_pixel_size * 7.8

            source = _get_source(
                visitless_path.parent / "Images-Disc1" / visitless_path.name,
                environment,
            )
            sample = None
            for p in partial_path.split("/"):
                if p.startswith("Sample"):
                    sample = int(p.replace("Sample", ""))
                    break
            else:
                logger.warning(f"Sample could not be indetified for {transferred_file}")
                return
            if source:
                environment.samples[source] = SampleInfo(
                    atlas=Path(partial_path), sample=sample
                )
                url = f"{str(environment.url.geturl())}/visits/{environment.visit}/{environment.murfey_session}/register_data_collection_group"
                dcg_data = {
                    "experiment_type": "single particle",
                    "experiment_type_id": 37,
                    "tag": str(source),
                    "atlas": str(
                        _atlas_destination(environment, source, transferred_file)
                        / environment.samples[source].atlas
                    ),
                    "sample": environment.samples[source].sample,
                    "atlas_pixel_size": atlas_pixel_size,
                }
                capture_post(url, json=dcg_data)
                gs_pix_positions = _get_grid_square_atlas_positions(
                    _atlas_destination(environment, source, transferred_file)
                    / environment.samples[source].atlas
                )
                for gs, pos_data in gs_pix_positions.items():
                    if pos_data:
                        capture_post(
                            f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/grid_square/{gs}",
                            json={
                                "tag": str(source),
                                "x_location": pos_data[0],
                                "y_location": pos_data[1],
                                "x_stage_position": pos_data[2],
                                "y_stage_position": pos_data[3],
                                "width": pos_data[4],
                                "height": pos_data[5],
                                "angle": pos_data[6],
                            },
                        )

        elif transferred_file.suffix == ".dm" and environment:
            gs_name = transferred_file.name.split("_")[1]
            fh_positions = _foil_hole_positions(transferred_file, int(gs_name))
            for fh, fh_data in fh_positions.items():
                capture_post(
                    f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/grid_square/{gs_name}/foil_hole",
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
                        "tag": str(source),
                        "image": fh_data.image,
                    },
                )
