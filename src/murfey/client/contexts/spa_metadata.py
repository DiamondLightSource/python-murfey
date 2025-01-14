import logging
from pathlib import Path
from typing import Optional

import requests
import xmltodict

from murfey.client.context import Context
from murfey.client.contexts.spa import _get_grid_square_atlas_positions, _get_source
from murfey.client.instance_environment import MurfeyInstanceEnvironment, SampleInfo
from murfey.util import authorised_requests, capture_post, get_machine_config_client

logger = logging.getLogger("murfey.client.contexts.spa_metadata")

requests.get, requests.post, requests.put, requests.delete = authorised_requests()


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

            source = _get_source(transferred_file, environment)
            if not source:
                logger.warning(
                    f"Source could not be indentified for {str(transferred_file)}"
                )
                return

            source_visit_dir = source.parent

            atlas_xml_path = list(
                (source_visit_dir / partial_path).parent.glob("Atlas_*.xml")
            )[0]
            with open(atlas_xml_path, "rb") as atlas_xml:
                atlas_xml_data = xmltodict.parse(atlas_xml)
                atlas_original_pixel_size = atlas_xml_data["MicroscopeImage"][
                    "SpatialScale"
                ]["pixelSize"]["x"]["numericValue"]

            # need to calculate the pixel size of the downscaled image
            atlas_pixel_size = atlas_original_pixel_size * 7.8

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
                registered_grid_squares = (
                    requests.get(
                        f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/grid_squares"
                    )
                    .json()
                    .get(str(source), [])
                )
                if registered_grid_squares:
                    gs_pix_positions = _get_grid_square_atlas_positions(
                        source_visit_dir / partial_path
                    )
                    for gs in registered_grid_squares:
                        pos_data = gs_pix_positions.get(str(gs["name"]))
                        if pos_data:
                            capture_post(
                                f"{str(environment.url.geturl())}/sessions/{environment.murfey_session}/grid_square/{gs['name']}",
                                json={
                                    "tag": gs["tag"],
                                    "readout_area_x": gs["readout_area_x"],
                                    "readout_area_y": gs["readout_area_y"],
                                    "thumbnail_size_x": gs["thumbnail_size_x"],
                                    "thumbnail_size_y": gs["thumbnail_size_y"],
                                    "pixel_size": gs["pixel_size"],
                                    "image": gs["image"],
                                    "x_location": pos_data[0],
                                    "y_location": pos_data[1],
                                    "x_stage_position": pos_data[2],
                                    "y_stage_position": pos_data[3],
                                    "width": pos_data[4],
                                    "height": pos_data[5],
                                    "angle": pos_data[6],
                                },
                            )
