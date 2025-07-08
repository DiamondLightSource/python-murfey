import logging
from pathlib import Path
from typing import Optional

import requests
import xmltodict

from murfey.client.context import Context
from murfey.client.contexts.spa import _file_transferred_to, _get_source
from murfey.client.contexts.spa_metadata import _atlas_destination
from murfey.client.instance_environment import MurfeyInstanceEnvironment, SampleInfo
from murfey.util.api import url_path_for
from murfey.util.client import authorised_requests, capture_post

logger = logging.getLogger("murfey.client.contexts.tomo_metadata")

requests.get, requests.post, requests.put, requests.delete = authorised_requests()


def ensure_dcg_exists(transferred_file: Path, environment: MurfeyInstanceEnvironment):
    # Make sure we have a data collection group
    source = _get_source(transferred_file, environment=environment)
    if not source:
        return None
    dcg_tag = str(source).replace(f"/{environment.visit}", "")
    url = f"{str(environment.url.geturl())}{url_path_for('workflow.router', 'register_dc_group', visit_name=environment.visit, session_id=environment.murfey_session)}"
    dcg_data = {
        "experiment_type": "single particle",
        "experiment_type_id": 37,
        "tag": dcg_tag,
    }
    capture_post(url, json=dcg_data)
    return dcg_tag


class TomographyMetadataContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path):
        super().__init__("Tomography_metadata", acquisition_software)
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

        if transferred_file.name == "Session.dm" and environment:
            logger.info("Tomography session metadata found")
            with open(transferred_file, "r") as session_xml:
                session_data = xmltodict.parse(session_xml.read())

            windows_path = session_data["TomographySession"]["AtlasId"]
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
                atlas_pixel_size = float(
                    atlas_xml_data["MicroscopeImage"]["SpatialScale"]["pixelSize"]["x"][
                        "numericValue"
                    ]
                )

            for p in partial_path.split("/"):
                if p.startswith("Sample"):
                    sample = int(p.replace("Sample", ""))
                    break
            else:
                logger.warning(f"Sample could not be identified for {transferred_file}")
                return
            environment.samples[source] = SampleInfo(
                atlas=Path(partial_path), sample=sample
            )
            url = f"{str(environment.url.geturl())}{url_path_for('workflow.router', 'register_dc_group', visit_name=environment.visit, session_id=environment.murfey_session)}"
            dcg_tag = "/".join(
                p for p in transferred_file.parent.parts if p != environment.visit
            ).replace("//", "/")
            dcg_data = {
                "experiment_type": "tomo",
                "experiment_type_id": 36,
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

        elif transferred_file.name == "SearchMap.xml" and environment:
            logger.info("Tomography session search map xml found")
            dcg_tag = ensure_dcg_exists(transferred_file, environment)
            with open(transferred_file, "r") as sm_xml:
                sm_data = xmltodict.parse(sm_xml.read())

            # This bit gets SearchMap location on Atlas
            sm_pixel_size = float(
                sm_data["MicroscopeImage"]["SpatialScale"]["pixelSize"]["x"][
                    "numericValue"
                ]
            )
            stage_position = sm_data["MicroscopeImage"]["microscopeData"]["stage"][
                "Position"
            ]
            sm_binning = float(
                sm_data["MicroscopeImage"]["microscopeData"]["acquisition"]["camera"][
                    "Binning"
                ]["a:x"]
            )

            # Get the stage transformation
            sm_transformations = sm_data["MicroscopeImage"]["CustomData"][
                "a:KeyValueOfstringanyType"
            ]
            stage_matrix: dict[str, float] = {}
            image_matrix: dict[str, float] = {}
            for key_val in sm_transformations:
                if key_val["a:Key"] == "ReferenceCorrectionForStage":
                    stage_matrix = {
                        "m11": float(key_val["a:Value"]["b:_m11"]),
                        "m12": float(key_val["a:Value"]["b:_m12"]),
                        "m21": float(key_val["a:Value"]["b:_m21"]),
                        "m22": float(key_val["a:Value"]["b:_m22"]),
                    }
                elif key_val["a:Key"] == "ReferenceCorrectionForImageShift":
                    image_matrix = {
                        "m11": float(key_val["a:Value"]["b:_m11"]),
                        "m12": float(key_val["a:Value"]["b:_m12"]),
                        "m21": float(key_val["a:Value"]["b:_m21"]),
                        "m22": float(key_val["a:Value"]["b:_m22"]),
                    }
            if not stage_matrix or not image_matrix:
                logger.error(
                    f"No stage or image shift matrix found for {transferred_file}"
                )

            ref_matrix = {
                "m11": float(
                    sm_data["MicroscopeImage"]["ReferenceTransformation"]["matrix"][
                        "a:_m11"
                    ]
                ),
                "m12": float(
                    sm_data["MicroscopeImage"]["ReferenceTransformation"]["matrix"][
                        "a:_m12"
                    ]
                ),
                "m21": float(
                    sm_data["MicroscopeImage"]["ReferenceTransformation"]["matrix"][
                        "a:_m21"
                    ]
                ),
                "m22": float(
                    sm_data["MicroscopeImage"]["ReferenceTransformation"]["matrix"][
                        "a:_m22"
                    ]
                ),
            }

            source = _get_source(transferred_file, environment=environment)
            image_path = (
                _file_transferred_to(
                    environment, source, transferred_file.parent / "SearchMap.jpg"
                )
                if source
                else ""
            )

            sm_url = f"{str(environment.url.geturl())}{url_path_for('session_control.tomo_router', 'register_search_map', session_id=environment.murfey_session, sm_name=transferred_file.parent.name)}"
            capture_post(
                sm_url,
                json={
                    "tag": dcg_tag,
                    "x_stage_position": float(stage_position["X"]),
                    "y_stage_position": float(stage_position["Y"]),
                    "pixel_size": sm_pixel_size,
                    "image": str(image_path),
                    "binning": sm_binning,
                    "reference_matrix": ref_matrix,
                    "stage_correction": stage_matrix,
                    "image_shift_correction": image_matrix,
                },
            )

        elif transferred_file.name == "SearchMap.dm" and environment:
            logger.info("Tomography session search map dm found")
            dcg_tag = ensure_dcg_exists(transferred_file, environment)
            with open(transferred_file, "r") as sm_xml:
                sm_data = xmltodict.parse(sm_xml.read())

            # This bit gets SearchMap size
            try:
                sm_width = int(sm_data["TileSetXml"]["ImageSize"]["a:width"])
                sm_height = int(sm_data["TileSetXml"]["ImageSize"]["a:height"])
            except KeyError:
                logger.warning(f"Unable to find size for SearchMap {transferred_file}")
                readout_width = int(
                    sm_data["TileSetXml"]["AcquisitionSettings"]["a:camera"][
                        "a:ReadoutArea"
                    ]["b:width"]
                )
                readout_height = int(
                    sm_data["TileSetXml"]["AcquisitionSettings"]["a:camera"][
                        "a:ReadoutArea"
                    ]["b:height"]
                )
                sm_width = int(
                    8005 * readout_width / max(readout_height, readout_width)
                )
                sm_height = int(
                    8005 * readout_height / max(readout_height, readout_width)
                )
                logger.warning(
                    f"Inserting incorrect width {sm_width}, height {sm_height} for SearchMap display"
                )

            sm_url = f"{str(environment.url.geturl())}{url_path_for('session_control.tomo_router', 'register_search_map', session_id=environment.murfey_session, sm_name=transferred_file.parent.name)}"
            capture_post(
                sm_url,
                json={
                    "tag": dcg_tag,
                    "height": sm_height,
                    "width": sm_width,
                },
            )

        elif transferred_file.name == "BatchPositionsList.xml" and environment:
            logger.info("Tomography session batch positions list found")
            dcg_tag = ensure_dcg_exists(transferred_file, environment)
            with open(transferred_file) as xml:
                for_parsing = xml.read()
            batch_xml = xmltodict.parse(for_parsing)

            batch_positions_list = batch_xml["BatchPositionsList"]["BatchPositions"][
                "BatchPositionParameters"
            ]
            if isinstance(batch_positions_list, dict):
                # Case of a single batch
                batch_positions_list = [batch_positions_list]

            for batch_position in batch_positions_list:
                batch_name = batch_position["Name"]
                search_map_name = batch_position["PositionOnTileSet"]["TileSetName"]
                batch_stage_location_x = float(
                    batch_position["PositionOnTileSet"]["StagePositionX"]
                )
                batch_stage_location_y = float(
                    batch_position["PositionOnTileSet"]["StagePositionY"]
                )

                # Always need search map before batch position
                sm_url = f"{str(environment.url.geturl())}{url_path_for('session_control.tomo_router', 'register_search_map', session_id=environment.murfey_session, sm_name=search_map_name)}"
                capture_post(
                    sm_url,
                    json={
                        "tag": dcg_tag,
                    },
                )

                # Then register batch position
                bp_url = f"{str(environment.url.geturl())}{url_path_for('session_control.tomo_router', 'register_batch_position', session_id=environment.murfey_session, batch_name=batch_name)}"
                capture_post(
                    bp_url,
                    json={
                        "tag": dcg_tag,
                        "x_stage_position": batch_stage_location_x,
                        "y_stage_position": batch_stage_location_y,
                        "x_beamshift": 0,
                        "y_beamshift": 0,
                        "search_map_name": search_map_name,
                    },
                )

                # Beamshifts
                if batch_position.get("AdditionalExposureTemplateAreas"):
                    beamshifts = batch_position["AdditionalExposureTemplateAreas"][
                        "ExposureTemplateAreaParameters"
                    ]
                    if type(beamshifts) is dict:
                        beamshifts = [beamshifts]
                    for beamshift in beamshifts:
                        beamshift_name = beamshift["Name"]
                        beamshift_position_x = float(beamshift["PositionX"])
                        beamshift_position_y = float(beamshift["PositionY"])

                        # Registration of beamshifted position
                        bp_url = f"{str(environment.url.geturl())}{url_path_for('session_control.tomo_router', 'register_batch_position', session_id=environment.murfey_session, batch_name=beamshift_name)}"
                        capture_post(
                            bp_url,
                            json={
                                "tag": dcg_tag,
                                "x_stage_position": batch_stage_location_x,
                                "y_stage_position": batch_stage_location_y,
                                "x_beamshift": beamshift_position_x,
                                "y_beamshift": beamshift_position_y,
                                "search_map_name": search_map_name,
                            },
                        )
