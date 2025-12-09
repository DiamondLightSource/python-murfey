import logging
from pathlib import Path
from typing import Optional

import xmltodict

from murfey.client.context import Context, ensure_dcg_exists
from murfey.client.contexts.spa import _file_transferred_to, _get_source
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.util.client import capture_post

logger = logging.getLogger("murfey.client.contexts.tomo_metadata")


class TomographyMetadataContext(Context):
    def __init__(self, acquisition_software: str, basepath: Path, token: str):
        super().__init__("Tomography_metadata", acquisition_software, token)
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

        if environment is None:
            logger.warning("No environment set")
            return

        metadata_source = _get_source(transferred_file, environment=environment)
        if not metadata_source:
            logger.warning(f"No source found for {str(transferred_file)}")
            return

        if transferred_file.name == "Session.dm":
            logger.info("Tomography session metadata found")
            ensure_dcg_exists(
                collection_type="tomo",
                metadata_source=metadata_source,
                environment=environment,
                token=self._token,
            )

        elif transferred_file.name == "SearchMap.xml":
            logger.info("Tomography session search map xml found")

            dcg_tag = ensure_dcg_exists(
                collection_type="tomo",
                metadata_source=metadata_source,
                environment=environment,
                token=self._token,
            )
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
                    environment,
                    source,
                    transferred_file.parent / "SearchMap.jpg",
                    self._token,
                )
                if source
                else ""
            )

            capture_post(
                base_url=str(environment.url.geturl()),
                router_name="session_control.tomo_router",
                function_name="register_search_map",
                token=self._token,
                session_id=environment.murfey_session,
                sm_name=transferred_file.parent.name,
                data={
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

        elif transferred_file.name == "SearchMap.dm":
            logger.info("Tomography session search map dm found")
            dcg_tag = ensure_dcg_exists(
                collection_type="tomo",
                metadata_source=metadata_source,
                environment=environment,
                token=self._token,
            )
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

            capture_post(
                base_url=str(environment.url.geturl()),
                router_name="session_control.tomo_router",
                function_name="register_search_map",
                token=self._token,
                session_id=environment.murfey_session,
                sm_name=transferred_file.parent.name,
                data={
                    "tag": dcg_tag,
                    "height": sm_height,
                    "width": sm_width,
                },
            )

        elif transferred_file.name == "BatchPositionsList.xml":
            logger.info("Tomography session batch positions list found")
            dcg_tag = ensure_dcg_exists(
                collection_type="tomo",
                metadata_source=metadata_source,
                environment=environment,
                token=self._token,
            )
            with open(transferred_file) as xml:
                for_parsing = xml.read()
            batch_xml = xmltodict.parse(for_parsing)

            batch_positions_from_xml = batch_xml["BatchPositionsList"]["BatchPositions"]
            if not batch_positions_from_xml:
                logger.info("No batch positions yet")
                return

            batch_positions_list = batch_positions_from_xml["BatchPositionParameters"]
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
                capture_post(
                    base_url=str(environment.url.geturl()),
                    router_name="session_control.tomo_router",
                    function_name="register_search_map",
                    token=self._token,
                    session_id=environment.murfey_session,
                    sm_name=search_map_name,
                    data={
                        "tag": dcg_tag,
                    },
                )

                # Then register batch position
                capture_post(
                    base_url=str(environment.url.geturl()),
                    router_name="session_control.tomo_router",
                    function_name="register_batch_position",
                    token=self._token,
                    session_id=environment.murfey_session,
                    batch_name=batch_name,
                    data={
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
                        capture_post(
                            base_url=str(environment.url.geturl()),
                            router_name="session_control.tomo_router",
                            function_name="register_batch_position",
                            token=self._token,
                            session_id=environment.murfey_session,
                            batch_name=beamshift_name,
                            data={
                                "tag": dcg_tag,
                                "x_stage_position": batch_stage_location_x,
                                "y_stage_position": batch_stage_location_y,
                                "x_beamshift": beamshift_position_x,
                                "y_beamshift": beamshift_position_y,
                                "search_map_name": search_map_name,
                            },
                        )
