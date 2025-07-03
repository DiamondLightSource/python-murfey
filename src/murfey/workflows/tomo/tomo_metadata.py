import logging

import numpy as np
from sqlmodel import Session, select

from murfey.server import _transport_object
from murfey.server.api.auth import MurfeySessionIDInstrument as MurfeySessionID
from murfey.server.gain import Camera
from murfey.util import sanitise
from murfey.util.config import get_machine_config
from murfey.util.db import DataCollectionGroup, SearchMap
from murfey.util.db import Session as MurfeySession
from murfey.util.db import TiltSeries
from murfey.util.models import BatchPositionParameters, SearchMapParameters

logger = logging.getLogger("murfey.client.util.tomo_metadata")


def register_search_map_in_database(
    session_id: MurfeySessionID,
    search_map_name: str,
    search_map_params: SearchMapParameters,
    murfey_db: Session,
    close_db: bool = True,
):
    dcg = murfey_db.exec(
        select(DataCollectionGroup)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == search_map_params.tag)
    ).one()
    try:
        # See if there is already a search map with this name and update if so
        search_map = murfey_db.exec(
            select(SearchMap)
            .where(SearchMap.name == search_map_name)
            .where(SearchMap.tag == search_map_params.tag)
            .where(SearchMap.session_id == session_id)
        ).one()
        search_map.x_stage_position = (
            search_map_params.x_stage_position or search_map.x_stage_position
        )
        search_map.y_stage_position = (
            search_map_params.y_stage_position or search_map.y_stage_position
        )
        search_map.pixel_size = search_map_params.pixel_size or search_map.pixel_size
        search_map.image = search_map_params.image or search_map.image
        search_map.binning = search_map_params.binning or search_map.binning
        search_map.reference_matrix_m11 = (
            search_map_params.reference_matrix.get("m11")
            or search_map.reference_matrix_m11
        )
        search_map.reference_matrix_m12 = (
            search_map_params.reference_matrix.get("m12")
            or search_map.reference_matrix_m12
        )
        search_map.reference_matrix_m21 = (
            search_map_params.reference_matrix.get("m21")
            or search_map.reference_matrix_m21
        )
        search_map.reference_matrix_m22 = (
            search_map_params.reference_matrix.get("m22")
            or search_map.reference_matrix_m22
        )
        search_map.stage_correction_m11 = (
            search_map_params.stage_correction.get("m11")
            or search_map.stage_correction_m11
        )
        search_map.stage_correction_m12 = (
            search_map_params.stage_correction.get("m12")
            or search_map.stage_correction_m12
        )
        search_map.stage_correction_m21 = (
            search_map_params.stage_correction.get("m21")
            or search_map.stage_correction_m21
        )
        search_map.stage_correction_m22 = (
            search_map_params.stage_correction.get("m22")
            or search_map.stage_correction_m22
        )
        search_map.image_shift_correction_m11 = (
            search_map_params.image_shift_correction.get("m11")
            or search_map.image_shift_correction_m11
        )
        search_map.image_shift_correction_m12 = (
            search_map_params.image_shift_correction.get("m12")
            or search_map.image_shift_correction_m12
        )
        search_map.image_shift_correction_m21 = (
            search_map_params.image_shift_correction.get("m21")
            or search_map.image_shift_correction_m21
        )
        search_map.image_shift_correction_m22 = (
            search_map_params.image_shift_correction.get("m22")
            or search_map.image_shift_correction_m22
        )
        search_map.height = search_map_params.height or search_map.height
        search_map.width = search_map_params.width or search_map.width
        if _transport_object:
            _transport_object.do_update_search_map(search_map.id, search_map_params)
    except Exception as e:
        logger.info(f"Registering new search map due to {e}", exc_info=True)
        if _transport_object:
            sm_ispyb_response = _transport_object.do_insert_search_map(
                dcg.atlas_id, search_map_params
            )
        else:
            # mock up response so that below still works
            sm_ispyb_response = {"success": False, "return_value": None}
        # Register new search map
        search_map = SearchMap(
            id=(
                sm_ispyb_response["return_value"]
                if sm_ispyb_response["success"]
                else None
            ),
            name=search_map_name,
            session_id=session_id,
            tag=search_map_params.tag,
            x_stage_position=search_map_params.x_stage_position,
            y_stage_position=search_map_params.y_stage_position,
            pixel_size=search_map_params.pixel_size,
            image=search_map_params.image,
            binning=search_map_params.binning,
            reference_matrix_m11=search_map_params.reference_matrix.get("m11"),
            reference_matrix_m12=search_map_params.reference_matrix.get("m12"),
            reference_matrix_m21=search_map_params.reference_matrix.get("m21"),
            reference_matrix_m22=search_map_params.reference_matrix.get("m22"),
            stage_correction_m11=search_map_params.stage_correction.get("m11"),
            stage_correction_m12=search_map_params.stage_correction.get("m12"),
            stage_correction_m21=search_map_params.stage_correction.get("m21"),
            stage_correction_m22=search_map_params.stage_correction.get("m22"),
            image_shift_correction_m11=search_map_params.image_shift_correction.get(
                "m11"
            ),
            image_shift_correction_m12=search_map_params.image_shift_correction.get(
                "m12"
            ),
            image_shift_correction_m21=search_map_params.image_shift_correction.get(
                "m21"
            ),
            image_shift_correction_m22=search_map_params.image_shift_correction.get(
                "m22"
            ),
            height=search_map_params.height,
            width=search_map_params.width,
        )

    murfey_session = murfey_db.exec(
        select(MurfeySession).where(MurfeySession.id == session_id)
    ).one()
    machine_config = get_machine_config(instrument_name=murfey_session.instrument_name)[
        murfey_session.instrument_name
    ]
    if all(
        [
            search_map.reference_matrix_m11,
            search_map.stage_correction_m11,
            search_map.x_stage_position,
            search_map.y_stage_position,
            search_map.pixel_size,
            search_map.height,
            search_map.width,
            dcg.atlas_pixel_size,
        ]
    ):
        # Work out the shifted positions if all required information is present
        reference_shift_matrix = np.array(
            [
                [
                    search_map.reference_matrix_m11,
                    search_map.reference_matrix_m12,
                ],
                [
                    search_map.reference_matrix_m21,
                    search_map.reference_matrix_m22,
                ],
            ]
        )
        stage_vector = np.array(
            [search_map.x_stage_position, search_map.y_stage_position]
        )
        stage_correction_matrix = np.array(
            [
                [
                    search_map.stage_correction_m11,
                    search_map.stage_correction_m12,
                ],
                [
                    search_map.stage_correction_m21,
                    search_map.stage_correction_m22,
                ],
            ]
        )
        corrected_vector = np.matmul(
            np.linalg.inv(reference_shift_matrix),
            np.matmul(
                stage_correction_matrix, np.matmul(reference_shift_matrix, stage_vector)
            ),
        )

        # Flip positions based on camera type
        camera = getattr(Camera, machine_config.camera)
        if camera == Camera.K3_FLIPY:
            corrected_vector = np.matmul(np.array([[1, 0], [0, -1]]), corrected_vector)
        elif camera == Camera.K3_FLIPX:
            corrected_vector = np.matmul(np.array([[-1, 0], [0, 1]]), corrected_vector)

        # Convert from metres to pixels
        search_map_params.height_on_atlas = int(
            search_map.height * search_map.pixel_size / dcg.atlas_pixel_size
        )
        search_map_params.width_on_atlas = int(
            search_map.width * search_map.pixel_size / dcg.atlas_pixel_size
        )
        search_map_params.x_location = float(
            corrected_vector[0] / dcg.atlas_pixel_size + 2003
        )
        search_map_params.y_location = float(
            corrected_vector[1] / dcg.atlas_pixel_size + 2003
        )
        search_map.x_location = search_map_params.x_location
        search_map.y_location = search_map_params.y_location
        if _transport_object:
            _transport_object.do_update_search_map(search_map.id, search_map_params)
    else:
        logger.info(
            f"Unable to register search map {sanitise(search_map_name)} position yet: "
            f"stage {sanitise(str(search_map_params.x_stage_position))}, "
            f"width {sanitise(str(search_map_params.width))}, "
            f"atlas pixel size {sanitise(str(dcg.atlas_pixel_size))}"
        )
    murfey_db.add(search_map)
    murfey_db.commit()
    if close_db:
        murfey_db.close()


def register_batch_position_in_database(
    session_id: MurfeySessionID,
    batch_name: str,
    batch_parameters: BatchPositionParameters,
    murfey_db: Session,
):
    search_map = murfey_db.exec(
        select(SearchMap)
        .where(SearchMap.name == batch_parameters.search_map_name)
        .where(SearchMap.tag == batch_parameters.tag)
        .where(SearchMap.session_id == session_id)
    ).one()

    try:
        tilt_series = murfey_db.exec(
            select(TiltSeries)
            .where(TiltSeries.tag == batch_name)
            .where(TiltSeries.rsync_source == batch_parameters.tag)
            .where(TiltSeries.session_id == session_id)
        ).one()
        if tilt_series.x_location:
            logger.info(
                f"Already did position analysis for tomogram {sanitise(batch_name)}"
            )
            return
    except Exception:
        tilt_series = TiltSeries(
            tag=batch_name,
            rsync_source=batch_parameters.tag,
            session_id=session_id,
            search_map_id=search_map.id,
        )

    # Get the pixel location on the searchmap
    if all(
        [
            search_map.reference_matrix_m11,
            search_map.stage_correction_m11,
            search_map.x_stage_position,
            search_map.y_stage_position,
            search_map.pixel_size,
            search_map.height,
            search_map.width,
        ]
    ):
        reference_shift_matrix = np.array(
            [
                [
                    search_map.reference_matrix_m11,
                    search_map.reference_matrix_m12,
                ],
                [
                    search_map.reference_matrix_m21,
                    search_map.reference_matrix_m22,
                ],
            ]
        )
        stage_correction_matrix = np.array(
            [
                [
                    search_map.stage_correction_m11,
                    search_map.stage_correction_m12,
                ],
                [
                    search_map.stage_correction_m21,
                    search_map.stage_correction_m22,
                ],
            ]
        )
        image_shift_matrix = np.array(
            [
                [
                    search_map.image_shift_correction_m11,
                    search_map.image_shift_correction_m12,
                ],
                [
                    search_map.image_shift_correction_m21,
                    search_map.image_shift_correction_m22,
                ],
            ]
        )

        stage_vector = np.array(
            [
                batch_parameters.x_stage_position - search_map.x_stage_position,
                batch_parameters.y_stage_position - search_map.y_stage_position,
            ]
        )

        corrected_vector = np.matmul(
            np.linalg.inv(reference_shift_matrix),
            np.matmul(
                np.linalg.inv(stage_correction_matrix),
                np.matmul(
                    np.linalg.inv(image_shift_matrix),
                    np.matmul(reference_shift_matrix, stage_vector),
                ),
            ),
        )
        centre_batch_pixel = corrected_vector / search_map.pixel_size + [
            search_map.width / 2,
            search_map.height / 2,
        ]
        tilt_series.x_location = (
            centre_batch_pixel[0] - batch_parameters.x_beamshift / search_map.pixel_size
        )
        tilt_series.y_location = (
            centre_batch_pixel[1] - batch_parameters.y_beamshift / search_map.pixel_size
        )
    else:
        logger.warning(
            f"Incomplete search map for position of {sanitise(batch_name)}: "
            f"stage {search_map.x_stage_position}, "
            f"width {search_map.width}, "
        )
    murfey_db.add(tilt_series)
    murfey_db.commit()
