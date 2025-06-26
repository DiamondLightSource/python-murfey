import logging

import numpy as np
from sqlmodel import Session, select

from murfey.server import _transport_object
from murfey.server.api.auth import MurfeySessionIDInstrument as MurfeySessionID
from murfey.server.gain import Camera
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
):
    dcg = murfey_db.exec(
        select(DataCollectionGroup)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == search_map_params.tag)
    ).one()
    try:
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
        search_map.reference_matrix = (
            search_map_params.reference_matrix or search_map.reference_matrix
        )
        search_map.stage_correction = (
            search_map_params.stage_correction or search_map.stage_correction
        )
        search_map.image_shift_correction = (
            search_map_params.image_shift_correction
            or search_map.image_shift_correction
        )
        search_map.height = search_map_params.height or search_map.height
        search_map.width = search_map_params.width or search_map.width
        if _transport_object:
            _transport_object.do_update_search_map(search_map.id, search_map_params)
    except Exception:
        if _transport_object:
            sm_ispyb_response = _transport_object.do_insert_search_map(
                dcg.atlas_id, search_map_params
            )
        else:
            # mock up response so that below still works
            sm_ispyb_response = {"success": False, "return_value": None}
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
            reference_matrix=search_map_params.reference_matrix,
            stage_correction=search_map_params.stage_correction,
            image_shift_correction=search_map_params.image_shift_correction,
            height=search_map_params.height,
            width=search_map_params.width,
        )
    murfey_db.add(search_map)
    murfey_db.commit()

    murfey_session = murfey_db.exec(
        select(MurfeySession).where(MurfeySession.id == session_id)
    ).one()
    machine_config = get_machine_config(instrument_name=murfey_session.instrument_name)[
        murfey_session.instrument_name
    ]

    if all(
        [
            search_map.reference_matrix,
            search_map.stage_correction,
            search_map.x_stage_position,
            search_map.y_stage_position,
            search_map.pixel_size,
            search_map.height,
            search_map.width,
            dcg.atlas_pixel_size,
            dcg.atlas_binning,
        ]
    ):
        M = np.array(
            [
                [
                    search_map.reference_matrix["m11"],
                    search_map.reference_matrix["m12"],
                ],
                [
                    search_map.reference_matrix["m21"],
                    search_map.reference_matrix["m22"],
                ],
            ]
        )
        B = np.array([search_map.x_stage_position, search_map.y_stage_position])
        R = np.array(
            [
                [
                    search_map.stage_correction["m11"],
                    search_map.stage_correction["m12"],
                ],
                [
                    search_map.stage_correction["m21"],
                    search_map.stage_correction["m22"],
                ],
            ]
        )
        vector_pixel = np.matmul(np.linalg.inv(M), np.matmul(R, np.matmul(M, B)))

        camera = getattr(Camera, machine_config.camera)
        if camera == Camera.FALCON or Camera.K3_FLIPY:
            vector_pixel = np.matmul(np.array([[1, 0], [0, -1]]), vector_pixel)
        elif camera == Camera.K3_FLIPX:
            vector_pixel = np.matmul(np.array([[-1, 0], [0, 1]]), vector_pixel)

        search_map_params.height_on_atlas = int(
            search_map.height * search_map.pixel_size / dcg.atlas_pixel_size
        )
        search_map_params.width_on_atlas = int(
            search_map.width * search_map.pixel_size / dcg.atlas_pixel_size
        )
        search_map_params.x_location = vector_pixel[0] / dcg.atlas_pixel_size + 2003
        search_map_params.y_location = vector_pixel[1] / dcg.atlas_pixel_size + 2003
        search_map.x_location = search_map_params.x_location
        search_map.y_location = search_map_params.y_location
        if _transport_object:
            _transport_object.do_update_search_map(search_map.id, search_map_params)
    murfey_db.add(search_map)
    murfey_db.commit()
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
            .where(TiltSeries.session_id == session_id)
        ).one()
        if tilt_series.x_location:
            logger.info(f"Already did position analysis for tomogram {batch_name}")
            return
    except Exception:
        tilt_series = TiltSeries(
            tag=batch_name,
            rsync_source=batch_parameters.tag,
            session_id=session_id,
            search_map_id=search_map.id,
        )

    # Get the pixel location on the searchmap
    M = np.array(
        [
            [
                search_map.reference_matrix["m11"],
                search_map.reference_matrix["m12"],
            ],
            [
                search_map.reference_matrix["m21"],
                search_map.reference_matrix["m22"],
            ],
        ]
    )
    R1 = np.array(
        [
            [
                search_map.stage_correction["m11"],
                search_map.stage_correction["m12"],
            ],
            [
                search_map.stage_correction["m21"],
                search_map.stage_correction["m22"],
            ],
        ]
    )
    R2 = np.array(
        [
            [
                search_map.image_shift_correction["m11"],
                search_map.image_shift_correction["m12"],
            ],
            [
                search_map.image_shift_correction["m21"],
                search_map.image_shift_correction["m22"],
            ],
        ]
    )

    A = np.array([search_map.x_stage_position, search_map.y_stage_position])
    B = np.array([batch_parameters.x_stage_position, batch_parameters.y_stage_position])

    vector_pixel = np.matmul(
        np.linalg.inv(M),
        np.matmul(np.linalg.inv(R1), np.matmul(np.linalg.inv(R2), np.matmul(M, B - A))),
    )
    centre_batch_pixel = vector_pixel / search_map.pixel_size + [
        search_map.width / 2,
        search_map.height / 2,
    ]
    tilt_series.x_location = (
        centre_batch_pixel[0]
        - BatchPositionParameters.x_beamshift / search_map.pixel_size
    )
    tilt_series.y_location = (
        centre_batch_pixel[1]
        - BatchPositionParameters.y_beamshift / search_map.pixel_size
    )
    murfey_db.add(tilt_series)
    murfey_db.commit()
