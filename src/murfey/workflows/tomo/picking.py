from logging import getLogger
from typing import Tuple

import numpy as np
from sqlalchemy import func
from sqlmodel import Session, select

from murfey.server import _transport_object
from murfey.server.feedback import _app_id, _murfey_id
from murfey.util.config import get_machine_config
from murfey.util.db import (
    AutoProcProgram,
    DataCollection,
    ParticleSizes,
    ProcessingJob,
    Session as MurfeySession,
    TomogramPicks,
    TomographyProcessingParameters,
)
from murfey.util.processing_params import default_tomo_parameters

logger = getLogger("murfey.workflows.tomo.feedback")


def _ids_tomo_classification(app_id: int, recipe: str, _db) -> Tuple[int, int]:
    dcg_id = (
        _db.exec(
            select(AutoProcProgram, ProcessingJob, DataCollection)
            .where(AutoProcProgram.id == app_id)
            .where(AutoProcProgram.pj_id == ProcessingJob.id)
            .where(ProcessingJob.dc_id == DataCollection.id)
        )
        .one()[2]
        .dcg_id
    )
    pj_id = (
        _db.exec(
            select(ProcessingJob, DataCollection)
            .where(DataCollection.dcg_id == dcg_id)
            .where(ProcessingJob.dc_id == DataCollection.id)
            .where(ProcessingJob.recipe == recipe)
        )
        .one()[0]
        .id
    )
    return dcg_id, pj_id


def _register_picked_tomogram_use_diameter(message: dict, _db: Session):
    """Received picked particles from the tomogram autopick service"""
    # Add this message to the table of seen messages
    dcg_id, pj_id = _ids_tomo_classification(
        message["program_id"], "em-tomo-class2d", _db
    )

    pick_params = TomogramPicks(
        pj_id=pj_id,
        tomogram=message["tomogram"],
        cbox_3d=message["cbox_3d"],
        particle_count=message["particle_count"],
        tomogram_pixel_size=message["pixel_size"],
    )
    _db.add(pick_params)
    _db.commit()
    _db.close()

    picking_db_len = _db.exec(
        select(func.count(ParticleSizes.id)).where(ParticleSizes.pj_id == pj_id)
    ).one()
    if picking_db_len > default_tomo_parameters.batch_size_2d:
        # If there are enough particles to get a diameter
        instrument_name = (
            _db.exec(
                select(MurfeySession).where(MurfeySession.id == message["session_id"])
            )
            .one()
            .instrument_name
        )
        machine_config = get_machine_config(instrument_name=instrument_name)[
            instrument_name
        ]
        tomo_params = _db.exec(
            select(TomographyProcessingParameters).where(
                TomographyProcessingParameters.dcg_id == dcg_id
            )
        ).one()

        particle_diameter = tomo_params.particle_diameter

        if not particle_diameter:
            # If the diameter has not been calculated then find it
            picking_db = _db.exec(
                select(ParticleSizes.particle_size).where(ParticleSizes.pj_id == pj_id)
            ).all()
            particle_diameter = np.quantile(list(picking_db), 0.75)
            tomo_params.particle_diameter = particle_diameter
            _db.add(tomo_params)
            _db.commit()

            tomo_pick_db = _db.exec(
                select(TomogramPicks).where(TomogramPicks.pj_id == pj_id)
            ).all()
            for saved_message in tomo_pick_db:
                # Send on all saved messages to extraction
                class_uuids = {
                    str(i + 1): m
                    for i, m in enumerate(
                        _murfey_id(_app_id(pj_id, _db), _db, number=50)
                    )
                }
                class2d_grp_uuid = _murfey_id(_app_id(pj_id, _db), _db)[0]
                _db.expunge(saved_message)
                zocalo_message: dict = {
                    "parameters": {
                        "tomogram": saved_message.tomogram,
                        "cbox_3d": saved_message.cbox_3d,
                        "pixel_size": saved_message.tomogram_pixel_size,
                        "particle_diameter": particle_diameter,
                        "kv": tomo_params.voltage,
                        "node_creator_queue": machine_config.node_creator_queue,
                        "session_id": message["session_id"],
                        "autoproc_program_id": _app_id(pj_id, _db),
                        "batch_size": default_tomo_parameters.batch_size_2d,
                        "nr_classes": default_tomo_parameters.nr_classes_2d,
                        "picker_id": None,
                        "class2d_grp_uuid": class2d_grp_uuid,
                        "class_uuids": class_uuids,
                    },
                    "recipes": ["em-tomo-class2d"],
                }
                if _transport_object:
                    zocalo_message["parameters"]["feedback_queue"] = (
                        _transport_object.feedback_queue
                    )
                    _transport_object.send(
                        "processing_recipe", zocalo_message, new_connection=True
                    )
        else:
            # If the diameter is known then just send the new message
            particle_diameter = tomo_params.particle_diameter
            class_uuids = {
                str(i + 1): m
                for i, m in enumerate(_murfey_id(_app_id(pj_id, _db), _db, number=50))
            }
            class2d_grp_uuid = _murfey_id(_app_id(pj_id, _db), _db)[0]
            zocalo_message = {
                "parameters": {
                    "tomogram": message["tomogram"],
                    "cbox_3d": message["cbox_3d"],
                    "pixel_size": message["pixel_size"],
                    "particle_diameter": particle_diameter,
                    "kv": tomo_params.voltage,
                    "node_creator_queue": machine_config.node_creator_queue,
                    "session_id": message["session_id"],
                    "autoproc_program_id": _app_id(pj_id, _db),
                    "batch_size": default_tomo_parameters.batch_size_2d,
                    "nr_classes": default_tomo_parameters.nr_classes_2d,
                    "picker_id": None,
                    "class2d_grp_uuid": class2d_grp_uuid,
                    "class_uuids": class_uuids,
                },
                "recipes": ["em-tomo-class2d"],
            }
            if _transport_object:
                zocalo_message["parameters"]["feedback_queue"] = (
                    _transport_object.feedback_queue
                )
                _transport_object.send(
                    "processing_recipe", zocalo_message, new_connection=True
                )
    else:
        # If not enough particles then save the new sizes
        particle_list = message.get("particle_diameters")
        assert isinstance(particle_list, list)
        for particle in particle_list:
            new_particle = ParticleSizes(pj_id=pj_id, particle_size=particle)
            _db.add(new_particle)
            _db.commit()
    _db.close()


def particles_tomogram(message: dict, murfey_db: Session) -> bool:
    _register_picked_tomogram_use_diameter(message, murfey_db)
    return True
