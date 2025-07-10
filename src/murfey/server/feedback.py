"""
Contains functions related to how Murfey will interact with messages that it relays
and receives. These functions should eventually be refactored into workflows module
files, but are stored here for now to prevent unnecessary dependency chains due to
being written and stored in 'murfey.server.__init__'.
"""

from __future__ import annotations

import logging
import math
import subprocess
import time
from datetime import datetime
from functools import partial, singledispatch
from importlib.metadata import EntryPoint  # For type hinting only
from pathlib import Path
from typing import Dict, List, NamedTuple, Tuple

import mrcfile
import numpy as np
from backports.entry_points_selectable import entry_points
from ispyb.sqlalchemy._auto_db_schema import (
    Atlas,
    AutoProcProgram,
    Base,
    DataCollection,
    DataCollectionGroup,
    ProcessingJob,
    ProcessingJobParameter,
)
from sqlalchemy import func
from sqlalchemy.exc import (
    InvalidRequestError,
    OperationalError,
    PendingRollbackError,
    SQLAlchemyError,
)
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlmodel import Session, create_engine, select

import murfey.server
import murfey.server.prometheus as prom
import murfey.util.db as db
from murfey.server.ispyb import ISPyBSession, get_session_id
from murfey.server.murfey_db import url  # murfey_db
from murfey.util import sanitise
from murfey.util.config import (
    MachineConfig,
    get_machine_config,
    get_microscope,
    get_security_config,
)
from murfey.util.processing_params import default_spa_parameters, motion_corrected_mrc
from murfey.util.tomo import midpoint

logger = logging.getLogger("murfey.server.feedback")


try:
    _url = url(get_security_config())
    engine = create_engine(_url)
    murfey_db = Session(engine, expire_on_commit=False)
except Exception:
    murfey_db = None


class ExtendedRecord(NamedTuple):
    record: Base  # type: ignore
    record_params: List[Base]  # type: ignore


class JobIDs(NamedTuple):
    dcgid: int
    dcid: int
    pid: int
    appid: int


def get_angle(tilt_file_name: str) -> float:
    for p in Path(tilt_file_name).name.split("_"):
        if "." in p:
            return float(p)
    raise ValueError(f"Tilt angle not found for file {tilt_file_name}")


def check_tilt_series_mc(tilt_series_id: int) -> bool:
    results = murfey_db.exec(
        select(db.Tilt, db.TiltSeries)
        .where(db.Tilt.tilt_series_id == db.TiltSeries.id)
        .where(db.TiltSeries.id == tilt_series_id)
    ).all()
    return (
        all(r[0].motion_corrected for r in results)
        and len(results) >= results[0][1].tilt_series_length
        and results[0][1].tilt_series_length > 0
    )


def get_all_tilts(tilt_series_id: int) -> List[str]:
    complete_results = murfey_db.exec(
        select(db.Tilt, db.TiltSeries, db.Session)
        .where(db.Tilt.tilt_series_id == db.TiltSeries.id)
        .where(db.TiltSeries.id == tilt_series_id)
        .where(db.TiltSeries.session_id == db.Session.id)
    ).all()
    if not complete_results:
        return []
    visit_name = complete_results[0][2].visit
    instrument_name = complete_results[0][2].instrument_name
    results = [r[0] for r in complete_results]
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    return [
        str(motion_corrected_mrc(Path(r.movie_path), visit_name, machine_config))
        for r in results
    ]


def get_job_ids(tilt_series_id: int, appid: int) -> JobIDs:
    results = murfey_db.exec(
        select(
            db.TiltSeries,
            db.AutoProcProgram,
            db.ProcessingJob,
            db.DataCollection,
            db.DataCollectionGroup,
            db.Session,
        )
        .where(db.TiltSeries.id == tilt_series_id)
        .where(db.DataCollection.tag == db.TiltSeries.tag)
        .where(db.ProcessingJob.id == db.AutoProcProgram.pj_id)
        .where(db.AutoProcProgram.id == appid)
        .where(db.ProcessingJob.dc_id == db.DataCollection.id)
        .where(db.DataCollectionGroup.id == db.DataCollection.dcg_id)
        .where(db.Session.id == db.TiltSeries.session_id)
    ).all()
    return JobIDs(
        dcgid=results[0][4].id,
        dcid=results[0][3].id,
        pid=results[0][2].id,
        appid=results[0][1].id,
    )


def get_tomo_proc_params(dcg_id: int, *args) -> db.TomographyProcessingParameters:
    results = murfey_db.exec(
        select(db.TomographyProcessingParameters).where(
            db.TomographyProcessingParameters.dcg_id == dcg_id
        )
    ).one()
    return results


def _murfey_id(app_id: int, _db, number: int = 1, close: bool = True) -> List[int]:
    murfey_ledger = [db.MurfeyLedger(app_id=app_id) for _ in range(number)]
    for ml in murfey_ledger:
        _db.add(ml)
    _db.commit()
    # There is a race condition between the IDs being read back from the database
    # after the insert and the insert being synchronised so allow multiple attempts
    attempts = 0
    while attempts < 100:
        try:
            for m in murfey_ledger:
                _db.refresh(m)
            res = [m.id for m in murfey_ledger if m.id is not None]
            break
        except (ObjectDeletedError, InvalidRequestError):
            pass
        attempts += 1
        time.sleep(0.1)
    else:
        raise RuntimeError(
            "Maximum number of attempts exceeded when producing new Murfey IDs"
        )
    if close:
        _db.close()
    return res


def _murfey_class2ds(
    murfey_ids: List[int], particles_file: str, app_id: int, _db, close: bool = False
):
    pj_id = _pj_id(app_id, _db, recipe="em-spa-class2d")
    class2ds = [
        db.Class2D(
            class_number=i,
            particles_file=particles_file,
            pj_id=pj_id,
            murfey_id=mid,
        )
        for i, mid in enumerate(murfey_ids)
    ]
    for c in class2ds:
        _db.add(c)
    _db.commit()
    if close:
        _db.close()


def _murfey_class3ds(murfey_ids: List[int], particles_file: str, app_id: int, _db):
    pj_id = _pj_id(app_id, _db, recipe="em-spa-class3d")
    class3ds = [
        db.Class3D(
            class_number=i,
            particles_file=str(Path(particles_file).parent),
            pj_id=pj_id,
            murfey_id=mid,
        )
        for i, mid in enumerate(murfey_ids)
    ]
    for c in class3ds:
        _db.add(c)
    _db.commit()
    _db.close()


def _murfey_refine(murfey_id: int, refine_dir: str, tag: str, app_id: int, _db):
    pj_id = _pj_id(app_id, _db, recipe="em-spa-refine")
    refine3d = db.Refine3D(
        tag=tag,
        refine_dir=refine_dir,
        pj_id=pj_id,
        murfey_id=murfey_id,
    )
    _db.add(refine3d)
    _db.commit()
    _db.close()


def _2d_class_murfey_ids(particles_file: str, app_id: int, _db) -> Dict[str, int]:
    pj_id = (
        _db.exec(select(db.AutoProcProgram).where(db.AutoProcProgram.id == app_id))
        .one()
        .pj_id
    )
    classes = _db.exec(
        select(db.Class2D).where(
            db.Class2D.particles_file == particles_file and db.Class2D.pj_id == pj_id
        )
    ).all()
    return {str(cl.class_number): cl.murfey_id for cl in classes}


def _3d_class_murfey_ids(particles_file: str, app_id: int, _db) -> Dict[str, int]:
    pj_id = (
        _db.exec(select(db.AutoProcProgram).where(db.AutoProcProgram.id == app_id))
        .one()
        .pj_id
    )
    classes = _db.exec(
        select(db.Class3D).where(
            db.Class3D.particles_file == str(Path(particles_file).parent)
            and db.Class3D.pj_id == pj_id
        )
    ).all()
    return {str(cl.class_number): cl.murfey_id for cl in classes}


def _refine_murfey_id(refine_dir: str, tag: str, app_id: int, _db) -> Dict[str, int]:
    pj_id = (
        _db.exec(select(db.AutoProcProgram).where(db.AutoProcProgram.id == app_id))
        .one()
        .pj_id
    )
    refined_class = _db.exec(
        select(db.Refine3D)
        .where(db.Refine3D.refine_dir == refine_dir)
        .where(db.Refine3D.pj_id == pj_id)
        .where(db.Refine3D.tag == tag)
    ).one()
    return refined_class.murfey_id


def _app_id(pj_id: int, _db) -> int:
    return (
        _db.exec(select(db.AutoProcProgram).where(db.AutoProcProgram.pj_id == pj_id))
        .one()
        .id
    )


def _pj_id(app_id: int, _db, recipe: str = "") -> int:
    if recipe:
        dc_id = (
            _db.exec(
                select(db.AutoProcProgram, db.ProcessingJob)
                .where(db.AutoProcProgram.id == app_id)
                .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
            )
            .one()[1]
            .dc_id
        )
        pj_id = (
            _db.exec(
                select(db.ProcessingJob)
                .where(db.ProcessingJob.dc_id == dc_id)
                .where(db.ProcessingJob.recipe == recipe)
            )
            .one()
            .id
        )
    else:
        pj_id = (
            _db.exec(select(db.AutoProcProgram).where(db.AutoProcProgram.id == app_id))
            .one()
            .pj_id
        )
    return pj_id


def _get_spa_params(
    app_id: int, _db
) -> Tuple[db.SPARelionParameters, db.SPAFeedbackParameters]:
    pj_id = _pj_id(app_id, _db, recipe="em-spa-preprocess")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(db.SPARelionParameters.pj_id == pj_id)
    ).one()
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(db.SPAFeedbackParameters.pj_id == pj_id)
    ).one()
    _db.expunge(relion_params)
    _db.expunge(feedback_params)
    return relion_params, feedback_params


def _release_2d_hold(message: dict, _db=murfey_db):
    relion_params, feedback_params = _get_spa_params(message["program_id"], _db)
    if not feedback_params.star_combination_job:
        feedback_params.star_combination_job = feedback_params.next_job + (
            3 if default_spa_parameters.do_icebreaker_jobs else 2
        )
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class2d")
    if feedback_params.rerun_class2d:
        first_class2d = _db.exec(
            select(db.Class2DParameters).where(db.Class2DParameters.pj_id == pj_id)
        ).first()
        instrument_name = (
            _db.exec(select(db.Session).where(db.Session.id == message["session_id"]))
            .one()
            .instrument_name
        )
        machine_config = get_machine_config(instrument_name=instrument_name)[
            instrument_name
        ]
        zocalo_message: dict = {
            "parameters": {
                "particles_file": first_class2d.particles_file,
                "class2d_dir": message["job_dir"],
                "batch_is_complete": first_class2d.complete,
                "batch_size": first_class2d.batch_size,
                "particle_diameter": relion_params.particle_diameter,
                "mask_diameter": relion_params.mask_diameter or 0,
                "combine_star_job_number": feedback_params.star_combination_job,
                "autoselect_min_score": feedback_params.class_selection_score or 0,
                "autoproc_program_id": message["program_id"],
                "nr_iter": default_spa_parameters.nr_iter_2d,
                "nr_classes": default_spa_parameters.nr_classes_2d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "picker_id": feedback_params.picker_ispyb_id,
                "class_uuids": _2d_class_murfey_ids(
                    first_class2d.particles_file, message["program_id"], _db
                ),
                "class2d_grp_uuid": _db.exec(
                    select(db.Class2DParameters)
                    .where(
                        db.Class2DParameters.particles_file
                        == first_class2d.particles_file
                    )
                    .where(db.Class2DParameters.pj_id == pj_id)
                )
                .one()
                .murfey_id,
                "session_id": message["session_id"],
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class2d"],
        }
        if first_class2d.complete:
            feedback_params.next_job += (
                4 if default_spa_parameters.do_icebreaker_jobs else 3
            )
        feedback_params.rerun_class2d = False
        _db.add(feedback_params)
        if first_class2d.complete:
            _db.delete(first_class2d)
        _db.commit()
        _db.close()
        if murfey.server._transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = murfey.server._transport_object.feedback_queue
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
    else:
        feedback_params.hold_class2d = False
        _db.add(feedback_params)
        _db.commit()
        _db.close()


def _release_3d_hold(message: dict, _db=murfey_db):
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class3d")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    class3d_params = _db.exec(
        select(db.Class3DParameters).where(db.Class3DParameters.pj_id == pj_id)
    ).one()
    if class3d_params.run:
        instrument_name = (
            _db.exec(select(db.Session).where(db.Session.id == message["session_id"]))
            .one()
            .instrument_name
        )
        machine_config = get_machine_config(instrument_name=instrument_name)[
            instrument_name
        ]
        zocalo_message: dict = {
            "parameters": {
                "particles_file": class3d_params.particles_file,
                "class3d_dir": class3d_params.class3d_dir,
                "batch_size": class3d_params.batch_size,
                "symmetry": relion_params.symmetry,
                "particle_diameter": relion_params.particle_diameter,
                "mask_diameter": relion_params.mask_diameter or 0,
                "do_initial_model": False if feedback_params.initial_model else True,
                "initial_model_file": feedback_params.initial_model,
                "picker_id": feedback_params.picker_ispyb_id,
                "class_uuids": _3d_class_murfey_ids(
                    class3d_params.particles_file, _app_id(pj_id, _db), _db
                ),
                "class3d_grp_uuid": _db.exec(
                    select(db.Class3DParameters)
                    .where(
                        db.Class3DParameters.particles_file
                        == class3d_params.particles_file
                    )
                    .where(db.Class3DParameters.pj_id == pj_id)
                )
                .one()
                .murfey_id,
                "nr_iter": default_spa_parameters.nr_iter_3d,
                "initial_model_iterations": default_spa_parameters.nr_iter_ini_model,
                "nr_classes": default_spa_parameters.nr_classes_3d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-class3d"), _db
                ),
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class3d"],
        }
        if murfey.server._transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = murfey.server._transport_object.feedback_queue
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        class3d_params.run = False
        _db.add(class3d_params)
    else:
        feedback_params.hold_class3d = False
    _db.add(feedback_params)
    _db.commit()
    _db.close()


def _release_refine_hold(message: dict, _db=murfey_db):
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-refine")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    refine_params = _db.exec(
        select(db.RefineParameters)
        .where(db.RefineParameters.pj_id == pj_id)
        .where(db.RefineParameters.tag == "first")
    ).one()
    symmetry_refine_params = _db.exec(
        select(db.RefineParameters)
        .where(db.RefineParameters.pj_id == pj_id)
        .where(db.RefineParameters.tag == "symmetry")
    ).one()
    if refine_params.run:
        instrument_name = (
            _db.exec(select(db.Session).where(db.Session.id == message["session_id"]))
            .one()
            .instrument_name
        )
        machine_config = get_machine_config(instrument_name=instrument_name)[
            instrument_name
        ]
        zocalo_message: dict = {
            "parameters": {
                "refine_job_dir": refine_params.refine_dir,
                "class3d_dir": refine_params.class3d_dir,
                "class_number": refine_params.class_number,
                "pixel_size": relion_params.angpix,
                "particle_diameter": relion_params.particle_diameter,
                "mask_diameter": relion_params.mask_diameter or 0,
                "symmetry": relion_params.symmetry,
                "node_creator_queue": machine_config.node_creator_queue,
                "nr_iter": default_spa_parameters.nr_iter_3d,
                "picker_id": feedback_params.picker_ispyb_id,
                "refined_class_uuid": _refine_murfey_id(
                    refine_dir=refine_params.refine_dir,
                    tag=refine_params.tag,
                    app_id=_app_id(pj_id, _db),
                    _db=_db,
                ),
                "refined_grp_uuid": refine_params.murfey_id,
                "symmetry_refined_class_uuid": _refine_murfey_id(
                    refine_dir=symmetry_refine_params.refine_dir,
                    tag=symmetry_refine_params.tag,
                    app_id=_app_id(pj_id, _db),
                    _db=_db,
                ),
                "symmetry_refined_grp_uuid": symmetry_refine_params.murfey_id,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-refine"), _db
                ),
            },
            "recipes": ["em-spa-refine"],
        }
        if murfey.server._transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = murfey.server._transport_object.feedback_queue
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        refine_params.run = False
        _db.add(refine_params)
    else:
        feedback_params.hold_refine = False
    _db.add(feedback_params)
    _db.commit()
    _db.close()


def _register_incomplete_2d_batch(message: dict, _db=murfey_db, demo: bool = False):
    """Received first batch from particle selection service"""
    # the general parameters are stored using the preprocessing auto proc program ID
    logger.info("Registering incomplete particle batch for 2D classification")
    instrument_name = (
        _db.exec(select(db.Session).where(db.Session.id == message["session_id"]))
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class2d")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    if feedback_params.hold_class2d:
        feedback_params.rerun_class2d = True
        _db.add(feedback_params)
        _db.commit()
        _db.close()
        return
    feedback_params.next_job = 10 if default_spa_parameters.do_icebreaker_jobs else 7
    feedback_params.hold_class2d = True
    relion_options = dict(relion_params)
    other_options = dict(feedback_params)
    if other_options["picker_ispyb_id"] is None:
        logger.info("No ISPyB particle picker ID yet")
        feedback_params.hold_class2d = False
        _db.add(feedback_params)
        _db.commit()
        _db.expunge(feedback_params)
        return
    _db.add(feedback_params)
    _db.commit()
    _db.expunge(feedback_params)
    class2d_message = message.get("class2d_message")
    assert isinstance(class2d_message, dict)
    if not _db.exec(
        select(func.count(db.Class2DParameters.particles_file))
        .where(db.Class2DParameters.particles_file == class2d_message["particles_file"])
        .where(db.Class2DParameters.pj_id == pj_id)
    ).one():
        class2d_params = db.Class2DParameters(
            pj_id=pj_id,
            murfey_id=_murfey_id(message["program_id"], _db)[0],
            particles_file=class2d_message["particles_file"],
            class2d_dir=class2d_message["class2d_dir"],
            batch_size=class2d_message["batch_size"],
            complete=False,
        )
        _db.add(class2d_params)
        _db.commit()
        murfey_ids = _murfey_id(message["program_id"], _db, number=50)
        _murfey_class2ds(
            murfey_ids, class2d_message["particles_file"], message["program_id"], _db
        )
    zocalo_message: dict = {
        "parameters": {
            "particles_file": class2d_message["particles_file"],
            "class2d_dir": f"{class2d_message['class2d_dir']}{other_options['next_job']:03}",
            "batch_is_complete": False,
            "particle_diameter": relion_options["particle_diameter"],
            "combine_star_job_number": -1,
            "picker_id": other_options["picker_ispyb_id"],
            "nr_iter": default_spa_parameters.nr_iter_2d,
            "batch_size": default_spa_parameters.batch_size_2d,
            "nr_classes": default_spa_parameters.nr_classes_2d,
            "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
            "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
            "mask_diameter": 0,
            "class_uuids": _2d_class_murfey_ids(
                class2d_message["particles_file"], _app_id(pj_id, _db), _db
            ),
            "class2d_grp_uuid": _db.exec(
                select(db.Class2DParameters).where(
                    db.Class2DParameters.particles_file
                    == class2d_message["particles_file"]
                    and db.Class2DParameters.pj_id == pj_id
                )
            )
            .one()
            .murfey_id,
            "session_id": message["session_id"],
            "autoproc_program_id": _app_id(
                _pj_id(message["program_id"], _db, recipe="em-spa-class2d"), _db
            ),
            "node_creator_queue": machine_config.node_creator_queue,
        },
        "recipes": ["em-spa-class2d"],
    }
    if murfey.server._transport_object:
        zocalo_message["parameters"][
            "feedback_queue"
        ] = murfey.server._transport_object.feedback_queue
        murfey.server._transport_object.send(
            "processing_recipe", zocalo_message, new_connection=True
        )
        logger.info("2D classification requested")
    if demo:
        logger.info("Incomplete 2D batch registered in demo mode")
        if not _db.exec(
            select(func.count(db.Class2DParameters.particles_file)).where(
                db.Class2DParameters.particles_file == class2d_message["particles_file"]
                and db.Class2DParameters.pj_id == pj_id
                and db.Class2DParameters.complete
            )
        ).one():
            _register_complete_2d_batch(message, _db=_db, demo=demo)
            message["class2d_message"]["particles_file"] = (
                message["class2d_message"]["particles_file"] + "_new"
            )
            _register_complete_2d_batch(message, _db=_db, demo=demo)
    _db.close()


def _register_complete_2d_batch(message: dict, _db=murfey_db, demo: bool = False):
    """Received full batch from particle selection service"""
    instrument_name = (
        _db.exec(select(db.Session).where(db.Session.id == message["session_id"]))
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    class2d_message = message.get("class2d_message")
    assert isinstance(class2d_message, dict)
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class2d")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    _db.expunge(relion_params)
    _db.expunge(feedback_params)
    if feedback_params.hold_class2d or feedback_params.picker_ispyb_id is None:
        feedback_params.rerun_class2d = True
        _db.add(feedback_params)
        _db.commit()
        # If waiting then save the message
        if _db.exec(
            select(func.count(db.Class2DParameters.particles_file))
            .where(db.Class2DParameters.pj_id == pj_id)
            .where(
                db.Class2DParameters.particles_file == class2d_message["particles_file"]
            )
        ).one():
            class2d_params = _db.exec(
                select(db.Class2DParameters)
                .where(db.Class2DParameters.pj_id == pj_id)
                .where(
                    db.Class2DParameters.particles_file
                    == class2d_message["particles_file"]
                )
            ).one()
            class2d_params.complete = True
            _db.add(class2d_params)
            _db.commit()
            _db.close()
        else:
            class2d_params = db.Class2DParameters(
                pj_id=pj_id,
                murfey_id=_murfey_id(message["program_id"], _db)[0],
                particles_file=class2d_message["particles_file"],
                class2d_dir=class2d_message["class2d_dir"],
                batch_size=class2d_message["batch_size"],
            )
            _db.add(class2d_params)
            _db.commit()
            _db.close()
            murfey_ids = _murfey_id(_app_id(pj_id, _db), _db, number=50)
            _murfey_class2ds(
                murfey_ids, class2d_message["particles_file"], _app_id(pj_id, _db), _db
            )
        if demo:
            _register_class_selection(
                {"session_id": message["session_id"], "class_selection_score": 0.5},
                _db=_db,
                demo=demo,
            )
    elif not feedback_params.class_selection_score:
        # For the first batch, start a container and set the database to wait
        job_number_after_first_batch = (
            10 if default_spa_parameters.do_icebreaker_jobs else 7
        )
        if (
            feedback_params.next_job is not None
            and feedback_params.next_job < job_number_after_first_batch
        ):
            feedback_params.next_job = job_number_after_first_batch
        if not feedback_params.star_combination_job:
            feedback_params.star_combination_job = feedback_params.next_job + (
                3 if default_spa_parameters.do_icebreaker_jobs else 2
            )
        if _db.exec(
            select(func.count(db.Class2DParameters.particles_file))
            .where(db.Class2DParameters.pj_id == pj_id)
            .where(
                db.Class2DParameters.particles_file == class2d_message["particles_file"]
            )
        ).one():
            class_uuids = _2d_class_murfey_ids(
                class2d_message["particles_file"], _app_id(pj_id, _db), _db
            )
            class2d_grp_uuid = (
                _db.exec(
                    select(db.Class2DParameters)
                    .where(db.Class2DParameters.pj_id == pj_id)
                    .where(
                        db.Class2DParameters.particles_file
                        == class2d_message["particles_file"]
                    )
                )
                .one()
                .murfey_id
            )
        else:
            class_uuids = {
                str(i + 1): m
                for i, m in enumerate(_murfey_id(_app_id(pj_id, _db), _db, number=50))
            }
            class2d_grp_uuid = _murfey_id(_app_id(pj_id, _db), _db)[0]
        zocalo_message: dict = {
            "parameters": {
                "particles_file": class2d_message["particles_file"],
                "class2d_dir": f"{class2d_message['class2d_dir']}{feedback_params.next_job:03}",
                "batch_is_complete": True,
                "particle_diameter": relion_params.particle_diameter,
                "mask_diameter": relion_params.mask_diameter or 0,
                "combine_star_job_number": feedback_params.star_combination_job,
                "autoselect_min_score": 0,
                "picker_id": feedback_params.picker_ispyb_id,
                "class_uuids": class_uuids,
                "class2d_grp_uuid": class2d_grp_uuid,
                "nr_iter": default_spa_parameters.nr_iter_2d,
                "batch_size": default_spa_parameters.batch_size_2d,
                "nr_classes": default_spa_parameters.nr_classes_2d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-class2d"), _db
                ),
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class2d"],
        }
        if murfey.server._transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = murfey.server._transport_object.feedback_queue
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        feedback_params.hold_class2d = True
        feedback_params.next_job += (
            4 if default_spa_parameters.do_icebreaker_jobs else 3
        )
        _db.add(feedback_params)
        _db.commit()
        _db.close()
    else:
        # Send all other messages on to a container
        if _db.exec(
            select(func.count(db.Class2DParameters.particles_file))
            .where(db.Class2DParameters.pj_id == pj_id)
            .where(
                db.Class2DParameters.particles_file == class2d_message["particles_file"]
            )
        ).one():
            class_uuids = _2d_class_murfey_ids(
                class2d_message["particles_file"], _app_id(pj_id, _db), _db
            )
            class2d_grp_uuid = (
                _db.exec(
                    select(db.Class2DParameters)
                    .where(db.Class2DParameters.pj_id == pj_id)
                    .where(
                        db.Class2DParameters.particles_file
                        == class2d_message["particles_file"]
                    )
                )
                .one()
                .murfey_id
            )
        else:
            class_uuids = {
                str(i + 1): m
                for i, m in enumerate(_murfey_id(_app_id(pj_id, _db), _db, number=50))
            }
            class2d_grp_uuid = _murfey_id(_app_id(pj_id, _db), _db)[0]
        zocalo_message = {
            "parameters": {
                "particles_file": class2d_message["particles_file"],
                "class2d_dir": f"{class2d_message['class2d_dir']}{feedback_params.next_job:03}",
                "batch_is_complete": True,
                "particle_diameter": relion_params.particle_diameter,
                "mask_diameter": relion_params.mask_diameter or 0,
                "combine_star_job_number": feedback_params.star_combination_job,
                "autoselect_min_score": feedback_params.class_selection_score or 0,
                "picker_id": feedback_params.picker_ispyb_id,
                "class_uuids": class_uuids,
                "class2d_grp_uuid": class2d_grp_uuid,
                "nr_iter": default_spa_parameters.nr_iter_2d,
                "batch_size": default_spa_parameters.batch_size_2d,
                "nr_classes": default_spa_parameters.nr_classes_2d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-class2d"), _db
                ),
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class2d"],
        }
        if murfey.server._transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = murfey.server._transport_object.feedback_queue
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        feedback_params.next_job += (
            3 if default_spa_parameters.do_icebreaker_jobs else 2
        )
        _db.add(feedback_params)
        _db.commit()
        _db.close()


def _flush_class2d(
    session_id: int,
    app_id: int,
    _db,
    relion_params: db.SPARelionParameters | None = None,
    feedback_params: db.SPAFeedbackParameters | None = None,
):
    instrument_name = (
        _db.exec(select(db.Session).where(db.Session.id == session_id))
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if not relion_params or feedback_params:
        pj_id_params = _pj_id(app_id, _db, recipe="em-spa-preprocess")
    if not relion_params:
        relion_params = _db.exec(
            select(db.SPARelionParameters).where(
                db.SPARelionParameters.pj_id == pj_id_params
            )
        ).one()
        _db.expunge(relion_params)
    if not feedback_params:
        feedback_params = _db.exec(
            select(db.SPAFeedbackParameters).where(
                db.SPAFeedbackParameters.pj_id == pj_id_params
            )
        ).one()
        _db.expunge(feedback_params)
    if not relion_params or not feedback_params:
        return
    pj_id = _pj_id(app_id, _db, recipe="em-spa-class2d")
    class2d_db = _db.exec(
        select(db.Class2DParameters)
        .where(db.Class2DParameters.pj_id == pj_id)
        .where(db.Class2DParameters.complete)
    ).all()
    if not feedback_params.next_job:
        feedback_params.next_job = (
            10 if default_spa_parameters.do_icebreaker_jobs else 7
        )
    if not feedback_params.star_combination_job:
        feedback_params.star_combination_job = feedback_params.next_job + (
            3 if default_spa_parameters.do_icebreaker_jobs else 2
        )
    for saved_message in class2d_db:
        # Send all held Class2D messages on with the selection score added
        _db.expunge(saved_message)
        zocalo_message: dict = {
            "parameters": {
                "particles_file": saved_message.particles_file,
                "class2d_dir": f"{saved_message.class2d_dir}{feedback_params.next_job:03}",
                "batch_is_complete": True,
                "batch_size": saved_message.batch_size,
                "particle_diameter": relion_params.particle_diameter,
                "mask_diameter": relion_params.mask_diameter or 0,
                "combine_star_job_number": feedback_params.star_combination_job,
                "autoselect_min_score": feedback_params.class_selection_score or 0,
                "picker_id": feedback_params.picker_ispyb_id,
                "class_uuids": _2d_class_murfey_ids(
                    saved_message.particles_file, _app_id(pj_id, _db), _db
                ),
                "class2d_grp_uuid": saved_message.murfey_id,
                "nr_iter": default_spa_parameters.nr_iter_2d,
                "nr_classes": default_spa_parameters.nr_classes_2d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "session_id": session_id,
                "autoproc_program_id": _app_id(pj_id, _db),
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class2d"],
        }
        if murfey.server._transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = murfey.server._transport_object.feedback_queue
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        feedback_params.next_job += (
            3 if default_spa_parameters.do_icebreaker_jobs else 2
        )
        _db.delete(saved_message)
    _db.add(feedback_params)
    _db.commit()


def _register_class_selection(message: dict, _db=murfey_db, demo: bool = False):
    """Received selection score from class selection service"""
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class2d")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    class2d_db = _db.exec(
        select(db.Class2DParameters).where(db.Class2DParameters.pj_id == pj_id)
    ).all()
    # Add the class selection score to the database
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    _db.expunge(feedback_params)

    if feedback_params.picker_ispyb_id is None:
        selection_stash = db.SelectionStash(
            pj_id=pj_id,
            class_selection_score=message["class_selection_score"] or 0,
        )
        _db.add(selection_stash)
        _db.commit()
        _db.close()
        return

    feedback_params.class_selection_score = message.get("class_selection_score") or 0
    feedback_params.hold_class2d = False
    next_job = feedback_params.next_job
    if demo:
        for saved_message in class2d_db:
            # Send all held Class2D messages on with the selection score added
            _db.expunge(saved_message)
            particles_file = saved_message.particles_file
            logger.info("Complete 2D classification registered in demo mode")
            _register_3d_batch(
                {
                    "session_id": message["session_id"],
                    "class3d_message": {
                        "particles_file": particles_file,
                        "class3d_dir": "Class3D",
                        "batch_size": 50000,
                    },
                },
                _db=_db,
                demo=demo,
            )
            logger.info("3D classification registered in demo mode")
            _register_3d_batch(
                {
                    "session_id": message["session_id"],
                    "class3d_message": {
                        "particles_file": particles_file + "_new",
                        "class3d_dir": "Class3D",
                        "batch_size": 50000,
                    },
                },
                _db=_db,
                demo=demo,
            )
            _register_initial_model(
                {
                    "session_id": message["session_id"],
                    "initial_model": "InitialModel/job015/model.mrc",
                },
                _db=_db,
                demo=demo,
            )
            next_job += 3 if default_spa_parameters.do_icebreaker_jobs else 2
        feedback_params.next_job = next_job
        _db.close()
    else:
        _flush_class2d(
            message["session_id"],
            message["program_id"],
            _db,
            relion_params=relion_params,
            feedback_params=feedback_params,
        )
    _db.add(feedback_params)
    for sm in class2d_db:
        _db.delete(sm)
    _db.commit()
    _db.close()


def _find_initial_model(visit: str, machine_config: MachineConfig) -> Path | None:
    if machine_config.initial_model_search_directory:
        visit_directory = (
            machine_config.rsync_basepath / str(datetime.now().year) / visit
        )
        possible_models = [
            p
            for p in (
                visit_directory / machine_config.initial_model_search_directory
            ).glob("*.mrc")
            if "rescaled" not in p.name
        ]
        if possible_models:
            return sorted(possible_models, key=lambda x: x.stat().st_ctime)[-1]
    return None


def _downscaled_box_size(
    particle_diameter: int, pixel_size: float
) -> Tuple[int, float]:
    box_size = int(math.ceil(1.2 * particle_diameter))
    box_size = box_size + box_size % 2
    for small_box_pix in (
        48,
        64,
        96,
        128,
        160,
        192,
        256,
        288,
        300,
        320,
        360,
        384,
        400,
        420,
        450,
        480,
        512,
        640,
        768,
        896,
        1024,
    ):
        # Don't go larger than the original box
        if small_box_pix > box_size:
            return box_size, pixel_size
        # If Nyquist freq. is better than 8.5 A, use this downscaled box, else step size
        small_box_angpix = pixel_size * box_size / small_box_pix
        if small_box_angpix < 4.25:
            return small_box_pix, small_box_angpix
    raise ValueError(f"Box size is too large: {box_size}")


def _resize_intial_model(
    downscaled_box_size: int,
    downscaled_pixel_size: float,
    input_path: Path,
    output_path: Path,
    executables: Dict[str, str],
    env: Dict[str, str],
) -> None:
    if executables.get("relion_image_handler"):
        comp_proc = subprocess.run(
            [
                f"{executables['relion_image_handler']}",
                "--i",
                str(input_path),
                "--new_box",
                str(downscaled_box_size),
                "--rescale_angpix",
                str(downscaled_pixel_size),
                "--o",
                str(output_path),
            ],
            capture_output=True,
            text=True,
            env=env,
        )
        with mrcfile.open(output_path) as rescaled_mrc:
            rescaled_mrc.header.cella = (
                downscaled_pixel_size,
                downscaled_pixel_size,
                downscaled_pixel_size,
            )
        if comp_proc.returncode:
            logger.error(
                f"Resizing initial model {input_path} failed \n {comp_proc.stdout}"
            )
    return None


def _register_3d_batch(message: dict, _db=murfey_db, demo: bool = False):
    """Received 3d batch from class selection service"""
    class3d_message = message.get("class3d_message")
    assert isinstance(class3d_message, dict)
    instrument_name = (
        _db.exec(select(db.Session).where(db.Session.id == message["session_id"]))
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class3d")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    relion_options = dict(relion_params)
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    other_options = dict(feedback_params)

    visit_name = (
        _db.exec(select(db.Session).where(db.Session.id == message["session_id"]))
        .one()
        .visit
    )

    provided_initial_model = _find_initial_model(visit_name, machine_config)
    if provided_initial_model and not feedback_params.initial_model:
        rescaled_initial_model_path = (
            provided_initial_model.parent
            / f"{provided_initial_model.stem}_rescaled_{pj_id}{provided_initial_model.suffix}"
        )
        if not rescaled_initial_model_path.is_file():
            _resize_intial_model(
                *_downscaled_box_size(
                    message["particle_diameter"],
                    relion_options["angpix"],
                ),
                provided_initial_model,
                rescaled_initial_model_path,
                machine_config.external_executables,
                machine_config.external_environment,
            )
            feedback_params.initial_model = str(rescaled_initial_model_path)
            other_options["initial_model"] = str(rescaled_initial_model_path)
            next_job = feedback_params.next_job
            class3d_dir = (
                f"{class3d_message['class3d_dir']}{(feedback_params.next_job+1):03}"
            )
            feedback_params.next_job += 1
            _db.add(feedback_params)
            _db.commit()

            class3d_grp_uuid = _murfey_id(message["program_id"], _db)[0]
            class_uuids = _murfey_id(message["program_id"], _db, number=4)
            class3d_params = db.Class3DParameters(
                pj_id=pj_id,
                murfey_id=class3d_grp_uuid,
                particles_file=class3d_message["particles_file"],
                class3d_dir=class3d_dir,
                batch_size=class3d_message["batch_size"],
            )
            _db.add(class3d_params)
            _db.commit()
            _murfey_class3ds(
                class_uuids,
                class3d_message["particles_file"],
                message["program_id"],
                _db,
            )

    if feedback_params.hold_class3d:
        # If waiting then save the message
        class3d_params = _db.exec(
            select(db.Class3DParameters).where(db.Class3DParameters.pj_id == pj_id)
        ).one()
        class3d_params.run = True
        class3d_params.particles_file = class3d_message["particles_file"]
        class3d_params.batch_size = class3d_message["batch_size"]
        _db.add(class3d_params)
        _db.commit()
        _db.close()
    elif not feedback_params.initial_model:
        # For the first batch, start a container and set the database to wait
        next_job = feedback_params.next_job
        class3d_dir = (
            f"{class3d_message['class3d_dir']}{(feedback_params.next_job+1):03}"
        )
        class3d_grp_uuid = _murfey_id(message["program_id"], _db)[0]
        class_uuids = _murfey_id(message["program_id"], _db, number=4)
        class3d_params = db.Class3DParameters(
            pj_id=pj_id,
            murfey_id=class3d_grp_uuid,
            particles_file=class3d_message["particles_file"],
            class3d_dir=class3d_dir,
            batch_size=class3d_message["batch_size"],
        )
        _db.add(class3d_params)
        _db.commit()
        _murfey_class3ds(
            class_uuids, class3d_message["particles_file"], message["program_id"], _db
        )

        feedback_params.hold_class3d = True
        next_job += 2
        feedback_params.next_job = next_job
        zocalo_message: dict = {
            "parameters": {
                "particles_file": class3d_message["particles_file"],
                "class3d_dir": class3d_dir,
                "batch_size": class3d_message["batch_size"],
                "symmetry": relion_options["symmetry"],
                "particle_diameter": relion_options["particle_diameter"],
                "mask_diameter": relion_options["mask_diameter"] or 0,
                "do_initial_model": True,
                "picker_id": other_options["picker_ispyb_id"],
                "class_uuids": {i + 1: m for i, m in enumerate(class_uuids)},
                "class3d_grp_uuid": class3d_grp_uuid,
                "nr_iter": default_spa_parameters.nr_iter_3d,
                "initial_model_iterations": default_spa_parameters.nr_iter_ini_model,
                "nr_classes": default_spa_parameters.nr_classes_3d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-class3d"), _db
                ),
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class3d"],
        }
        if murfey.server._transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = murfey.server._transport_object.feedback_queue
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        _db.add(feedback_params)
        _db.commit()
        _db.close()
    else:
        # Send all other messages on to a container
        class3d_params = _db.exec(
            select(db.Class3DParameters).where(db.Class3DParameters.pj_id == pj_id)
        ).one()
        zocalo_message = {
            "parameters": {
                "particles_file": class3d_message["particles_file"],
                "class3d_dir": class3d_params.class3d_dir,
                "batch_size": class3d_message["batch_size"],
                "symmetry": relion_options["symmetry"],
                "particle_diameter": relion_options["particle_diameter"],
                "mask_diameter": relion_options["mask_diameter"] or 0,
                "do_initial_model": False,
                "initial_model_file": other_options["initial_model"],
                "picker_id": other_options["picker_ispyb_id"],
                "class_uuids": _3d_class_murfey_ids(
                    class3d_params.particles_file, _app_id(pj_id, _db), _db
                ),
                "class3d_grp_uuid": class3d_params.murfey_id,
                "nr_iter": default_spa_parameters.nr_iter_3d,
                "initial_model_iterations": default_spa_parameters.nr_iter_ini_model,
                "nr_classes": default_spa_parameters.nr_classes_3d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-class3d"), _db
                ),
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class3d"],
        }
        if murfey.server._transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = murfey.server._transport_object.feedback_queue
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        feedback_params.hold_class3d = True
        _db.add(feedback_params)
        _db.commit()
        _db.close()


def _register_initial_model(message: dict, _db=murfey_db, demo: bool = False):
    """Received initial model from 3d classification service"""
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    # Add the initial model file to the database
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    feedback_params.initial_model = message.get("initial_model")
    _db.add(feedback_params)
    _db.commit()
    _db.close()


def _flush_tomography_preprocessing(message: dict):
    session_id = message["session_id"]
    instrument_name = (
        murfey_db.exec(select(db.Session).where(db.Session.id == session_id))
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    stashed_files = murfey_db.exec(
        select(db.PreprocessStash)
        .where(db.PreprocessStash.session_id == session_id)
        .where(db.PreprocessStash.group_tag == message["data_collection_group_tag"])
    ).all()
    if not stashed_files:
        return
    collected_ids = murfey_db.exec(
        select(
            db.DataCollectionGroup,
        )
        .where(db.DataCollectionGroup.session_id == session_id)
        .where(db.DataCollectionGroup.tag == message["data_collection_group_tag"])
    ).first()
    proc_params = get_tomo_proc_params(collected_ids.id)
    if not proc_params:
        visit_name = message["visit_name"].replace("\r\n", "").replace("\n", "")
        logger.warning(
            f"No tomography processing parameters found for Murfey session {sanitise(str(message['session_id']))} on visit {sanitise(visit_name)}"
        )
        return

    recipe_name = machine_config.recipes.get("em-tomo-preprocess", "em-tomo-preprocess")

    for f in stashed_files:
        collected_ids = murfey_db.exec(
            select(
                db.DataCollectionGroup,
                db.DataCollection,
                db.ProcessingJob,
                db.AutoProcProgram,
            )
            .where(db.DataCollectionGroup.session_id == session_id)
            .where(db.DataCollectionGroup.tag == message["data_collection_group_tag"])
            .where(db.DataCollection.dcg_id == db.DataCollectionGroup.id)
            .where(db.DataCollection.tag == f.tag)
            .where(db.ProcessingJob.dc_id == db.DataCollection.id)
            .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
            .where(db.ProcessingJob.recipe == recipe_name)
        ).one()
        detached_ids = [c.id for c in collected_ids]

        murfey_ids = _murfey_id(detached_ids[3], murfey_db, number=1, close=False)
        p = Path(f.mrc_out)
        if not p.parent.exists():
            p.parent.mkdir(parents=True)
        movie = db.Movie(
            murfey_id=murfey_ids[0],
            path=f.file_path,
            image_number=f.image_number,
            tag=f.tag,
        )
        murfey_db.add(movie)
        zocalo_message: dict = {
            "recipes": [recipe_name],
            "parameters": {
                "node_creator_queue": machine_config.node_creator_queue,
                "dcid": detached_ids[1],
                "autoproc_program_id": detached_ids[3],
                "movie": f.file_path,
                "mrc_out": f.mrc_out,
                "pixel_size": proc_params.pixel_size,
                "kv": proc_params.voltage,
                "image_number": f.image_number,
                "microscope": get_microscope(),
                "mc_uuid": murfey_ids[0],
                "ft_bin": proc_params.motion_corr_binning,
                "fm_dose": proc_params.dose_per_frame,
                "frame_count": proc_params.frame_count,
                "gain_ref": (
                    str(machine_config.rsync_basepath / proc_params.gain_ref)
                    if proc_params.gain_ref
                    else proc_params.gain_ref
                ),
                "fm_int_file": proc_params.eer_fractionation_file or "",
            },
        }
        logger.info(
            f"Launching tomography preprocessing with Zocalo message: {zocalo_message}"
        )
        if murfey.server._transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = murfey.server._transport_object.feedback_queue
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        else:
            feedback_callback(
                {},
                {
                    "register": "motion_corrected",
                    "movie": f.file_path,
                    "mrc_out": f.mrc_out,
                    "movie_id": murfey_ids[0],
                    "program_id": detached_ids[3],
                },
            )
        murfey_db.delete(f)
        murfey_db.commit()
        murfey_db.close()


def _flush_grid_square_records(message: dict, _db=murfey_db, demo: bool = False):
    tag = message["tag"]
    session_id = message["session_id"]
    gs_ids = []
    for gs in _db.exec(
        select(db.GridSquare)
        .where(db.GridSquare.session_id == session_id)
        .where(db.GridSquare.tag == tag)
    ).all():
        gs_ids.append(gs.id)
        if demo:
            logger.info(f"Flushing grid square {gs.name}")
    for i in gs_ids:
        _flush_foil_hole_records(i, _db=_db, demo=demo)


def _flush_foil_hole_records(grid_square_id: int, _db=murfey_db, demo: bool = False):
    for fh in _db.exec(
        select(db.FoilHole).where(db.FoilHole.grid_square_id == grid_square_id)
    ).all():
        if demo:
            logger.info(f"Flushing foil hole: {fh.name}")


def _register_refinement(message: dict, _db=murfey_db, demo: bool = False):
    """Received class to refine from 3D classification"""
    instrument_name = (
        _db.exec(select(db.Session).where(db.Session.id == message["session_id"]))
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-refine")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    relion_options = dict(relion_params)
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    other_options = dict(feedback_params)

    if feedback_params.hold_refine:
        # If waiting then save the message
        refine_params = _db.exec(
            select(db.RefineParameters)
            .where(db.RefineParameters.pj_id == pj_id)
            .where(db.RefineParameters.tag == "first")
        ).one()
        # refine_params.refine_dir is not set as it will be the same as before
        refine_params.run = True
        refine_params.class3d_dir = message["class3d_dir"]
        refine_params.class_number = message["best_class"]
        _db.add(refine_params)
        _db.commit()
        _db.close()
    else:
        # Send all other messages on to a container
        try:
            refine_params = _db.exec(
                select(db.RefineParameters)
                .where(db.RefineParameters.pj_id == pj_id)
                .where(db.RefineParameters.tag == "first")
            ).one()
            symmetry_refine_params = _db.exec(
                select(db.RefineParameters)
                .where(db.RefineParameters.pj_id == pj_id)
                .where(db.RefineParameters.tag == "symmetry")
            ).one()
        except SQLAlchemyError:
            next_job = feedback_params.next_job
            refine_dir = f"{message['refine_dir']}{(feedback_params.next_job + 2):03}"
            refined_grp_uuid = _murfey_id(message["program_id"], _db)[0]
            refined_class_uuid = _murfey_id(message["program_id"], _db)[0]
            symmetry_refined_grp_uuid = _murfey_id(message["program_id"], _db)[0]
            symmetry_refined_class_uuid = _murfey_id(message["program_id"], _db)[0]

            refine_params = db.RefineParameters(
                tag="first",
                pj_id=pj_id,
                murfey_id=refined_grp_uuid,
                refine_dir=refine_dir,
                class3d_dir=message["class3d_dir"],
                class_number=message["best_class"],
            )
            symmetry_refine_params = db.RefineParameters(
                tag="symmetry",
                pj_id=pj_id,
                murfey_id=symmetry_refined_grp_uuid,
                refine_dir=refine_dir,
                class3d_dir=message["class3d_dir"],
                class_number=message["best_class"],
            )
            _db.add(refine_params)
            _db.add(symmetry_refine_params)
            _db.commit()
            _murfey_refine(
                murfey_id=refined_class_uuid,
                refine_dir=refine_dir,
                tag="first",
                app_id=message["program_id"],
                _db=_db,
            )
            _murfey_refine(
                murfey_id=symmetry_refined_class_uuid,
                refine_dir=refine_dir,
                tag="symmetry",
                app_id=message["program_id"],
                _db=_db,
            )

            if relion_options["symmetry"] == "C1":
                # Extra Refine, Mask, PostProcess beyond for determined symmetry
                next_job += 8
            else:
                # Select and Extract particles, then Refine, Mask, PostProcess
                next_job += 5
            feedback_params.next_job = next_job

        zocalo_message: dict = {
            "parameters": {
                "refine_job_dir": refine_params.refine_dir,
                "class3d_dir": message["class3d_dir"],
                "class_number": message["best_class"],
                "pixel_size": relion_options["angpix"],
                "particle_diameter": relion_options["particle_diameter"],
                "mask_diameter": relion_options["mask_diameter"] or 0,
                "symmetry": relion_options["symmetry"],
                "node_creator_queue": machine_config.node_creator_queue,
                "nr_iter": default_spa_parameters.nr_iter_3d,
                "picker_id": other_options["picker_ispyb_id"],
                "refined_class_uuid": _refine_murfey_id(
                    refine_dir=refine_params.refine_dir,
                    tag=refine_params.tag,
                    app_id=_app_id(pj_id, _db),
                    _db=_db,
                ),
                "refined_grp_uuid": refine_params.murfey_id,
                "symmetry_refined_class_uuid": _refine_murfey_id(
                    refine_dir=symmetry_refine_params.refine_dir,
                    tag=symmetry_refine_params.tag,
                    app_id=_app_id(pj_id, _db),
                    _db=_db,
                ),
                "symmetry_refined_grp_uuid": symmetry_refine_params.murfey_id,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-refine"), _db
                ),
            },
            "recipes": ["em-spa-refine"],
        }
        if murfey.server._transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = murfey.server._transport_object.feedback_queue
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        feedback_params.hold_refine = True
        _db.add(feedback_params)
        _db.commit()
        _db.close()


def _register_bfactors(message: dict, _db=murfey_db, demo: bool = False):
    """Received refined class to calculate b-factor"""
    instrument_name = (
        _db.exec(select(db.Session).where(db.Session.id == message["session_id"]))
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-refine")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    relion_options = dict(relion_params)
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()

    if message["symmetry"] != relion_params.symmetry:
        # Currently don't do anything with a symmetrised re-run of the refinement
        logger.info(
            f"Received symmetrised structure of {sanitise(message['symmetry'])}"
        )
        return True

    if not feedback_params.hold_refine:
        logger.warning("B-Factors requested but refine hold is off")
        return False

    # Add b-factor for refinement run
    bfactor_run = db.BFactors(
        pj_id=pj_id,
        bfactor_directory=f"{message['project_dir']}/Refine3D/bfactor_{message['number_of_particles']}",
        number_of_particles=message["number_of_particles"],
        resolution=message["resolution"],
    )
    _db.add(bfactor_run)
    _db.commit()

    # All messages should create b-factor jobs as the refine hold is on at this point
    try:
        bfactor_params = _db.exec(
            select(db.BFactorParameters).where(db.BFactorParameters.pj_id == pj_id)
        ).one()
    except SQLAlchemyError:
        bfactor_params = db.BFactorParameters(
            pj_id=pj_id,
            project_dir=message["project_dir"],
            batch_size=message["number_of_particles"],
            refined_grp_uuid=message["refined_grp_uuid"],
            refined_class_uuid=message["refined_class_uuid"],
            class_reference=message["class_reference"],
            class_number=message["class_number"],
            mask_file=message["mask_file"],
        )
        _db.add(bfactor_params)
        _db.commit()

    bfactor_particle_count = default_spa_parameters.bfactor_min_particles
    while bfactor_particle_count < bfactor_params.batch_size:
        bfactor_run_name = (
            f"{bfactor_params.project_dir}/BFactors/bfactor_{bfactor_particle_count}"
        )
        try:
            bfactor_run = _db.exec(
                select(db.BFactors)
                .where(db.BFactors.pj_id == pj_id)
                .where(db.BFactors.bfactor_directory == bfactor_run_name)
            ).one()
            bfactor_run.resolution = 0
        except SQLAlchemyError:
            bfactor_run = db.BFactors(
                pj_id=pj_id,
                bfactor_directory=bfactor_run_name,
                number_of_particles=bfactor_particle_count,
                resolution=0,
            )
        _db.add(bfactor_run)
        _db.commit()

        bfactor_particle_count *= 2

        zocalo_message: dict = {
            "parameters": {
                "bfactor_directory": bfactor_run.bfactor_directory,
                "class_reference": bfactor_params.class_reference,
                "class_number": bfactor_params.class_number,
                "number_of_particles": bfactor_run.number_of_particles,
                "batch_size": bfactor_params.batch_size,
                "pixel_size": message["pixel_size"],
                "mask": bfactor_params.mask_file,
                "particle_diameter": relion_options["particle_diameter"],
                "mask_diameter": relion_options["mask_diameter"] or 0,
                "node_creator_queue": machine_config.node_creator_queue,
                "picker_id": feedback_params.picker_ispyb_id,
                "refined_grp_uuid": bfactor_params.refined_grp_uuid,
                "refined_class_uuid": bfactor_params.refined_class_uuid,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-refine"), _db
                ),
            },
            "recipes": ["em-spa-bfactor"],
        }
        if murfey.server._transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = murfey.server._transport_object.feedback_queue
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
    _db.close()
    return True


def _save_bfactor(message: dict, _db=murfey_db, demo: bool = False):
    """Received b-factor from refinement run"""
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-refine")
    bfactor_run = _db.exec(
        select(db.BFactors)
        .where(db.BFactors.pj_id == pj_id)
        .where(db.BFactors.number_of_particles == message["number_of_particles"])
    ).one()
    bfactor_run.resolution = message["resolution"]
    _db.add(bfactor_run)
    _db.commit()

    # Find all the resolutions in the b-factors table
    all_bfactors = _db.exec(select(db.BFactors).where(db.BFactors.pj_id == pj_id)).all()
    particle_counts = [bf.number_of_particles for bf in all_bfactors]
    resolutions = [bf.resolution for bf in all_bfactors]

    if all(resolutions):
        # Calculate b-factor and add to ispyb class table
        bfactor_fitting = np.polyfit(
            np.log(particle_counts), 1 / np.array(resolutions) ** 2, 1
        )
        refined_class_uuid = message["refined_class_uuid"]

        # Request an ispyb insert of the b-factor fitting parameters
        if murfey.server._transport_object:
            murfey.server._transport_object.send(
                "ispyb_connector",
                {
                    "ispyb_command": "buffer",
                    "buffer_lookup": {
                        "particle_classification_id": refined_class_uuid,
                    },
                    "buffer_command": {
                        "ispyb_command": "insert_particle_classification"
                    },
                    "program_id": message["program_id"],
                    "bfactor_fit_intercept": str(bfactor_fitting[1]),
                    "bfactor_fit_linear": str(bfactor_fitting[0]),
                },
                new_connection=True,
            )

        # Clean up the b-factors table and release the hold
        [_db.delete(bf) for bf in all_bfactors]
        _db.commit()
        _release_refine_hold(message)
    _db.close()


def feedback_callback(header: dict, message: dict) -> None:
    try:
        record = None
        if "environment" in message:
            params = message["recipe"][str(message["recipe-pointer"])].get(
                "parameters", {}
            )
            message = message["payload"]
            message.update(params)
        if message["register"] == "motion_corrected":
            collected_ids = murfey_db.exec(
                select(
                    db.DataCollectionGroup,
                    db.DataCollection,
                    db.ProcessingJob,
                    db.AutoProcProgram,
                )
                .where(db.DataCollection.dcg_id == db.DataCollectionGroup.id)
                .where(db.ProcessingJob.dc_id == db.DataCollection.id)
                .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
                .where(db.AutoProcProgram.id == message["program_id"])
            ).one()
            session_id = collected_ids[0].session_id

            # Find the autoprocprogram id for the alignment recipe
            alignment_ids = murfey_db.exec(
                select(
                    db.DataCollection,
                    db.ProcessingJob,
                    db.AutoProcProgram,
                )
                .where(db.ProcessingJob.dc_id == db.DataCollection.id)
                .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
                .where(db.DataCollection.id == collected_ids[1].id)
                .where(db.ProcessingJob.recipe == "em-tomo-align")
            ).one()

            relevant_tilt_and_series = murfey_db.exec(
                select(db.Tilt, db.TiltSeries)
                .where(db.Tilt.movie_path == message.get("movie"))
                .where(db.Tilt.tilt_series_id == db.TiltSeries.id)
                .where(db.TiltSeries.session_id == session_id)
            ).one()
            relevant_tilt = relevant_tilt_and_series[0]
            relevant_tilt_series = relevant_tilt_and_series[1]
            relevant_tilt.motion_corrected = True
            murfey_db.add(relevant_tilt)
            murfey_db.commit()
            if (
                check_tilt_series_mc(relevant_tilt_series.id)
                and not relevant_tilt_series.processing_requested
                and relevant_tilt_series.tilt_series_length > 2
            ):
                instrument_name = (
                    murfey_db.exec(
                        select(db.Session).where(db.Session.id == session_id)
                    )
                    .one()
                    .instrument_name
                )
                machine_config = get_machine_config(instrument_name=instrument_name)[
                    instrument_name
                ]
                tilts = get_all_tilts(relevant_tilt_series.id)
                ids = get_job_ids(relevant_tilt_series.id, alignment_ids[2].id)
                preproc_params = get_tomo_proc_params(ids.dcgid)
                stack_file = (
                    Path(message["mrc_out"]).parents[3]
                    / "Tomograms"
                    / "job006"
                    / "tomograms"
                    / f"{relevant_tilt_series.tag}_stack.mrc"
                )
                if not stack_file.parent.exists():
                    stack_file.parent.mkdir(parents=True)
                tilt_offset = midpoint([float(get_angle(t)) for t in tilts])
                zocalo_message = {
                    "recipes": ["em-tomo-align"],
                    "parameters": {
                        "input_file_list": str([[t, str(get_angle(t))] for t in tilts]),
                        "path_pattern": "",  # blank for now so that it works with the tomo_align service changes
                        "dcid": ids.dcid,
                        "appid": ids.appid,
                        "stack_file": str(stack_file),
                        "dose_per_frame": preproc_params.dose_per_frame,
                        "frame_count": preproc_params.frame_count,
                        "kv": preproc_params.voltage,
                        "tilt_axis": preproc_params.tilt_axis,
                        "pixel_size": preproc_params.pixel_size,
                        "manual_tilt_offset": -tilt_offset,
                        "node_creator_queue": machine_config.node_creator_queue,
                        "search_map_id": relevant_tilt_series.search_map_id,
                        "x_location": relevant_tilt_series.x_location,
                        "y_location": relevant_tilt_series.y_location,
                    },
                }
                if murfey.server._transport_object:
                    logger.info(
                        f"Sending Zocalo message for processing: {zocalo_message}"
                    )
                    murfey.server._transport_object.send(
                        "processing_recipe", zocalo_message, new_connection=True
                    )
                else:
                    logger.info(
                        f"No transport object found. Zocalo message would be {zocalo_message}"
                    )
                relevant_tilt_series.processing_requested = True
                murfey_db.add(relevant_tilt_series)

            prom.preprocessed_movies.labels(processing_job=collected_ids[2].id).inc()
            murfey_db.commit()
            murfey_db.close()
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "data_collection_group":
            ispyb_session_id = get_session_id(
                microscope=message["microscope"],
                proposal_code=message["proposal_code"],
                proposal_number=message["proposal_number"],
                visit_number=message["visit_number"],
                db=ISPyBSession(),
            )
            if dcg_murfey := murfey_db.exec(
                select(db.DataCollectionGroup)
                .where(db.DataCollectionGroup.session_id == message["session_id"])
                .where(db.DataCollectionGroup.tag == message.get("tag"))
            ).all():
                dcgid = dcg_murfey[0].id
            else:
                if ispyb_session_id is None:
                    murfey_dcg = db.DataCollectionGroup(
                        session_id=message["session_id"],
                        tag=message.get("tag"),
                    )
                else:
                    record = DataCollectionGroup(
                        sessionId=ispyb_session_id,
                        experimentType=message["experiment_type"],
                        experimentTypeId=message["experiment_type_id"],
                    )
                    dcgid = _register(record, header)
                    atlas_record = Atlas(
                        dataCollectionGroupId=dcgid,
                        atlasImage=message.get("atlas", ""),
                        pixelSize=message.get("atlas_pixel_size", 0),
                        cassetteSlot=message.get("sample"),
                    )
                    if murfey.server._transport_object:
                        atlas_id = murfey.server._transport_object.do_insert_atlas(
                            atlas_record
                        )["return_value"]
                    murfey_dcg = db.DataCollectionGroup(
                        id=dcgid,
                        atlas_id=atlas_id,
                        session_id=message["session_id"],
                        tag=message.get("tag"),
                    )
                murfey_db.add(murfey_dcg)
                murfey_db.commit()
                murfey_db.close()
            if murfey.server._transport_object:
                if dcgid is None:
                    time.sleep(2)
                    murfey.server._transport_object.transport.nack(header, requeue=True)
                    return None
                murfey.server._transport_object.transport.ack(header)
            if dcg_hooks := entry_points().select(
                group="murfey.hooks", name="data_collection_group"
            ):
                try:
                    for hook in dcg_hooks:
                        hook.load()(dcgid, session_id=message["session_id"])
                except Exception:
                    logger.error(
                        "Call to data collection group hook failed", exc_info=True
                    )
            return None
        elif message["register"] == "atlas_update":
            if murfey.server._transport_object:
                murfey.server._transport_object.do_update_atlas(
                    message["atlas_id"],
                    message["atlas"],
                    message["atlas_pixel_size"],
                    message["sample"],
                )
                murfey.server._transport_object.transport.ack(header)
            if dcg_hooks := entry_points().select(
                group="murfey.hooks", name="data_collection_group"
            ):
                try:
                    for hook in dcg_hooks:
                        hook.load()(message["dcgid"], session_id=message["session_id"])
                except Exception:
                    logger.error(
                        "Call to data collection group hook failed", exc_info=True
                    )
            return None
        elif message["register"] == "data_collection":
            logger.debug(
                "Received message named 'data_collection' containing the following items:\n"
                f"{', '.join([f'{sanitise(key)}: {sanitise(str(value))}' for key, value in message.items()])}"
            )
            murfey_session_id = message["session_id"]
            ispyb_session_id = get_session_id(
                microscope=message["microscope"],
                proposal_code=message["proposal_code"],
                proposal_number=message["proposal_number"],
                visit_number=message["visit_number"],
                db=ISPyBSession(),
            )
            dcg = murfey_db.exec(
                select(db.DataCollectionGroup)
                .where(db.DataCollectionGroup.session_id == murfey_session_id)
                .where(db.DataCollectionGroup.tag == message["source"])
            ).all()
            if dcg:
                dcgid = dcg[0].id
                # flush_data_collections(message["source"], murfey_db)
            else:
                logger.warning(
                    "No data collection group ID was found for image directory "
                    f"{sanitise(message['image_directory'])} and source "
                    f"{sanitise(message['source'])}"
                )
                if murfey.server._transport_object:
                    murfey.server._transport_object.transport.nack(header, requeue=True)
                return None
            if dc_murfey := murfey_db.exec(
                select(db.DataCollection)
                .where(db.DataCollection.tag == message.get("tag"))
                .where(db.DataCollection.dcg_id == dcgid)
            ).all():
                dcid = dc_murfey[0].id
            else:
                if ispyb_session_id is None:
                    murfey_dc = db.DataCollection(
                        tag=message.get("tag"),
                        dcg_id=dcgid,
                    )
                else:
                    record = DataCollection(
                        SESSIONID=ispyb_session_id,
                        experimenttype=message["experiment_type"],
                        imageDirectory=message["image_directory"],
                        imageSuffix=message["image_suffix"],
                        voltage=message["voltage"],
                        dataCollectionGroupId=dcgid,
                        pixelSizeOnImage=message["pixel_size"],
                        imageSizeX=message["image_size_x"],
                        imageSizeY=message["image_size_y"],
                        slitGapHorizontal=message.get("slit_width"),
                        magnification=message.get("magnification"),
                        exposureTime=message.get("exposure_time"),
                        totalExposedDose=message.get("total_exposed_dose"),
                        c2aperture=message.get("c2aperture"),
                        phasePlate=int(message.get("phase_plate", 0)),
                    )
                    dcid = _register(
                        record,
                        header,
                        tag=(
                            message.get("tag")
                            if message["experiment_type"] == "tomography"
                            else ""
                        ),
                    )
                    murfey_dc = db.DataCollection(
                        id=dcid,
                        tag=message.get("tag"),
                        dcg_id=dcgid,
                    )
                murfey_db.add(murfey_dc)
                murfey_db.commit()
                dcid = murfey_dc.id
                murfey_db.close()
            if dcid is None and murfey.server._transport_object:
                murfey.server._transport_object.transport.nack(header, requeue=True)
                return None
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "processing_job":
            murfey_session_id = message["session_id"]
            logger.info("registering processing job")
            dc = murfey_db.exec(
                select(db.DataCollection, db.DataCollectionGroup)
                .where(db.DataCollection.dcg_id == db.DataCollectionGroup.id)
                .where(db.DataCollectionGroup.session_id == murfey_session_id)
                .where(db.DataCollectionGroup.tag == message["source"])
                .where(db.DataCollection.tag == message["tag"])
            ).all()
            if dc:
                _dcid = dc[0][0].id
            else:
                logger.warning(
                    f"No data collection ID found for {sanitise(message['tag'])}"
                )
                if murfey.server._transport_object:
                    murfey.server._transport_object.transport.nack(header, requeue=True)
                return None
            if pj_murfey := murfey_db.exec(
                select(db.ProcessingJob)
                .where(db.ProcessingJob.recipe == message["recipe"])
                .where(db.ProcessingJob.dc_id == _dcid)
            ).all():
                pid = pj_murfey[0].id
            else:
                if ISPyBSession() is None:
                    murfey_pj = db.ProcessingJob(recipe=message["recipe"], dc_id=_dcid)
                else:
                    record = ProcessingJob(
                        dataCollectionId=_dcid, recipe=message["recipe"]
                    )
                    run_parameters = message.get("parameters", {})
                    assert isinstance(run_parameters, dict)
                    if message.get("job_parameters"):
                        job_parameters = [
                            ProcessingJobParameter(parameterKey=k, parameterValue=v)
                            for k, v in message["job_parameters"].items()
                        ]
                        pid = _register(ExtendedRecord(record, job_parameters), header)
                    else:
                        pid = _register(record, header)
                    murfey_pj = db.ProcessingJob(
                        id=pid, recipe=message["recipe"], dc_id=_dcid
                    )
                murfey_db.add(murfey_pj)
                murfey_db.commit()
                pid = murfey_pj.id
                murfey_db.close()
            if pid is None and murfey.server._transport_object:
                murfey.server._transport_object.transport.nack(header, requeue=True)
                return None
            prom.preprocessed_movies.labels(processing_job=pid)
            if not murfey_db.exec(
                select(db.AutoProcProgram).where(db.AutoProcProgram.pj_id == pid)
            ).all():
                if ISPyBSession() is None:
                    murfey_app = db.AutoProcProgram(pj_id=pid)
                else:
                    record = AutoProcProgram(
                        processingJobId=pid, processingStartTime=datetime.now()
                    )
                    appid = _register(record, header)
                    if appid is None and murfey.server._transport_object:
                        murfey.server._transport_object.transport.nack(
                            header, requeue=True
                        )
                        return None
                    murfey_app = db.AutoProcProgram(id=appid, pj_id=pid)
                murfey_db.add(murfey_app)
                murfey_db.commit()
                murfey_db.close()
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "flush_tomography_preprocess":
            _flush_tomography_preprocessing(message)
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "spa_processing_parameters":
            session_id = message["session_id"]
            collected_ids = murfey_db.exec(
                select(
                    db.DataCollectionGroup,
                    db.DataCollection,
                    db.ProcessingJob,
                    db.AutoProcProgram,
                )
                .where(db.DataCollectionGroup.session_id == session_id)
                .where(db.DataCollectionGroup.tag == message["tag"])
                .where(db.DataCollection.dcg_id == db.DataCollectionGroup.id)
                .where(db.ProcessingJob.dc_id == db.DataCollection.id)
                .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
                .where(db.ProcessingJob.recipe == "em-spa-preprocess")
            ).one()
            pj_id = collected_ids[2].id
            if not murfey_db.exec(
                select(db.SPARelionParameters).where(
                    db.SPARelionParameters.pj_id == pj_id
                )
            ).all():
                instrument_name = (
                    murfey_db.exec(
                        select(db.Session).where(db.Session.id == session_id)
                    )
                    .one()
                    .instrument_name
                )
                machine_config = get_machine_config(instrument_name=instrument_name)[
                    instrument_name
                ]
                params = db.SPARelionParameters(
                    pj_id=collected_ids[2].id,
                    angpix=float(message["pixel_size_on_image"]) * 1e10,
                    dose_per_frame=message["dose_per_frame"],
                    gain_ref=(
                        str(machine_config.rsync_basepath / message["gain_ref"])
                        if message["gain_ref"] and machine_config.data_transfer_enabled
                        else message["gain_ref"]
                    ),
                    voltage=message["voltage"],
                    motion_corr_binning=message["motion_corr_binning"],
                    eer_fractionation_file=message["eer_fractionation_file"],
                    symmetry=message["symmetry"],
                )
                feedback_params = db.SPAFeedbackParameters(
                    pj_id=collected_ids[2].id,
                    estimate_particle_diameter=True,
                    hold_class2d=False,
                    hold_class3d=False,
                    class_selection_score=0,
                    star_combination_job=0,
                    initial_model="",
                    next_job=0,
                )
                murfey_db.add(params)
                murfey_db.add(feedback_params)
                murfey_db.commit()
                logger.info(
                    f"SPA processing parameters registered for processing job {collected_ids[2].id}"
                )
                murfey_db.close()
            else:
                logger.info(
                    f"SPA processing parameters already exist for processing job ID {pj_id}"
                )
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "tomography_processing_parameters":
            session_id = message["session_id"]
            collected_ids = murfey_db.exec(
                select(
                    db.DataCollectionGroup,
                    db.DataCollection,
                    db.ProcessingJob,
                    db.AutoProcProgram,
                )
                .where(db.DataCollectionGroup.session_id == session_id)
                .where(db.DataCollectionGroup.tag == message["tag"])
                .where(db.DataCollection.dcg_id == db.DataCollectionGroup.id)
                .where(db.DataCollection.tag == message["tilt_series_tag"])
                .where(db.ProcessingJob.dc_id == db.DataCollection.id)
                .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
                .where(db.ProcessingJob.recipe == "em-tomo-preprocess")
            ).one()
            if not murfey_db.exec(
                select(func.count(db.TomographyProcessingParameters.dcg_id)).where(
                    db.TomographyProcessingParameters.dcg_id == collected_ids[0].id
                )
            ).one():
                params = db.TomographyProcessingParameters(
                    dcg_id=collected_ids[0].id,
                    pixel_size=float(message["pixel_size_on_image"]) * 10**10,
                    voltage=message["voltage"],
                    dose_per_frame=message["dose_per_frame"],
                    frame_count=message["frame_count"],
                    tilt_axis=message["tilt_axis"],
                    motion_corr_binning=message["motion_corr_binning"],
                    gain_ref=message["gain_ref"],
                    eer_fractionation_file=message["eer_fractionation_file"],
                )
                murfey_db.add(params)
                murfey_db.commit()
                murfey_db.close()
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "done_incomplete_2d_batch":
            _release_2d_hold(message)
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "incomplete_particles_file":
            _register_incomplete_2d_batch(message)
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "complete_particles_file":
            _register_complete_2d_batch(message)
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "save_class_selection_score":
            _register_class_selection(message)
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "done_3d_batch":
            _release_3d_hold(message)
            if message.get("do_refinement"):
                _register_refinement(message)
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "run_class3d":
            _register_3d_batch(message)
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "save_initial_model":
            _register_initial_model(message)
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "done_particle_selection":
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "done_class_selection":
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "atlas_registered":
            _flush_grid_square_records(message)
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif message["register"] == "done_refinement":
            bfactors_registered = _register_bfactors(message)
            if murfey.server._transport_object:
                if bfactors_registered:
                    murfey.server._transport_object.transport.ack(header)
                else:
                    murfey.server._transport_object.transport.nack(header)
            return None
        elif message["register"] == "done_bfactor":
            _save_bfactor(message)
            if murfey.server._transport_object:
                murfey.server._transport_object.transport.ack(header)
            return None
        elif (
            message["register"] in entry_points().select(group="murfey.workflows").names
        ):
            # Search for corresponding workflow
            workflows: list[EntryPoint] = list(
                entry_points().select(
                    group="murfey.workflows", name=message["register"]
                )
            )  # Returns either 1 item or empty list
            if not workflows:
                logger.error(f"No workflow found for {sanitise(message['register'])}")
                if murfey.server._transport_object:
                    murfey.server._transport_object.transport.nack(
                        header, requeue=False
                    )
                return None
            # Run the workflow if a match is found
            workflow: EntryPoint = workflows[0]
            result = workflow.load()(
                message=message,
                murfey_db=murfey_db,
            )
            if murfey.server._transport_object:
                if result:
                    murfey.server._transport_object.transport.ack(header)
                else:
                    # Send it directly to DLQ without trying to rerun it
                    murfey.server._transport_object.transport.nack(
                        header, requeue=False
                    )
            if not result:
                logger.error(
                    f"Workflow {sanitise(message['register'])} returned {result}"
                )
            return None
        logger.error(f"No workflow found for {sanitise(message['register'])}")
        if murfey.server._transport_object:
            murfey.server._transport_object.transport.nack(header, requeue=False)
        return None
    except PendingRollbackError:
        murfey_db.rollback()
        murfey_db.close()
        logger.warning("Murfey database required a rollback")
        if murfey.server._transport_object:
            murfey.server._transport_object.transport.nack(header, requeue=True)
    except OperationalError:
        logger.warning("Murfey database error encountered", exc_info=True)
        time.sleep(1)
        if murfey.server._transport_object:
            murfey.server._transport_object.transport.nack(header, requeue=True)
    except Exception:
        logger.warning(
            "Exception encountered in server RabbitMQ callback", exc_info=True
        )
        if murfey.server._transport_object:
            murfey.server._transport_object.transport.nack(header, requeue=False)
    return None


@singledispatch
def _register(record, header: dict, **kwargs):
    raise NotImplementedError(f"Not method to register {record} or type {type(record)}")


@_register.register
def _(record: Base, header: dict, **kwargs):  # type: ignore
    if not murfey.server._transport_object:
        logger.error(
            f"No transport object found when processing record {record}. Message header: {header}"
        )
        return None
    try:
        if isinstance(record, DataCollection):
            return murfey.server._transport_object.do_insert_data_collection(
                record, **kwargs
            )["return_value"]
        if isinstance(record, DataCollectionGroup):
            return murfey.server._transport_object.do_insert_data_collection_group(
                record
            )["return_value"]
        if isinstance(record, ProcessingJob):
            return murfey.server._transport_object.do_create_ispyb_job(record)[
                "return_value"
            ]
        if isinstance(record, AutoProcProgram):
            return murfey.server._transport_object.do_update_processing_status(record)[
                "return_value"
            ]
        # session = Session()
        # session.add(record)
        # session.commit()
        # murfey.server._transport_object.transport.ack(header, requeue=False)
        return getattr(record, record.__table__.primary_key.columns[0].name)

    except SQLAlchemyError as e:
        logger.error(f"Murfey failed to insert ISPyB record {record}", e, exc_info=True)
        # murfey.server._transport_object.transport.nack(header)
        return None
    except AttributeError as e:
        logger.error(
            f"Murfey could not find primary key when inserting record {record}",
            e,
            exc_info=True,
        )
        return None


@_register.register
def _(extended_record: ExtendedRecord, header: dict, **kwargs):
    if not murfey.server._transport_object:
        raise ValueError(
            "Transport object should not be None if a database record is being updated"
        )
    return murfey.server._transport_object.do_create_ispyb_job(
        extended_record.record, params=extended_record.record_params
    )["return_value"]


def feedback_listen():
    if murfey.server._transport_object:
        if not murfey.server._transport_object.feedback_queue:
            murfey.server._transport_object.feedback_queue = (
                murfey.server._transport_object.transport._subscribe_temporary(
                    channel_hint="", callback=None, sub_id=None
                )
            )
        murfey.server._transport_object._connection_callback = partial(
            murfey.server._transport_object.transport.subscribe,
            murfey.server._transport_object.feedback_queue,
            feedback_callback,
            acknowledgement=True,
        )
        murfey.server._transport_object.transport.subscribe(
            murfey.server._transport_object.feedback_queue,
            feedback_callback,
            acknowledgement=True,
        )
