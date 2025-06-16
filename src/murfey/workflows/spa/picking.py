from logging import getLogger
from typing import List

import numpy as np
from sqlalchemy import func
from sqlmodel import Session, select

import murfey.server.prometheus as prom
from murfey.server import _transport_object
from murfey.server.feedback import (
    _app_id,
    _flush_class2d,
    _pj_id,
    _register_class_selection,
    _register_incomplete_2d_batch,
)
from murfey.util.config import get_machine_config
from murfey.util.db import (
    AutoProcProgram,
    CtfParameters,
    DataCollection,
    Movie,
    NotificationParameter,
    NotificationValue,
    ParticleSizes,
    ProcessingJob,
    SelectionStash,
)
from murfey.util.db import Session as MurfeySession
from murfey.util.db import SPAFeedbackParameters, SPARelionParameters
from murfey.util.processing_params import default_spa_parameters

logger = getLogger("murfey.workflows.spa.picking")


def _register_picked_particles_use_diameter(
    message: dict, _db: Session, demo: bool = False
):
    """Received picked particles from the autopick service"""
    # Add this message to the table of seen messages
    params_to_forward = message.get("extraction_parameters")
    assert isinstance(params_to_forward, dict)
    pj_id = _pj_id(message["program_id"], _db)
    ctf_params = CtfParameters(
        pj_id=pj_id,
        micrographs_file=params_to_forward["micrographs_file"],
        extract_file=params_to_forward["extract_file"],
        coord_list_file=params_to_forward["coord_list_file"],
        ctf_image=params_to_forward["ctf_values"]["CtfImage"],
        ctf_max_resolution=params_to_forward["ctf_values"]["CtfMaxResolution"],
        ctf_figure_of_merit=params_to_forward["ctf_values"]["CtfFigureOfMerit"],
        defocus_u=params_to_forward["ctf_values"]["DefocusU"],
        defocus_v=params_to_forward["ctf_values"]["DefocusV"],
        defocus_angle=params_to_forward["ctf_values"]["DefocusAngle"],
    )
    _db.add(ctf_params)
    _db.commit()
    _db.close()

    picking_db_len = _db.exec(
        select(func.count(ParticleSizes.id)).where(ParticleSizes.pj_id == pj_id)
    ).one()
    if picking_db_len > default_spa_parameters.nr_picks_before_diameter:
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
        relion_params = _db.exec(
            select(SPARelionParameters).where(SPARelionParameters.pj_id == pj_id)
        ).one()
        relion_options = dict(relion_params)
        feedback_params = _db.exec(
            select(SPAFeedbackParameters).where(SPAFeedbackParameters.pj_id == pj_id)
        ).one()

        particle_diameter = relion_params.particle_diameter

        if feedback_params.picker_ispyb_id is None:
            if demo or not _transport_object:
                feedback_params.picker_ispyb_id = 1000
            else:
                assert feedback_params.picker_murfey_id is not None
                feedback_params.picker_ispyb_id = _transport_object.do_buffer_lookup(
                    message["program_id"], feedback_params.picker_murfey_id
                )
                if feedback_params.picker_ispyb_id is not None:
                    _flush_class2d(message["session_id"], message["program_id"], _db)
            _db.add(feedback_params)
            _db.commit()
            selection_stash = _db.exec(
                select(SelectionStash).where(SelectionStash.pj_id == pj_id)
            ).all()
            for s in selection_stash:
                _register_class_selection(
                    {
                        "session_id": s.session_id,
                        "class_selection_score": s.class_selection_score or 0,
                    },
                    _db=_db,
                    demo=demo,
                )
                _db.delete(s)
                _db.commit()

        if not particle_diameter:
            # If the diameter has not been calculated then find it
            picking_db = _db.exec(
                select(ParticleSizes.particle_size).where(ParticleSizes.pj_id == pj_id)
            ).all()
            particle_diameter = np.quantile(list(picking_db), 0.75)
            relion_params.particle_diameter = particle_diameter
            _db.add(relion_params)
            _db.commit()

            ctf_db = _db.exec(
                select(CtfParameters).where(CtfParameters.pj_id == pj_id)
            ).all()
            for saved_message in ctf_db:
                # Send on all saved messages to extraction
                _db.expunge(saved_message)
                zocalo_message: dict = {
                    "parameters": {
                        "micrographs_file": saved_message.micrographs_file,
                        "coord_list_file": saved_message.coord_list_file,
                        "output_file": saved_message.extract_file,
                        "pixel_size": (
                            relion_options["angpix"]
                            * relion_options["motion_corr_binning"]
                        ),
                        "ctf_image": saved_message.ctf_image,
                        "ctf_max_resolution": saved_message.ctf_max_resolution,
                        "ctf_figure_of_merit": saved_message.ctf_figure_of_merit,
                        "defocus_u": saved_message.defocus_u,
                        "defocus_v": saved_message.defocus_v,
                        "defocus_angle": saved_message.defocus_angle,
                        "particle_diameter": particle_diameter,
                        "downscale": relion_options["downscale"],
                        "kv": relion_options["voltage"],
                        "node_creator_queue": machine_config.node_creator_queue,
                        "session_id": message["session_id"],
                        "autoproc_program_id": _app_id(
                            _pj_id(message["program_id"], _db, recipe="em-spa-extract"),
                            _db,
                        ),
                        "batch_size": default_spa_parameters.batch_size_2d,
                    },
                    "recipes": ["em-spa-extract"],
                }
                if _transport_object:
                    zocalo_message["parameters"][
                        "feedback_queue"
                    ] = _transport_object.feedback_queue
                    _transport_object.send(
                        "processing_recipe", zocalo_message, new_connection=True
                    )
        else:
            # If the diameter is known then just send the new message
            particle_diameter = relion_params.particle_diameter
            zocalo_message = {
                "parameters": {
                    "micrographs_file": params_to_forward["micrographs_file"],
                    "coord_list_file": params_to_forward["coord_list_file"],
                    "output_file": params_to_forward["extract_file"],
                    "pixel_size": (
                        relion_options["angpix"] * relion_options["motion_corr_binning"]
                    ),
                    "ctf_image": params_to_forward["ctf_values"]["CtfImage"],
                    "ctf_max_resolution": params_to_forward["ctf_values"][
                        "CtfMaxResolution"
                    ],
                    "ctf_figure_of_merit": params_to_forward["ctf_values"][
                        "CtfFigureOfMerit"
                    ],
                    "defocus_u": params_to_forward["ctf_values"]["DefocusU"],
                    "defocus_v": params_to_forward["ctf_values"]["DefocusV"],
                    "defocus_angle": params_to_forward["ctf_values"]["DefocusAngle"],
                    "particle_diameter": particle_diameter,
                    "downscale": relion_options["downscale"],
                    "kv": relion_options["voltage"],
                    "node_creator_queue": machine_config.node_creator_queue,
                    "session_id": message["session_id"],
                    "autoproc_program_id": _app_id(
                        _pj_id(message["program_id"], _db, recipe="em-spa-extract"), _db
                    ),
                    "batch_size": default_spa_parameters.batch_size_2d,
                },
                "recipes": ["em-spa-extract"],
            }
            if _transport_object:
                zocalo_message["parameters"][
                    "feedback_queue"
                ] = _transport_object.feedback_queue
                _transport_object.send(
                    "processing_recipe", zocalo_message, new_connection=True
                )
            if demo:
                _register_incomplete_2d_batch(
                    {
                        "session_id": message["session_id"],
                        "program_id": message["program_id"],
                        "class2d_message": {
                            "particles_file": "Select/job009/particles_split_1.star",
                            "class2d_dir": "Class2D",
                            "batch_size": 50000,
                        },
                    },
                    _db=_db,
                    demo=demo,
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


def _register_picked_particles_use_boxsize(message: dict, _db: Session):
    """Received picked particles from the autopick service"""
    # Add this message to the table of seen messages
    params_to_forward = message.get("extraction_parameters")
    assert isinstance(params_to_forward, dict)

    instrument_name = (
        _db.exec(select(MurfeySession).where(MurfeySession.id == message["session_id"]))
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    pj_id = _pj_id(message["program_id"], _db)
    ctf_params = CtfParameters(
        pj_id=pj_id,
        micrographs_file=params_to_forward["micrographs_file"],
        coord_list_file=params_to_forward["coord_list_file"],
        extract_file=params_to_forward["extract_file"],
        ctf_image=params_to_forward["ctf_values"]["CtfImage"],
        ctf_max_resolution=params_to_forward["ctf_values"]["CtfMaxResolution"],
        ctf_figure_of_merit=params_to_forward["ctf_values"]["CtfFigureOfMerit"],
        defocus_u=params_to_forward["ctf_values"]["DefocusU"],
        defocus_v=params_to_forward["ctf_values"]["DefocusV"],
        defocus_angle=params_to_forward["ctf_values"]["DefocusAngle"],
    )
    _db.add(ctf_params)
    _db.commit()
    _db.close()

    # Set particle diameter as zero and send box sizes
    relion_params = _db.exec(
        select(SPARelionParameters).where(SPARelionParameters.pj_id == pj_id)
    ).one()
    feedback_params = _db.exec(
        select(SPAFeedbackParameters).where(SPAFeedbackParameters.pj_id == pj_id)
    ).one()

    if feedback_params.picker_ispyb_id is None and _transport_object:
        assert feedback_params.picker_murfey_id is not None
        feedback_params.picker_ispyb_id = _transport_object.do_buffer_lookup(
            message["program_id"], feedback_params.picker_murfey_id
        )
        if feedback_params.picker_ispyb_id is not None:
            _flush_class2d(message["session_id"], message["program_id"], _db)
        _db.add(feedback_params)
        _db.commit()
        selection_stash = _db.exec(
            select(SelectionStash).where(SelectionStash.pj_id == pj_id)
        ).all()
        for s in selection_stash:
            _register_class_selection(
                {
                    "session_id": s.session_id,
                    "class_selection_score": s.class_selection_score or 0,
                },
                _db=_db,
            )
            _db.delete(s)
            _db.commit()

    # Send the message to extraction with the box sizes
    zocalo_message: dict = {
        "parameters": {
            "micrographs_file": params_to_forward["micrographs_file"],
            "coord_list_file": params_to_forward["coord_list_file"],
            "output_file": params_to_forward["extract_file"],
            "pixel_size": relion_params.angpix * relion_params.motion_corr_binning,
            "ctf_image": params_to_forward["ctf_values"]["CtfImage"],
            "ctf_max_resolution": params_to_forward["ctf_values"]["CtfMaxResolution"],
            "ctf_figure_of_merit": params_to_forward["ctf_values"]["CtfFigureOfMerit"],
            "defocus_u": params_to_forward["ctf_values"]["DefocusU"],
            "defocus_v": params_to_forward["ctf_values"]["DefocusV"],
            "defocus_angle": params_to_forward["ctf_values"]["DefocusAngle"],
            "particle_diameter": relion_params.particle_diameter,
            "boxsize": relion_params.boxsize,
            "small_boxsize": relion_params.small_boxsize,
            "downscale": relion_params.downscale,
            "kv": relion_params.voltage,
            "node_creator_queue": machine_config.node_creator_queue,
            "session_id": message["session_id"],
            "autoproc_program_id": _app_id(
                _pj_id(message["program_id"], _db, recipe="em-spa-extract"), _db
            ),
            "batch_size": default_spa_parameters.batch_size_2d,
        },
        "recipes": ["em-spa-extract"],
    }
    if _transport_object:
        zocalo_message["parameters"][
            "feedback_queue"
        ] = _transport_object.feedback_queue
        _transport_object.send("processing_recipe", zocalo_message, new_connection=True)
    _db.close()


def _request_email(
    failed_params: List[str], dcg_id: int, session_id: int, murfey_db: Session
) -> None:
    session = murfey_db.exec(
        select(MurfeySession).where(MurfeySession.id == session_id)
    ).one()
    config = get_machine_config(instrument_name=session.instrument_name)[
        session.instrument_name
    ]
    if _transport_object:
        _transport_object.send(
            config.notifications_queue,
            {
                "groupId": dcg_id,
                "message": f"The following parameters consistently exceeded the user set bounds: {failed_params}",
            },
            new_connection=True,
        )
        logger.debug(
            f"Sent notification to {config.notifications_queue!r} for "
            f"visit {session.visit!r}, data collection group ID {dcg_id} about the following abnormal parameters: \n"
            f"{', '.join([f'{p}' for p in failed_params])}"
        )
    return None


def _check_notifications(message: dict, murfey_db: Session) -> None:
    data_collection_hierarchy = murfey_db.exec(
        select(DataCollection, ProcessingJob, AutoProcProgram)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
        .where(AutoProcProgram.id == message["program_id"])
    ).all()
    dcgid = data_collection_hierarchy[0][0].dcg_id
    notification_parameters = murfey_db.exec(
        select(NotificationParameter).where(NotificationParameter.dcg_id == dcgid)
    ).all()
    failures = []
    for param in notification_parameters:
        if message.get(param.name) is not None:
            # Load instances of current parameter from database
            param_values = murfey_db.exec(
                select(NotificationValue).where(
                    NotificationValue.notification_parameter_id == param.id
                )
            ).all()
            param_values.sort(key=lambda x: x.index)

            # Drop oldest value if number of entries exceeds threshold
            param_value_to_drop = None
            if len(param_values) >= 25:
                param_value_to_drop = param_values[0]
                param_values = param_values[1:]

            # Add newest value to end of list
            param_values.append(
                NotificationValue(
                    notification_parameter_id=param.id,
                    index=param_values[-1].index + 1 if len(param_values) else 0,
                    within_bounds=param.min_value
                    <= message[param.name]
                    <= param.max_value,
                )
            )

            # Trigger message if this param has consistently exceeded the set threshold
            if (
                len(param_values) >= 25
                and sum(p.within_bounds for p in param_values) / len(param_values)
                < 0.25
            ):
                # If notifications disabled, enable them now
                trigger = False
                if not param.notification_active:
                    # Use a variable to trigger the notification for the first
                    # time within the first 500 messages received
                    if param_values[-1].index < 500:
                        logger.debug(
                            f"First abnormal instance of parameter {param.name!r} detected"
                        )
                        trigger = True
                    param.notification_active = True

                if param.num_instances_since_triggered >= 500 or trigger:
                    if not trigger:
                        logger.debug(
                            f"Parameter {param.name!r} has exceeded normal operating thresholds"
                        )
                    failures.append(param.name)
                    param.num_instances_since_triggered = 0
            else:
                # Only reset to False if there are more than 500 instances
                # to stop multiple triggers within the first 500
                if param.notification_active and param_values[-1].index > 500:
                    param.notification_active = False

            # Delete oldest value
            if param_value_to_drop is not None:
                murfey_db.delete(param_value_to_drop)

            # Add newest value and increment record of instances
            murfey_db.add(param_values[-1])
            param.num_instances_since_triggered += 1

    murfey_db.add_all(notification_parameters)
    murfey_db.commit()
    murfey_db.close()
    if failures:
        logger.debug(
            "Requested email notification for the following abnormal parameters: \n"
            f"{', '.join([f'{p}' for p in failures])}"
        )
        _request_email(failures, dcgid, message["session_id"], murfey_db)
    return None


def particles_picked(message: dict, murfey_db: Session) -> bool:
    movie = murfey_db.exec(
        select(Movie).where(Movie.murfey_id == message["motion_correction_id"])
    ).one()
    movie.preprocessed = True
    murfey_db.add(movie)
    murfey_db.commit()
    feedback_params = murfey_db.exec(
        select(SPAFeedbackParameters).where(
            SPAFeedbackParameters.pj_id == _pj_id(message["program_id"], murfey_db)
        )
    ).one()
    if feedback_params.estimate_particle_diameter:
        _register_picked_particles_use_diameter(message, murfey_db)
    else:
        _register_picked_particles_use_boxsize(message, murfey_db)
    prom.preprocessed_movies.labels(
        processing_job=_pj_id(message["program_id"], murfey_db)
    ).inc()
    _check_notifications(message, murfey_db)
    return True
