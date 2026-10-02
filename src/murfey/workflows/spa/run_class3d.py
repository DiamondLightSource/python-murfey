import math
import subprocess
from datetime import datetime
from logging import getLogger
from pathlib import Path

import mrcfile
import numpy as np
from sqlmodel import Session as SQLModelSession, select

import murfey.server
import murfey.util.db as MurfeyDB
from murfey.server.feedback import (
    _3d_class_murfey_ids,
    _allocation_floor,
    _app_id,
    _murfey_id,
    _pj_id,
    _reserve_pipeline_job_numbers,
)
from murfey.util.config import MachineConfig, get_machine_config
from murfey.util.processing_params import default_spa_parameters

logger = getLogger(__name__)


def _murfey_class3ds(murfey_ids: list[int], particles_file: str, app_id: int, _db):
    pj_id = _pj_id(app_id, _db, recipe="em-spa-class3d")
    class3ds = [
        MurfeyDB.Class3D(
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


def _find_initial_model(visit: str, machine_config: MachineConfig) -> Path | None:
    if machine_config.initial_model_search_directory:
        visit_directory = (
            (machine_config.rsync_basepath or Path("")).resolve()
            / str(datetime.now().year)
            / visit
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
    particle_diameter_ang: float, pixel_size: float
) -> tuple[int, float]:
    particle_diameter = particle_diameter_ang / pixel_size
    box_size = int(math.ceil(1.2 * particle_diameter))
    box_size = box_size + box_size % 2
    for small_box_pix in (
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
        # If Nyquist freq. is better than 7.5 A, use this downscaled box, else step size
        small_box_angpix = pixel_size * box_size / small_box_pix
        if small_box_angpix < 3.75:
            return small_box_pix, small_box_angpix
    raise ValueError(f"Box size is too large: {box_size}")


def _resize_initial_model(
    downscaled_box_size: int,
    downscaled_pixel_size: float,
    input_path: Path,
    output_path: Path,
    symmetry: str,
    executables: dict[str, str],
    env: dict[str, str],
) -> None:
    with mrcfile.open(input_path) as input_mrc:
        input_size_x = input_mrc.header.nx
        input_size_y = input_mrc.header.ny
        input_size_z = input_mrc.header.nz
    if executables.get("clip") and not input_size_x == input_size_y == input_size_z:
        # If the initial model is not a cube, do some padding
        input_path_cube = input_path.parent / f"{input_path.stem}_cube.mrc"
        clip_proc = subprocess.run(
            [
                f"{executables['clip']}",
                "resize",
                "-ox",
                str(max(input_size_x, input_size_y, input_size_z)),
                "-oy",
                str(max(input_size_x, input_size_y, input_size_z)),
                "-oz",
                str(max(input_size_x, input_size_y, input_size_z)),
                str(input_path),
                str(input_path_cube),
            ],
            capture_output=True,
            text=True,
            env=env,
        )
        input_path = input_path_cube
        if clip_proc.returncode:
            logger.error(
                f"Clipping initial model {input_path} failed \n {clip_proc.stdout}"
            )
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
                "--force_header_angpix",
                str(downscaled_pixel_size),
                "--o",
                str(output_path),
            ],
            capture_output=True,
            text=True,
            env=env,
        )
        logger.info(
            f"Initial model rescaling finished with code {comp_proc.returncode}"
        )
        if comp_proc.returncode:
            logger.error(
                f"Resizing initial model {input_path} failed"
                f"\n {comp_proc.stdout} \n {comp_proc.stderr}"
            )
            raise RuntimeError(f"Resizing initial model {input_path} failed")
    if executables.get("relion_align_symmetry") and symmetry != "C1":
        align_proc = subprocess.run(
            [
                f"{executables['relion_align_symmetry']}",
                "--i",
                str(output_path),
                "--o",
                str(output_path),
                "--sym",
                symmetry,
                "--apply_sym",
            ],
            capture_output=True,
            text=True,
            env=env,
        )
        logger.info(
            f"Initial model symmetrisation finished with code {align_proc.returncode}"
        )
        if align_proc.returncode:
            logger.error(
                f"Applying symmetry to initial model {input_path} failed"
                f"\n {align_proc.stdout} \n {align_proc.stderr}"
            )
            raise RuntimeError(f"Symmetrising initial model {input_path} failed")
    return None


def _register_3d_batch(message: dict, _db):
    """Received 3d batch from class selection service"""
    class3d_message = message.get("class3d_message")
    assert isinstance(class3d_message, dict)
    instrument_name = (
        _db.exec(
            select(MurfeyDB.Session).where(MurfeyDB.Session.id == message["session_id"])
        )
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class3d")
    relion_params = _db.exec(
        select(MurfeyDB.SPARelionParameters).where(
            MurfeyDB.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    relion_options = dict(relion_params)
    feedback_params = _db.exec(
        select(MurfeyDB.ClassificationFeedbackParameters).where(
            MurfeyDB.ClassificationFeedbackParameters.pj_id == pj_id_params
        )
    ).one()

    visit_name = (
        _db.exec(
            select(MurfeyDB.Session).where(MurfeyDB.Session.id == message["session_id"])
        )
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
            _resize_initial_model(
                *_downscaled_box_size(
                    relion_options["particle_diameter"],
                    relion_options["angpix"],
                ),
                provided_initial_model,
                rescaled_initial_model_path,
                relion_options["symmetry"],
                machine_config.external_executables,
                machine_config.external_environment,
            )
        feedback_params.initial_model = str(rescaled_initial_model_path)
        # Reserve the Class3D (base) job up front so
        # the Class3D number cannot be reused before the job is registered.
        class3d_job = _reserve_pipeline_job_numbers(
            visit_name, 1, _allocation_floor(feedback_params)
        )
        feedback_params.next_job = class3d_job + 1
        class3d_dir = f"{class3d_message['class3d_dir']}{class3d_job:03}"
        _db.add(feedback_params)
        _db.commit()

        class3d_grp_uuid = _murfey_id(message["program_id"], _db)[0]
        class_uuids = _murfey_id(message["program_id"], _db, number=4)
        class3d_params = MurfeyDB.Class3DParameters(
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
            select(MurfeyDB.Class3DParameters).where(
                MurfeyDB.Class3DParameters.pj_id == pj_id
            )
        ).one()
        class3d_params.run = True
        class3d_params.particles_file = class3d_message["particles_file"]
        class3d_params.batch_size = class3d_message["batch_size"]
        _db.add(class3d_params)
        _db.commit()
        _db.close()
    elif not feedback_params.initial_model:
        # For the first batch, start a container and set the database to wait.
        # Reserve the InitialModel (base) + Class3D (base + 1) jobs.
        initial_model_job = _reserve_pipeline_job_numbers(
            visit_name, 2, _allocation_floor(feedback_params)
        )
        class3d_job = initial_model_job + 1
        feedback_params.next_job = initial_model_job + 2
        class3d_dir = f"{class3d_message['class3d_dir']}{(class3d_job):03}"
        class3d_grp_uuid = _murfey_id(message["program_id"], _db)[0]
        class_uuids = _murfey_id(message["program_id"], _db, number=4)
        class3d_params = MurfeyDB.Class3DParameters(
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
        zocalo_message: dict = {
            "parameters": {
                "particles_file": class3d_message["particles_file"],
                "class3d_dir": class3d_dir,
                "batch_size": class3d_message["batch_size"],
                "symmetry": relion_options["symmetry"],
                "particle_diameter": relion_options["particle_diameter"],
                "mask_diameter": relion_options["mask_diameter"] or 0,
                "do_initial_model": True,
                "class_uuids": {i + 1: m for i, m in enumerate(class_uuids)},
                "class3d_grp_uuid": class3d_grp_uuid,
                "nr_iter": default_spa_parameters.nr_iter_3d,
                "seed": int(np.random.randint(1, 100)),
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
            "recipes": [machine_config.recipes.get("em-spa-class3d", "em-spa-class3d")],
        }
        if murfey.server._transport_object:
            zocalo_message["parameters"]["feedback_queue"] = (
                murfey.server._transport_object.feedback_queue
            )
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        _db.add(feedback_params)
        _db.commit()
        _db.close()
    else:
        # Send all other messages on to a container
        class3d_params = _db.exec(
            select(MurfeyDB.Class3DParameters).where(
                MurfeyDB.Class3DParameters.pj_id == pj_id
            )
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
                "initial_model_file": feedback_params.initial_model,
                "class_uuids": _3d_class_murfey_ids(
                    class3d_params.particles_file, _app_id(pj_id, _db), _db
                ),
                "class3d_grp_uuid": class3d_params.murfey_id,
                "nr_iter": default_spa_parameters.nr_iter_3d,
                "seed": int(np.random.randint(1, 100)),
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
            "recipes": [machine_config.recipes.get("em-spa-class3d", "em-spa-class3d")],
        }
        if murfey.server._transport_object:
            zocalo_message["parameters"]["feedback_queue"] = (
                murfey.server._transport_object.feedback_queue
            )
            murfey.server._transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        feedback_params.hold_class3d = True
        _db.add(feedback_params)
        _db.commit()
        _db.close()


def run_class3d(message: dict, murfey_db: SQLModelSession) -> dict[str, bool]:
    session_processing_parameters = murfey_db.exec(
        select(MurfeyDB.SessionProcessingParameters).where(
            MurfeyDB.SessionProcessingParameters.session_id == message["session_id"]
        )
    ).all()
    if (
        not session_processing_parameters
        or session_processing_parameters[0].run_class3d
    ):
        _register_3d_batch(message, murfey_db)
    return {"success": True}
