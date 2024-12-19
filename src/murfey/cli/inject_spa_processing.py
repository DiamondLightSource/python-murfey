import argparse
import os
import time
from pathlib import Path

import sqlalchemy
import workflows
import zocalo
from sqlmodel import Session, create_engine, select

from murfey.server.ispyb import TransportManager
from murfey.server.murfey_db import url
from murfey.util.config import get_machine_config, get_microscope, get_security_config
from murfey.util.db import (
    AutoProcProgram,
    ClientEnvironment,
    DataCollection,
    DataCollectionGroup,
    Movie,
    ProcessingJob,
    SPAFeedbackParameters,
    SPARelionParameters,
)
from murfey.util.processing_params import default_spa_parameters


def run():
    parser = argparse.ArgumentParser(
        description="Inject movies for SPA processing from Murfey database movie store"
    )

    parser.add_argument(
        "--tag",
        dest="tag",
        type=str,
        required=True,
        help="Tag from Murfey database Movie table",
    )
    parser.add_argument(
        "-s",
        "--session-id",
        dest="session_id",
        required=True,
        type=int,
        help="Murfey session ID",
    )
    parser.add_argument(
        "--max-image-number",
        dest="max_image_number",
        default=-1,
        type=int,
        help="Upper end of image number range",
    )
    parser.add_argument(
        "--min-image-number",
        dest="min_image_number",
        default=1,
        type=int,
        help="Lower end of image number range",
    )
    parser.add_argument(
        "--injection-delay",
        dest="injection_delay",
        default=0.5,
        type=float,
        help="Time spacing between processing requests in seconds",
    )
    parser.add_argument(
        "--ignore-processing-status",
        dest="check_preproc",
        default=True,
        action="store_false",
        help="Inject movies even when they are already tagged as preprocessed",
    )
    parser.add_argument(
        "-m",
        "--microscope",
        dest="microscope",
        type=str,
        default="",
        help="Microscope as specified in the Murfey machine configuration",
    )
    parser.add_argument(
        "--eer-fractionation-file",
        dest="eer_fractionation_file",
        default=None,
        help="Path to EER fractionation file if relevant",
    )

    zc = zocalo.configuration.from_file()
    zc.activate()
    zc.add_command_line_options(parser)
    workflows.transport.add_command_line_options(parser, transport_argument=True)

    args = parser.parse_args()
    if args.microscope:
        os.environ["BEAMLINE"] = args.microscope

    machine_config = get_machine_config()
    security_config = get_security_config()
    _url = url(machine_config)
    engine = create_engine(_url)
    murfey_db = Session(engine)

    _transport_object = TransportManager(args.transport)
    _transport_object.feedback_queue = security_config.feedback_queue

    query = (
        select(Movie)
        .where(Movie.tag == args.tag)
        .where(Movie.image_number >= args.min_image_number)
    )
    if args.max_image_number > 0:
        query = query.where(Movie.image_number <= args.max_image_number)
    if args.check_preproc:
        query = query.where(Movie.preprocessed.is_(False))
    movies = murfey_db.exec(query).all()

    visit_name = (
        murfey_db.exec(
            select(ClientEnvironment).where(
                ClientEnvironment.session_id == args.session_id
            )
        )
        .one()
        .visit
    )

    try:
        collected_ids = murfey_db.exec(
            select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
            .where(DataCollectionGroup.session_id == args.session_id)
            .where(DataCollectionGroup.tag == args.tag)
            .where(DataCollection.dcg_id == DataCollectionGroup.id)
            .where(ProcessingJob.dc_id == DataCollection.id)
            .where(AutoProcProgram.pj_id == ProcessingJob.id)
            .where(ProcessingJob.recipe == "em-spa-preprocess")
        ).one()
        params = murfey_db.exec(
            select(SPARelionParameters, SPAFeedbackParameters)
            .where(SPARelionParameters.pj_id == collected_ids[2].id)
            .where(SPAFeedbackParameters.pj_id == SPARelionParameters.pj_id)
        ).one()
        proc_params: dict | None = dict(params[0])
        feedback_params = params[1]
        if feedback_params.picker_murfey_id is None:
            raise ValueError("No ISPyB picker ID was found")
    except sqlalchemy.exc.NoResultFound:
        proc_params = None

    for m in movies:
        parts = Path(m.path).parts
        visit_idx = parts.index(visit_name)
        core = Path("/") / Path(*parts[: visit_idx + 1])
        ppath = Path("/") / Path(*parts)
        sub_dataset = ppath.relative_to(core).parts[0]
        extra_path = machine_config.processed_extra_directory
        for i, p in enumerate(ppath.parts):
            if p.startswith("raw"):
                movies_path_index = i
                break
        else:
            raise ValueError(f"{m.path} does not contain a raw directory")
        mrc_out = (
            core
            / machine_config.processed_directory_name
            / sub_dataset
            / extra_path
            / "MotionCorr"
            / "job002"
            / "Movies"
            / "/".join(ppath.parts[movies_path_index + 1 : -1])
            / str(ppath.stem + "_motion_corrected.mrc")
        )
        if proc_params:

            detached_ids = [c.id for c in collected_ids]

            if not mrc_out.parent.exists():
                mrc_out.parent.mkdir(parents=True, exist_ok=True)
            if not Path(m.path).is_file():
                continue
            zocalo_message = {
                "recipes": ["em-spa-preprocess"],
                "parameters": {
                    "feedback_queue": _transport_object.feedback_queue,
                    "node_creator_queue": machine_config.node_creator_queue,
                    "dcid": detached_ids[1],
                    "kv": proc_params["voltage"],
                    "autoproc_program_id": detached_ids[3],
                    "movie": m.path,
                    "mrc_out": str(mrc_out),
                    "pixel_size": proc_params["angpix"],
                    "image_number": m.image_number,
                    "microscope": get_microscope(),
                    "mc_uuid": m.murfey_id,
                    "ft_bin": proc_params["motion_corr_binning"],
                    "fm_dose": proc_params["dose_per_frame"],
                    "gain_ref": proc_params["gain_ref"],
                    "picker_uuid": feedback_params.picker_murfey_id,
                    "session_id": args.session_id,
                    "particle_diameter": proc_params["particle_diameter"] or 0,
                    "fm_int_file": args.eer_fractionation_file,
                    "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                },
            }
            _transport_object.send("processing_recipe", zocalo_message)
            print(f"Requested processing for {m.path}")
            time.sleep(args.injection_delay)
