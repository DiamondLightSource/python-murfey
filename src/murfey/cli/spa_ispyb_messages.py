import argparse
import os
import time
from datetime import datetime
from pathlib import Path

import requests
import workflows
import xmltodict
import zocalo
from ispyb.sqlalchemy._auto_db_schema import (
    AutoProcProgram,
    DataCollection,
    DataCollectionGroup,
    ProcessingJob,
)
from sqlmodel import Session as MurfeySession
from sqlmodel import create_engine, select

from murfey.client.contexts.spa import _get_xml_list_index
from murfey.server import _murfey_id, _register
from murfey.server.config import get_machine_config, get_microscope
from murfey.server.ispyb import Session, TransportManager, get_session_id
from murfey.server.murfey_db import url
from murfey.util import db


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
        "-u",
        "--url",
        dest="url",
        required=True,
        type=str,
        help="URL of Murfey server",
    )
    parser.add_argument(
        "-v",
        "--visit",
        dest="visit",
        required=True,
        type=str,
        help="Visit name",
    )
    parser.add_argument(
        "--image-directory",
        dest="image_directory",
        required=True,
        type=str,
        help="Path to directory containing image files",
    )
    parser.add_argument(
        "--suffic",
        dest="suffix",
        required=True,
        type=str,
        help="Movie suffix",
    )
    parser.add_argument(
        "--metadata-file",
        dest="metadata_file",
        required=True,
        type=str,
        help="Path to metadata file",
    )
    parser.add_argument(
        "-m",
        "--microscope",
        dest="microscope",
        type=str,
        required=True,
        help="Microscope as specified in the Murfey machine configuration",
    )
    parser.add_argument(
        "--flush-preprocess",
        dest="flush_preprocess",
        default=False,
        action="store_true",
        help="Flush Murfey preprocess stash after creating ISPyB entries",
    )
    parser.add_argument(
        "--eer-fractionation-file",
        dest="eer_fractionation_file",
        default=None,
        help="Path to EER fractionation file if relevant",
    )
    parser.add_argument(
        "--dose-per-frame",
        dest="dose_per_frame",
        default=None,
        help="Dose per frame overwrite",
    )
    parser.add_argument(
        "--injection-delay",
        dest="injection_delay",
        default=1,
        type=float,
        help="Time spacing between processing requests in seconds",
    )

    zc = zocalo.configuration.from_file()
    zc.activate()
    zc.add_command_line_options(parser)
    workflows.transport.add_command_line_options(parser, transport_argument=True)

    args = parser.parse_args()

    if args.microscope:
        os.environ["BEAMLINE"] = args.microscope

    machine_config = get_machine_config()
    _url = url(machine_config)
    engine = create_engine(_url)
    murfey_db = MurfeySession(engine)

    with open(args.metadata_file, "r") as xml:
        data = xmltodict.parse(xml.read())

    metadata = {}
    metadata["voltage"] = (
        float(data["MicroscopeImage"]["microscopeData"]["gun"]["AccelerationVoltage"])
        / 1000
    )
    metadata["image_size_x"] = data["MicroscopeImage"]["microscopeData"]["acquisition"][
        "camera"
    ]["ReadoutArea"]["a:width"]
    metadata["image_size_y"] = data["MicroscopeImage"]["microscopeData"]["acquisition"][
        "camera"
    ]["ReadoutArea"]["a:height"]
    metadata["pixel_size_on_image"] = float(
        data["MicroscopeImage"]["SpatialScale"]["pixelSize"]["x"]["numericValue"]
    )
    magnification = data["MicroscopeImage"]["microscopeData"]["optics"][
        "TemMagnification"
    ]["NominalMagnification"]
    metadata["magnification"] = magnification
    try:
        dose_index = _get_xml_list_index(
            "Dose",
            data["MicroscopeImage"]["CustomData"]["a:KeyValueOfstringanyType"],
        )
        metadata["total_exposed_dose"] = round(
            float(
                data["MicroscopeImage"]["CustomData"]["a:KeyValueOfstringanyType"][
                    dose_index
                ]["a:Value"]["#text"]
            )
            * (1e-20),
            2,
        )  # convert e / m^2 to e / A^2
    except ValueError:
        metadata["total_exposed_dose"] = 1
    try:
        num_fractions = int(
            data["MicroscopeImage"]["microscopeData"]["acquisition"]["camera"][
                "CameraSpecificInput"
            ]["a:KeyValueOfstringanyType"][2]["a:Value"]["b:NumberOffractions"]
        )
    except (KeyError, IndexError):
        num_fractions = 1
    metadata["c2aperture"] = data["MicroscopeImage"]["CustomData"][
        "a:KeyValueOfstringanyType"
    ][3]["a:Value"]["#text"]
    metadata["exposure_time"] = data["MicroscopeImage"]["microscopeData"][
        "acquisition"
    ]["camera"]["ExposureTime"]
    try:
        metadata["slit_width"] = data["MicroscopeImage"]["microscopeData"]["optics"][
            "EnergyFilter"
        ]["EnergySelectionSlitWidth"]
    except KeyError:
        metadata["slit_width"] = None
    metadata["phase_plate"] = (
        1
        if data["MicroscopeImage"]["CustomData"]["a:KeyValueOfstringanyType"][11][
            "a:Value"
        ]["#text"]
        == "true"
        else 0
    )
    binning_factor_xml = int(
        data["MicroscopeImage"]["microscopeData"]["acquisition"]["camera"]["Binning"][
            "a:x"
        ]
    )
    binning_factor = 1
    server_config = requests.get(f"{args.url}/machine/").json()
    if server_config.get("superres"):
        # If camera is capable of superres and collection is in superres
        binning_factor = 2
    elif not server_config.get("superres"):
        binning_factor_xml = 2
    if magnification:
        ps_from_mag = (
            server_config.get("calibrations", {})
            .get("magnification", {})
            .get(magnification)
        )
        if ps_from_mag:
            metadata["pixel_size_on_image"] = float(ps_from_mag) * 1e-10
        else:
            metadata["pixel_size_on_image"] /= binning_factor
    metadata["image_size_x"] = str(int(metadata["image_size_x"]) * binning_factor)
    metadata["image_size_y"] = str(int(metadata["image_size_y"]) * binning_factor)
    metadata["motion_corr_binning"] = 1 if binning_factor_xml == 2 else 2
    metadata["gain_ref"] = (
        f"data/{datetime.now().year}/{args.visit}/processing/gain.mrc"
        if args.gain_ref is None
        else args.gain_ref
    )
    metadata["gain_ref_superres"] = (
        f"data/{datetime.now().year}/{args.visit}/processing/gain_superres.mrc"
        if args.gain_ref_superres is None
        else args.gain_ref_superres
    )
    if args.dose_per_frame:
        metadata["dose_per_frame"] = args.dose_per_frame
    else:
        metadata["dose_per_frame"] = round(
            metadata["total_exposed_dose"] / num_fractions, 3
        )

    metadata["use_cryolo"] = True
    metadata["symmetry"] = "C1"
    metadata["mask_diameter"] = None
    metadata["boxsize"] = None
    metadata["downscale"] = True
    metadata["small_boxsize"] = None
    metadata["eer_fractionation"] = args.eer_fractionation_file
    metadata["source"] = args.tag
    metadata["particle_diameter"] = 0
    metadata["estimate_particle_diameter"] = True

    ispyb_session_id = (
        get_session_id(
            microscope=args.microscope,
            proposal_code=args.visit[:2],
            proposal_number=args.visit[2:].split("-")[0],
            visit_number=args.visit.split("-")[1],
            db=Session(),
        ),
    )

    record = DataCollectionGroup(
        sessionId=ispyb_session_id,
        experimentType="SPA",
        experimentTypeId=37,
    )
    dcgid = _register(record, {})
    murfey_dcg = db.DataCollectionGroup(
        id=dcgid,
        session_id=args.session_id,
        tag=args.tag,
    )
    murfey_db.add(murfey_dcg)
    murfey_db.commit()
    murfey_db.close()

    record = DataCollection(
        SESSIONID=ispyb_session_id,
        experimenttype="SPA",
        imageDirectory=args.image_directory,
        imageSuffix=args.suffix,
        voltage=metadata["voltage"],
        dataCollectionGroupId=dcgid,
        pixelSizeOnImage=str(float(metadata["pixel_size_on_image"]) * 1e9),
        imageSizeX=metadata["image_size_x"],
        imageSizeY=metadata["image_size_y"],
        slitGapHorizontal=metadata.get("slit_width"),
        magnification=metadata.get("magnification"),
        exposureTime=metadata.get("exposure_time"),
        totalExposedDose=metadata.get("total_exposed_dose"),
        c2aperture=metadata.get("c2aperture"),
        phasePlate=int(metadata.get("phase_plate", 0)),
    )
    dcid = _register(
        record,
        {},
        tag="",
    )
    murfey_dc = db.DataCollection(
        id=dcid,
        tag=args.tag,
        dcg_id=dcgid,
    )
    murfey_db.add(murfey_dc)
    murfey_db.commit()
    murfey_db.close()

    for recipe in (
        "em-spa-preprocess",
        "em-spa-extract",
        "em-spa-class2d",
        "em-spa-class3d",
    ):
        record = ProcessingJob(dataCollectionId=dcid, recipe=recipe)
        pid = _register(record, {})
        murfey_pj = db.ProcessingJob(id=pid, recipe=recipe, dc_id=dcid)
        murfey_db.add(murfey_pj)
        murfey_db.commit()
        record = AutoProcProgram(
            processingJobId=pid, processingStartTime=datetime.now()
        )
        appid = _register(record, {})
        murfey_app = db.AutoProcProgram(id=appid, pj_id=pid)
        murfey_db.add(murfey_app)
        murfey_db.commit()
        murfey_db.close()

    collected_ids = murfey_db.exec(
        select(
            db.DataCollectionGroup,
            db.DataCollection,
            db.ProcessingJob,
            db.AutoProcProgram,
        )
        .where(db.DataCollectionGroup.session_id == args.session_id)
        .where(db.DataCollectionGroup.tag == args.tag)
        .where(db.DataCollection.dcg_id == db.DataCollectionGroup.id)
        .where(db.ProcessingJob.dc_id == db.DataCollection.id)
        .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
        .where(db.ProcessingJob.recipe == "em-spa-preprocess")
    ).one()
    machine_config = get_machine_config()
    params = db.SPARelionParameters(
        pj_id=collected_ids[2].id,
        angpix=float(metadata["pixel_size_on_image"]) * 1e10,
        dose_per_frame=metadata["dose_per_frame"],
        gain_ref=(
            str(machine_config.rsync_basepath / metadata["gain_ref"])
            if metadata["gain_ref"]
            else metadata["gain_ref"]
        ),
        voltage=metadata["voltage"],
        motion_corr_binning=metadata["motion_corr_binning"],
        eer_grouping=metadata["eer_fractionation"],
        symmetry=metadata["symmetry"],
        particle_diameter=metadata["particle_diameter"],
        downscale=metadata["downscale"],
        boxsize=metadata["boxsize"],
        small_boxsize=metadata["small_boxsize"],
        mask_diameter=metadata["mask_diameter"],
    )
    feedback_params = db.SPAFeedbackParameters(
        pj_id=collected_ids[2].id,
        estimate_particle_diameter=not bool(metadata["particle_diameter"]),
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
    murfey_db.close()

    if args.flush_preprocess:
        _transport_object = TransportManager(args.transport)
        _transport_object.feedback_queue = machine_config.feedback_queue
        stashed_files = murfey_db.exec(
            select(db.PreprocessStash)
            .where(db.PreprocessStash.session_id == args.session_id)
            .where(db.PreprocessStash.tag == args.tag)
        ).all()
        murfey_ids = _murfey_id(
            collected_ids[3].id,
            murfey_db,
            number=2 * len(stashed_files),
            close=False,
        )
        if feedback_params.picker_murfey_id is None:
            feedback_params.picker_murfey_id = murfey_ids[1]
            murfey_db.add(feedback_params)

        for i, f in enumerate(stashed_files):
            mrcp = Path(f.mrc_out)
            if not mrcp.parent.exists():
                mrcp.parent.mkdir(parents=True)
            movie = db.Movie(
                murfey_id=murfey_ids[2 * i],
                path=f.file_path,
                image_number=f.image_number,
                tag=f.tag,
            )
            murfey_db.add(movie)
            zocalo_message = {
                "recipes": ["em-spa-preprocess"],
                "parameters": {
                    "feedback_queue": machine_config.feedback_queue,
                    "dcid": collected_ids[1].id,
                    "kv": metadata["voltage"],
                    "autoproc_program_id": collected_ids[3].id,
                    "movie": f.file_path,
                    "mrc_out": f.mrc_out,
                    "pixel_size": float(metadata["pixel_size_on_image"]) * 1e10,
                    "image_number": f.image_number,
                    "microscope": get_microscope(),
                    "mc_uuid": murfey_ids[2 * i],
                    "ft_bin": metadata["motion_corr_binning"],
                    "fm_dose": metadata["dose_per_frame"],
                    "gain_ref": (
                        str(machine_config.rsync_basepath / metadata["gain_ref"])
                        if metadata["gain_ref"]
                        else metadata["gain_ref"]
                    ),
                    "downscale": metadata["downscale"],
                    "picker_uuid": murfey_ids[2 * i + 1],
                    "session_id": args.session_id,
                    "particle_diameter": metadata["particle_diameter"] or 0,
                    "fm_int_file": f.eer_fractionation_file,
                },
            }
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
            murfey_db.delete(f)
            time.sleep(args.injection_delay)
        murfey_db.commit()
        murfey_db.close()
