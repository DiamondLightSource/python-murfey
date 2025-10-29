import logging
from datetime import datetime

import ispyb.sqlalchemy._auto_db_schema as ISPyBDB
from sqlmodel import select
from sqlmodel.orm.session import Session as SQLModelSession

import murfey.server.prometheus as prom
import murfey.util.db as MurfeyDB
from murfey.server import _transport_object
from murfey.server.ispyb import ISPyBSession
from murfey.util import sanitise

logger = logging.getLogger("murfey.workflows.register_processing_job")


def run(message: dict, murfey_db: SQLModelSession, demo: bool = False):
    # Faill immediately if not transport manager is set
    if _transport_object is None:
        logger.error("Unable to find transport manager")
        return {"success": False, "requeue": False}

    logger.info(f"Registering the following processing job: \n{message}")

    murfey_session_id = message["session_id"]
    dc = murfey_db.exec(
        select(MurfeyDB.DataCollection, MurfeyDB.DataCollectionGroup)
        .where(MurfeyDB.DataCollection.dcg_id == MurfeyDB.DataCollectionGroup.id)
        .where(MurfeyDB.DataCollectionGroup.session_id == murfey_session_id)
        .where(MurfeyDB.DataCollectionGroup.tag == message["source"])
        .where(MurfeyDB.DataCollection.tag == message["tag"])
    ).all()

    if dc:
        _dcid = dc[0][0].id
    else:
        logger.warning(f"No data collection ID found for {sanitise(message['tag'])}")
        return {"success": False, "requeue": True}
    if pj_murfey := murfey_db.exec(
        select(MurfeyDB.ProcessingJob)
        .where(MurfeyDB.ProcessingJob.recipe == message["recipe"])
        .where(MurfeyDB.ProcessingJob.dc_id == _dcid)
    ).all():
        pid = pj_murfey[0].id
    else:
        if ISPyBSession() is None:
            murfey_pj = MurfeyDB.ProcessingJob(recipe=message["recipe"], dc_id=_dcid)
        else:
            record = ISPyBDB.ProcessingJob(
                dataCollectionId=_dcid, recipe=message["recipe"]
            )
            run_parameters = message.get("parameters", {})
            assert isinstance(run_parameters, dict)
            if message.get("job_parameters"):
                job_parameters = [
                    ISPyBDB.ProcessingJobParameter(parameterKey=k, parameterValue=v)
                    for k, v in message["job_parameters"].items()
                ]
                pid = _transport_object.do_create_ispyb_job(
                    record, params=job_parameters
                ).get("return_value", None)
            else:
                pid = _transport_object.do_create_ispyb_job(record).get(
                    "return_value", None
                )
            murfey_pj = MurfeyDB.ProcessingJob(
                id=pid, recipe=message["recipe"], dc_id=_dcid
            )
        murfey_db.add(murfey_pj)
        murfey_db.commit()
        pid = murfey_pj.id
        murfey_db.close()

    if pid is None:
        return {"success": False, "requeue": True}

    # Update Prometheus counter for preprocessed movies
    prom.preprocessed_movies.labels(processing_job=pid)

    # Register AutoProcProgram database entry if it doesn't already exist
    if not murfey_db.exec(
        select(MurfeyDB.AutoProcProgram).where(MurfeyDB.AutoProcProgram.pj_id == pid)
    ).all():
        if ISPyBSession() is None:
            murfey_app = MurfeyDB.AutoProcProgram(pj_id=pid)
        else:
            record = ISPyBDB.AutoProcProgram(
                processingJobId=pid, processingStartTime=datetime.now()
            )
            appid = _transport_object.do_update_processing_status(record).get(
                "return_value", None
            )
            if appid is None:
                return {"success": False, "requeue": True}
            murfey_app = MurfeyDB.AutoProcProgram(id=appid, pj_id=pid)
        murfey_db.add(murfey_app)
        murfey_db.commit()
        murfey_db.close()
    return {"success": True}
