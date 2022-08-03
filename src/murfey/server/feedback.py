from __future__ import annotations

import logging
from functools import singledispatch

from ispyb.sqlalchemy._auto_db_schema import (
    AutoProcProgram,
    Base,
    DataCollection,
    ProcessingJob,
)
from sqlalchemy.exc import SQLAlchemyError

from murfey.server import _transport_object
from murfey.server.ispyb import DB
from murfey.util.state import global_state

log = logging.getLogger("murfey.server.feedback")


def feedback_callback(header: dict, message: dict):
    record = None
    if message["register"] == "motion_corrected":
        if global_state.get("motion_corrected") and isinstance(
            global_state["motion_corrected"], list
        ):
            global_state["motion_corrected"].append(message["movie"])
        else:
            global_state["motion_corrected"] = [message["movie"]]
    elif message["register"] == "data_collection":
        record = DataCollection(imageDirectory=message["image_directory"])
    elif message["register"] == "processing_job":
        record = ProcessingJob(
            dataCollectionId=message["data_collection_id"], recipe=message["recipe"]
        )
    elif message["register"] == "auto_proc_program":
        record = AutoProcProgram(processingJobId=message["processing_job_id"])
    if record:
        _register(record, header)


@singledispatch
def _register(record, header):
    raise NotImplementedError(f"Not method to register {record} or type {type(record)}")


@_register.register
def _(record: Base, header) -> int | None:
    if not _transport_object:
        log.error(
            f"No transport object found when processing record {record}. Message header: {header}"
        )
        return None
    try:
        DB.add(record)
        DB.commit()
        _transport_object.transport.ack(header)
        return getattr(record, record.__table__.primary_key.columns[0].name)
    except SQLAlchemyError as e:
        log.error(f"Murfey failed to insert ISPyB record {record}", e, exc_info=True)
        _transport_object.transport.nack(header)
        return None
    except AttributeError as e:
        log.error(
            f"Murfey could not find primary key when inserting record {record}",
            e,
            exc_info=True,
        )
        _transport_object.transport.nack(header)
        return None


def feedback_listen():
    if _transport_object:
        _transport_object.transport.subscribe("murfey", feedback_callback)
