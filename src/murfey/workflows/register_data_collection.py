import logging

import ispyb.sqlalchemy._auto_db_schema as ISPyBDB
from sqlmodel import select
from sqlmodel.orm.session import Session as SQLModelSession

import murfey.util.db as MurfeyDB
from murfey.server import _transport_object
from murfey.server.ispyb import ISPyBSession, get_session_id
from murfey.util import sanitise

logger = logging.getLogger("murfey.workflows.register_data_collection")


def run(
    message: dict, murfey_db: SQLModelSession, demo: bool = False
) -> dict[str, bool]:
    # Fail immediately if transport manager was not provided
    if _transport_object is None:
        logger.error("Unable to find transport manager")
        return {"success": False, "requeue": False}

    logger.info(f"Registering the following data collection: \n{message}")

    murfey_session_id = message["session_id"]
    ispyb_session_id = get_session_id(
        microscope=message["microscope"],
        proposal_code=message["proposal_code"],
        proposal_number=message["proposal_number"],
        visit_number=message["visit_number"],
        db=ISPyBSession(),
    )
    dcg = murfey_db.exec(
        select(MurfeyDB.DataCollectionGroup)
        .where(MurfeyDB.DataCollectionGroup.session_id == murfey_session_id)
        .where(MurfeyDB.DataCollectionGroup.tag == message["source"])
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
        return {"success": False, "requeue": True}

    if dc_murfey := murfey_db.exec(
        select(MurfeyDB.DataCollection)
        .where(MurfeyDB.DataCollection.tag == message.get("tag"))
        .where(MurfeyDB.DataCollection.dcg_id == dcgid)
    ).all():
        dcid = dc_murfey[0].id
    else:
        if ispyb_session_id is None:
            murfey_dc = MurfeyDB.DataCollection(
                tag=message.get("tag"),
                dcg_id=dcgid,
            )
        else:
            record = ISPyBDB.DataCollection(
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
            dcid = _transport_object.do_insert_data_collection(
                record,
                tag=(
                    message.get("tag")
                    if message["experiment_type"] == "tomography"
                    else ""
                ),
            ).get("return_value", None)
            murfey_dc = MurfeyDB.DataCollection(
                id=dcid,
                tag=message.get("tag"),
                dcg_id=dcgid,
            )
        murfey_db.add(murfey_dc)
        murfey_db.commit()
        dcid = murfey_dc.id
        murfey_db.close()

    if dcid is None:
        logger.error(
            "Failed to register the following data collection: \n"
            f"{message} \n"
            "Requeueing message"
        )
        return {"success": False, "requeue": True}
    return {"success": True}
