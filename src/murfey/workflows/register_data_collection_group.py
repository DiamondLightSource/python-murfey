import logging
import time

import ispyb.sqlalchemy._auto_db_schema as ISPyBDB
from backports.entry_points_selectable import entry_points
from sqlmodel import select
from sqlmodel.orm.session import Session as SQLModelSession

import murfey.util.db as MurfeyDB
from murfey.server import _transport_object
from murfey.server.ispyb import ISPyBSession, get_session_id

logger = logging.getLogger("murfey.workflows.register_data_collection_group")


def run(
    message: dict, murfey_db: SQLModelSession, demo: bool = False
) -> dict[str, bool]:
    # Fail immediately if no transport wrapper is found
    if _transport_object is None:
        logger.error("Unable to find transport manager")
        return {"success": False, "requeue": False}

    ispyb_session_id = get_session_id(
        microscope=message["microscope"],
        proposal_code=message["proposal_code"],
        proposal_number=message["proposal_number"],
        visit_number=message["visit_number"],
        db=ISPyBSession(),
    )
    if dcg_murfey := murfey_db.exec(
        select(MurfeyDB.DataCollectionGroup)
        .where(MurfeyDB.DataCollectionGroup.session_id == message["session_id"])
        .where(MurfeyDB.DataCollectionGroup.tag == message.get("tag"))
    ).all():
        dcgid = dcg_murfey[0].id
    else:
        if ispyb_session_id is None:
            murfey_dcg = MurfeyDB.DataCollectionGroup(
                session_id=message["session_id"],
                tag=message.get("tag"),
            )
            dcgid = murfey_dcg.id
        else:
            record = ISPyBDB.DataCollectionGroup(
                sessionId=ispyb_session_id,
                experimentType=message["experiment_type"],
                experimentTypeId=message["experiment_type_id"],
            )

            dcgid = _transport_object.do_insert_data_collection_group(record).get(
                "return_value", None
            )
            atlas_record = ISPyBDB.Atlas(
                dataCollectionGroupId=dcgid,
                atlasImage=message.get("atlas", ""),
                pixelSize=message.get("atlas_pixel_size", 0),
                cassetteSlot=message.get("sample"),
            )
            if _transport_object:
                atlas_id = _transport_object.do_insert_atlas(atlas_record)[
                    "return_value"
                ]
            else:
                atlas_id = None
            murfey_dcg = MurfeyDB.DataCollectionGroup(
                id=dcgid,
                atlas_id=atlas_id,
                atlas=message.get("atlas", ""),
                atlas_pixel_size=message.get("atlas_pixel_size"),
                sample=message.get("sample"),
                session_id=message["session_id"],
                tag=message.get("tag"),
            )
        murfey_db.add(murfey_dcg)
        murfey_db.commit()
        murfey_db.close()
    if dcgid is None:
        time.sleep(2)
        return {"success": False, "requeue": True}
    if dcg_hooks := entry_points().select(
        group="murfey.hooks", name="data_collection_group"
    ):
        try:
            for hook in dcg_hooks:
                hook.load()(dcgid, session_id=message["session_id"])
        except Exception:
            logger.error("Call to data collection group hook failed", exc_info=True)
    return {"success": True}
