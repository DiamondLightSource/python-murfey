import logging
import time

import ispyb.sqlalchemy._auto_db_schema as ISPyBDB
from sqlmodel.orm.session import Session as SQLModelSession

from murfey.server import _transport_object

logger = logging.getLogger("murfey.workflows.register_data_collection_group")


def run(
    message: dict, murfey_db: SQLModelSession, demo: bool = False
) -> dict[str, bool]:
    # Fail immediately if no transport wrapper is found
    if _transport_object is None:
        logger.error("Unable to find transport manager")
        return {"success": False, "requeue": False}

    logger.info(f"Updating the experiment type for data collection group: \n{message}")

    record = ISPyBDB.DataCollectionGroup(
        dataCollectionGroupId=message["dcgid"],
        experimentTypeId=message["experiment_type_id"],
    )
    dcgid = _transport_object.do_update_data_collection_group(record).get(
        "return_value", None
    )

    if dcgid is None:
        time.sleep(2)
        logger.error(
            "Failed to update the following data collection group: \n"
            f"{message} \n"
            "Requeuing message"
        )
        return {"success": False, "requeue": True}

    return {"success": True}
