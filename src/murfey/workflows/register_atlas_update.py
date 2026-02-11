import logging
from importlib.metadata import entry_points
from pathlib import Path

from sqlmodel import select
from sqlmodel.orm.session import Session as SQLModelSession

from murfey.server import _transport_object
from murfey.util.db import DataCollectionGroup

logger = logging.getLogger("murfey.workflows.register_atlas_update")


def run(
    message: dict,
    murfey_db: SQLModelSession,  # Defined for compatibility but unused
):
    if _transport_object is None:
        logger.error("Unable to find transport manager")
        return {"success": False, "requeue": False}

    logger.info(f"Registering updated atlas: \n{message}")

    _transport_object.do_update_atlas(
        message["atlas_id"],
        message["atlas"],
        message["atlas_pixel_size"],
        message["sample"],
    )

    # Find out how many dcgs we have with this atlas
    if (
        message.get("atlas")
        and message.get("sample")
        and "atlas" in Path(message.get("tag", "/")).parts
    ):
        dcgs_atlas = murfey_db.exec(
            select(DataCollectionGroup)
            .where(DataCollectionGroup.session_id == message["session_id"])
            .where(DataCollectionGroup.atlas == message["atlas"])
            .where(DataCollectionGroup.sample == message["sample"])
        ).all()
        if len(dcgs_atlas) > 1:
            # Skip hooks if this is an atlas and there is a processing dcg present
            logger.info(f"Skipping data collection group hooks for {message['tag']}")
            return {"success": True}

    if dcg_hooks := entry_points(group="murfey.hooks", name="data_collection_group"):
        try:
            for hook in dcg_hooks:
                hook.load()(message["dcgid"], session_id=message["session_id"])
        except Exception:
            logger.error("Call to data collection group hook failed", exc_info=True)
    return {"success": True}
