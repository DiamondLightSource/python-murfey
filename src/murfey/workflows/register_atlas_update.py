import logging
from importlib.metadata import entry_points
from pathlib import Path

from sqlmodel import select
from sqlmodel.orm.session import Session as SQLModelSession

import murfey.server
from murfey.util.db import Atlas, DataCollectionGroup
from murfey.util.models import AtlasParameters

logger = logging.getLogger("murfey.workflows.register_atlas_update")


def run(
    message: dict,
    murfey_db: SQLModelSession,  # Defined for compatibility but unused
):
    if murfey.server._transport_object is None:
        logger.error("Unable to find transport manager")
        return {"success": False, "requeue": False}

    logger.info(f"Registering updated atlas: \n{message}")
    murfey_session_id = int(message["session_id"])

    if atlas_murfey := (
        murfey_db.exec(
            select(Atlas)
            .where(Atlas.dcg_id == message["dcgid"])
            .where(Atlas.atlas_type == message["atlas_type"])
        ).one_or_none()
    ):
        atlas_murfey.image = message["atlas"] or atlas_murfey.image
        atlas_murfey.cassette_slot = message["sample"] or atlas_murfey.cassette_slot
        atlas_murfey.pixel_size = (
            message["atlas_pixel_size"] or atlas_murfey.atlas_pixel_size
        )

        murfey.server._transport_object.do_update_atlas(
            dcg_id=message["dcgid"],
            atlas_id=message["atlas_id"],
            atlas_image=message["atlas"],
            pixel_size=message["atlas_pixel_size"],
            slot=message["sample"],
            # Extract optional parameters
            collection_mode=message.get("collection_mode"),
            colour_flags=message.get("colour_flags", {}),
        )
    else:
        atlas_id = murfey.server._transport_object.do_insert_atlas(
            message["dcgid"],
            AtlasParameters(
                atlas_type=message["atlas_type"],
                image=message["atlas"],
                pixel_size=-message["atlas_pixel_size"],
                cassette_slot=message["sample"],
            ),
        ).get("return_value")
        atlas_murfey = Atlas(
            id=atlas_id,
            dcg_id=message["dcgid"],
            atlas_type=message["atlas_type"],
            image=message["atlas"],
            cassette_slot=message["sample"],
            pixel_size=message["atlas_pixel_size"],
        )
    murfey_db.add(atlas_murfey)
    murfey_db.commit()

    # Find out how many dcgs we have with this atlas
    if (
        message.get("atlas")
        and message.get("sample") is not None
        and "atlas" in Path(message.get("tag", "/")).parts
    ):
        dcgs_atlas = murfey_db.exec(
            select(DataCollectionGroup)
            .where(DataCollectionGroup.session_id == murfey_session_id)
            .where(DataCollectionGroup.atlas == message["atlas"])
            .where(DataCollectionGroup.sample == message["sample"])
        ).all()
        if len(dcgs_atlas) > 1:
            # Skip hooks if this is an atlas and there is a processing dcg present
            logger.info(f"Skipping data collection group hooks for {message['tag']}")
            return {"success": True}

    # Only send Scaup sample registration if we didn't have a sample before
    if not message.get("sample_registered") and (
        dcg_hooks := entry_points(group="murfey.hooks", name="data_collection_group")
    ):
        try:
            for hook in dcg_hooks:
                hook.load()(message["dcgid"], session_id=murfey_session_id)
        except Exception:
            logger.error("Call to data collection group hook failed", exc_info=True)
    return {"success": True}
