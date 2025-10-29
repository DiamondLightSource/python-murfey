import logging
from importlib.metadata import entry_points

from sqlmodel.orm.session import Session as SQLModelSession

from murfey.server import _transport_object

logger = logging.getLogger("murfey.workflows.register_atlas_update")


def run(
    message: dict,
    murfey_db: SQLModelSession,  # Defined for compatibility but unused
    demo: bool = False,
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
    if dcg_hooks := entry_points(group="murfey.hooks", name="data_collection_group"):
        try:
            for hook in dcg_hooks:
                hook.load()(message["dcgid"], session_id=message["session_id"])
        except Exception:
            logger.error("Call to data collection group hook failed", exc_info=True)
    return {"success": True}
