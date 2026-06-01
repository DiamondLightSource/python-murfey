import json
import logging
from typing import Any

from sqlmodel import Session as SQLModelSession

from murfey.util.models import LamellaSiteInfo

logger = logging.getLogger("murfey.workflows.fib.register_milling_progress")


def run(message: dict[str, Any], murfey_db: SQLModelSession):
    try:
        session_id = int(message["session_id"])
        site_info = LamellaSiteInfo(**message["site_info"])
        logger.debug(
            "Received the following FIB metadata for registration:\n"
            f"{json.dumps(site_info.model_dump(exclude_none=True), indent=2, default=str)}"
        )
    except Exception:
        logger.error("Error parsing contents of message", exc_info=True)
        return {"success": False, "requeue": False}
