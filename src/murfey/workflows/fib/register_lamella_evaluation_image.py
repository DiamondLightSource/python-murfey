import json
import logging
from typing import Any

from sqlmodel import Session

logger = logging.getLogger(__name__)


def run(
    message: dict[str, Any],
    murfey_db: Session,
):
    logger.debug(
        "Received message containing the following:\n"
        f"{json.dumps(message, indent=2, default=str)}"
    )
    return {"success": True}
