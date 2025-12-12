import json
import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Request

logger = logging.getLogger("murfey.server.api.logging")

router = APIRouter(
    prefix="/logging",
    tags=["Logging"],
)


@router.post("/logs")
async def forward_logs(request: Request):
    """
    Receives a list of stringified JSON log records from the instrument server,
    unpacks them, and forwards them through the handlers set up on the backend.
    """

    data: list[str] = await request.json()
    for line in data:
        log_data: dict[str, Any] = json.loads(line)
        logger_name = log_data["name"]
        log_data.pop("msecs", None)
        log_data.pop("relativeCreated", None)
        client_timestamp = log_data.pop("created", 0)
        if client_timestamp:
            log_data["client_time"] = datetime.fromtimestamp(
                client_timestamp
            ).isoformat()
        log_data["client_host"] = request.client.host if request.client else None
        logging.getLogger(logger_name).handle(logging.makeLogRecord(log_data))
