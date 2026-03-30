from logging import getLogger

from sqlmodel import Session, select

from murfey.util.config import get_machine_config
from murfey.util.db import (
    Movie,
    Session as MurfeySession,
)

logger = getLogger("murfey.workflows.spa.ctf_estimation")

try:
    from smartem_backend.api_client import SmartEMAPIClient
    from smartem_backend.model.http_request import MicrographUpdateRequest
    from smartem_backend.model.http_response import MicrographResponse
    from smartem_backend.mq_publisher import publish_ctf_estimation_completed
    from smartem_common.entity_status import MicrographStatus

    SMARTEM_ACTIVE = True
except ImportError:
    SMARTEM_ACTIVE = False


def ctf_estimated(message: dict, murfey_db: Session) -> dict[str, bool]:
    if not SMARTEM_ACTIVE:
        return {"success": True}
    movie = murfey_db.exec(
        select(Movie).where(Movie.murfey_id == message["motion_correction_id"])
    ).one()
    if movie.smartem_uuid:
        try:
            session = murfey_db.exec(
                select(MurfeySession).where(MurfeySession.id == message["session_id"])
            ).one()
            machine_config = get_machine_config(
                instrument_name=session.instrument_name
            )[session.instrument_name]
            if machine_config.smartem_api_url:
                smartem_client = SmartEMAPIClient(
                    base_url=machine_config.smartem_api_url, logger=logger
                )
                update = MicrographUpdateRequest(status=MicrographStatus.CTF_COMPLETED)
                smartem_client._request(
                    "put",
                    f"micrographs/{movie.smartem_uuid}",
                    update,
                    MicrographResponse,
                )
                publish_ctf_estimation_completed(
                    micrograph_uuid=movie.smartem_uuid,
                    ctf_max_res=message["ctf_max_resolution"],
                )
        except Exception:
            logger.warning(
                "Failed to emit CTF estimation complete event to smartem",
                exc_info=True,
            )
            return {"success": False}
    return {"success": True}
