import asyncio
from logging import getLogger

from sqlmodel import Session, select

from murfey.util.config import get_machine_config, get_rabbitmq_url
from murfey.util.db import (
    Movie,
    Session as MurfeySession,
)

logger = getLogger("murfey.workflows.spa.ctf_estimation")

try:
    from smartem_backend.api_client import SmartEMAPIClient
    from smartem_backend.model.http_request import (
        CtfEstimationRegisteredRequest,
        MicrographUpdateRequest,
    )
    from smartem_backend.model.http_response import (
        MicrographResponse,
        ProcessingFeedbackPublishResponse,
    )
    from smartem_backend.model.mq_event import (
        CtfCompleteBody,
        MessageQueueEventType,
    )
    from smartem_backend.rmq.publisher import AioPikaPublisher
    from smartem_common.entity_status import MicrographStatus

    from murfey.util.config import get_smartem_keycloak_client

    if keycloak_client := get_smartem_keycloak_client():
        SMARTEM_ACTIVE = True
    else:
        SMARTEM_ACTIVE = False
except ImportError:
    keycloak_client = None
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
                    base_url=machine_config.smartem_api_url,
                    logger=logger,
                    keycloak_client=keycloak_client,
                )
                update = MicrographUpdateRequest(status=MicrographStatus.CTF_COMPLETED)
                smartem_client._request(
                    "put",
                    f"micrographs/{movie.smartem_uuid}",
                    update,
                    MicrographResponse,
                )
                registered_request = CtfEstimationRegisteredRequest(
                    quality=True, metric_name="ctfmaxresolution"
                )  # True is a placeholder until we figure out the best way to calculate this
                smartem_client._request(
                    "post",
                    f"micrographs/{movie.smartem_uuid}/ctf_estimation/registered",
                    registered_request,
                    ProcessingFeedbackPublishResponse,
                )

                async def _publish_ctf_completed(
                    micrograph_uuid: str, ctf_max_resolution: float
                ) -> None:
                    publisher = AioPikaPublisher(
                        url=get_rabbitmq_url(),
                        exchange_name="smartem",
                        routing_key="smartem",
                        exchange_type="fanout",
                    )
                    await publisher.connect()
                    try:
                        await publisher.publish_event(
                            MessageQueueEventType.CTF_COMPLETE,
                            CtfCompleteBody(
                                event_type=MessageQueueEventType.CTF_COMPLETE,
                                micrograph_uuid=micrograph_uuid,
                                ctf_max_resolution_estimate=ctf_max_resolution,
                            ),
                        )
                    except Exception:
                        logger.warning(
                            f"smartem failed to ctf estimation completion {micrograph_uuid}",
                            exc_info=True,
                        )
                    finally:
                        await publisher.close()

                asyncio.run(
                    _publish_ctf_completed(
                        movie.smartem_uuid, message.get("ctf_max_resolution", 1000)
                    )
                )

        except Exception:
            logger.warning(
                "Failed to emit CTF estimation complete event to smartem",
                exc_info=True,
            )
            return {"success": False}
    return {"success": True}
