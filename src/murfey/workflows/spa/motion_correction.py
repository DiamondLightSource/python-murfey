import asyncio
from logging import getLogger

from sqlmodel import Session, select

from murfey.util.config import get_machine_config, get_rabbitmq_url
from murfey.util.db import (
    Movie,
    Session as MurfeySession,
)

logger = getLogger("murfey.workflows.spa.motion_correction")

try:
    from smartem_backend.api_client import SmartEMAPIClient
    from smartem_backend.model.http_request import (
        MicrographUpdateRequest,
        MotionCorrectionRegisteredRequest,
    )
    from smartem_backend.model.http_response import (
        MicrographResponse,
        ProcessingFeedbackPublishResponse,
    )
    from smartem_backend.model.mq_event import (
        MessageQueueEventType,
        MotionCorrectionCompleteBody,
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


def motion_corrected(message: dict, murfey_db: Session) -> dict[str, bool]:
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
                update = MicrographUpdateRequest(
                    status=MicrographStatus.MOTION_CORRECTION_COMPLETED
                )
                smartem_client._request(
                    "put",
                    f"micrographs/{movie.smartem_uuid}",
                    update,
                    MicrographResponse,
                )
                registered_request = MotionCorrectionRegisteredRequest(
                    quality=True, metric_name="motioncorrection"
                )  # True is a placeholder until we figure out the best way to calculate this
                smartem_client._request(
                    "post",
                    f"micrographs/{movie.smartem_uuid}/motion_correction/registered",
                    registered_request,
                    ProcessingFeedbackPublishResponse,
                )

                async def _publish_motion_correction_completed(
                    micrograph_uuid: str, total_motion: float, average_motion: float
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
                            MessageQueueEventType.MOTION_CORRECTION_COMPLETE,
                            MotionCorrectionCompleteBody(
                                event_type=MessageQueueEventType.MOTION_CORRECTION_COMPLETE,
                                micrograph_uuid=micrograph_uuid,
                                total_motion=total_motion,
                                average_motion=average_motion,
                            ),
                        )
                    except Exception:
                        logger.warning(
                            f"smartem failed to motion correction completion {micrograph_uuid}",
                            exc_info=True,
                        )
                    finally:
                        await publisher.close()

                asyncio.run(
                    _publish_motion_correction_completed(
                        movie.smartem_uuid,
                        message.get("total_motion", 1000),
                        message.get("average_motion", 1000),
                    )
                )

        except Exception:
            logger.warning(
                "Failed to emit motion correction complete event to smartem",
                exc_info=True,
            )
            return {"success": False}
    return {"success": True}
