import re
from typing import Dict, Tuple

from sqlmodel import Session, select

from murfey.util.db import NotificationParameter


def notification_setup(
    message: dict, murfey_db: Session, num_instances_between_triggers: int = 500
) -> bool:
    parameters: Dict[str, Tuple[float, float]] = {}
    for k in message.keys():
        parameter_name = ""
        if k.endswith(("Min", "Max")):
            parameter_name = k[:-3]
        else:
            continue
        snake_parameter_name = re.sub(r"(?<!^)(?=[A-Z])", "_", parameter_name).lower()
        if snake_parameter_name in parameters.keys():
            continue
        parameters[snake_parameter_name] = (
            message.get(f"{parameter_name}Min", 0),
            message.get(f"{parameter_name}Max", 10000),
        )
    dcgid = message["dcg"]
    existing_notification_parameters = murfey_db.exec(
        select(NotificationParameter).where(NotificationParameter.dcg_id == dcgid)
    ).all()
    new_notification_parameters: list[NotificationParameter] = []
    for k, v in parameters.items():
        for enp in existing_notification_parameters:
            if enp.name == k:
                enp.min_value = v[0]
                enp.max_value = v[1]
                break
        else:
            if v[0] is not None and v[1] is not None:
                new_notification_parameters.append(
                    NotificationParameter(
                        dcg_id=dcgid,
                        name=k,
                        min_value=v[0],
                        max_value=v[1],
                        num_instances_since_triggered=num_instances_between_triggers,
                    )
                )
    murfey_db.add_all(existing_notification_parameters + new_notification_parameters)
    murfey_db.commit()
    murfey_db.close()
    return True
