from __future__ import annotations

from murfey.server import _transport_object
from murfey.util.state import global_state


def feedback_callback(header: dict, message: dict):
    if message["register"] == "motion corrected":
        if isinstance(global_state["motion_corrected"], list):
            global_state["motion_corrected"].append(message["movie"])


def feedback_listen():
    _transport_object.transport.subscribe("murfey", feedback_callback)
