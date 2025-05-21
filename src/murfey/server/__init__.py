from __future__ import annotations

from typing import TYPE_CHECKING

# Classes are only imported for type checking purposes
if TYPE_CHECKING:
    from uvicorn import Server

    from murfey.server.ispyb import TransportManager

_running_server: Server | None = None
_transport_object: TransportManager | None = None
