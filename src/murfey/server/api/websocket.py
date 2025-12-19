from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, TypeVar

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlmodel import Session, select

from murfey.server.murfey_db import get_murfey_db_session
from murfey.util import sanitise
from murfey.util.db import ClientEnvironment

T = TypeVar("T")

ws = APIRouter(prefix="/ws", tags=["Websocket"])
log = logging.getLogger("murfey.server.api.websocket")


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[int | str, WebSocket] = {}

    async def connect(
        self,
        websocket: WebSocket,
        client_id: int | str,
        register_client: bool = True,
    ):
        await websocket.accept()
        self.active_connections[client_id] = websocket
        if register_client:
            if not isinstance(client_id, int):
                raise ValueError(
                    "To register a client the client ID must be an integer"
                )
            self._register_new_client(client_id)

    @staticmethod
    def _register_new_client(client_id: int):
        log.debug(f"Registering new client with ID {client_id}")
        new_client = ClientEnvironment(client_id=client_id, connected=True)
        murfey_db: Session = next(get_murfey_db_session())
        murfey_db.add(new_client)
        murfey_db.commit()
        murfey_db.close()

    def disconnect(self, client_id: int | str, unregister_client: bool = True):
        self.active_connections.pop(client_id)
        if unregister_client:
            murfey_db: Session = next(get_murfey_db_session())
            client_env = murfey_db.exec(
                select(ClientEnvironment).where(
                    ClientEnvironment.client_id == client_id
                )
            ).one()
            murfey_db.delete(client_env)
            murfey_db.commit()
            murfey_db.close()

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await self.active_connections[connection].send_text(message)


manager = ConnectionManager()


@ws.websocket("/connect/{client_id}")
async def websocket_connection_endpoint(
    websocket: WebSocket,
    client_id: int | str,
):
    await manager.connect(websocket, client_id, register_client=False)
    await manager.broadcast(f"Client {client_id} joined")
    try:
        while True:
            data = await websocket.receive_text()
            try:
                json_data: dict = json.loads(data)
                if json_data.get("type") == "log":  # and isinstance(json_data, dict)
                    json_data.pop("type")
                    await forward_log(json_data, websocket)
                elif json_data.get("message") == "refresh":
                    await manager.broadcast(json.dumps(json_data))

            except Exception:
                await manager.broadcast(f"Client #{client_id} sent message {data}")
    except WebSocketDisconnect:
        log.info(f"Disconnecting Client {sanitise(str(client_id))}")
        manager.disconnect(client_id, unregister_client=False)
        await manager.broadcast(f"Client #{client_id} disconnected")


async def forward_log(logrecord: dict[str, Any], websocket: WebSocket):
    record_name = logrecord["name"]
    logrecord.pop("msecs", None)
    logrecord.pop("relativeCreated", None)
    client_timestamp = logrecord.pop("created", 0)
    if client_timestamp:
        logrecord["client_time"] = datetime.fromtimestamp(client_timestamp).isoformat()
    logrecord["client_host"] = websocket.client.host
    logging.getLogger(record_name).handle(logging.makeLogRecord(logrecord))


@ws.delete("/connect/{client_id}")
async def close_websocket_connection(client_id: int | str):
    client_id_str = str(client_id).replace("\r\n", "").replace("\n", "")
    log.info(f"Disconnecting {client_id_str}")
    manager.disconnect(client_id)
    await manager.broadcast(f"Client #{client_id} disconnected")
