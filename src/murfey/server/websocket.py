from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, Generic, TypeVar

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlmodel import select

import murfey.server.prometheus as prom
from murfey.server.murfey_db import get_murfey_db_session
from murfey.util.db import ClientEnvironment
from murfey.util.state import State, global_state

T = TypeVar("T")

ws = APIRouter(prefix="/ws", tags=["websocket"])
log = logging.getLogger("murfey.server.websocket")


class ConnectionManager(Generic[T]):
    def __init__(self, state: State[T]):
        self.active_connections: Dict[int, WebSocket] = {}
        self._state = state
        self._state.subscribe(self._broadcast_state_update)

    async def connect(self, websocket: WebSocket, client_id: int):
        await websocket.accept()
        self.active_connections[client_id] = websocket
        self._register_new_client(client_id)
        await websocket.send_json({"message": "state-full", "state": self._state.data})

    @staticmethod
    def _register_new_client(client_id: int):
        new_client = ClientEnvironment(client_id=client_id, connected=True)
        murfey_db = next(get_murfey_db_session())
        murfey_db.add(new_client)
        murfey_db.commit()
        murfey_db.close()

    def disconnect(self, websocket: WebSocket, client_id: int):
        self.active_connections.pop(client_id)
        murfey_db = next(get_murfey_db_session())
        client_env = murfey_db.exec(
            select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
        ).one()
        murfey_db.delete(client_env)
        murfey_db.commit()
        murfey_db.close()

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await self.active_connections[connection].send_text(message)

    async def _broadcast_state_update(
        self, attribute: str, value: T | None, message: str = "state-update"
    ):
        for connection in self.active_connections:
            await self.active_connections[connection].send_json(
                {"message": message, "attribute": attribute, "value": value}
            )

    async def set_state(self, attribute: str, value: T):
        log.info(f"State attribute {attribute!r} set to {value!r}")
        await self._state.set(attribute, value)

    async def delete_state(self, attribute: str):
        log.info(f"State attribute {attribute!r} removed")
        await self._state.delete(attribute)


manager = ConnectionManager(global_state)


@ws.websocket("/test/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: int):
    await manager.connect(websocket, client_id)
    await manager.broadcast(f"Client {client_id} joined")
    await manager.set_state(f"Client {client_id}", "joined")
    try:
        while True:
            data = await websocket.receive_text()
            try:
                json_data = json.loads(data)
                if json_data["type"] == "log":  # and isinstance(json_data, dict)
                    json_data.pop("type")
                    await forward_log(json_data, websocket)
            except Exception:
                await manager.broadcast(f"Client #{client_id} sent message {data}")
    except WebSocketDisconnect:
        log.info(f"Disconnecting Client {client_id}")
        murfey_db = next(get_murfey_db_session())
        client_env = murfey_db.exec(
            select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
        ).one()
        prom.monitoring_switch.labels(visit=client_env.visit).set(0)
        manager.disconnect(websocket, client_id)
        await manager.broadcast(f"Client #{client_id} disconnected")
        await manager.delete_state(f"Client {client_id}")


async def check_connections(active_connections):
    log.info("Checking connections")
    for connection in active_connections:
        log.info("Checking response")
        try:
            await asyncio.wait_for(connection.receive(), timeout=6)
        except asyncio.TimeoutError:
            log.info(f"Disconnecting Client {connection[0]}")
            manager.disconnect(connection[0], connection[1])


async def forward_log(logrecord: dict[str, Any], websocket: WebSocket):
    record_name = logrecord["name"]
    logrecord.pop("msecs", None)
    logrecord.pop("relativeCreated", None)
    client_timestamp = logrecord.pop("created", 0)
    if client_timestamp:
        logrecord["client_time"] = datetime.fromtimestamp(client_timestamp).isoformat()
    logrecord["client_host"] = websocket.client.host
    logging.getLogger(record_name).handle(logging.makeLogRecord(logrecord))


@ws.delete("/test/{client_id}")
async def close_ws_connection(client_id: int):
    murfey_db = next(get_murfey_db_session())
    client_env = murfey_db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
    ).one()
    client_env.connected = False
    visit_name = client_env.visit
    murfey_db.add(client_env)
    murfey_db.commit()
    murfey_db.close()
    client_id_str = str(client_id).replace("\r\n", "").replace("\n", "")
    log.info(f"Disconnecting {client_id_str}")
    manager.disconnect(manager.active_connections[client_id], client_id)
    prom.monitoring_switch.labels(visit=visit_name).set(0)
    await manager.broadcast(f"Client #{client_id} disconnected")
