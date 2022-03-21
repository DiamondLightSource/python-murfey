from __future__ import annotations

import asyncio
import json
import logging
from typing import Dict, Generic, TypeVar

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from murfey.util.state import State, global_state

T = TypeVar("T")

ws = APIRouter(prefix="/ws", tags=["websocket"])
log = logging.getLogger("murfey.server.websocket")


class ConnectionManager(Generic[T]):
    def __init__(self, state: State[T]):
        self.active_connections: Dict[str, WebSocket] = {}
        self._state = state
        self._state.subscribe(self._broadcast_state_update)

    async def connect(self, websocket: WebSocket, client_id):
        await websocket.accept()
        self.active_connections[client_id] = websocket
        await websocket.send_json({"message": "state-full", "state": self._state.data})

    def disconnect(self, websocket: WebSocket, client_id):
        self.active_connections.pop(client_id)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            log.info(f"Sending '{message}'")
            await self.active_connections[connection].send_text(message)

    async def _broadcast_state_update(self, attribute: str, value: T | None):
        for connection in self.active_connections:
            await self.active_connections[connection].send_json(
                {"message": "state-update", "attribute": attribute, "value": value}
            )

    async def set_state(self, attribute: str, value: T):
        log.info(f"State attribute {attribute!r} set to {value!r}")
        await self._state.update(attribute, value)


manager = ConnectionManager(global_state)


@ws.websocket("/ws/test/{client_id}")
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
                    await forward_log(json_data)
            except Exception:
                await manager.broadcast(f"Client #{client_id} sent message {data}")
    except WebSocketDisconnect:
        log.info(f"Disconnecting Client {client_id}")
        manager.disconnect(websocket, client_id)
        await manager.broadcast(f"Client #{client_id} disconnected")
        await manager.set_state(f"Client {client_id}", "left")


async def check_connections(active_connections):
    log.info("Checking connections")
    for connection in active_connections:
        log.info("Checking response")
        try:
            await asyncio.wait_for(connection.receive(), timeout=6)
        except asyncio.TimeoutError:
            log.info(f"Disconnecting Client {connection[0]}")
            manager.disconnect(connection[0], connection[1])


async def forward_log(logrecord):
    record_name = logrecord["name"]
    logging.getLogger(record_name).handle(logging.makeLogRecord(logrecord))


@ws.delete("/ws/test/{client_id}")
async def close_ws_connection(client_id):
    log.info("Disconnecting", client_id)
    manager.disconnect(manager.active_connections[client_id], client_id)
    await manager.broadcast(f"Client #{client_id} disconnected")
