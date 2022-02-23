from __future__ import annotations

import asyncio
from typing import List

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

ws = APIRouter(prefix="/ws", tags=["websocket"])


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.queue = asyncio.Queue()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_individual_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        print("Connections", self.active_connections)
        for connection in self.active_connections:
            print(connection, message)
            await connection.send_text(message)


manager = ConnectionManager()


@ws.websocket("/ws/test/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: int):
    await manager.connect(websocket)
    await check_connections(manager.active_connections)
    print(
        f"active connection statuses: {[ac.client_state for ac in manager.active_connections]}"
    )

    await manager.broadcast(f"Client {client_id} joined")
    try:
        while True:
            await asyncio.sleep(5)
            file = await manager.queue.get()
            await manager.broadcast(f"Client #{client_id} uploaded file {file}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcast(f"Client #{client_id} disconnected")


async def check_connections(active_connections):
    print("Checking connections")
    for connection in active_connections:
        print("Checking response")
        try:
            await asyncio.wait_for(connection.receive(), timeout=6)
        except asyncio.TimeoutError:
            print(f"Disconnecting client {connection}")
            manager.disconnect(connection)
