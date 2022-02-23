from __future__ import annotations

import asyncio
from typing import Dict

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

ws = APIRouter(prefix="/ws", tags=["websocket"])


class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.queue = asyncio.Queue()

    async def connect(self, websocket: WebSocket, client_id):
        await websocket.accept()
        self.active_connections[client_id] = websocket
        # self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket, client_id):
        self.active_connections.pop(client_id)
        # self.active_connections.remove(websocket)

    async def send_individual_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        print("Connections", self.active_connections)
        for connection in self.active_connections:
            print(connection, message)
            await self.active_connections[connection].send_text(message)


manager = ConnectionManager()


@ws.websocket("/ws/test/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: int):
    await manager.connect(websocket, client_id)
    # await check_connections(manager.active_connections)
    # print(
    #    f"active connection statuses: {[ac.client_state for ac in manager.active_connections]}"
    # )

    await manager.broadcast(f"Client {client_id} joined")
    try:
        while True:
            await asyncio.sleep(5)
            file = await manager.queue.get()
            await manager.broadcast(f"Client #{client_id} uploaded file {file}")
    except WebSocketDisconnect:
        print("Disconnecting", websocket, client_id)
        manager.disconnect(websocket, client_id)
        await manager.broadcast(f"Client #{client_id} disconnected")


async def check_connections(active_connections):
    print("Checking connections")
    for connection in active_connections:
        print("Checking response")
        try:
            await asyncio.wait_for(connection.receive(), timeout=6)
        except asyncio.TimeoutError:
            print(f"Disconnecting client {connection}")
            manager.disconnect(connection[0], connection[1])


@ws.delete("/ws/test/{client_id}")
async def close_ws_connection(client_id):
    print("Disconnecting", client_id)
    manager.disconnect(manager.active_connections[client_id], client_id)
    await manager.broadcast(f"Client #{client_id} disconnected")
