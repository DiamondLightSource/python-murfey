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

    def disconnect(self, websocket: WebSocket, client_id):
        self.active_connections.pop(client_id)

    async def send_individual_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            print(f"Sending '{message}'")
            await self.active_connections[connection].send_text(message)


manager = ConnectionManager()


@ws.websocket("/ws/test/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: int):
    await manager.connect(websocket, client_id)

    await manager.broadcast(f"Client {client_id} joined")
    try:
        while True:
            data = await websocket.receive_text()
            await manager.broadcast(f"Client #{client_id} sent message {data}")
    except WebSocketDisconnect:
        print(f"Disconnecting Client {client_id}")
        manager.disconnect(websocket, client_id)
        await manager.broadcast(f"Client #{client_id} disconnected")


async def check_connections(active_connections):
    print("Checking connections")
    for connection in active_connections:
        print("Checking response")
        try:
            await asyncio.wait_for(connection.receive(), timeout=6)
        except asyncio.TimeoutError:
            print(f"Disconnecting Client {connection[0]}")
            manager.disconnect(connection[0], connection[1])


@ws.delete("/ws/test/{client_id}")
async def close_ws_connection(client_id):
    print("Disconnecting", client_id)
    manager.disconnect(manager.active_connections[client_id], client_id)
    await manager.broadcast(f"Client #{client_id} disconnected")
