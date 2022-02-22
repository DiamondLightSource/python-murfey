from __future__ import annotations

from typing import List

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

ws = APIRouter(prefix="/ws", tags=["websocket"])


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_individual_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


manager = ConnectionManager()


@ws.websocket("/ws/{client_id}")
async def manage_connection(websocket: WebSocket, client_id: str):
    try:
        await manager.connect(websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcast(f"Client {client_id} disconnected")
        print("Client disconnected")


async def update_clients(file_name):
    print("CONNECTIONS", manager.active_connections)
    try:
        await manager.broadcast(f"File transferred {file_name}")
    except WebSocketDisconnect:
        await manager.broadcast("Client disconnected")
        print("Client disconnected")
