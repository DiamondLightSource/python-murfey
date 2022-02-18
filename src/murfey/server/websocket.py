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


@ws.websocket("/ws/test")
async def update_clients(websocket: WebSocket, client_id):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            print("Received data: {}".format(data))
            await manager.send_individual_message(f"Received {data}", websocket)
            await manager.broadcast(f"Client {client_id} sent {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcast(f"Client {client_id} disconnected")
        print("Client disconnected")
