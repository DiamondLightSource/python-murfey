from __future__ import annotations

import random

import websocket
from websocket import create_connection


def open_websocket_connection():
    id = str(random.randint(0, 100))
    url = "ws://127.0.0.1:8000/ws/test/" + id
    ws = create_connection(url)
    print(ws.connected)
    print(f"Websocket connection opened for Client {id}")
    return ws


def receive_messages(ws):
    while True:
        result = ws.recv()
        print("Received ", result)
    # Do other stuff with the received message


def close_websocket_connection(ws):
    print("Closing websocket connection")
    ws.close()


def on_message(message):
    print(message)


def on_error(ws, error):
    print(error.text)


def on_close(ws):
    print("Closing connection")
    ws.close()
    print("### closed ###")


def on_open():
    print("Opened connection")


def websocket_app():
    websocket.enableTrace(True)
    id = str(random.randint(0, 1000))
    url = "ws://127.0.0.1:8000/ws/test/" + id
    ws = websocket.WebSocketApp(url, on_close=on_close)
    ws.run_forever()
