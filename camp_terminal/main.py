import asyncio
import os
import threading
import paho.mqtt.client as mqtt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import uvicorn

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
PORT = int(os.environ.get("PORT", "8080"))

app = FastAPI()
connected_clients: set[WebSocket] = set()
broadcast_loop: asyncio.AbstractEventLoop = None


async def broadcast(message: str):
    dead = set()
    for ws in connected_clients:
        try:
            await ws.send_text(message)
        except Exception:
            dead.add(ws)
    connected_clients.difference_update(dead)


def on_mqtt_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    if broadcast_loop and broadcast_loop.is_running():
        asyncio.run_coroutine_threadsafe(broadcast(payload), broadcast_loop)


def start_mqtt():
    client = mqtt.Client()
    client.on_message = on_mqtt_message
    while True:
        try:
            client.connect(MQTT_HOST, 1883, 60)
            break
        except Exception as e:
            print(f"MQTT connect failed: {e}, retrying...")
            import time; time.sleep(2)
    client.subscribe("/yurucamp/terminal")
    client.loop_forever()


@app.on_event("startup")
async def startup():
    global broadcast_loop
    broadcast_loop = asyncio.get_event_loop()
    t = threading.Thread(target=start_mqtt, daemon=True)
    t.start()


@app.get("/")
async def index():
    html = Path("/app/templates/index.html").read_text()
    return HTMLResponse(html)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    mqtt_pub = mqtt.Client()
    try:
        mqtt_pub.connect(MQTT_HOST, 1883, 60)
    except Exception:
        pass

    try:
        while True:
            data = await websocket.receive_text()
            mqtt_pub.publish("/yurucamp/outbound/terminal", data)
    except WebSocketDisconnect:
        connected_clients.discard(websocket)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
