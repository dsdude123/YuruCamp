import asyncio
import logging
import os
import threading
import paho.mqtt.client as mqtt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import uvicorn

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("camp_terminal")

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
PORT = int(os.environ.get("PORT", "8080"))

app = FastAPI()
connected_clients: set[WebSocket] = set()
broadcast_loop: asyncio.AbstractEventLoop = None


async def broadcast(message: str):
    if not connected_clients:
        logger.debug("No WebSocket clients connected, dropping message: %r", message)
        return
    logger.debug("Broadcasting to %d WebSocket client(s): %r", len(connected_clients), message)
    dead = set()
    for ws in connected_clients:
        try:
            await ws.send_text(message)
        except Exception as e:
            logger.warning("Failed to send to WebSocket client, removing: %s", e)
            dead.add(ws)
    if dead:
        logger.info("Removed %d dead WebSocket client(s)", len(dead))
    connected_clients.difference_update(dead)


def on_mqtt_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    logger.debug("MQTT message on [%s]: %r", msg.topic, payload)
    if broadcast_loop and broadcast_loop.is_running():
        asyncio.run_coroutine_threadsafe(broadcast(payload), broadcast_loop)
    else:
        logger.warning("Broadcast loop not running, dropping MQTT message: %r", payload)


def start_mqtt():
    client = mqtt.Client()
    client.on_message = on_mqtt_message

    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            logger.info("MQTT connected to %s", MQTT_HOST)
        else:
            logger.error("MQTT connect refused (rc=%d)", rc)

    def on_disconnect(c, userdata, rc):
        logger.warning("MQTT disconnected (rc=%d)", rc)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    while True:
        try:
            logger.info("Connecting to MQTT at %s:1883...", MQTT_HOST)
            client.connect(MQTT_HOST, 1883, 60)
            break
        except Exception as e:
            logger.warning("MQTT connect failed: %s, retrying in 2s...", e)
            import time; time.sleep(2)

    client.subscribe("/yurucamp/terminal")
    logger.info("Subscribed to /yurucamp/terminal")
    client.loop_forever()


@app.on_event("startup")
async def startup():
    global broadcast_loop
    broadcast_loop = asyncio.get_event_loop()
    logger.info("Starting MQTT listener thread")
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
    client_host = websocket.client.host if websocket.client else "unknown"
    logger.info("WebSocket client connected from %s (total=%d)", client_host, len(connected_clients))

    mqtt_pub = mqtt.Client()
    try:
        mqtt_pub.connect(MQTT_HOST, 1883, 60)
        mqtt_pub.loop_start()
        logger.debug("Per-WS MQTT publisher connected for %s", client_host)
    except Exception as e:
        logger.warning("Per-WS MQTT publisher connect failed for %s: %s", client_host, e)

    try:
        while True:
            data = await websocket.receive_text()
            logger.debug("WebSocket message from %s: %r", client_host, data)
            mqtt_pub.publish("/yurucamp/outbound/terminal", data)
            logger.debug("Published to /yurucamp/outbound/terminal: %r", data)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected: %s (total=%d)", client_host, len(connected_clients) - 1)
        connected_clients.discard(websocket)
    finally:
        try:
            mqtt_pub.loop_stop()
            mqtt_pub.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    logger.info("Starting camp_terminal on port %d", PORT)
    uvicorn.run(app, host="0.0.0.0", port=PORT)
