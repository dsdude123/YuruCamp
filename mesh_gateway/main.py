import asyncio
import logging
import os
import time
from pathlib import Path
import paho.mqtt.client as mqtt
from meshcore import MeshCore, EventType

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("mesh_gateway")

SERIAL_PORT = os.environ.get("SERIAL_PORT", "/dev/ttyACM0")
BAUD_RATE = int(os.environ.get("BAUD_RATE", "115200"))
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
ROOM_SERVER_PUBKEY = os.environ.get("ROOM_SERVER_PUBKEY", "").strip().lower()
ROOM_SERVER_PASSWORD = os.environ.get("ROOM_SERVER_PASSWORD", "")
MAX_MSG_CHARS = 138
CHUNK_DELAY = float(os.environ.get("CHUNK_DELAY", "5.0"))
READY_FILE = Path(os.environ.get("READY_FILE", "/tmp/ready"))

outbound_queue: asyncio.Queue = asyncio.Queue()
mesh_ready = False


def mark_ready():
    try:
        READY_FILE.touch()
        logger.info("Ready file written at %s", READY_FILE)
    except OSError as e:
        logger.warning("Failed to write ready file %s: %s", READY_FILE, e)


def mark_unready():
    try:
        READY_FILE.unlink()
        logger.info("Ready file removed")
    except FileNotFoundError:
        pass
    except OSError as e:
        logger.warning("Failed to remove ready file %s: %s", READY_FILE, e)


def split_on_spaces(text: str, max_chars: int) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]

    chunks: list[str] = []
    remaining = text
    while len(remaining) > max_chars:
        cut = remaining.rfind(" ", 0, max_chars + 1)
        if cut <= 0:
            cut = max_chars

        piece = remaining[:cut].rstrip()
        if piece:
            chunks.append(piece)
        remaining = remaining[cut:].lstrip()

    if remaining:
        chunks.append(remaining)
    return chunks


def make_mqtt_client(loop: asyncio.AbstractEventLoop, subscribed_event: asyncio.Event):
    client = mqtt.Client()

    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            logger.info("MQTT connected to %s", MQTT_HOST)
        else:
            logger.error("MQTT connect refused (rc=%d)", rc)
            return
        c.subscribe("/yurucamp/outbound/#")
        logger.debug("Subscribed to /yurucamp/outbound/#")
        loop.call_soon_threadsafe(subscribed_event.set)
        if mesh_ready:
            mark_ready()

    def on_disconnect(c, userdata, rc):
        logger.warning("MQTT disconnected (rc=%d)", rc)
        mark_unready()

    def on_message(c, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="replace")
        logger.debug("MQTT outbound queued [%s]: %r", msg.topic, payload)
        loop.call_soon_threadsafe(outbound_queue.put_nowait, payload)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    return client


async def connect_mqtt(client: mqtt.Client):
    while True:
        try:
            logger.info("Connecting to MQTT at %s:1883...", MQTT_HOST)
            client.connect(MQTT_HOST, 1883, 60)
            client.loop_start()
            return
        except Exception as e:
            logger.warning("MQTT connect failed: %s, retrying in 2s...", e)
            await asyncio.sleep(2)


async def resolve_room_contact(mc: MeshCore, pubkey_hex: str) -> dict:
    ev = await mc.commands.get_contacts()
    if ev.type == EventType.ERROR:
        raise RuntimeError(f"Failed to fetch contacts: {ev.payload}")
    contacts = ev.payload or {}
    for name, c in contacts.items():
        if c.get("public_key", "").lower().startswith(pubkey_hex):
            logger.info(
                "Found room server contact '%s' (pubkey=%s)",
                name, c["public_key"],
            )
            return c
    raise RuntimeError(
        f"Room server with pubkey prefix {pubkey_hex} not found in contacts. "
        "Make sure the room server has adverted and the device has imported it."
    )


async def outbound_worker(mc: MeshCore, room_contact: dict):
    while True:
        text = await outbound_queue.get()
        chunks = split_on_spaces(text, MAX_MSG_CHARS)
        if not chunks:
            logger.debug("Outbound message empty after stripping, skipping")
            continue
        logger.info(
            "Sending to room server %s in %d chunk(s): %r",
            room_contact.get("adv_name", room_contact["public_key"][:12]),
            len(chunks), text,
        )
        for i, chunk in enumerate(chunks):
            if i > 0:
                await asyncio.sleep(CHUNK_DELAY)
            logger.debug(
                "Sending chunk %d/%d (%d chars): %r",
                i + 1, len(chunks), len(chunk), chunk,
            )
            try:
                result = await mc.commands.send_msg_with_retry(
                    room_contact, chunk, int(time.time())
                )
                if result is None:
                    logger.error(
                        "send_msg ack timeout on chunk %d/%d", i + 1, len(chunks)
                    )
                elif result.type == EventType.ERROR:
                    logger.error(
                        "send_msg error on chunk %d/%d: %s",
                        i + 1, len(chunks), result.payload,
                    )
                else:
                    logger.debug(
                        "Mesh send OK to room (chunk %d/%d)",
                        i + 1, len(chunks),
                    )
            except Exception as e:
                logger.error(
                    "Mesh send exception on chunk %d/%d: %s",
                    i + 1, len(chunks), e,
                )


async def main():
    global mesh_ready
    mark_unready()

    if not ROOM_SERVER_PUBKEY:
        raise RuntimeError("ROOM_SERVER_PUBKEY must be set (hex public key of the room server)")
    if len(ROOM_SERVER_PUBKEY) < 12 or any(c not in "0123456789abcdef" for c in ROOM_SERVER_PUBKEY):
        raise RuntimeError(
            f"ROOM_SERVER_PUBKEY must be a hex string (>=12 chars); got {ROOM_SERVER_PUBKEY!r}"
        )

    loop = asyncio.get_event_loop()
    mqtt_subscribed = asyncio.Event()
    mqtt_client = make_mqtt_client(loop, mqtt_subscribed)
    await connect_mqtt(mqtt_client)

    while True:
        try:
            logger.info("Connecting to mesh device at %s @ %d baud...", SERIAL_PORT, BAUD_RATE)
            mc = await MeshCore.create_serial(SERIAL_PORT, BAUD_RATE)
            logger.info("Mesh connected: %s @ %d", SERIAL_PORT, BAUD_RATE)
            break
        except Exception as e:
            logger.warning("Mesh connect failed: %s, retrying in 5s...", e)
            await asyncio.sleep(5)

    room_contact = await resolve_room_contact(mc, ROOM_SERVER_PUBKEY)
    room_pubkey_full = room_contact["public_key"].lower()

    if ROOM_SERVER_PASSWORD:
        logger.info("Logging in to room server...")
        login_ev = await mc.commands.send_login_sync(room_contact, ROOM_SERVER_PASSWORD)
        if login_ev is None:
            logger.warning("Room server login timed out (continuing anyway)")
        else:
            logger.info("Room server login response: %s", login_ev.type)

    def on_contact_msg(event):
        payload = event.payload
        text = payload.get("text", "")
        sender = (payload.get("pubkey_prefix") or payload.get("sender_node_id") or "").lower()
        logger.debug("Contact msg received: sender=%s text=%r", sender, text)
        if not text:
            return
        if sender and not room_pubkey_full.startswith(sender) and not sender.startswith(ROOM_SERVER_PUBKEY):
            logger.debug("Ignoring contact msg from %s (not room server)", sender)
            return
        logger.info("Inbound room msg from %s: %r", sender, text)
        result = mqtt_client.publish("/yurucamp/inbound", text)
        logger.debug("Published to /yurucamp/inbound (mid=%s)", result.mid)

    mc.subscribe(EventType.CONTACT_MSG_RECV, on_contact_msg)
    logger.info("Subscribed to CONTACT_MSG_RECV events")

    await mc.start_auto_message_fetching()
    logger.info("Auto message fetching started")

    logger.info("Waiting for MQTT subscription to be active before signaling ready")
    await mqtt_subscribed.wait()
    mesh_ready = True
    mark_ready()

    await outbound_worker(mc, room_contact)


if __name__ == "__main__":
    asyncio.run(main())
