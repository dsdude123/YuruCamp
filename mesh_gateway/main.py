import asyncio
import logging
import os
import time
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
CHANNEL_NAME = os.environ.get("CHANNEL_NAME", "#yurucamp-ft")
MAX_CHANNELS = 40
MAX_MSG_BYTES = int(os.environ.get("MAX_MSG_BYTES", "140"))
CHUNK_DELAY = float(os.environ.get("CHUNK_DELAY", "5.0"))

outbound_queue: asyncio.Queue = asyncio.Queue()


def split_on_spaces(text: str, max_bytes: int) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if len(text.encode("utf-8")) <= max_bytes:
        return [text]

    chunks: list[str] = []
    remaining = text
    while len(remaining.encode("utf-8")) > max_bytes:
        used = 0
        char_fits = 0
        for i, ch in enumerate(remaining):
            cb = len(ch.encode("utf-8"))
            if used + cb > max_bytes:
                break
            used += cb
            char_fits = i + 1

        cut = remaining.rfind(" ", 0, char_fits)
        if cut <= 0:
            cut = char_fits

        piece = remaining[:cut].rstrip()
        if piece:
            chunks.append(piece)
        remaining = remaining[cut:].lstrip()

    if remaining:
        chunks.append(remaining)
    return chunks


def make_mqtt_client():
    client = mqtt.Client()

    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            logger.info("MQTT connected to %s", MQTT_HOST)
        else:
            logger.error("MQTT connect refused (rc=%d)", rc)
        c.subscribe("/yurucamp/outbound/#")
        logger.debug("Subscribed to /yurucamp/outbound/#")

    def on_disconnect(c, userdata, rc):
        logger.warning("MQTT disconnected (rc=%d)", rc)

    def on_message(c, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="replace")
        logger.debug("MQTT outbound queued [%s]: %r", msg.topic, payload)
        outbound_queue.put_nowait(payload)

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


async def resolve_channel_index(mc: MeshCore, name: str) -> int:
    target = name.lstrip("#").lower()
    hashtag_name = "#" + target
    first_empty = None
    for idx in range(MAX_CHANNELS):
        ev = await mc.commands.get_channel(idx)
        if ev.type == EventType.ERROR:
            break
        ch_name = ev.payload.get("channel_name", "")
        if ch_name and ch_name.lstrip("#").lower() == target:
            logger.info("Found channel '%s' at index %d", hashtag_name, idx)
            return idx
        if first_empty is None and not ch_name:
            first_empty = idx

    if first_empty is None:
        raise RuntimeError(
            f"Channel '{name}' not found and no empty slot to create it"
        )

    logger.info("Channel '%s' not on device, creating at index %d", hashtag_name, first_empty)
    ev = await mc.commands.set_channel(first_empty, hashtag_name)
    if ev.type == EventType.ERROR:
        raise RuntimeError(f"Failed to create channel '{hashtag_name}': {ev.payload}")
    logger.info("Channel '%s' created at index %d", hashtag_name, first_empty)
    return first_empty


async def outbound_worker(mc: MeshCore, channel_idx: int):
    while True:
        text = await outbound_queue.get()
        chunks = split_on_spaces(text, MAX_MSG_BYTES)
        if not chunks:
            logger.debug("Outbound message empty after stripping, skipping")
            continue
        logger.info(
            "Sending to mesh channel %d in %d chunk(s): %r",
            channel_idx, len(chunks), text,
        )
        for i, chunk in enumerate(chunks):
            if i > 0:
                await asyncio.sleep(CHUNK_DELAY)
            logger.debug(
                "Sending chunk %d/%d (%d bytes): %r",
                i + 1, len(chunks), len(chunk.encode("utf-8")), chunk,
            )
            try:
                result = await mc.commands.send_chan_msg(
                    channel_idx, chunk, int(time.time())
                )
                if result.type == EventType.ERROR:
                    logger.error(
                        "send_chan_msg error on chunk %d/%d: %s",
                        i + 1, len(chunks), result.payload,
                    )
                else:
                    logger.debug(
                        "Mesh send OK for channel %d (chunk %d/%d)",
                        channel_idx, i + 1, len(chunks),
                    )
            except Exception as e:
                logger.error(
                    "Mesh send exception on chunk %d/%d: %s",
                    i + 1, len(chunks), e,
                )


async def main():
    mqtt_client = make_mqtt_client()
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

    channel_idx = await resolve_channel_index(mc, CHANNEL_NAME)
    logger.info("Using channel '%s' at index %d", CHANNEL_NAME, channel_idx)

    def on_channel_msg(event):
        payload = event.payload
        incoming_idx = payload.get("channel_idx")
        text = payload.get("text", "")
        sender = payload.get("sender_node_id", "unknown")
        logger.debug(
            "Channel msg received: channel_idx=%s sender=%s text=%r",
            incoming_idx, sender, text,
        )
        if incoming_idx != channel_idx:
            logger.debug(
                "Ignoring channel msg for idx %s (watching %d)", incoming_idx, channel_idx
            )
            return
        if not text:
            logger.debug("Channel msg from %s has no text, skipping", sender)
            return
        logger.info("Inbound channel msg from %s: %r", sender, text)
        result = mqtt_client.publish("/yurucamp/inbound", text)
        logger.debug("Published to /yurucamp/inbound (mid=%s)", result.mid)

    def on_contact_msg(event):
        payload = event.payload
        text = payload.get("text", "")
        sender = payload.get("sender_node_id", "unknown")
        logger.debug("Contact msg received: sender=%s text=%r", sender, text)
        if not text:
            logger.debug("Contact msg from %s has no text, skipping", sender)
            return
        logger.info("Inbound contact msg from %s: %r", sender, text)
        result = mqtt_client.publish("/yurucamp/inbound", text)
        logger.debug("Published to /yurucamp/inbound (mid=%s)", result.mid)

    mc.subscribe(EventType.CHANNEL_MSG_RECV, on_channel_msg)
    mc.subscribe(EventType.CONTACT_MSG_RECV, on_contact_msg)
    logger.info("Subscribed to CHANNEL_MSG_RECV and CONTACT_MSG_RECV events")

    await mc.start_auto_message_fetching()
    logger.info("Auto message fetching started")

    await outbound_worker(mc, channel_idx)


if __name__ == "__main__":
    asyncio.run(main())
