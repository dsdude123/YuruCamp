import asyncio
import os
import time
import paho.mqtt.client as mqtt
from serial.tools import list_ports
from meshcore import MeshCore, EventType

SERIAL_PORT = os.environ.get("SERIAL_PORT")
BAUD_RATE = int(os.environ.get("BAUD_RATE", "115200"))

# Known USB VID:PID pairs for common meshcore-compatible boards.
# pid=None matches any product from that vendor.
KNOWN_USB_IDS = {
    (0x10C4, 0xEA60),  # Silicon Labs CP210x (Heltec, T-Beam, etc.)
    (0x1A86, 0x7523),  # QinHeng CH340
    (0x1A86, 0x55D4),  # QinHeng CH9102
    (0x303A, 0x1001),  # Espressif ESP32-S2/S3 native USB
    (0x239A, None),    # Adafruit (nRF52840 boards)
    (0x2E8A, None),    # Raspberry Pi (RP2040)
    (0x1915, None),    # Nordic Semiconductor
}


def autodetect_serial_port() -> str:
    ports = list(list_ports.comports())
    for p in ports:
        if p.vid is None:
            continue
        for vid, pid in KNOWN_USB_IDS:
            if p.vid == vid and (pid is None or p.pid == pid):
                print(f"Auto-detected meshcore device on {p.device} "
                      f"(VID:PID={p.vid:04x}:{p.pid or 0:04x}, {p.description})")
                return p.device
    for p in ports:
        if p.device.startswith(("/dev/ttyACM", "/dev/ttyUSB")):
            print(f"Falling back to {p.device} ({p.description})")
            return p.device
    raise RuntimeError(
        "No serial device found. Set SERIAL_PORT env var explicitly. "
        f"Available ports: {[p.device for p in ports] or 'none'}"
    )
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
CHANNEL_NAME = os.environ.get("CHANNEL_NAME", "#yurucamp-ft")
MAX_CHANNELS = 40

outbound_queue: asyncio.Queue = asyncio.Queue()


def make_mqtt_client():
    client = mqtt.Client()

    def on_connect(c, userdata, flags, rc):
        print(f"MQTT connected (rc={rc})")
        c.subscribe("/yurucamp/outbound/#")

    def on_message(c, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="replace")
        outbound_queue.put_nowait(payload)

    client.on_connect = on_connect
    client.on_message = on_message
    return client


async def connect_mqtt(client: mqtt.Client):
    while True:
        try:
            client.connect(MQTT_HOST, 1883, 60)
            client.loop_start()
            return
        except Exception as e:
            print(f"MQTT connect failed: {e}, retrying in 2s...")
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
            return idx
        if first_empty is None and not ch_name:
            first_empty = idx

    if first_empty is None:
        raise RuntimeError(
            f"Channel '{name}' not found and no empty slot to create it"
        )

    print(f"Channel '{hashtag_name}' not on device, creating at index {first_empty}")
    ev = await mc.commands.set_channel(first_empty, hashtag_name)
    if ev.type == EventType.ERROR:
        raise RuntimeError(f"Failed to create channel '{hashtag_name}': {ev.payload}")
    return first_empty


async def outbound_worker(mc: MeshCore, channel_idx: int):
    while True:
        text = await outbound_queue.get()
        try:
            result = await mc.commands.send_chan_msg(channel_idx, text, int(time.time()))
            if result.type == EventType.ERROR:
                print(f"send_chan_msg error: {result.payload}")
        except Exception as e:
            print(f"Mesh send error: {e}")


async def main():
    mqtt_client = make_mqtt_client()
    await connect_mqtt(mqtt_client)

    while True:
        try:
            port = SERIAL_PORT or autodetect_serial_port()
            mc = await MeshCore.create_serial(port, BAUD_RATE)
            print(f"Mesh connected: {port} @ {BAUD_RATE}")
            break
        except Exception as e:
            print(f"Mesh connect failed: {e}, retrying in 5s...")
            await asyncio.sleep(5)

    channel_idx = await resolve_channel_index(mc, CHANNEL_NAME)
    print(f"Using channel '{CHANNEL_NAME}' at index {channel_idx}")

    def on_channel_msg(event):
        if event.payload.get("channel_idx") != channel_idx:
            return
        text = event.payload.get("text", "")
        if text:
            mqtt_client.publish("/yurucamp/inbound", text)

    def on_contact_msg(event):
        text = event.payload.get("text", "")
        if text:
            mqtt_client.publish("/yurucamp/inbound", text)

    mc.subscribe(EventType.CHANNEL_MSG_RECV, on_channel_msg)
    mc.subscribe(EventType.CONTACT_MSG_RECV, on_contact_msg)

    await outbound_worker(mc, channel_idx)


if __name__ == "__main__":
    asyncio.run(main())
