import asyncio
import os
import time
import paho.mqtt.client as mqtt
from meshcore import MeshCore, EventType

SERIAL_PORT = os.environ.get("SERIAL_PORT", "/dev/ttyUSB0")
BAUD_RATE = int(os.environ.get("BAUD_RATE", "115200"))
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
CHANNEL_INDEX = int(os.environ.get("CHANNEL_INDEX", "0"))

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
    loop = asyncio.get_event_loop()
    while True:
        try:
            client.connect(MQTT_HOST, 1883, 60)
            client.loop_start()
            return
        except Exception as e:
            print(f"MQTT connect failed: {e}, retrying in 2s...")
            await asyncio.sleep(2)


async def outbound_worker(mc: MeshCore):
    while True:
        text = await outbound_queue.get()
        try:
            result = await mc.commands.send_chan_msg(CHANNEL_INDEX, text, int(time.time()))
            if result.type == EventType.ERROR:
                print(f"send_chan_msg error: {result.payload}")
        except Exception as e:
            print(f"Mesh send error: {e}")


async def main():
    mqtt_client = make_mqtt_client()
    await connect_mqtt(mqtt_client)

    while True:
        try:
            mc = await MeshCore.create_serial(SERIAL_PORT, BAUD_RATE)
            print(f"Mesh connected: {SERIAL_PORT} @ {BAUD_RATE}")
            break
        except Exception as e:
            print(f"Mesh connect failed: {e}, retrying in 5s...")
            await asyncio.sleep(5)

    def on_channel_msg(event):
        text = event.payload.get("text", "")
        if text:
            mqtt_client.publish("/yurucamp/inbound", text)

    def on_contact_msg(event):
        text = event.payload.get("text", "")
        if text:
            mqtt_client.publish("/yurucamp/inbound", text)

    mc.subscribe(EventType.CHANNEL_MSG_RECV, on_channel_msg)
    mc.subscribe(EventType.CONTACT_MSG_RECV, on_contact_msg)

    await outbound_worker(mc)


if __name__ == "__main__":
    asyncio.run(main())
