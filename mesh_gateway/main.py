import os
import serial
import threading
import time
import paho.mqtt.client as mqtt

SERIAL_PORT = os.environ.get("SERIAL_PORT", "/dev/ttyUSB0")
BAUD_RATE = int(os.environ.get("BAUD_RATE", "115200"))
MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")

CHUNK_SIZE = 200
CHUNK_DELAY = 0.1

ser = None
mqtt_client = None


def chunk_message(text):
    words = text.split(" ")
    chunks = []
    current = ""
    for word in words:
        if current and len(current) + 1 + len(word) > CHUNK_SIZE:
            chunks.append(current)
            current = word
        else:
            current = (current + " " + word).strip() if current else word
    if current:
        chunks.append(current)
    return chunks


def serial_read_loop():
    while True:
        try:
            line = ser.readline().decode("utf-8", errors="replace").strip()
            if line:
                mqtt_client.publish("/yurucamp/inbound", line)
        except Exception as e:
            print(f"Serial read error: {e}")
            time.sleep(1)


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    for chunk in chunk_message(payload):
        try:
            ser.write((chunk + "\n").encode("utf-8"))
            time.sleep(CHUNK_DELAY)
        except Exception as e:
            print(f"Serial write error: {e}")


def main():
    global ser, mqtt_client

    try:
        ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
        print(f"Serial open: {SERIAL_PORT} @ {BAUD_RATE}")
    except Exception as e:
        print(f"Serial open failed: {e} — running without serial")
        ser = None

    mqtt_client = mqtt.Client()
    mqtt_client.on_message = on_message

    while True:
        try:
            mqtt_client.connect(MQTT_HOST, 1883, 60)
            break
        except Exception as e:
            print(f"MQTT connect failed: {e}, retrying...")
            time.sleep(2)

    mqtt_client.subscribe("/yurucamp/outbound/#")

    if ser:
        t = threading.Thread(target=serial_read_loop, daemon=True)
        t.start()

    mqtt_client.loop_forever()


if __name__ == "__main__":
    main()
