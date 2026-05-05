import os
import time
import paho.mqtt.client as mqtt

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")


def parse_command(payload):
    """Return (cmd, remainder) if payload starts with !cmd, else (None, payload)."""
    if payload.startswith("!"):
        parts = payload[1:].split(" ", 1)
        cmd = parts[0].lower()
        remainder = parts[1] if len(parts) > 1 else ""
        return cmd, remainder
    return None, payload


def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode("utf-8", errors="replace")

    if topic == "/yurucamp/inbound":
        cmd, remainder = parse_command(payload)
        if cmd:
            client.publish(f"/yurucamp/{cmd}", remainder)
        client.publish("/yurucamp/terminal", payload)

    elif topic == "/yurucamp/outbound":
        client.publish("/yurucamp/terminal", payload)

    elif topic == "/yurucamp/outbound/terminal":
        cmd, remainder = parse_command(payload)
        if cmd:
            client.publish(f"/yurucamp/{cmd}", remainder)
        client.publish("/yurucamp/terminal", payload)


def main():
    client = mqtt.Client()
    client.on_message = on_message

    while True:
        try:
            client.connect(MQTT_HOST, 1883, 60)
            break
        except Exception as e:
            print(f"MQTT connect failed: {e}, retrying...")
            time.sleep(2)

    client.subscribe([
        ("/yurucamp/inbound", 0),
        ("/yurucamp/outbound", 0),
        ("/yurucamp/outbound/terminal", 0),
    ])

    print("CampRouter running")
    client.loop_forever()


if __name__ == "__main__":
    main()
