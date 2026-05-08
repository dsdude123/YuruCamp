import logging
import os
import time
import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("camp_router")

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")


def strip_sender_prefix(payload):
    """Strip a sender prefix like '[name] ' or 'name: ' from the payload, if present."""
    if payload.startswith("[") and "] " in payload:
        return payload.split("] ", 1)[1]
    if ": " in payload:
        prefix, rest = payload.split(": ", 1)
        if "\n" not in prefix:
            return rest
    return payload


def parse_command(payload):
    """Return (cmd, remainder) if payload (after any sender prefix) starts with !cmd, else (None, payload)."""
    body = strip_sender_prefix(payload)
    if body.startswith("!"):
        parts = body[1:].split(" ", 1)
        cmd = parts[0].lower()
        remainder = parts[1] if len(parts) > 1 else ""
        return cmd, remainder
    return None, payload


def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode("utf-8", errors="replace")
    logger.debug("Received on [%s]: %r", topic, payload)

    if topic == "/yurucamp/inbound":
        logger.info("Inbound message: %r", payload)
        cmd, remainder = parse_command(payload)
        if cmd:
            dest = f"/yurucamp/{cmd}"
            logger.info("Command %r detected, routing to %s with args %r", cmd, dest, remainder)
            client.publish(dest, remainder)
        client.publish("/yurucamp/terminal", payload)
        logger.debug("Forwarded inbound to /yurucamp/terminal")

    elif topic == "/yurucamp/outbound":
        logger.info("Outbound message: %r", payload)
        client.publish("/yurucamp/terminal", payload)
        logger.debug("Forwarded outbound to /yurucamp/terminal")

    elif topic == "/yurucamp/outbound/terminal":
        logger.info("Terminal outbound message: %r", payload)
        cmd, remainder = parse_command(payload)
        if cmd:
            dest = f"/yurucamp/{cmd}"
            logger.info("Command %r detected, routing to %s with args %r", cmd, dest, remainder)
            client.publish(dest, remainder)
        client.publish("/yurucamp/terminal", payload)
        logger.debug("Forwarded terminal outbound to /yurucamp/terminal")

    else:
        logger.warning("Received message on unexpected topic [%s]: %r", topic, payload)


def main():
    client = mqtt.Client()
    client.on_message = on_message

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
            time.sleep(2)

    subscriptions = [
        ("/yurucamp/inbound", 0),
        ("/yurucamp/outbound", 0),
        ("/yurucamp/outbound/terminal", 0),
    ]
    client.subscribe(subscriptions)
    logger.info("Subscribed to: %s", [t for t, _ in subscriptions])

    logger.info("CampRouter running")
    client.loop_forever()


if __name__ == "__main__":
    main()
