import asyncio
import logging
import os
import time
import httpx
import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("camp_grok")

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
XAI_API_KEY = os.environ.get("XAI_API_KEY", "")
XAI_API_URL = os.environ.get("XAI_API_URL", "https://api.x.ai/v1/chat/completions")
GROK_MODEL = os.environ.get("GROK_MODEL", "grok-3-mini")
GROK_SYSTEM_PROMPT = os.environ.get(
    "GROK_SYSTEM_PROMPT",
    "You are Grok, replying over a low-bandwidth meshcore radio link. "
    "Answer in plain text, no markdown, in 2 short sentences or fewer.",
)
GROK_MAX_TOKENS = int(os.environ.get("GROK_MAX_TOKENS", "200"))
GROK_TIMEOUT = float(os.environ.get("GROK_TIMEOUT", "30"))

mqtt_client = mqtt.Client()
request_queue: asyncio.Queue = asyncio.Queue()
event_loop: asyncio.AbstractEventLoop | None = None


async def ask_grok(client: httpx.AsyncClient, prompt: str) -> str:
    body = {
        "model": GROK_MODEL,
        "messages": [
            {"role": "system", "content": GROK_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": GROK_MAX_TOKENS,
    }
    headers = {
        "Authorization": f"Bearer {XAI_API_KEY}",
        "Content-Type": "application/json",
    }
    logger.debug("POST %s model=%s", XAI_API_URL, GROK_MODEL)
    resp = await client.post(XAI_API_URL, json=body, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    choices = data.get("choices") or []
    if not choices:
        raise RuntimeError("xAI returned no choices")
    content = (choices[0].get("message") or {}).get("content") or ""
    return content.strip()


async def worker():
    async with httpx.AsyncClient(timeout=GROK_TIMEOUT) as client:
        while True:
            payload = await request_queue.get()
            prompt = payload.strip()
            logger.info("Grok request: %r", prompt)
            if not prompt:
                mqtt_client.publish("/yurucamp/outbound", "Grok: empty prompt")
                continue
            if not XAI_API_KEY:
                mqtt_client.publish("/yurucamp/outbound", "Grok: XAI_API_KEY not set")
                continue
            try:
                answer = await ask_grok(client, prompt)
            except httpx.HTTPStatusError as e:
                logger.error("xAI HTTP error: %s %s", e.response.status_code, e.response.text)
                answer = f"Grok error: HTTP {e.response.status_code}"
            except Exception as e:
                logger.error("Grok request failed: %s", e, exc_info=True)
                answer = f"Grok unavailable: {type(e).__name__}"
            if not answer:
                answer = "Grok: empty response"
            mqtt_client.publish("/yurucamp/outbound", answer)
            logger.info("Published Grok reply (%d chars)", len(answer))


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    logger.debug("MQTT message [%s]: %r", msg.topic, payload)
    if event_loop is None:
        logger.warning("Event loop not ready, dropping request")
        return
    event_loop.call_soon_threadsafe(request_queue.put_nowait, payload)


def connect_mqtt():
    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            logger.info("MQTT connected to %s", MQTT_HOST)
            c.subscribe("/yurucamp/grok")
            logger.debug("Subscribed to /yurucamp/grok")
        else:
            logger.error("MQTT connect refused (rc=%d)", rc)

    def on_disconnect(c, userdata, rc):
        logger.warning("MQTT disconnected (rc=%d)", rc)

    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message

    while True:
        try:
            logger.info("Connecting to MQTT at %s:1883...", MQTT_HOST)
            mqtt_client.connect(MQTT_HOST, 1883, 60)
            mqtt_client.loop_start()
            return
        except Exception as e:
            logger.warning("MQTT connect failed: %s, retrying in 2s...", e)
            time.sleep(2)


async def main_async():
    global event_loop
    event_loop = asyncio.get_running_loop()
    connect_mqtt()
    await worker()


def main():
    logger.info("camp_grok starting (model=%s)", GROK_MODEL)
    if not XAI_API_KEY:
        logger.warning("XAI_API_KEY is empty; requests will be rejected")
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
