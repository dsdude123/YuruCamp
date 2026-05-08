import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
import httpx
import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("camp_rally")

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
EVENT_ID = os.environ.get("EVENT_ID")
if EVENT_ID is None:
    raise RuntimeError("EVENT_ID environment variable is not set")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "30"))
BASE_URL = "https://rc.statusas.com"

STATUS_MAP = {0: "Completed", 2: "Expected", 3: "DNS"}

mqtt_client = mqtt.Client()
prev_states: dict[tuple, str] = {}


def connect_mqtt():
    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            logger.info("MQTT connected to %s", MQTT_HOST)
        else:
            logger.error("MQTT connect refused (rc=%d)", rc)

    def on_disconnect(c, userdata, rc):
        logger.warning("MQTT disconnected (rc=%d)", rc)

    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect

    while True:
        try:
            logger.info("Connecting to MQTT at %s:1883...", MQTT_HOST)
            mqtt_client.connect(MQTT_HOST, 1883, 60)
            mqtt_client.loop_start()
            return
        except Exception as e:
            logger.warning("MQTT connect failed: %s, retrying in 2s...", e)
            time.sleep(2)


async def fetch_live_stages(client: httpx.AsyncClient) -> list[dict]:
    now = datetime.now(timezone.utc).timestamp() * 1000
    logger.debug("Fetching live stages for event %s (now=%d)", EVENT_ID, int(now))
    resp = await client.get(f"{BASE_URL}/itinerary/stages", params={"eventId": EVENT_ID})
    resp.raise_for_status()
    stages = resp.json()
    live = [
        s for s in stages
        if s.get("liveTimestamp", 0) <= now <= s.get("closedTimestamp", 0)
    ]
    logger.debug("Got %d total stages, %d live", len(stages), len(live))
    return live


async def fetch_stage_times(client: httpx.AsyncClient, stage_id: int) -> list[dict]:
    logger.debug("Fetching times for stage %d", stage_id)
    resp = await client.get(f"{BASE_URL}/times/stage-times", params={"stageId": stage_id})
    resp.raise_for_status()
    times = resp.json()
    logger.debug("Got %d time entries for stage %d", len(times), stage_id)
    return times


async def poll():
    device_id = str(uuid.uuid4())
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"{BASE_URL}/events/{EVENT_ID}/itinerary",
        "device-id": device_id,
    }
    cookies = {"deviceId": device_id}
    async with httpx.AsyncClient(timeout=15, headers=headers, cookies=cookies) as client:
        logger.info("Bootstrapping identity (device_id=%s)", device_id)
        resp = await client.get(f"{BASE_URL}/identity")
        resp.raise_for_status()
        logger.info("Identity established: %s", resp.json())
        while True:
            try:
                stages = await fetch_live_stages(client)
                if not stages:
                    logger.info("No live stages at this time")
                else:
                    logger.info("Processing %d live stage(s)", len(stages))
                    for stage in stages:
                        stage_id = stage["id"]
                        stage_name = stage.get("name", str(stage_id))
                        logger.debug("Checking stage '%s' (id=%d)", stage_name, stage_id)
                        times = await fetch_stage_times(client, stage_id)
                        changes = 0
                        for entry in times:
                            identifier = entry.get("identifier", "?")
                            make = entry.get("make", "")
                            status_code = entry.get("status")
                            status_name = STATUS_MAP.get(status_code, str(status_code))
                            key = (stage_id, identifier)
                            prev = prev_states.get(key)
                            if prev != status_name:
                                logger.debug(
                                    "Car %s state change: %s -> %s (stage=%s)",
                                    identifier, prev, status_name, stage_name,
                                )
                                prev_states[key] = status_name
                                msg = f"Car {identifier} – {make} status changed to {status_name} on {stage_name}"
                                mqtt_client.publish("/yurucamp/outbound", msg)
                                logger.info("Published status change: %s", msg)
                                changes += 1
                        logger.debug("Stage '%s': %d change(s) published", stage_name, changes)
            except Exception as e:
                logger.error("Poll error: %s", e, exc_info=True)
            logger.debug("Sleeping %ds before next poll", POLL_INTERVAL)
            await asyncio.sleep(POLL_INTERVAL)


def main():
    logger.info("camp_rally starting (event_id=%s, poll_interval=%ds)", EVENT_ID, POLL_INTERVAL)
    connect_mqtt()
    asyncio.run(poll())


if __name__ == "__main__":
    main()
