import asyncio
import os
import time
from datetime import datetime, timezone
import httpx
import paho.mqtt.client as mqtt

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
EVENT_ID = os.environ.get("EVENT_ID", "4491")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "30"))
BASE_URL = "https://rc.statusas.com"

STATUS_MAP = {0: "Completed", 2: "Expected", 3: "DNS"}

mqtt_client = mqtt.Client()
prev_states: dict[tuple, str] = {}


def connect_mqtt():
    while True:
        try:
            mqtt_client.connect(MQTT_HOST, 1883, 60)
            mqtt_client.loop_start()
            print("MQTT connected")
            return
        except Exception as e:
            print(f"MQTT connect failed: {e}, retrying...")
            time.sleep(2)


async def fetch_live_stages(client: httpx.AsyncClient) -> list[dict]:
    now = datetime.now(timezone.utc).timestamp() * 1000
    resp = await client.get(f"{BASE_URL}/itinerary/stages", params={"eventId": EVENT_ID})
    resp.raise_for_status()
    stages = resp.json()
    return [
        s for s in stages
        if s.get("liveTimestamp", 0) <= now <= s.get("closedTimestamp", 0)
    ]


async def fetch_stage_times(client: httpx.AsyncClient, stage_id: int) -> list[dict]:
    resp = await client.get(f"{BASE_URL}/times/stage-times", params={"stageId": stage_id})
    resp.raise_for_status()
    return resp.json()


async def poll():
    async with httpx.AsyncClient(timeout=15) as client:
        while True:
            try:
                stages = await fetch_live_stages(client)
                if not stages:
                    print("No live stages")
                else:
                    for stage in stages:
                        stage_id = stage["id"]
                        stage_name = stage.get("name", str(stage_id))
                        times = await fetch_stage_times(client, stage_id)
                        for entry in times:
                            identifier = entry.get("identifier", "?")
                            make = entry.get("make", "")
                            status_code = entry.get("status")
                            status_name = STATUS_MAP.get(status_code, str(status_code))
                            key = (stage_id, identifier)
                            if prev_states.get(key) != status_name:
                                prev_states[key] = status_name
                                msg = f"Car {identifier} – {make} status changed to {status_name} on {stage_name}"
                                mqtt_client.publish("/yurucamp/outbound", msg)
                                print(msg)
            except Exception as e:
                print(f"Poll error: {e}")
            await asyncio.sleep(POLL_INTERVAL)


def main():
    connect_mqtt()
    asyncio.run(poll())


if __name__ == "__main__":
    main()
