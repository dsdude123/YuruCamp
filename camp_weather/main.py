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
logger = logging.getLogger("camp_weather")

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
LAT = float(os.environ.get("WEATHER_LAT", "45.5946"))
LON = float(os.environ.get("WEATHER_LON", "-121.1787"))
LOCATION_NAME = os.environ.get("WEATHER_LOCATION", "The Dalles, OR")
USER_AGENT = os.environ.get(
    "WEATHER_USER_AGENT", "YuruCamp/1.0 (off-grid command center)"
)
FORECAST_PERIODS = int(os.environ.get("WEATHER_FORECAST_PERIODS", "4"))

NWS_BASE = "https://api.weather.gov"

mqtt_client = mqtt.Client()
request_queue: asyncio.Queue = asyncio.Queue()
event_loop: asyncio.AbstractEventLoop | None = None
_points_cache: dict | None = None

COMPASS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]


def c_to_f(c):
    return None if c is None else c * 9 / 5 + 32


def kmh_to_mph(kmh):
    return None if kmh is None else kmh * 0.621371


def deg_to_compass(deg):
    if deg is None:
        return ""
    return COMPASS[round(deg / 45) % 8]


async def get_points(client: httpx.AsyncClient) -> dict:
    global _points_cache
    if _points_cache is not None:
        return _points_cache
    url = f"{NWS_BASE}/points/{LAT},{LON}"
    logger.debug("Fetching points metadata: %s", url)
    resp = await client.get(url)
    resp.raise_for_status()
    _points_cache = resp.json()["properties"]
    logger.info(
        "Points metadata cached (forecast=%s, stations=%s)",
        _points_cache.get("forecast"),
        _points_cache.get("observationStations"),
    )
    return _points_cache


async def fetch_current(client: httpx.AsyncClient, stations_url: str) -> dict | None:
    logger.debug("Fetching observation stations: %s", stations_url)
    resp = await client.get(stations_url)
    resp.raise_for_status()
    stations = resp.json().get("features", [])
    if not stations:
        logger.warning("No observation stations returned")
        return None
    for st in stations[:3]:
        sid = st["properties"]["stationIdentifier"]
        try:
            r = await client.get(f"{NWS_BASE}/stations/{sid}/observations/latest")
            r.raise_for_status()
            obs = r.json()["properties"]
            logger.debug("Got observation from station %s", sid)
            return obs
        except Exception as e:
            logger.warning("Station %s observation failed: %s", sid, e)
    return None


async def fetch_forecast(client: httpx.AsyncClient, forecast_url: str) -> list[dict]:
    logger.debug("Fetching forecast: %s", forecast_url)
    resp = await client.get(forecast_url)
    resp.raise_for_status()
    periods = resp.json()["properties"]["periods"]
    logger.debug("Got %d forecast periods", len(periods))
    return periods


def format_current(obs: dict | None) -> str:
    if not obs:
        return "Now: N/A"
    temp_c = (obs.get("temperature") or {}).get("value")
    desc = obs.get("textDescription") or ""
    wind_kmh = (obs.get("windSpeed") or {}).get("value")
    wind_dir = (obs.get("windDirection") or {}).get("value")
    parts = ["Now:"]
    if temp_c is not None:
        parts.append(f"{round(c_to_f(temp_c))}F")
    if desc:
        parts.append(desc)
    if wind_kmh is not None:
        mph = round(kmh_to_mph(wind_kmh))
        if mph <= 0:
            parts.append("calm")
        else:
            compass = deg_to_compass(wind_dir)
            parts.append(f"{compass} {mph}mph".strip())
    return " ".join(parts)


def format_forecast(periods: list[dict], n: int) -> list[str]:
    lines = []
    for p in periods[:n]:
        name = p.get("name", "")
        temp = p.get("temperature", "")
        unit = p.get("temperatureUnit", "F")
        short = p.get("shortForecast", "")
        lines.append(f"{name}: {temp}{unit} {short}".strip())
    return lines


async def build_report(client: httpx.AsyncClient) -> str:
    points = await get_points(client)
    forecast_url = points["forecast"]
    stations_url = points["observationStations"]

    obs_task = asyncio.create_task(fetch_current(client, stations_url))
    fc_task = asyncio.create_task(fetch_forecast(client, forecast_url))

    obs = None
    periods: list[dict] = []
    try:
        obs = await obs_task
    except Exception as e:
        logger.error("Current conditions fetch failed: %s", e)
    try:
        periods = await fc_task
    except Exception as e:
        logger.error("Forecast fetch failed: %s", e)

    lines = [LOCATION_NAME, format_current(obs)]
    lines.extend(format_forecast(periods, FORECAST_PERIODS))
    return "\n".join(lines)


async def worker():
    headers = {"User-Agent": USER_AGENT, "Accept": "application/geo+json"}
    async with httpx.AsyncClient(timeout=20, headers=headers) as client:
        while True:
            payload = await request_queue.get()
            logger.info("Weather request received: %r", payload)
            try:
                report = await build_report(client)
            except Exception as e:
                logger.error("Weather report failed: %s", e, exc_info=True)
                report = f"{LOCATION_NAME}\nWeather unavailable: {type(e).__name__}"
            mqtt_client.publish("/yurucamp/outbound", report)
            logger.info("Published weather report (%d chars)", len(report))


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
            c.subscribe("/yurucamp/weather")
            logger.debug("Subscribed to /yurucamp/weather")
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
    logger.info(
        "camp_weather starting (location=%s, lat=%.4f, lon=%.4f)",
        LOCATION_NAME, LAT, LON,
    )
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
