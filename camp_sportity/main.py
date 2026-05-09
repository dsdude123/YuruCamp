import asyncio
import hashlib
import io
import json
import logging
import os
import re
import time
from pathlib import Path

import httpx
import paho.mqtt.client as mqtt
from bs4 import BeautifulSoup
from pypdf import PdfReader

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("camp_sportity")

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
SPORTITY_URL = os.environ.get(
    "SPORTITY_URL",
    "https://webapp.sportity.com/event/OTR2026/751bfffa-ae55-43db-a6cd-e6ff0d64ce2e",
)
POLL_INTERVAL = int(os.environ.get("SPORTITY_POLL_INTERVAL", str(30 * 60)))
DATA_DIR = Path(os.environ.get("SPORTITY_DATA_DIR", "/data"))
STATE_FILE = DATA_DIR / "state.json"
FILES_DIR = DATA_DIR / "files"

XAI_API_KEY = os.environ.get("XAI_API_KEY", "")
XAI_API_URL = os.environ.get("XAI_API_URL", "https://api.x.ai/v1/responses")
GROK_MODEL = os.environ.get("GROK_MODEL", "grok-4.3")
GROK_TIMEOUT = float(os.environ.get("GROK_TIMEOUT", "60"))
PDF_TEXT_LIMIT = int(os.environ.get("SPORTITY_PDF_TEXT_LIMIT", "30000"))

OUTBOUND_TOPIC = "/yurucamp/outbound"
SPORTITY_TOPIC = "/yurucamp/sportity"

mqtt_client = mqtt.Client()
command_queue: asyncio.Queue = asyncio.Queue()
event_loop: asyncio.AbstractEventLoop | None = None


# ---------- HTML scrape ----------

def scrape_documents(html: str) -> list[dict]:
    """Return ordered list of dicts {uuid,name,url,folder} for Sportity-hosted documents.

    External links, emails, and folders themselves are excluded. The folder field is the
    nearest ancestor folder name (for context), or empty string for root items.
    """
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("#root")
    if root is None:
        return []

    results: list[dict] = []

    def walk(node, folder_path: list[str]):
        for child in node.children:
            if not getattr(child, "name", None):
                continue
            classes = child.get("class") or []
            if "list-group-item" in classes and "item" in classes:
                a = child.find("a", recursive=False) or child.find("a")
                if not a:
                    continue
                href = (a.get("href") or "").strip()
                h3 = a.find("h3")
                name = h3.get_text(" ", strip=True) if h3 else a.get_text(" ", strip=True)
                # Strip leading icon noise (BeautifulSoup .get_text already drops <i> contents,
                # but icon glyph chars may remain as nbsp).
                name = re.sub(r"\s+", " ", name).strip()
                icon = h3.find("i") if h3 else None
                icon_classes = (icon.get("class") if icon else []) or []

                is_folder = "fa-folder" in icon_classes or href.startswith("#parent-")
                is_sportity_doc = href.startswith("https://app-cdn.sportity.com/")

                if is_sportity_doc:
                    results.append({
                        "uuid": child.get("id") or "",
                        "name": name,
                        "url": href,
                        "folder": " / ".join(folder_path),
                    })
                # Folder items are siblings of a separate <div class="list-group" id="parent-XXX">
                # which we visit via the outer walk; nothing to recurse into here.
            elif "list-group" in classes:
                # Nested folder container. Its sibling list-group-item has the folder name.
                # Find the matching folder name from the previous sibling chain.
                folder_name = ""
                # The id is "parent-<uuid>"; the matching folder item has id "<uuid>".
                gid = (child.get("id") or "")
                if gid.startswith("parent-"):
                    target_id = gid[len("parent-"):]
                    folder_item = soup.find(id=target_id)
                    if folder_item:
                        h3 = folder_item.find("h3")
                        if h3:
                            folder_name = re.sub(r"\s+", " ", h3.get_text(" ", strip=True)).strip()
                walk(child, folder_path + ([folder_name] if folder_name else []))
            else:
                walk(child, folder_path)

    walk(root, [])

    # De-duplicate while preserving order (folder children may also be matched by outer walk).
    seen: set[str] = set()
    unique: list[dict] = []
    for r in results:
        key = r["uuid"] or r["url"]
        if key in seen:
            continue
        seen.add(key)
        unique.append(r)
    return unique


# ---------- State / storage ----------

def load_state() -> dict | None:
    if not STATE_FILE.exists():
        return None
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception as e:
        logger.error("Failed to load state: %s", e)
        return None


def save_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)


def file_path_for(uuid: str) -> Path:
    return FILES_DIR / f"{uuid}.pdf"


# ---------- PDF helpers ----------

def extract_pdf_text(data: bytes) -> str:
    try:
        reader = PdfReader(io.BytesIO(data))
        parts = []
        for page in reader.pages:
            try:
                parts.append(page.extract_text() or "")
            except Exception:
                pass
        text = "\n".join(parts)
        return text[:PDF_TEXT_LIMIT]
    except Exception as e:
        logger.warning("PDF text extraction failed: %s", e)
        return ""


# ---------- Grok ----------

def _extract_output_text(data: dict) -> str:
    parts: list[str] = []
    for item in data.get("output") or []:
        if item.get("type") != "message":
            continue
        for chunk in item.get("content") or []:
            if chunk.get("type") == "output_text":
                t = chunk.get("text") or ""
                if t:
                    parts.append(t)
    return "\n".join(parts).strip()


async def grok_call(client: httpx.AsyncClient, system: str, user: str) -> str:
    if not XAI_API_KEY:
        return "Grok unavailable (no API key)"
    body = {
        "model": GROK_MODEL,
        "input": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "max_output_tokens": 1024,
    }
    headers = {
        "Authorization": f"Bearer {XAI_API_KEY}",
        "Content-Type": "application/json",
    }
    try:
        r = await client.post(XAI_API_URL, json=body, headers=headers, timeout=GROK_TIMEOUT)
        r.raise_for_status()
        return _extract_output_text(r.json()) or ""
    except httpx.HTTPStatusError as e:
        logger.error("Grok HTTP %s: %s", e.response.status_code, e.response.text[:300])
        return f"Grok HTTP {e.response.status_code}"
    except Exception as e:
        logger.error("Grok error: %s", e)
        return f"Grok error: {type(e).__name__}"


SUMMARY_SYSTEM = (
    "You summarise rally event documents for a low-bandwidth mesh radio. "
    "Reply in plain text, one sentence, under 100 characters. No markdown, no preamble."
)
DIFF_SYSTEM = (
    "You compare two versions of a rally document and describe what changed for a low-bandwidth "
    "mesh radio. Reply in plain text, one sentence, under 100 characters. Focus on substantive "
    "changes only. No markdown, no preamble."
)
CONTEXT_SYSTEM = (
    "You answer questions about a rally event document for a low-bandwidth mesh radio. "
    "Reply in plain text, at most two short sentences. No markdown."
)


async def summarise_new(client: httpx.AsyncClient, name: str, text: str) -> str:
    if not text.strip():
        return "no extractable text"
    prompt = f"Document title: {name}\n\nContents:\n{text}"
    return (await grok_call(client, SUMMARY_SYSTEM, prompt)) or "summary unavailable"


async def summarise_diff(client: httpx.AsyncClient, name: str, old: str, new: str) -> str:
    if not (old.strip() or new.strip()):
        return "content changed"
    prompt = (
        f"Document title: {name}\n\nPREVIOUS VERSION:\n{old}\n\nNEW VERSION:\n{new}"
    )
    return (await grok_call(client, DIFF_SYSTEM, prompt)) or "updated"


async def answer_context(client: httpx.AsyncClient, name: str, text: str, question: str) -> str:
    prompt = (
        f"Document title: {name}\n\nDocument contents:\n{text}\n\nUser question: {question}"
    )
    return (await grok_call(client, CONTEXT_SYSTEM, prompt)) or "no answer"


# ---------- Core sync logic ----------

async def fetch_html(client: httpx.AsyncClient) -> str:
    r = await client.get(SPORTITY_URL, timeout=30)
    r.raise_for_status()
    return r.text


async def download_pdf(client: httpx.AsyncClient, url: str) -> bytes:
    r = await client.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def publish(msg: str) -> None:
    logger.info("OUT: %s", msg)
    mqtt_client.publish(OUTBOUND_TOPIC, msg)


def reassign_ids(docs: list[dict]) -> None:
    """Assign sequential integer IDs starting at 1 in current page order."""
    for idx, d in enumerate(docs, start=1):
        d["id"] = idx


async def initial_sync(client: httpx.AsyncClient) -> dict:
    logger.info("Initial Sportity sync")
    html = await fetch_html(client)
    docs = scrape_documents(html)
    logger.info("Discovered %d documents", len(docs))
    out: list[dict] = []
    for d in docs:
        try:
            data = await download_pdf(client, d["url"])
        except Exception as e:
            logger.error("Initial download failed for %s: %s", d["name"], e)
            continue
        FILES_DIR.mkdir(parents=True, exist_ok=True)
        file_path_for(d["uuid"]).write_bytes(data)
        out.append({
            "uuid": d["uuid"],
            "name": d["name"],
            "url": d["url"],
            "folder": d["folder"],
            "hash": hash_bytes(data),
        })
    reassign_ids(out)
    state = {"docs": out}
    save_state(state)
    logger.info("Initial state saved with %d documents", len(out))
    return state


async def sync_cycle(client: httpx.AsyncClient, state: dict) -> dict:
    logger.info("Sportity sync cycle")
    try:
        html = await fetch_html(client)
    except Exception as e:
        logger.error("Failed to fetch Sportity page: %s", e)
        return state

    discovered = scrape_documents(html)
    by_uuid_old = {d["uuid"]: d for d in state.get("docs", [])}
    seen_uuids: set[str] = set()
    new_docs: list[dict] = []

    for d in discovered:
        uuid = d["uuid"]
        seen_uuids.add(uuid)
        old = by_uuid_old.get(uuid)
        if old is None:
            # NEW file
            try:
                data = await download_pdf(client, d["url"])
            except Exception as e:
                logger.error("Download failed for new %s: %s", d["name"], e)
                continue
            FILES_DIR.mkdir(parents=True, exist_ok=True)
            file_path_for(uuid).write_bytes(data)
            text = extract_pdf_text(data)
            summary = await summarise_new(client, d["name"], text)
            publish(f"{d['name']} was uploaded - {summary}")
            new_docs.append({
                "uuid": uuid,
                "name": d["name"],
                "url": d["url"],
                "folder": d["folder"],
                "hash": hash_bytes(data),
            })
        else:
            # Existing -- check for changes
            try:
                data = await download_pdf(client, d["url"])
            except Exception as e:
                logger.error("Download failed for %s: %s", d["name"], e)
                # Keep old entry as-is
                new_docs.append({**old, "name": d["name"], "url": d["url"], "folder": d["folder"]})
                continue
            new_hash = hash_bytes(data)
            if new_hash == old.get("hash"):
                new_docs.append({**old, "name": d["name"], "url": d["url"], "folder": d["folder"]})
                continue
            # Changed: read old PDF text from disk before overwriting
            old_path = file_path_for(uuid)
            old_text = ""
            if old_path.exists():
                try:
                    old_text = extract_pdf_text(old_path.read_bytes())
                except Exception:
                    pass
            old_path.parent.mkdir(parents=True, exist_ok=True)
            old_path.write_bytes(data)
            new_text = extract_pdf_text(data)
            change = await summarise_diff(client, d["name"], old_text, new_text)
            publish(f"{d['name']} was updated - {change}")
            new_docs.append({
                "uuid": uuid,
                "name": d["name"],
                "url": d["url"],
                "folder": d["folder"],
                "hash": new_hash,
            })

    # Deletions
    for uuid, old in by_uuid_old.items():
        if uuid in seen_uuids:
            continue
        publish(f"{old['name']} was deleted")
        try:
            file_path_for(uuid).unlink(missing_ok=True)
        except Exception:
            pass

    reassign_ids(new_docs)
    state = {"docs": new_docs}
    save_state(state)
    return state


# ---------- Commands ----------

def chunk_for_mesh(text: str, limit: int = 138) -> list[str]:
    """Break into chunks that fit a single mesh frame, splitting on spaces.

    The gateway also chunks, but we split here to keep the list command from being one giant
    line that the gateway has to fragment unpredictably.
    """
    out: list[str] = []
    for line in text.split("\n"):
        if len(line) <= limit:
            out.append(line)
            continue
        words = line.split(" ")
        cur = ""
        for w in words:
            if not cur:
                cur = w
            elif len(cur) + 1 + len(w) <= limit:
                cur = f"{cur} {w}"
            else:
                out.append(cur)
                cur = w
        if cur:
            out.append(cur)
    return out


async def handle_command(client: httpx.AsyncClient, payload: str, state: dict) -> None:
    body = payload.strip()
    if not body:
        publish("Sportity: usage: !sportity list | !sportity context <id> <question>")
        return
    parts = body.split(None, 1)
    cmd = parts[0].lower()
    rest = parts[1] if len(parts) > 1 else ""

    if cmd == "list":
        docs = state.get("docs", [])
        if not docs:
            publish("Sportity: no documents")
            return
        lines = [f"{d['id']}. {d['name']}" for d in docs]
        msg = "Sportity files:\n" + "\n".join(lines)
        publish(msg)
        return

    if cmd == "context":
        sub = rest.split(None, 1)
        if len(sub) < 2 or not sub[0].isdigit():
            publish("Sportity: usage: !sportity context <id> <question>")
            return
        wanted = int(sub[0])
        question = sub[1].strip()
        doc = next((d for d in state.get("docs", []) if d.get("id") == wanted), None)
        if not doc:
            publish(f"Sportity: no file with id {wanted}")
            return
        path = file_path_for(doc["uuid"])
        if not path.exists():
            publish(f"Sportity: file {wanted} not on disk")
            return
        text = extract_pdf_text(path.read_bytes())
        answer = await answer_context(client, doc["name"], text, question)
        publish(f"{doc['name']}: {answer}")
        return

    publish(f"Sportity: unknown command '{cmd}'")


# ---------- MQTT plumbing ----------

def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    logger.debug("MQTT [%s]: %r", msg.topic, payload)
    if event_loop is None:
        return
    event_loop.call_soon_threadsafe(command_queue.put_nowait, payload)


def connect_mqtt():
    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            logger.info("MQTT connected to %s", MQTT_HOST)
            c.subscribe(SPORTITY_TOPIC)
        else:
            logger.error("MQTT connect refused (rc=%d)", rc)

    def on_disconnect(c, userdata, rc):
        logger.warning("MQTT disconnected (rc=%d)", rc)

    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message
    while True:
        try:
            mqtt_client.connect(MQTT_HOST, 1883, 60)
            mqtt_client.loop_start()
            return
        except Exception as e:
            logger.warning("MQTT connect failed: %s, retrying in 2s...", e)
            time.sleep(2)


# ---------- Main loop ----------

async def main_async():
    global event_loop
    event_loop = asyncio.get_running_loop()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)
    connect_mqtt()

    state = load_state()
    async with httpx.AsyncClient(follow_redirects=True) as client:
        if state is None:
            try:
                state = await initial_sync(client)
            except Exception as e:
                logger.error("Initial sync failed: %s", e, exc_info=True)
                state = {"docs": []}

        next_poll = time.monotonic()
        while True:
            timeout = max(0.0, next_poll - time.monotonic())
            try:
                payload = await asyncio.wait_for(command_queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                payload = None

            if payload is not None:
                try:
                    await handle_command(client, payload, state)
                except Exception as e:
                    logger.error("Command failed: %s", e, exc_info=True)
                    publish(f"Sportity error: {type(e).__name__}")
                continue

            try:
                state = await sync_cycle(client, state)
            except Exception as e:
                logger.error("Sync cycle failed: %s", e, exc_info=True)
            next_poll = time.monotonic() + POLL_INTERVAL


def main():
    logger.info("camp_sportity starting (poll=%ds, url=%s)", POLL_INTERVAL, SPORTITY_URL)
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
