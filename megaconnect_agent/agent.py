"""MegaGuard Connect — πράκτορας για το Home Assistant.

Συνδέει το σπίτι με τον server του κέντρου ΜΟΝΟ με εξερχόμενη σύνδεση (HTTPS):
- στο ξεκίνημα στέλνει ποιοι αισθητήρες υπάρχουν (hello),
- κάθε 60″ «ζω» (heartbeat),
- κάθε αλλαγή κατάστασης αισθητήρα (events), σε παρτίδες, με επανάληψη αν κοπεί το internet.
Δεν ανοίγει καμία πόρτα στο σπίτι και δεν δέχεται εντολές απ' έξω.
"""
import asyncio
import json
import os
import time
from collections import deque
from datetime import datetime, timezone

import aiohttp

VERSION = "1.0.2"
OPTIONS = os.environ.get("MC_OPTIONS", "/data/options.json")
HA_WS = os.environ.get("MC_HA_WS", "ws://supervisor/core/websocket")
TOKEN = os.environ.get("SUPERVISOR_TOKEN") or os.environ.get("HASSIO_TOKEN") or ""
# Μόνο ό,τι αφορά ασφάλεια / ζωή: όχι φώτα, διακόπτες, LED, Wi-Fi κ.λπ.
DOMAINS = ["binary_sensor", "sensor", "event", "input_boolean", "lock", "alarm_control_panel"]
# Από τους sensor κρατάμε μόνο ό,τι αφορά ασφάλεια / ζωή (όχι π.χ. ενέργεια κάθε δευτερόλεπτο).
SENSOR_CLASSES = {"battery", "temperature", "humidity", "carbon_monoxide", "gas", "moisture", "smoke"}

queue = deque(maxlen=2000)       # γεγονότα που περιμένουν να σταλούν
entities = {}                    # entity_id -> {name, device_class, area}


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_options():
    with open(OPTIONS, encoding="utf-8") as f:
        o = json.load(f)
    url = str(o.get("server_url", "")).rstrip("/")
    key = str(o.get("home_key", "")).strip()
    if not url.startswith("https://") and not url.startswith("http://127.0.0.1") and not url.startswith("http://localhost"):
        raise SystemExit("server_url πρέπει να είναι https://")
    if len(key) < 20:
        raise SystemExit("Λείπει το κλειδί του σπιτιού (home_key). Πάρτε το από την κονσόλα: Σπίτια → Νέο σπίτι.")
    return url, key


def wanted(entity_id, attrs):
    domain = entity_id.split(".")[0]
    if domain not in DOMAINS:
        return False
    if domain == "sensor":
        return (attrs or {}).get("device_class") in SENSOR_CLASSES or "action" in entity_id
    return True


class Server:
    def __init__(self, session, url, key):
        self.s, self.url, self.headers = session, url, {"X-Home-Key": key, "Content-Type": "application/json"}

    async def post(self, path, body):
        async with self.s.post(f"{self.url}{path}", json=body, headers=self.headers, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 401:
                raise SystemExit("Ο server απέρριψε το κλειδί του σπιτιού. Ελέγξτε το home_key.")
            if r.status >= 400:
                raise RuntimeError(f"HTTP {r.status}")
            return await r.json()


async def ha_call(ws, msg_id, payload):
    await ws.send_json({"id": msg_id, **payload})
    while True:
        m = await ws.receive_json()
        if m.get("id") == msg_id and m.get("type") == "result":
            return m.get("result")


async def heartbeat(server):
    while True:
        try:
            await server.post("/home/agent/heartbeat", {})
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001
            log(f"«ζω» απέτυχε: {e}")
        await asyncio.sleep(60)


async def sender(server):
    while True:
        if queue:
            batch = [queue[i] for i in range(min(len(queue), 100))]
            try:
                await server.post("/home/agent/events", {"events": batch})
                for _ in batch:
                    queue.popleft()
                continue
            except SystemExit:
                raise
            except Exception as e:  # noqa: BLE001
                log(f"αποστολή γεγονότων απέτυχε ({len(queue)} σε αναμονή): {e}")
                await asyncio.sleep(10)
        await asyncio.sleep(2)


async def ha_loop(session, server):
    """Σύνδεση στο Home Assistant, λίστα αισθητήρων, και παρακολούθηση αλλαγών. Ξανασυνδέεται αν κοπεί."""
    wait = 5
    while True:
        try:
            async with session.ws_connect(HA_WS, heartbeat=30) as ws:
                hello = await ws.receive_json()
                if hello.get("type") == "auth_required":
                    await ws.send_json({"type": "auth", "access_token": TOKEN})
                    ok = await ws.receive_json()
                    if ok.get("type") != "auth_ok":
                        raise RuntimeError("το Home Assistant δεν δέχτηκε το token")
                    ha_version = ok.get("ha_version", "")
                else:
                    ha_version = ""
                states = await ha_call(ws, 1, {"type": "get_states"}) or []
                ereg = await ha_call(ws, 2, {"type": "config/entity_registry/list"}) or []
                dreg = await ha_call(ws, 3, {"type": "config/device_registry/list"}) or []
                areg = await ha_call(ws, 4, {"type": "config/area_registry/list"}) or []
                areas = {a["area_id"]: a.get("name") for a in areg}
                dev_area = {d["id"]: d.get("area_id") for d in dreg}
                ent_area = {e["entity_id"]: (e.get("area_id") or dev_area.get(e.get("device_id"))) for e in ereg}
                entities.clear()
                listing = []
                for st in states:
                    eid, attrs = st["entity_id"], st.get("attributes", {})
                    if not wanted(eid, attrs):
                        continue
                    info = {"name": attrs.get("friendly_name"), "device_class": attrs.get("device_class"), "area": areas.get(ent_area.get(eid))}
                    entities[eid] = info
                    listing.append({"entity_id": eid, "state": st.get("state"), **info})
                await server.post("/home/agent/hello", {"agent_version": VERSION, "ha_version": ha_version, "entities": listing})
                log(f"Συνδέθηκε · {len(listing)} αισθητήρες · Home Assistant {ha_version}")
                await ws.send_json({"id": 5, "type": "subscribe_events", "event_type": "state_changed"})
                wait = 5
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    m = json.loads(msg.data)
                    if m.get("type") != "event":
                        continue
                    data = m["event"].get("data", {})
                    new, old = data.get("new_state") or {}, data.get("old_state") or {}
                    eid = data.get("entity_id", "")
                    attrs = new.get("attributes", {})
                    if not wanted(eid, attrs):
                        continue
                    if new.get("state") == old.get("state") and eid.split(".")[0] != "event":
                        continue  # μόνο αλλαγή χαρακτηριστικών, όχι κατάστασης
                    info = entities.get(eid) or {"name": attrs.get("friendly_name"), "device_class": attrs.get("device_class"), "area": None}
                    queue.append({"entity_id": eid, "state": new.get("state"), "prev_state": old.get("state"),
                                  "at": new.get("last_changed") or datetime.now(timezone.utc).isoformat(), **info})
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001
            log(f"σύνδεση με το Home Assistant: {e} · νέα προσπάθεια σε {wait}″")
        await asyncio.sleep(wait)
        wait = min(wait * 2, 120)


async def main():
    url, key = load_options()
    log(f"MegaGuard Connect {VERSION} → {url}")
    if not TOKEN:
        log("ΠΡΟΣΟΧΗ: δεν βρέθηκε το token του Home Assistant (SUPERVISOR_TOKEN) — η σύνδεση με το Home Assistant δεν θα γίνει.")
    async with aiohttp.ClientSession() as session:
        server = Server(session, url, key)
        await asyncio.gather(heartbeat(server), sender(server), ha_loop(session, server))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except SystemExit as e:
        log(f"ΣΤΑΜΑΤΗΣΕ: {e}")
        time.sleep(30)  # για να φαίνεται το μήνυμα στο log πριν την επανεκκίνηση
        raise
