#!/usr/bin/env python3
"""GPS demo -- live position + supply-voltage table fed by TAIP/NMEA reports.

The data no longer arrives over MQTT: the router sends location reports straight
to a location server over UDP (or TCP), and the supply voltage rides along
inside the same sentence. This dashboard *is* that location server: it binds the
port, decodes every sentence and paints the result live.

The expected payload is what mqtt_power.py emits (see loc_test_server.py for the
plain-text version of the same decode):

    >RPV57862+0000000+0000000000000090;ID=0000;VOLT=12.1;*6C<
     |  |     |       |       | | ||    |       |
     |  |     |       |       | | ||    |       +-- non-standard supply voltage
     |  |     |       |       | | ||    +---------- TAIP vehicle ID
     |  |     |       |       | | |+--------------- age of the fix
     |  |     |       |       | | +---------------- fix mode 0=2D 1=3D 9=no-fix
     |  |     |       |       | +------------------ heading, degrees
     |  |     |       |       +-------------------- speed, mph
     |  |     |       +---------------------------- longitude, decimal degrees
     |  |     +------------------------------------ latitude, decimal degrees
     |  +------------------------------------------ time of day, seconds UTC
     +--------------------------------------------- TAIP position/velocity report

NMEA is understood too ($GPRMC / $GPGGA for the position, the proprietary
$PDGIVLT for the voltage), and because those arrive as three separate sentences
the fields are merged into one report instead of overwriting each other.

Unrecognised lines still render (they land in the message log verbatim), so a
stray packet on the port will not take the dashboard down.

Where the device is, not just its coordinates:
  * the Track panel plots the fixes on a braille mini map, scaled in metres,
    with the current position marked — drawn from the reports, no network;
  * --geocode turns the coordinates into a city/region name through OSM
    Nominatim. It is off by default because it sends the position to a third
    party; requests are cached, throttled and only repeated after moving.

Listener: 0.0.0.0:5000 UDP by default, no auth, no TLS.

Requirements:
    pip install rich --break-system-packages

Usage:
    python3 gps_dashboard.py
    python3 gps_dashboard.py --port 5005
    python3 gps_dashboard.py --proto tcp --port 5005
    python3 gps_dashboard.py --geocode        # also show the city the fix is in
    python3 gps_dashboard.py --demo           # simulated reports, no device needed
    python3 gps_dashboard.py --altscreen      # draw on the alternate screen instead

Point the router (or the test publisher) at it:
    LOC_HOST=127.0.0.1 LOC_PORT=5000 FAKE_VOLTAGE=12.3 \
    FAKE_LAT=37.39438 FAKE_LON=-122.03846 FAKE_SPEED_MS=13.4 FAKE_HEADING=126 \
    python3 mqtt_power.py

If the port cannot be bound the script falls back to DEMO mode (reports are
generated locally and the header says so) so a presentation never dies on a port
already in use. Use --no-fallback to fail loudly instead.

No flicker, by design:
  * the screen is repainted only when the data really changed (about once per
    report), never on a timer;
  * bursts are coalesced, so the three sentences of an NMEA datagram cost one
    repaint instead of three;
  * every column has a fixed width and the height is measured before drawing,
    so the layout never resizes or overflows into scrolling;
  * each frame is wrapped in synchronized output (DEC 2026) so the terminal
    swaps it in atomically instead of showing a half-drawn screen.
"""

import argparse
import json
import math
import random
import re
import socket
import threading
import time
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone

from rich.console import Console, Group
from rich.live import Live
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 5000
HISTORY_LEN = 32
RECENT_LEN = 6

# the fields this dashboard charts, and the range the voltage gauge is drawn against
VOLTAGE_KEY = "voltage"
SPEED_KEY = "speed_mph"
VOLTAGE_MIN = 9.0
VOLTAGE_MAX = 15.0

MPH_TO_KMH = 1.609344

# track drawn on the mini map, and the smallest area it will zoom to: a parked
# vehicle jittering by 5 m must not fill the whole canvas
TRACK_LEN = 240
TRACK_MIN_STEP_M = 2.0
MAP_MIN_SPAN_M = 400.0        # never zoom in tighter than this, so streets show

# reverse geocoding (opt-in, --geocode): OSM Nominatim asks for a real
# User-Agent and at most one request per second. We stay far below that.
GEOCODE_URL = "https://nominatim.openstreetmap.org/reverse"
GEOCODE_AGENT = "gps_dashboard/1.0 (TAIP location demo)"
GEOCODE_MOVE_M = 150.0        # re-ask only after moving this far
GEOCODE_TIMEOUT_S = 6.0

# street basemap under the track, from OSM Overpass. Same deal: cached, and
# refetched only when the track leaves the area we already have.
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_TIMEOUT_S = 30.0
BASEMAP_MIN_INTERVAL_S = 45.0
BASEMAP_ROADS = ("motorway|trunk|primary|secondary|tertiary|residential|"
                 "unclassified|living_street")
BASEMAP_MAX_WAYS = 500
BASEMAP_LABELS = 4            # street names drawn on the canvas

# how important a road is: picks the drawing tier and which names get labelled
ROAD_RANK = {"motorway": 5, "trunk": 4, "primary": 3, "secondary": 2,
             "tertiary": 2, "residential": 1, "unclassified": 1,
             "living_street": 0}

# what the demo generator pretends to be, mirroring a real sample
DEMO_VEHICLE_ID = "0000"
DEMO_VOLTAGE = 12.1
DEMO_SPEED_MPH = 30.0
MPH_TO_MS = 0.44704

# A lap around Hopkins, Minnesota, driven by the demo: east along Mainstreet to
# Blake Rd, north to Minnetonka Blvd, west to Shady Oak Rd and south back to
# Mainstreet. Roughly 2.6 x 0.9 km, and it closes on a street rather than on a
# diagonal shortcut, so the shape on the map is a plausible drive.
DEMO_ROUTE = [
    (44.92512, -93.42800),   # Mainstreet & Shady Oak Rd
    (44.92507, -93.42000),   # Mainstreet, heading east
    (44.92500, -93.41480),   # Mainstreet & 11th Ave S
    (44.92494, -93.40600),
    (44.92486, -93.39900),
    (44.92480, -93.39680),   # Mainstreet & Blake Rd
    (44.92750, -93.39650),   # Blake Rd, heading north
    (44.93060, -93.39630),
    (44.93250, -93.39620),   # Blake Rd & Minnetonka Blvd
    (44.93265, -93.40600),   # Minnetonka Blvd, heading west
    (44.93280, -93.41800),
    (44.93300, -93.42890),   # Minnetonka Blvd & Shady Oak Rd
    (44.93080, -93.42900),   # Shady Oak Rd, heading south
    (44.92750, -93.42860),
]

# DEC private mode 2026: "hold the display until I'm done writing this frame".
# Terminals that don't know it ignore both sequences.
SYNC_BEGIN = "\x1b[?2026h"
SYNC_END = "\x1b[?2026l"

state_lock = threading.Lock()
shared_state = {
    "report": {},         # merged location report: field -> value
    "last_raw": None,     # the datagram as it arrived, text
    "last_lines": [],     # [(sentence, decoded summary), ...] of that datagram
    "last_source": None,  # "10.10.65.229:49653"
    "last_receive": None,
    "listening": False,
    "pkts_recv": 0,
    "sentences": 0,
    "bad_checksum": 0,
    "unknown": 0,
    "last_error": None,
    "demo": False,
    "history": {},        # metric name -> deque of values
    "recent": deque(maxlen=RECENT_LEN),
    "track": deque(maxlen=TRACK_LEN),   # (lat, lon) of the fixes, for the map
    "geo": {"place": None, "detail": None, "status": None},
    "basemap": {"ways": [], "bbox": None, "status": None},
    "revision": 0,        # bumped on every visible change, drives the redraw
}


def bump(**changes):
    """Apply state changes and mark the screen as needing a redraw."""
    with state_lock:
        shared_state.update(changes)
        shared_state["revision"] += 1


# --------------------------------------------------------------------------- #
# TAIP / NMEA decoding
# --------------------------------------------------------------------------- #

# >RPV{tod:5}{±lat:2+5}{±lon:3+5}{speed:3}{heading:3}{fix}{age}
TAIP_PV_RE = re.compile(
    r">RPV(\d{5})([+-]\d{2})(\d{5})([+-]\d{3})(\d{5})(\d{3})(\d{3})(\d)(\d)"
)
TAIP_FRAME_RE = re.compile(r"(>.*\*)([0-9A-Fa-f]{2})<")
NMEA_FRAME_RE = re.compile(r"\$(.*)\*([0-9A-Fa-f]{2})\s*$")

FIX_MODES = {"0": "2D", "1": "3D", "9": "no-fix"}


def taip_checksum_ok(line):
    """Return True/False/None for a TAIP sentence's checksum (None if absent).

    The checksum is the XOR of every byte from '>' through '*' inclusive, which
    is why the non-standard ;VOLT= field does not break it: it sits inside the
    checksummed range.
    """
    match = TAIP_FRAME_RE.match(line)
    if not match:
        return None
    body, given = match.group(1), int(match.group(2), 16)
    calc = 0
    for ch in body:
        calc ^= ord(ch)
    return calc == given


def nmea_checksum_ok(line):
    """Return True/False/None for an NMEA sentence's checksum (None if absent)."""
    match = NMEA_FRAME_RE.match(line)
    if not match:
        return None
    body, given = match.group(1), int(match.group(2), 16)
    calc = 0
    for ch in body:                      # between '$' and '*', both excluded
        calc ^= ord(ch)
    return calc == given


def _to_float(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def parse_taip(line):
    """Decode a TAIP PV report into report fields. Empty dict if it is not one."""
    if not line.startswith(">RPV"):
        return {}
    fields = {"kind": "TAIP PV", "checksum": taip_checksum_ok(line)}

    match = TAIP_PV_RE.match(line)
    if match:
        tod, lat_d, lat_f, lon_d, lon_f, speed, heading, fix, age = match.groups()
        fields.update({
            "tod": int(tod),
            "latitude": float(f"{lat_d}.{lat_f}"),
            "longitude": float(f"{lon_d}.{lon_f}"),
            SPEED_KEY: int(speed),
            "heading": int(heading),
            "fix": FIX_MODES.get(fix, fix),
            "age": int(age),
        })

    vehicle = re.search(r";ID=([^;*]+)", line)
    if vehicle:
        fields["vehicle_id"] = vehicle.group(1)
    volt = re.search(r";VOLT=([^;*]+)", line)
    if volt is not None and _to_float(volt.group(1)) is not None:
        fields[VOLTAGE_KEY] = _to_float(volt.group(1))
    return fields


def _nmea_coord(value, hemisphere):
    """NMEA ddmm.mmmm + hemisphere letter -> signed decimal degrees."""
    number = _to_float(value)
    if number is None:
        return None
    degrees = int(number // 100)
    minutes = number - degrees * 100
    decimal = degrees + minutes / 60.0
    return -decimal if hemisphere in ("S", "W") else decimal


def parse_nmea(line):
    """Decode the NMEA sentences mqtt_power.py can send. Empty dict if unknown.

    Position comes in $GPRMC/$GPGGA and the voltage in the proprietary
    $PDGIVLT, so each sentence contributes only part of the report.
    """
    if not line.startswith("$"):
        return {}
    body = line[1:].split("*")[0]
    parts = body.split(",")
    talker = parts[0]
    fields = {"kind": f"NMEA {talker}", "checksum": nmea_checksum_ok(line)}

    if talker.endswith("RMC") and len(parts) >= 9:
        fields["fix"] = "3D" if parts[2] == "A" else "no-fix"
        lat = _nmea_coord(parts[3], parts[4])
        lon = _nmea_coord(parts[5], parts[6])
        if lat is not None:
            fields["latitude"] = lat
        if lon is not None:
            fields["longitude"] = lon
        knots = _to_float(parts[7])
        if knots is not None:
            fields[SPEED_KEY] = int(round(knots * 1.150779))   # knots -> mph
        heading = _to_float(parts[8])
        if heading is not None:
            fields["heading"] = int(round(heading)) % 360
    elif talker.endswith("GGA") and len(parts) >= 10:
        lat = _nmea_coord(parts[2], parts[3])
        lon = _nmea_coord(parts[4], parts[5])
        if lat is not None:
            fields["latitude"] = lat
        if lon is not None:
            fields["longitude"] = lon
        if parts[6] in ("0", ""):
            fields["fix"] = "no-fix"
        satellites = _to_float(parts[7])
        if satellites is not None:
            fields["satellites"] = int(satellites)
        altitude = _to_float(parts[9])
        if altitude is not None:
            fields["altitude_m"] = altitude
    elif talker == "PDGIVLT" and len(parts) >= 2:
        volt = _to_float(parts[1])
        if volt is not None:
            fields[VOLTAGE_KEY] = volt
    return fields


def parse_sentence(line):
    """Decode one line of a location report. Empty dict when nothing is known."""
    return parse_taip(line) or parse_nmea(line)


def checksum_text(value):
    return {True: "OK", False: "BAD", None: "n/a"}[value]


def summarize(fields):
    """One-line human decode of a parsed sentence, like loc_test_server prints."""
    if not fields:
        return "(unrecognised)"
    bits = [f"{fields['kind']}  checksum={checksum_text(fields.get('checksum'))}"]
    # only what this sentence really carried: a $GPGGA has no speed to report
    if "latitude" in fields:
        bits.append(f"lat={fields['latitude']:.5f} lon={fields['longitude']:.5f}")
    if SPEED_KEY in fields:
        bits.append(f"speed={fields[SPEED_KEY]}mph")
    if "heading" in fields:
        bits.append(f"heading={fields['heading']}")
    if "fix" in fields:
        bits.append(f"fix={fields['fix']}")
    if "vehicle_id" in fields:
        bits.append(f"ID={fields['vehicle_id']}")
    if VOLTAGE_KEY in fields:
        bits.append(f"VOLT={fields[VOLTAGE_KEY]}")
    return "  ".join(bits)


# --------------------------------------------------------------------------- #
# geometry: the map and the "did we move?" checks work in metres
# --------------------------------------------------------------------------- #

M_PER_DEG_LAT = 110574.0
M_PER_DEG_LON = 111320.0                 # at the equator, scaled by cos(lat)


def metres_offset(lat, lon, lat0, lon0):
    """Local flat-earth offset in metres from (lat0, lon0). Fine for a few km."""
    x = (lon - lon0) * M_PER_DEG_LON * math.cos(math.radians(lat0))
    y = (lat - lat0) * M_PER_DEG_LAT
    return x, y


def metres_between(a, b):
    x, y = metres_offset(a[0], a[1], b[0], b[1])
    return math.hypot(x, y)


def distance_str(metres):
    if metres < 1000:
        return f"{metres:.0f} m"
    return f"{metres / 1000:.1f} km"


# --------------------------------------------------------------------------- #
# state updates
# --------------------------------------------------------------------------- #

def record_datagram(source, data):
    """Single entry point for "a location report arrived", shared with demo mode.

    One datagram can hold several sentences (NMEA sends three), so the parsed
    fields are merged into the running report: a $PDGIVLT voltage does not erase
    the position that came in the $GPRMC right before it.
    """
    if isinstance(data, bytes):
        text = data.decode("ascii", "replace")
    else:
        text = str(data)

    decoded = []
    for raw in text.splitlines():
        line = raw.strip()
        if line:
            decoded.append((line, parse_sentence(line)))

    now = datetime.now()
    with state_lock:
        shared_state["last_raw"] = text
        shared_state["last_lines"] = [(line, summarize(f)) for line, f in decoded]
        shared_state["last_source"] = source
        shared_state["last_receive"] = now
        shared_state["pkts_recv"] += 1

        for line, fields in decoded:
            shared_state["sentences"] += 1
            if not fields:
                shared_state["unknown"] += 1
            elif fields.get("checksum") is False:
                shared_state["bad_checksum"] += 1
            # merge: only the fields this sentence actually carried
            merged = {k: v for k, v in fields.items() if k != "checksum"}
            shared_state["report"].update(merged)
            shared_state["report"]["checksum"] = fields.get("checksum")
            shared_state["report"]["received"] = now
            for key in (VOLTAGE_KEY, SPEED_KEY):
                if key in fields:
                    hist = shared_state["history"].setdefault(
                        key, deque(maxlen=HISTORY_LEN))
                    hist.append(fields[key])
            shared_state["recent"].appendleft(
                (now.strftime("%H:%M:%S"), source, " ".join(line.split()))
            )

        # the map plots real fixes only, and only once the vehicle actually
        # moved: GPS noise around a parked truck is not a track
        report = shared_state["report"]
        point = (report.get("latitude"), report.get("longitude"))
        if report.get("fix") in ("2D", "3D") and None not in point:
            track = shared_state["track"]
            if not track or metres_between(point, track[-1]) >= TRACK_MIN_STEP_M:
                track.append(point)
        shared_state["revision"] += 1


# --------------------------------------------------------------------------- #
# reverse geocoding: coordinates -> city name (opt-in, --geocode)
# --------------------------------------------------------------------------- #

def geocode_fetch(lat, lon):
    """Ask Nominatim what is at these coordinates. Returns (place, detail).

    Kept deliberately small: one HTTP GET, a short timeout, and any failure is
    the caller's problem to display, never an exception that reaches the UI.
    """
    query = urllib.parse.urlencode({
        "format": "jsonv2", "lat": f"{lat:.5f}", "lon": f"{lon:.5f}",
        "zoom": 14, "addressdetails": 1,
    })
    request = urllib.request.Request(f"{GEOCODE_URL}?{query}",
                                     headers={"User-Agent": GEOCODE_AGENT})
    with urllib.request.urlopen(request, timeout=GEOCODE_TIMEOUT_S) as response:
        data = json.load(response)

    address = data.get("address") or {}
    town = next((address[k] for k in ("city", "town", "village", "hamlet",
                                     "municipality", "suburb", "county")
                 if address.get(k)), None)
    region = address.get("state") or address.get("region")
    country = (address.get("country_code") or "").upper()
    place = ", ".join(part for part in (town, region, country) if part)
    return place or data.get("display_name"), data.get("display_name")


def set_geo(**changes):
    """Update the geocoding slot, repainting only when something really changed."""
    with state_lock:
        current = dict(shared_state["geo"])
        merged = {**current, **changes}
        if merged == current:
            return
        shared_state["geo"] = merged
        shared_state["revision"] += 1


def geocode_loop(args, stop_event):
    """Name the current position, cheaply: cached, throttled, movement-driven."""
    cache = {}
    asked_key = None
    last_call = 0.0
    while not stop_event.is_set():
        with state_lock:
            report = dict(shared_state["report"])
        lat, lon = report.get("latitude"), report.get("longitude")
        if report.get("fix") not in ("2D", "3D") or lat is None or lon is None:
            set_geo(status="waiting for a valid fix", place=None, detail=None)
            stop_event.wait(1.0)
            continue

        # ~3 decimals is about 100 m: enough to reuse an answer while parked
        key = (round(lat, 3), round(lon, 3))
        if key in cache:
            place, detail = cache[key]
            set_geo(place=place, detail=detail, status=None)
            asked_key = key
            stop_event.wait(1.0)
            continue

        moved_enough = asked_key is None or metres_between(
            (lat, lon), (asked_key[0], asked_key[1])) >= GEOCODE_MOVE_M
        elapsed = time.monotonic() - last_call
        if not moved_enough or elapsed < args.geocode_interval:
            stop_event.wait(1.0)
            continue

        set_geo(status="looking up…")
        last_call = time.monotonic()
        try:
            place, detail = geocode_fetch(lat, lon)
        except Exception as exc:                 # network, HTTP, JSON: all the same
            set_geo(status=f"geocode failed: {type(exc).__name__}")
            stop_event.wait(max(5.0, args.geocode_interval))
            continue
        cache[key] = (place, detail)
        asked_key = key
        set_geo(place=place, detail=detail, status=None)
        stop_event.wait(1.0)


# --------------------------------------------------------------------------- #
# street basemap: the roads under the track, from OSM Overpass
# --------------------------------------------------------------------------- #

def track_bbox(track, pad_m=250.0):
    """Bounding box around the track, padded, as (south, west, north, east)."""
    lats = [lat for lat, _ in track]
    lons = [lon for _, lon in track]
    lat0 = (min(lats) + max(lats)) / 2
    pad_lat = pad_m / M_PER_DEG_LAT
    pad_lon = pad_m / (M_PER_DEG_LON * max(math.cos(math.radians(lat0)), 0.01))
    return (min(lats) - pad_lat, min(lons) - pad_lon,
            max(lats) + pad_lat, max(lons) + pad_lon)


def bbox_covers(outer, inner):
    """Is `inner` fully inside `outer`? Decides whether a refetch is needed."""
    if not outer:
        return False
    return (outer[0] <= inner[0] and outer[1] <= inner[1]
            and outer[2] >= inner[2] and outer[3] >= inner[3])


def fetch_basemap(bbox):
    """Ask Overpass for the roads in this box. Returns [(name, kind, points)]."""
    south, west, north, east = bbox
    query = (f"[out:json][timeout:25];"
             f'way["highway"~"^({BASEMAP_ROADS})$"]'
             f"({south:.5f},{west:.5f},{north:.5f},{east:.5f});"
             f"out geom;")
    request = urllib.request.Request(
        OVERPASS_URL,
        data=urllib.parse.urlencode({"data": query}).encode(),
        headers={"User-Agent": GEOCODE_AGENT},
    )
    with urllib.request.urlopen(request, timeout=OVERPASS_TIMEOUT_S) as response:
        data = json.load(response)

    ways = []
    for element in data.get("elements", []):
        points = [(node["lat"], node["lon"]) for node in element.get("geometry") or []
                  if node.get("lat") is not None and node.get("lon") is not None]
        if len(points) >= 2:
            tags = element.get("tags") or {}
            ways.append((tags.get("name"), tags.get("highway"), points))
    return ways[:BASEMAP_MAX_WAYS]


def set_basemap(**changes):
    with state_lock:
        current = dict(shared_state["basemap"])
        merged = {**current, **changes}
        if merged == current:
            return
        shared_state["basemap"] = merged
        shared_state["revision"] += 1


def basemap_loop(args, stop_event):
    """Keep a road network around the track, refetched only when it moves off it."""
    last_call = 0.0
    while not stop_event.is_set():
        with state_lock:
            track = list(shared_state["track"])
            have = shared_state["basemap"]["bbox"]
        if not track:
            set_basemap(status="waiting for a valid fix")
            stop_event.wait(2.0)
            continue

        needed = track_bbox(track)
        if bbox_covers(have, needed):
            stop_event.wait(2.0)
            continue
        if time.monotonic() - last_call < BASEMAP_MIN_INTERVAL_S:
            stop_event.wait(2.0)
            continue

        # fetch a generously padded box, so driving on does not refetch at once
        box = track_bbox(track, pad_m=1200.0)
        set_basemap(status="loading roads…")
        last_call = time.monotonic()
        try:
            ways = fetch_basemap(box)
        except Exception as exc:
            set_basemap(status=f"basemap failed: {type(exc).__name__}")
            stop_event.wait(BASEMAP_MIN_INTERVAL_S)
            continue
        set_basemap(ways=ways, bbox=box,
                    status=None if ways else "no roads in this area")


# --------------------------------------------------------------------------- #
# listeners
# --------------------------------------------------------------------------- #

def udp_loop(args, stop_event):
    """Bind the UDP port and feed every datagram into the table."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((args.host, args.port))
    except OSError as exc:
        bump(last_error=f"udp bind {args.host}:{args.port}: {exc}", listening=False)
        sock.close()
        return
    sock.settimeout(0.5)                  # so ctrl-c is honoured promptly
    bump(listening=True)
    try:
        while not stop_event.is_set():
            try:
                data, addr = sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError as exc:
                bump(last_error=f"udp recv: {exc}")
                break
            record_datagram(f"{addr[0]}:{addr[1]}", data)
    finally:
        bump(listening=False)
        sock.close()


def tcp_connection(conn, addr, stop_event):
    """Read one connection line by line: a TCP sender may split the report."""
    source = f"{addr[0]}:{addr[1]}"
    buffer = b""
    conn.settimeout(0.5)
    with conn:
        while not stop_event.is_set():
            try:
                chunk = conn.recv(65535)
            except socket.timeout:
                continue
            except OSError:
                break
            if not chunk:
                break                     # peer closed
            buffer += chunk
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                if line.strip():
                    record_datagram(source, line)
        if buffer.strip():                # last report arrived without a newline
            record_datagram(source, buffer)


def tcp_loop(args, stop_event):
    """Accept TCP connections and hand each one to its own reader thread."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((args.host, args.port))
        sock.listen(5)
    except OSError as exc:
        bump(last_error=f"tcp bind {args.host}:{args.port}: {exc}", listening=False)
        sock.close()
        return
    sock.settimeout(0.5)
    bump(listening=True)
    try:
        while not stop_event.is_set():
            try:
                conn, addr = sock.accept()
            except socket.timeout:
                continue
            except OSError as exc:
                bump(last_error=f"tcp accept: {exc}")
                break
            threading.Thread(target=tcp_connection, args=(conn, addr, stop_event),
                             daemon=True).start()
    finally:
        bump(listening=False)
        sock.close()


# --------------------------------------------------------------------------- #
# demo generator
# --------------------------------------------------------------------------- #

def taip_checksum(text):
    """TAIP checksum: XOR of every byte from '>' through '*' inclusive."""
    checksum = 0
    for ch in text:
        checksum ^= ord(ch)
    return checksum


def _split_dm(value, scale):
    """Split a positive number into integer part and `scale`-fraction, safely."""
    whole = int(value)
    frac = int(round((value - whole) * scale))
    if frac >= scale:                     # rounding pushed us to the next integer
        whole += 1
        frac -= scale
    return whole, frac


def route_legs(route):
    """Precompute (start, end, length_m, bearing) for each segment of a route."""
    legs = []
    for start, end in zip(route, route[1:] + route[:1]):     # closed loop
        x, y = metres_offset(end[0], end[1], start[0], start[1])
        length = math.hypot(x, y)
        heading = int(round(math.degrees(math.atan2(x, y)))) % 360
        legs.append((start, end, length, heading))
    return legs


@dataclass
class DemoDrive:
    """A vehicle driving laps around Hopkins, MN, sampled once per report.

    A random walk over lat/lon looks like scattered noise on a map, which is
    exactly what it is. Following a real street loop instead gives a track worth
    drawing, and heading/speed that agree with the positions.
    """
    voltage: float = DEMO_VOLTAGE
    speed_mph: float = DEMO_SPEED_MPH
    leg: int = 0                          # which route segment we are on
    along_m: float = 0.0                  # metres travelled into that segment
    latitude: float = DEMO_ROUTE[0][0]
    longitude: float = DEMO_ROUTE[0][1]
    heading: int = 0

    def __post_init__(self):
        self.legs = route_legs(DEMO_ROUTE)

    def step(self, elapsed_s):
        """Advance along the loop for `elapsed_s`, then read off the new position."""
        self.voltage = round(min(14.4, max(10.6, self.voltage + random.uniform(-0.15, 0.15))), 2)
        # traffic: speed wanders, and corners are taken slower than straights
        self.speed_mph = min(45.0, max(8.0, self.speed_mph + random.uniform(-5, 5)))
        distance = self.speed_mph * MPH_TO_MS * max(elapsed_s, 0.1)

        while distance > 0:
            start, end, length, heading = self.legs[self.leg]
            remaining = length - self.along_m
            if distance < remaining:
                self.along_m += distance
                distance = 0.0
            else:                         # turn the corner into the next street
                distance -= remaining
                self.leg = (self.leg + 1) % len(self.legs)
                self.along_m = 0.0
            start, end, length, heading = self.legs[self.leg]
            frac = self.along_m / length if length else 0.0
            self.latitude = round(start[0] + (end[0] - start[0]) * frac, 5)
            self.longitude = round(start[1] + (end[1] - start[1]) * frac, 5)
            self.heading = heading
        return self

    def sentence(self):
        """Render the report exactly the way mqtt_power.py builds TAIP PV."""
        now = time.gmtime()
        tod = now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec
        lat_i, lat_f = _split_dm(abs(self.latitude), 100000)
        lon_i, lon_f = _split_dm(abs(self.longitude), 100000)
        body = (f"RPV{tod:05d}"
                f"{'+' if self.latitude >= 0 else '-'}{lat_i:02d}{lat_f:05d}"
                f"{'+' if self.longitude >= 0 else '-'}{lon_i:03d}{lon_f:05d}"
                f"{min(int(round(self.speed_mph)), 999):03d}{self.heading:03d}12")
        prefix = f">{body};ID={DEMO_VEHICLE_ID};VOLT={self.voltage};*"
        return f"{prefix}{taip_checksum(prefix):02X}<"


def demo_loop(args, stop_event):
    """No device: drive the Hopkins loop and feed TAIP through the real decoder."""
    drive = DemoDrive()
    while not stop_event.is_set():
        record_datagram("127.0.0.1:0 (demo)", drive.step(args.interval).sentence())
        stop_event.wait(args.interval)


# --------------------------------------------------------------------------- #
# rendering
# --------------------------------------------------------------------------- #

SPARK = "▁▂▃▄▅▆▇█"
COMPASS = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
           "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
ARROWS = ["↑", "↗", "→", "↘", "↓", "↙", "←", "↖"]


def spark(values, lo, hi, width=HISTORY_LEN):
    """Sparkline padded to a constant width so column sizes never jump."""
    out = []
    for value in list(values)[-width:]:
        frac = (value - lo) / (hi - lo) if hi > lo else 0.0
        frac = min(max(frac, 0.0), 1.0)
        out.append(SPARK[int(frac * (len(SPARK) - 1))])
    return "".join(out).ljust(width)


def is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def voltage_color(value):
    """Rough 12 V automotive supply: out of band is red, near the edges yellow."""
    if value < 11.0 or value > 15.0:
        return "red"
    if value < 11.8 or value > 14.6:
        return "yellow"
    return "green"


def voltage_bar(value, width=18):
    """Gauge drawn against a fixed 9–15 V scale, so the bar means the same thing."""
    if not is_number(value):
        return "—"
    frac = (value - VOLTAGE_MIN) / (VOLTAGE_MAX - VOLTAGE_MIN)
    frac = min(max(frac, 0.0), 1.0)
    filled = int(width * frac)
    color = voltage_color(value)
    return f"[{color}]{'█' * filled}{'░' * (width - filled)}[/{color}] {value:6.2f} V"


def voltage_window(history):
    """Sparkline bounds: follow the data, but keep a floor so noise is not a mountain."""
    if not history:
        return 0.0, 1.0
    lo, hi = min(history), max(history)
    if hi - lo < 0.2:
        mid = (lo + hi) / 2
        lo, hi = mid - 0.1, mid + 0.1
    return lo, hi


def speed_window(history):
    """Speed is charted from 0, so a standstill reads as a standstill."""
    if not history:
        return 0.0, 1.0
    return 0.0, max(max(history), 5.0)


def has_fix(report):
    return report.get("fix") in ("2D", "3D")


def coord_str(report, key, hemis):
    """Latitude/longitude cell: dimmed and flagged while there is no fix."""
    value = report.get(key)
    if not is_number(value):
        return "—"
    letter = hemis[0] if value >= 0 else hemis[1]
    text = f"{value:+.5f}°  [dim]{abs(value):.5f}° {letter}[/dim]"
    if not has_fix(report):
        return f"[dim]{value:+.5f}°  (no fix — not a real position)[/dim]"
    return text


def heading_str(report):
    if not is_number(report.get("heading")):
        return "—"
    heading = report["heading"] % 360
    point = COMPASS[int((heading + 11.25) % 360 / 22.5)]
    arrow = ARROWS[int((heading + 22.5) % 360 / 45)]
    return f"{arrow} {heading:3d}°  [dim]{point}[/dim]"


def speed_str(report):
    speed = report.get(SPEED_KEY)
    if not is_number(speed):
        return "—"
    return f"{speed:3d} mph  [dim]{speed * MPH_TO_KMH:5.1f} km/h[/dim]"


def fix_str(report):
    fix = report.get("fix")
    if fix is None:
        return "—"
    age = report.get("age")
    age_text = f"  [dim]age={age}[/dim]" if is_number(age) else ""
    if fix == "no-fix":
        return f"[bold red]● no fix[/bold red]{age_text}"
    return f"[bold green]● {fix} fix[/bold green]{age_text}"


def tod_str(report):
    """TAIP time of day is seconds since midnight UTC: show it as a clock."""
    tod = report.get("tod")
    if not is_number(tod):
        return "—"
    hours, rest = divmod(int(tod), 3600)
    minutes, seconds = divmod(rest, 60)
    skew = ""
    now = datetime.now(timezone.utc)
    now_tod = now.hour * 3600 + now.minute * 60 + now.second
    delta = (now_tod - int(tod) + 43200) % 86400 - 43200
    if abs(delta) > 5:
        skew = f"  [dim]({delta:+d}s vs this host)[/dim]"
    return f"{hours:02d}:{minutes:02d}:{seconds:02d} UTC  [dim]({int(tod)})[/dim]{skew}"


def checksum_str(report):
    state = report.get("checksum")
    if state is True:
        return "[green]OK[/green]"
    if state is False:
        return "[bold red]BAD — sentence corrupted in transit[/bold red]"
    return "[dim]n/a (no checksum in the sentence)[/dim]"


def maps_str(report):
    lat, lon = report.get("latitude"), report.get("longitude")
    if not (is_number(lat) and is_number(lon) and has_fix(report)):
        return "[dim]—[/dim]"
    return f"[dim]https://maps.google.com/?q={lat:.5f},{lon:.5f}[/dim]"


def place_str(args):
    """City/region cell: the name when we have it, why we don't when we don't."""
    with state_lock:
        geo = dict(shared_state["geo"])
    if not args.geocode:
        return "[dim]off (--no-geocode / --offline)[/dim]"
    if geo.get("place"):
        return f"[bold]{escape(geo['place'])}[/bold]"
    return f"[dim]{escape(str(geo.get('status') or 'waiting…'))}[/dim]"


def address_str(args):
    with state_lock:
        detail = shared_state["geo"].get("detail")
    if not (args.geocode and detail):
        return "[dim]—[/dim]"
    return f"[dim]{escape(detail)}[/dim]"


# Braille cells are 2x4 subpixels, so one character holds 8 plottable dots and a
# subpixel comes out roughly square in a normal terminal font.
BRAILLE_BASE = 0x2800
BRAILLE_DOTS = ((0x01, 0x08),
                (0x02, 0x10),
                (0x04, 0x20),
                (0x40, 0x80))


def line_points(start, end):
    """Bresenham between two subpixels: consecutive fixes become a drawn road.

    Plotting only the fixes themselves leaves a handful of unconnected specks —
    at one report every 10 s the gaps are hundreds of metres wide.
    """
    x0, y0 = start
    x1, y1 = end
    dx, dy = abs(x1 - x0), abs(y1 - y0)
    step_x = 1 if x1 > x0 else -1
    step_y = 1 if y1 > y0 else -1
    error = dx - dy
    while True:
        yield x0, y0
        if x0 == x1 and y0 == y1:
            return
        doubled = 2 * error
        if doubled > -dy:
            error -= dy
            x0 += step_x
        if doubled < dx:
            error += dx
            y0 += step_y


def map_geometry(track, width, max_height):
    """Pick the canvas height, centre and metres-per-subpixel for this track.

    The height follows the shape of the route: a 3 km east-west loop in a tall
    square canvas is mostly empty space, which is what made the panel look
    broken. Both axes keep the same scale, so nothing is stretched.
    """
    sub_w = width * 2
    lats = [lat for lat, _ in track]
    lons = [lon for _, lon in track]
    lat0 = (min(lats) + max(lats)) / 2
    lon0 = (min(lons) + max(lons)) / 2

    offsets = [metres_offset(lat, lon, lat0, lon0) for lat, lon in track]
    span_x = max(x for x, _ in offsets) - min(x for x, _ in offsets)
    span_y = max(y for _, y in offsets) - min(y for _, y in offsets)
    span_x = max(span_x, MAP_MIN_SPAN_M)
    span_y = max(span_y, MAP_MIN_SPAN_M * 0.25)

    # subpixels are square, so matching the aspect ratio means sub_h/sub_w =
    # span_y/span_x; 4 subpixels per character row, and leave a little margin
    rows = math.ceil(sub_w * (span_y / span_x) * 1.15 / 4)
    height = min(max(rows, 6), max(6, max_height))
    sub_h = height * 4
    # 12% margin: without it the newest fix lands on the last column, where the
    # "you are here" marker is the first thing a narrow panel crops away
    scale = max(span_x * 1.12 / (sub_w - 1), span_y * 1.12 / (sub_h - 1))
    return offsets, height, sub_w, sub_h, scale, lat0, lon0


def plot_polyline(grid, pixels, width, height, sub_w, sub_h):
    """Draw a projected polyline into a braille grid, clipping to the canvas."""
    for start, end in zip(pixels, pixels[1:]):
        # a road that only passes far outside the box costs nothing to skip
        if (max(start[0], end[0]) < -sub_w or min(start[0], end[0]) > 2 * sub_w
                or max(start[1], end[1]) < -sub_h or min(start[1], end[1]) > 2 * sub_h):
            continue
        for col, row in line_points(start, end):
            if 0 <= col < sub_w and 0 <= row < sub_h:
                grid[row // 4][col // 2] |= BRAILLE_DOTS[row % 4][col % 2]


def street_labels(ways, project, width, height, taken):
    """Pick a few street names and a free spot on the canvas to write them in.

    Biggest road first (a boulevard before a cul-de-sac, and the longer of two
    equals), one label per name, and only where the text fits without landing on
    a label already placed: a legible map beats a complete one.
    """
    scored = []
    for name, kind, points in ways:
        if not name:
            continue
        pixels = [project(point) for point in points]
        inside = [(col, row) for col, row in pixels
                  if 0 <= col < width * 2 and 0 <= row < height * 4]
        if len(inside) < 2:
            continue
        length = sum(math.hypot(b[0] - a[0], b[1] - a[1])
                     for a, b in zip(inside, inside[1:]))
        scored.append((ROAD_RANK.get(kind, 0), length, name, inside))
    scored.sort(key=lambda item: (-item[0], -item[1]))

    labels = {}
    seen = set()
    for _rank, _length, name, inside in scored:
        if len(labels) >= BASEMAP_LABELS or name in seen:
            continue
        col, row = inside[len(inside) // 2]
        cell_row, cell_col = row // 4, col // 2
        text = name if len(name) <= 18 else name[:17] + "…"
        start = cell_col + 1
        if start + len(text) > width:            # would run off the right edge
            start = cell_col - len(text) - 1
        if start < 0 or cell_row >= height:
            continue
        spots = {(cell_row, start + offset) for offset in range(len(text))}
        if spots & taken:
            continue
        taken |= spots
        seen.add(name)
        labels[(cell_row, start)] = text
    return labels


def render_cells(cells):
    """Turn a row of (char, style) into markup, grouping runs of equal style."""
    parts = []
    run_style, run_chars = None, []
    for char, style in cells:
        if style != run_style:
            if run_chars:
                parts.append((run_style, "".join(run_chars)))
            run_style, run_chars = style, [char]
        else:
            run_chars.append(char)
    if run_chars:
        parts.append((run_style, "".join(run_chars)))
    return "".join(f"[{style}]{escape(text)}[/{style}]" if style else escape(text)
                   for style, text in parts)


def map_lines(track, width, max_height, basemap=None):
    """Draw the streets, the track on top, and the street names over both."""
    offsets, height, sub_w, sub_h, scale, lat0, lon0 = map_geometry(
        track, width, max_height)

    def to_subpixel(offset):
        x, y = offset
        return (int(round(sub_w / 2 + x / scale)), int(round(sub_h / 2 - y / scale)))

    def project(point):
        return to_subpixel(metres_offset(point[0], point[1], lat0, lon0))

    # two tiers, like a paper map: through roads legible, side streets faint
    major = [[0] * width for _ in range(height)]
    minor = [[0] * width for _ in range(height)]
    ways = (basemap or {}).get("ways") or []
    for _name, kind, points in ways:
        layer = major if ROAD_RANK.get(kind, 0) >= 2 else minor
        plot_polyline(layer, [project(point) for point in points],
                      width, height, sub_w, sub_h)

    trail = [[0] * width for _ in range(height)]
    pixels = [to_subpixel(offset) for offset in offsets]
    plot_polyline(trail, pixels, width, height, sub_w, sub_h)
    if len(pixels) == 1:                  # a single fix still deserves a dot
        col, row = pixels[0]
        if 0 <= col < sub_w and 0 <= row < sub_h:
            trail[row // 4][col // 2] |= BRAILLE_DOTS[row % 4][col % 2]

    # where the drive started and where it is now, one character each
    markers = {}
    if len(pixels) > 1:
        col, row = pixels[0]
        markers[(row // 4, col // 2)] = ("○", "bold yellow")
    col, row = pixels[-1]
    markers[(row // 4, col // 2)] = ("◉", "bold green")

    taken = set(markers)
    labels = street_labels(ways, project, width, height, taken) if ways else {}

    lines = []
    for row_index in range(height):
        cells = []
        for col_index in range(width):
            if (row_index, col_index) in markers:
                cells.append(markers[(row_index, col_index)])
            elif trail[row_index][col_index]:
                cells.append((chr(BRAILLE_BASE + trail[row_index][col_index]), "bold cyan"))
            elif major[row_index][col_index]:
                cells.append((chr(BRAILLE_BASE + major[row_index][col_index]), "grey58"))
            elif minor[row_index][col_index]:
                cells.append((chr(BRAILLE_BASE + minor[row_index][col_index]), "grey30"))
            else:
                cells.append((" ", None))
        for (label_row, label_col), text in labels.items():
            if label_row == row_index:
                for offset, char in enumerate(text):
                    cells[label_col + offset] = (char, "yellow")
        lines.append(render_cells(cells))

    travelled = sum(metres_between(a, b) for a, b in zip(track, track[1:]))
    bar = 10                              # 10 characters of scale bar = 20 subpixels
    footer = (f"├{'─' * (bar - 2)}┤ {distance_str(scale * bar * 2)}  ·  "
              f"{len(track)} fixes, {distance_str(travelled)} driven")
    status = (basemap or {}).get("status")
    if status:
        footer += f"  ·  {status}"
    elif ways:
        footer += f"  ·  {len(ways)} OSM roads"
    lines.append(f"[dim]{escape(footer)}[/dim]")
    return lines


def map_panel(width, room):
    """Mini map: OSM streets, the track over them, and where the device is now."""
    with state_lock:
        track = list(shared_state["track"])
        basemap = dict(shared_state["basemap"])
        place = shared_state["geo"].get("place")
    title = f"Track · {escape(place)}" if place else "Track"
    # the canvas may end up shorter than the room available: map_lines sizes it
    # to the shape of the route instead of stretching it to fill the panel
    if track and width >= 8:
        body = map_lines(track, width, max(4, room - 3), basemap)
    else:
        body = ["", "  [dim]no valid fix yet — nothing to plot[/dim]"]
    text = Text.from_markup("\n".join(body[:room - 2]))
    text.no_wrap = True                   # one canvas row stays one screen row
    text.overflow = "crop"
    return Panel(text, title=title, border_style="grey42")


def cell(value):
    """Fallback formatting for a report field we know nothing about."""
    if isinstance(value, float):
        return f"{value:.2f}"
    return escape(str(value))


def box_row(boxes, gap, arrow_row, arrow_labels):
    """Render side-by-side ASCII boxes joined by labelled arrows.

    boxes:        [(width, [interior lines]), ...]
    arrow_labels: [(text above the arrow, text below), ...] one per gap
    """
    height = max(len(lines) for _, lines in boxes) + 2
    columns = []
    for width, lines in boxes:
        inner = width - 2
        col = ["┌" + "─" * inner + "┐"]
        for i in range(height - 2):
            body = lines[i] if i < len(lines) else ""
            col.append("│ " + body[:inner - 2].ljust(inner - 2) + " │")
        col.append("└" + "─" * inner + "┘")
        columns.append(col)

    arrow = " " + "─" * (gap - 3) + "▶ "
    rows = []
    for i in range(height):
        row = columns[0][i]
        for index, col in enumerate(columns[1:]):
            top, bottom = arrow_labels[index]
            if i == arrow_row:
                joiner = arrow
            elif i == arrow_row - 1:
                joiner = top.center(gap)
            elif i == arrow_row + 1:
                joiner = bottom.center(gap)
            else:
                joiner = " " * gap
            row += joiner + col[i]
        rows.append(row)
    return rows


def topology_panel(args):
    with state_lock:
        listening = shared_state["listening"]
        pkts = shared_state["pkts_recv"]
        sentences = shared_state["sentences"]
        demo = shared_state["demo"]
        source = shared_state["last_source"]
        report = dict(shared_state["report"])

    proto = args.proto.upper()
    if demo:
        status = "● simulated"
        bind_line = "SIMULATED (demo)"
    else:
        status = "● listening" if listening else "● offline  "
        bind_line = f"{args.host}:{args.port}"

    device = str(report.get("vehicle_id") or "unknown")[:15]
    origin = (source or "waiting…")[:15]

    rows = box_row(
        boxes=[
            (19, ["DEVICE", f"ID={device}", origin, f"packets: {pkts}"]),
            (26, [f"{proto} LISTENER", bind_line, "no auth · no TLS",
                  f"sentences: {sentences}"]),
            (19, ["DASHBOARD", "gps_dashboard", status, f"decoded: {sentences}"]),
        ],
        gap=11,
        arrow_row=2,
        arrow_labels=[(f"{proto.lower()} send", args.format_label), ("decode", "TAIP/NMEA")],
    )

    text = Text("\n".join(rows))
    text.highlight_regex(r"[┌┐└┘─│▶]", "grey42")
    text.highlight_words(["● listening", "● simulated"], "bold green")
    text.highlight_words(["● offline"], "bold red")
    text.highlight_words(["DEVICE", f"{proto} LISTENER", "DASHBOARD"], "bold white")
    text.highlight_words(["decode", "TAIP/NMEA", f"{proto.lower()} send",
                          args.format_label], "cyan")
    text.highlight_words([bind_line, f"ID={device}"], "yellow")
    return text


KNOWN_FIELDS = ("kind", "checksum", "received", "tod", "latitude", "longitude",
                SPEED_KEY, "heading", "fix", "age", "vehicle_id", VOLTAGE_KEY)


def report_table(args):
    with state_lock:
        report = dict(shared_state["report"])
        history = {k: list(v) for k, v in shared_state["history"].items()}

    table = Table(title="Current location report (decoded from the wire)",
                  expand=True, title_style="bold")
    table.add_column("Field", style="bold", width=18, no_wrap=True)
    table.add_column("Value", width=38, no_wrap=True)
    table.add_column("Last samples", style="cyan", ratio=1, no_wrap=True, overflow="crop")

    if not report:
        table.add_row("—", f"waiting for {args.proto} reports on "
                           f"{args.host}:{args.port}…", "")
        return table

    volt_hist = history.get(VOLTAGE_KEY, [])
    volt_lo, volt_hi = voltage_window(volt_hist)
    speed_hist = history.get(SPEED_KEY, [])
    speed_lo, speed_hi = speed_window(speed_hist)

    table.add_row("Internal voltage", voltage_bar(report.get(VOLTAGE_KEY)),
                  spark(volt_hist, volt_lo, volt_hi))
    if volt_hist:
        table.add_row(
            "Min · avg · max",
            f"[dim]{min(volt_hist):.2f} · "
            f"{sum(volt_hist) / len(volt_hist):.2f} · {max(volt_hist):.2f} V[/dim]",
            f"[dim]scale {volt_lo:.2f} – {volt_hi:.2f} V, "
            f"{len(volt_hist)} samples[/dim]",
        )
    table.add_row("Latitude", coord_str(report, "latitude", "NS"), "")
    table.add_row("Longitude", coord_str(report, "longitude", "EW"), "")
    table.add_row("Place", place_str(args), address_str(args))
    table.add_row("Speed", speed_str(report),
                  spark(speed_hist, speed_lo, speed_hi) if speed_hist else "")
    table.add_row("Heading", heading_str(report), "")
    table.add_row("Fix", fix_str(report), "")
    table.add_row("Sentence", f"[bold]{escape(str(report.get('kind', '—')))}[/bold]"
                              f"   checksum {checksum_str(report)}", "")
    table.add_row("Vehicle ID", f"[bold]{escape(str(report.get('vehicle_id', '—')))}[/bold]", "")
    table.add_row("Report time", tod_str(report), "")
    table.add_row("Source field", "[dim];VOLT= (non-standard TAIP extension)[/dim]", "")
    table.add_row("Map", maps_str(report), "")

    # anything else a sentence decided to add (NMEA satellites, altitude, …)
    for key, value in report.items():
        if key not in KNOWN_FIELDS:
            table.add_row(escape(str(key)), cell(value), "")
    return table


def detail_tables(log_rows, raw_lines):
    """Message log and the last datagram, each capped to the rows it was given."""
    with state_lock:
        recent = list(shared_state["recent"])[:log_rows]
        raw = shared_state["last_raw"]
        decoded = list(shared_state["last_lines"])

    log = Table(title="Last sentences received", expand=True, title_style="bold")
    log.add_column("Time", style="dim", width=8, no_wrap=True)
    log.add_column("From", style="yellow", width=16, no_wrap=True, overflow="ellipsis")
    log.add_column("Sentence", style="white", ratio=1, no_wrap=True, overflow="ellipsis")
    if recent:
        for when, source, sentence in recent:
            # sentence and source come off the wire: brackets are not markup
            log.add_row(when, escape(str(source)), escape(sentence))
    else:
        log.add_row("—", "—", "no reports yet")

    if raw is None:
        body, title = ["(nothing received yet)"], "Raw datagram"
    else:
        body, title = [], "Raw datagram + decode"
        for line, summary in decoded:
            body.append(f"RAW : {line}")
            body.append(f"DEC : {summary}")
        if not body:                       # datagram with no printable sentence
            body = [" ".join(raw.split()) or "(empty)"]
    if len(body) > raw_lines:
        body = body[:max(1, raw_lines - 1)] + ["  …"]
    # no_wrap keeps one sentence on one screen row, so the height is predictable
    text = Text("\n".join(body), style="green", no_wrap=True, overflow="crop")
    return log, Panel(text, title=title, border_style="grey42")


def is_stale(args, last_receive):
    if not last_receive:
        return False
    return (datetime.now() - last_receive).total_seconds() > max(5.0, args.interval * 3)


def rendered_height(console, renderable):
    return len(console.render_lines(renderable, console.options.update(height=None), pad=False))


def render(args, console):
    with state_lock:
        err = shared_state["last_error"]
        demo = shared_state["demo"]
        last_receive = shared_state["last_receive"]
        pkts = shared_state["pkts_recv"]
        sentences = shared_state["sentences"]
        bad = shared_state["bad_checksum"]
        unknown = shared_state["unknown"]

    parts = [topology_panel(args), "", report_table(args)]
    if demo:
        parts.insert(0, Text(
            f"DEMO MODE — nothing bound on {args.host}:{args.port}, "
            "TAIP reports are generated locally",
            style="bold black on yellow",
        ))

    if last_receive:
        stamp = last_receive.strftime("%H:%M:%S")
        if is_stale(args, last_receive):
            stamp += "  [bold red](stale — nothing arriving)[/bold red]"
    else:
        stamp = "—"
    footer = (f"packets: {pkts}   sentences: {sentences}   bad checksum: {bad}   "
              f"unknown: {unknown}   last report: {stamp}   ctrl-c to quit")
    if err and not demo:
        footer += f"\n[bold red]error: {err}[/bold red]"

    def wrap(sections):
        return Panel(
            Group(*sections),
            title=f"[bold]TAIP/NMEA location dashboard[/bold] · "
                  f"{args.proto} {args.host}:{args.port}",
            subtitle=footer,
            border_style="blue",
        )

    # Measure what the essential part costs, then spend whatever rows are left on
    # the sentence log and the raw datagram. Guessing the height instead of
    # measuring it is what makes a dashboard overflow and scroll (= flicker).
    # one row spare: if the render fills the window exactly it scrolls, and a
    # scrolling live region is the flicker
    base = wrap(parts)
    room = console.size.height - 1 - rendered_height(console, base)
    if room < 6:
        return base

    return wrap(parts + [detail_row(console, room)])


def detail_row(console, room):
    """Bottom row: sentence log, mini map and raw datagram, as many as fit.

    The map needs a character width to plot into, and a grid column's real width
    is only known while rendering, so the share is computed here from the ratios
    and rounded *down*: too narrow just leaves a blank margin, while too wide
    would wrap and wrapping is what makes the layout scroll.
    """
    inner = console.size.width - 4          # inside the outer panel
    if inner >= 140:
        specs = [("log", 3), ("map", 3), ("raw", 2)]
    elif inner >= 96:
        specs = [("log", 3), ("map", 2)]
    else:
        specs = [("log", 3), ("raw", 2)]    # too narrow for a useful canvas
    total = sum(ratio for _, ratio in specs)

    # a log table costs 5 rows of chrome, the raw panel costs 2
    log, raw = detail_tables(max(1, min(RECENT_LEN, room - 5)), max(1, room - 2))
    grid = Table.grid(expand=True, padding=(0, 1))
    cells = []
    for name, ratio in specs:
        grid.add_column(ratio=ratio)
        if name == "log":
            cells.append(log)
        elif name == "raw":
            cells.append(raw)
        else:
            cells.append(map_panel(max(8, int(inner * ratio / total) - 6), room))
    grid.add_row(*cells)
    return grid


def state_signature(args, console):
    """Cheap fingerprint of everything the screen shows: redraw only when it changes."""
    with state_lock:
        revision = shared_state["revision"]
        last_receive = shared_state["last_receive"]
    size = console.size
    return (revision, is_stale(args, last_receive), size.width, size.height)


# --------------------------------------------------------------------------- #

def port_bindable(host, port, proto):
    """Can we take the port? Checked up front so the fallback happens before the UI."""
    kind = socket.SOCK_STREAM if proto == "tcp" else socket.SOCK_DGRAM
    sock = socket.socket(socket.AF_INET, kind)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def main():
    parser = argparse.ArgumentParser(
        description="Location-report listener (TAIP/NMEA over UDP/TCP) + live dashboard"
    )
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--proto", choices=("udp", "tcp"), default="udp")
    parser.add_argument("--interval", type=float, default=10.0,
                        help="expected seconds between reports (staleness + demo pacing)")
    parser.add_argument("--demo", action="store_true",
                        help="simulate reports, never bind a port")
    # Both OSM lookups are on by default: the whole point of the panel is to say
    # *where* the device is. They send the coordinates to openstreetmap.org, so
    # --offline (or the individual flags) keeps everything on this machine.
    parser.add_argument("--no-geocode", dest="geocode", action="store_false",
                        help="do not name the position via OSM Nominatim")
    parser.add_argument("--no-basemap", dest="basemap", action="store_false",
                        help="do not fetch OSM streets under the track")
    parser.add_argument("--offline", action="store_true",
                        help="no network lookups at all: implies "
                             "--no-geocode --no-basemap")
    parser.add_argument("--geocode-interval", type=float, default=15.0,
                        help="minimum seconds between geocoding requests")
    parser.add_argument("--no-fallback", action="store_true",
                        help="exit instead of falling back to demo mode")
    parser.add_argument("--altscreen", action="store_true",
                        help="draw on the alternate screen instead of inline")
    args = parser.parse_args()
    args.format_label = "TAIP/NMEA"       # shown on the topology arrow
    if args.offline:
        args.geocode = args.basemap = False

    console = Console()
    if args.geocode or args.basemap:
        console.print("[yellow]OSM lookups on: the position is sent to "
                      "openstreetmap.org (--offline to disable)[/yellow]")

    demo = args.demo
    if not demo and not port_bindable(args.host, args.port, args.proto):
        if args.no_fallback:
            console.print(f"[bold red]cannot bind {args.proto} "
                          f"{args.host}:{args.port} (port in use?)[/bold red]")
            console.print("free the port, or pick another one with --port")
            raise SystemExit(1)
        console.print(f"[yellow]cannot bind {args.proto} {args.host}:{args.port} "
                      f"→ demo mode[/yellow]")
        demo = True
        time.sleep(1.0)

    with state_lock:
        shared_state["demo"] = demo

    stop_event = threading.Event()
    if demo:
        threading.Thread(target=demo_loop, args=(args, stop_event), daemon=True).start()
    else:
        listener = tcp_loop if args.proto == "tcp" else udp_loop
        threading.Thread(target=listener, args=(args, stop_event), daemon=True).start()
    if args.geocode:
        threading.Thread(target=geocode_loop, args=(args, stop_event), daemon=True).start()
    if args.basemap:
        threading.Thread(target=basemap_loop, args=(args, stop_event), daemon=True).start()

    # No auto-refresh and no timed redraws: the screen is repainted only when the
    # state actually changed, and each repaint is wrapped in synchronized output
    # so the terminal shows the new frame in one go instead of mid-redraw.
    try:
        with Live(console=console, auto_refresh=False, screen=args.altscreen,
                  vertical_overflow="crop") as live:
            pending = drawn = None
            while True:
                current = state_signature(args, console)
                # repaint once the state has settled for a tick: the three
                # sentences of an NMEA datagram land milliseconds apart, this
                # coalesces them into a single repaint instead of three in a row
                if current == pending and current != drawn:
                    console.file.write(SYNC_BEGIN)
                    live.update(render(args, console), refresh=True)
                    console.file.write(SYNC_END)
                    console.file.flush()
                    drawn = current
                pending = current
                time.sleep(0.1)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        time.sleep(0.3)


if __name__ == "__main__":
    main()
