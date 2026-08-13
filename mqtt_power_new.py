#!/usr/bin/env python3
"""Publish the Digi TX40's internal (power-input) voltage to an MQTT broker.

Target: Digi TX40 (DAL OS). The power-input voltage is exposed through the
runtime database (runt) at ``system.supply_voltage`` (value already in volts,
e.g. 12.1). That key is tried directly; if a future firmware renames it the
script falls back to walking the runt tree (system/metrics first) to
auto-detect a voltage-like key. Set TX40_VOLTAGE_KEY to pin it explicitly.

Falls back to a FAKE_VOLTAGE env var when the digidevice module is absent, so
the script can be tested off-device. Only the internal voltage is published
(plus a timestamp for the reading).

Optionally, when LOC_HOST is set, the script ALSO builds a GPS position report
in TAIP (default) or NMEA and sends it over UDP/TCP to a location server, with
the internal voltage attached in a non-standard field (TAIP and NMEA have no
standard field for supply voltage). The location step is skipped entirely when
LOC_HOST is unset, so default behaviour is unchanged. Off-device the position
comes from FAKE_LAT / FAKE_LON (and friends) for testing.
"""

import json
import os
import socket
import sys
import time

import paho.mqtt.client as mqtt

try:
    from digidevice import runt  # alias of acl.runt on DAL OS
    ON_DAL = True
except ImportError:
    runt = None
    ON_DAL = False

HOST = os.getenv("MQTT_HOST", "10.10.65.214")
PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_BASE = os.getenv("MQTT_TOPIC_BASE", "router")
KEEPALIVE = 60
QOS = 0

# --- Location forwarding ------------------------------------------------------
# EDIT THESE for your deployment, or override any of them at runtime with the
# matching LOC_* environment variables. On the TX40 this script is usually
# pasted into a DAL scheduled task, which CANNOT set environment variables, so
# set the target here. Leave LOC_HOST_DEFAULT = "" to disable location
# forwarding entirely (voltage-over-MQTT still runs).
LOC_HOST_DEFAULT = "10.10.65.214"   # location server IP ("" disables the feature)
LOC_PORT_DEFAULT = 5000             # location server port
LOC_FORMAT_DEFAULT = "taip"         # "taip" or "nmea"
LOC_PROTO_DEFAULT = "udp"           # "udp" or "tcp"
LOC_VEHICLE_ID_DEFAULT = "0000"     # TAIP vehicle ID (4 chars)

LOC_HOST = os.getenv("LOC_HOST", LOC_HOST_DEFAULT) or None
LOC_PORT = int(os.getenv("LOC_PORT", str(LOC_PORT_DEFAULT)) or "0")
LOC_FORMAT = os.getenv("LOC_FORMAT", LOC_FORMAT_DEFAULT).lower()   # "taip" or "nmea"
LOC_PROTO = os.getenv("LOC_PROTO", LOC_PROTO_DEFAULT).lower()      # "udp" or "tcp"
LOC_VEHICLE_ID = os.getenv("LOC_VEHICLE_ID", LOC_VEHICLE_ID_DEFAULT)
# runt keys under "location." that describe the current GNSS fix.
LOCATION_KEYS = ("latitude", "longitude", "altitude", "direction",
                 "horizontal_velocity", "hdop", "num_satellites")

# Known direct key(s) for the internal voltage, tried before any tree walk.
KNOWN_KEYS = ("system.supply_voltage",)
# Runt subtrees most likely to hold the voltage, walked first for speed.
PRIORITY_ROOTS = ("system", "metrics")
# Leaf-name / path fragments that identify a voltage reading.
VOLT_LEAVES = ("vin", "v_in", "vsys", "vbatt", "vbat")
# Broader terms printed as hints when no voltage key is found.
RELATED_TERMS = ("volt", "vin", "vsys", "vbat", "power", "current",
                 "supply", "psu", "batt", "ignition", "chassis")


def to_num(value):
    """runt returns strings; convert to int/float when possible."""
    if value in (None, ""):
        return None
    for cast in (int, float):
        try:
            return cast(value)
        except (TypeError, ValueError):
            continue
    return value


def rget(key):
    """runt.get() that never raises on a missing/renamed key."""
    try:
        value = runt.get(key)
    except Exception:
        return None
    return value or None


def rkeys(key=None):
    """runt.keys() that never raises; [] for a leaf or on error. None = top level."""
    try:
        return (runt.keys(key) if key else runt.keys()) or []
    except Exception:
        return []


def walk_runt(prefix, depth=0):
    """Yield (full_key, value) for every leaf under prefix in the runt tree."""
    if depth > 8:  # defensive: the runt tree is shallow and finite
        return
    children = rkeys(prefix)
    if not children:  # leaf node
        value = rget(prefix)
        if value is not None:
            yield prefix, value
        return
    for child in children:
        yield from walk_runt(f"{prefix}.{child}", depth + 1)


def iter_all_leaves(limit=50000):
    """Yield (key, value) for every runt leaf, searching PRIORITY_ROOTS first."""
    top = rkeys()
    ordered = [r for r in PRIORITY_ROOTS if r in top]
    ordered += [r for r in top if r not in PRIORITY_ROOTS]
    count = 0
    for root in ordered:
        for key, value in walk_runt(root):
            count += 1
            if count > limit:
                return
            yield key, value


def is_voltage_key(key):
    k = key.lower()
    leaf = k.rsplit(".", 1)[-1]
    return "volt" in k or leaf in VOLT_LEAVES


def find_voltage_key():
    """Locate the runt key holding the internal voltage. Returns None if absent.

    Honors TX40_VOLTAGE_KEY; otherwise auto-detects by walking the runt tree.
    """
    override = os.getenv("TX40_VOLTAGE_KEY")
    if override:
        return override
    # Fast path: the known key, no tree walk needed.
    for key in KNOWN_KEYS:
        if rget(key) is not None:
            return key
    # Fallback: bounded scan of the likely subtrees.
    for root in PRIORITY_ROOTS:
        for key, _ in walk_runt(root):
            if is_voltage_key(key):
                return key
    return None


def read_voltage():
    """Return (source_key, voltage) for the TX40 internal voltage, or (None, None).

    The raw value is returned as-is (some firmware reports millivolts) so no
    scaling is silently applied.
    """
    key = find_voltage_key()
    if not key:
        return None, None
    return key, to_num(rget(key))


def report_missing():
    """Print hints to stderr when no voltage key was found."""
    override = os.getenv("TX40_VOLTAGE_KEY")
    if override:
        print(f"TX40_VOLTAGE_KEY={override} returned no value.", file=sys.stderr)
    # Full tree only when explicitly requested; otherwise stay in the likely roots.
    if os.getenv("DISCOVER_FULL"):
        leaves = iter_all_leaves()
        scope = "whole runt tree"
    else:
        leaves = (kv for root in PRIORITY_ROOTS for kv in walk_runt(root))
        scope = " + ".join(PRIORITY_ROOTS)
    print(f"No voltage-like runt key found. Related power keys ({scope}):", file=sys.stderr)
    shown = 0
    for key, val in leaves:
        if any(t in key.lower() for t in RELATED_TERMS):
            print(f"  {key} = {val}", file=sys.stderr)
            shown += 1
            if shown >= 40:
                print("  ... (truncated)", file=sys.stderr)
                break
    if not shown:
        print("  (none here — try DISCOVER_FULL=1 for a whole-tree scan)", file=sys.stderr)
    print("Set TX40_VOLTAGE_KEY to the right key once identified.", file=sys.stderr)


def read_location():
    """Return a dict describing the current GNSS fix.

    On-device the values come from the runt ``location.*`` subtree; off-device
    they come from FAKE_LAT / FAKE_LON (and optional friends) so the location
    path can be exercised without hardware. Missing values are left as None.
    """
    if ON_DAL:
        loc = {k: to_num(rget(f"location.{k}")) for k in LOCATION_KEYS}
        loc["quality"] = rget("location.quality") or ""
    else:
        loc = {
            "latitude": to_num(os.getenv("FAKE_LAT")),
            "longitude": to_num(os.getenv("FAKE_LON")),
            "altitude": to_num(os.getenv("FAKE_ALT", "0")),
            "direction": to_num(os.getenv("FAKE_HEADING", "0")),
            "horizontal_velocity": to_num(os.getenv("FAKE_SPEED_MS", "0")),
            "hdop": to_num(os.getenv("FAKE_HDOP", "0")),
            "num_satellites": to_num(os.getenv("FAKE_SATS", "0")),
            "quality": os.getenv("FAKE_QUALITY", "3D"),
        }
    return loc


def has_fix(loc):
    """True when the reading looks like a real position (not the 0/0 no-signal state)."""
    quality = (loc.get("quality") or "").lower()
    if "no fix" in quality or "invalid" in quality:
        return False
    lat, lon = loc.get("latitude"), loc.get("longitude")
    if not lat and not lon:  # 0/0 or None/None => no usable fix
        return False
    return True


def _f(loc, key, default=0.0):
    """Fetch a location value as a float, tolerating None/str."""
    try:
        return float(loc.get(key))
    except (TypeError, ValueError):
        return default


def taip_checksum(text):
    """TAIP checksum: XOR of every byte from '>' through '*' inclusive."""
    checksum = 0
    for ch in text:
        checksum ^= ord(ch)
    return checksum


def build_taip(loc, voltage, vehicle_id):
    """Build a TAIP PV (position/velocity) report with voltage in a ;VOLT= field.

    Layout: >RPV{tod:5}{±lat:2+5}{±lon:3+5}{speed:3}{heading:3}{fix}{age};ID=...;*CS<
    Latitude/longitude are decimal degrees; speed is mph; the ;VOLT= field is a
    non-standard extension (like the PS-team 5-char-ID approach) carried inside
    the checksummed range so the sentence stays checksum-valid.
    """
    lat, lon = _f(loc, "latitude"), _f(loc, "longitude")
    speed_mph = int(round(_f(loc, "horizontal_velocity") * 2.23694))
    heading = int(round(_f(loc, "direction"))) % 360
    now = time.gmtime()
    tod = now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec

    if has_fix(loc):
        fix_mode = 1 if "3d" in (loc.get("quality") or "").lower() else 0
        age = 2
    else:
        fix_mode, age = 9, 0

    lat_i, lat_f = _split_dm(abs(lat), 100000)
    lon_i, lon_f = _split_dm(abs(lon), 100000)
    body = (f"RPV{tod:05d}"
            f"{'+' if lat >= 0 else '-'}{lat_i:02d}{lat_f:05d}"
            f"{'+' if lon >= 0 else '-'}{lon_i:03d}{lon_f:05d}"
            f"{min(speed_mph, 999):03d}{heading:03d}{fix_mode:d}{age:d}")
    volt_field = f";VOLT={voltage}" if voltage is not None else ""
    prefix = f">{body};ID={vehicle_id}{volt_field};*"
    return f"{prefix}{taip_checksum(prefix):02X}<"


def _split_dm(value, scale):
    """Split a positive number into integer part and `scale`-fraction, rounding safely."""
    whole = int(value)
    frac = int(round((value - whole) * scale))
    if frac >= scale:  # rounding pushed us to the next integer
        whole += 1
        frac -= scale
    return whole, frac


def nmea_checksum(text):
    """NMEA checksum: XOR of every byte between '$' and '*' (both excluded)."""
    checksum = 0
    for ch in text:
        checksum ^= ord(ch)
    return checksum


def _nmea_coord(value, width, hemis):
    """Format decimal degrees as NMEA ddmm.mmmm and pick the hemisphere letter."""
    magnitude = abs(value)
    degrees = int(magnitude)
    minutes = (magnitude - degrees) * 60
    return f"{degrees:0{width}d}{minutes:07.4f}", hemis[0 if value >= 0 else 1]


def _wrap_nmea(body):
    """Prefix '$', append '*CS' checksum to an NMEA sentence body."""
    return f"${body}*{nmea_checksum(body):02X}"


def build_nmea(loc, voltage):
    """Build $GPRMC + $GPGGA plus a proprietary $PDGIVLT voltage sentence.

    $PDGIVLT is a non-standard Digi sentence carrying the supply voltage;
    servers that don't recognise it ignore it while still parsing position.
    """
    lat, lon = _f(loc, "latitude"), _f(loc, "longitude")
    lat_s, ns = _nmea_coord(lat, 2, "NS")
    lon_s, ew = _nmea_coord(lon, 3, "EW")
    now = time.gmtime()
    hhmmss, ddmmyy = time.strftime("%H%M%S", now), time.strftime("%d%m%y", now)
    fix = has_fix(loc)
    knots = _f(loc, "horizontal_velocity") * 1.94384
    heading = _f(loc, "direction")
    sats = int(_f(loc, "num_satellites"))
    hdop, alt = _f(loc, "hdop"), _f(loc, "altitude")

    rmc = (f"GPRMC,{hhmmss}.00,{'A' if fix else 'V'},{lat_s},{ns},{lon_s},{ew},"
           f"{knots:.1f},{heading:.1f},{ddmmyy},,,{'A' if fix else 'N'}")
    gga = (f"GPGGA,{hhmmss}.00,{lat_s},{ns},{lon_s},{ew},{1 if fix else 0},"
           f"{sats:02d},{hdop:.1f},{alt:.1f},M,0.0,M,,")
    sentences = [_wrap_nmea(rmc), _wrap_nmea(gga)]
    if voltage is not None:
        sentences.append(_wrap_nmea(f"PDGIVLT,{voltage}"))
    return "\r\n".join(sentences) + "\r\n"


def build_location_payload(loc, voltage):
    """Render the outgoing location report bytes for the configured LOC_FORMAT."""
    if LOC_FORMAT == "nmea":
        text = build_nmea(loc, voltage)
    else:
        text = build_taip(loc, voltage, LOC_VEHICLE_ID) + "\r\n"
    return text.encode("ascii", "replace")


def send_location_report(voltage, loc):
    """Send the position+voltage report to the location server. Best-effort.

    Returns True on success (or when the feature is disabled), False on error.
    """
    if not LOC_HOST:
        return True  # feature off; nothing to do
    if not LOC_PORT:
        print("LOC_HOST set but LOC_PORT is missing/0; skipping location send.",
              file=sys.stderr)
        return False

    data = build_location_payload(loc, voltage)
    try:
        if LOC_PROTO == "tcp":
            with socket.create_connection((LOC_HOST, LOC_PORT), timeout=5) as sock:
                sock.sendall(data)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.sendto(data, (LOC_HOST, LOC_PORT))
            finally:
                sock.close()
    except OSError as exc:
        print(f"Location send to {LOC_HOST}:{LOC_PORT} ({LOC_PROTO}) failed: {exc}",
              file=sys.stderr)
        return False

    if not has_fix(loc):
        print("Note: no GNSS fix; position sent as 0/0 with an invalid-fix flag.",
              file=sys.stderr)
    print(f"Sent {LOC_FORMAT.upper()} to {LOC_HOST}:{LOC_PORT} ({LOC_PROTO}): "
          f"{data.decode('ascii', 'replace').strip()}")
    return True


def new_client():
    """paho-mqtt 2.x requires an explicit callback API version; 1.x does not."""
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        return mqtt.Client()


def publish_mqtt(device, voltage, loc):
    """Publish the voltage (and lat/lon when a fix is present) over MQTT.

    Returns True on success, False on any connection/publish error.
    """
    payload = {"internal_voltage": voltage, "timestamp": int(time.time())}
    if has_fix(loc):
        payload["latitude"] = loc.get("latitude")
        payload["longitude"] = loc.get("longitude")
    topic = f"{TOPIC_BASE}/{device or 'tx40'}/voltage"

    client = new_client()
    try:
        client.connect(HOST, PORT, KEEPALIVE)
    except OSError as exc:
        print(f"Connection to {HOST}:{PORT} failed: {exc}", file=sys.stderr)
        return False

    client.loop_start()
    try:
        info = client.publish(topic, json.dumps(payload), qos=QOS)
        try:
            info.wait_for_publish(timeout=5)
        except TypeError:  # paho-mqtt < 1.6 has no timeout argument
            info.wait_for_publish()
    finally:
        client.loop_stop()
        client.disconnect()

    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        print(f"Publish failed: rc={info.rc}", file=sys.stderr)
        return False

    print(f"Published to {topic}: {json.dumps(payload)}")
    return True


def main():
    if ON_DAL:
        runt.start()

    try:
        if ON_DAL:
            device = rget("system.serial")
            source_key, voltage = read_voltage()
        else:
            device = os.getenv("MQTT_DEVICE", "tx40-test")
            source_key = "env:FAKE_VOLTAGE"
            voltage = to_num(os.getenv("FAKE_VOLTAGE"))

        if voltage is None:
            print("Internal voltage not available; nothing published.", file=sys.stderr)
            if ON_DAL:
                report_missing()
            else:
                print("Off-device: set FAKE_VOLTAGE to test.", file=sys.stderr)
            return 1

        loc = read_location()
        print(f"Voltage {voltage} from {source_key}.")
        # Both sends are best-effort and independent; either failing => exit 1.
        mqtt_ok = publish_mqtt(device, voltage, loc)
        loc_ok = send_location_report(voltage, loc)
        return 0 if (mqtt_ok and loc_ok) else 1
    finally:
        if ON_DAL:
            runt.stop()


if __name__ == "__main__":
    sys.exit(main())
