#!/usr/bin/env python3
"""Tiny location-report test server for mqtt_power.py.

Listens on a UDP (default) or TCP port and prints every location report it
receives, with a timestamp, the raw bytes, and a best-effort decode of the
TAIP / NMEA fields — including the non-standard voltage field. Use it to *see*
that mqtt_power.py is emitting position + voltage correctly before pointing the
router at a real location server.

Usage:
    python3 loc_test_server.py                 # UDP on 0.0.0.0:5000
    python3 loc_test_server.py --port 5005
    python3 loc_test_server.py --proto tcp --port 5005

Then run the publisher against it, e.g.:
    LOC_HOST=127.0.0.1 LOC_PORT=5000 FAKE_VOLTAGE=12.3 \
    FAKE_LAT=37.39438 FAKE_LON=-122.03846 FAKE_SPEED_MS=13.4 FAKE_HEADING=126 \
    python3 mqtt_power.py
"""

import argparse
import re
import socket
import sys
import time


def _validate_taip_checksum(line):
    """Return True/False/None for a TAIP sentence's checksum (None if absent)."""
    m = re.match(r"(>.*\*)([0-9A-Fa-f]{2})<", line)
    if not m:
        return None
    body, given = m.group(1), int(m.group(2), 16)
    calc = 0
    for ch in body:  # '>' through '*' inclusive
        calc ^= ord(ch)
    return calc == given


def _validate_nmea_checksum(line):
    """Return True/False/None for an NMEA sentence's checksum (None if absent)."""
    m = re.match(r"\$(.*)\*([0-9A-Fa-f]{2})\s*$", line)
    if not m:
        return None
    body, given = m.group(1), int(m.group(2), 16)
    calc = 0
    for ch in body:  # between '$' and '*'
        calc ^= ord(ch)
    return calc == given


def decode(line):
    """Pull out the human-interesting bits of a TAIP or NMEA sentence."""
    bits = []
    if line.startswith(">RPV"):
        ok = _validate_taip_checksum(line)
        bits.append(f"TAIP PV  checksum={'OK' if ok else 'BAD' if ok is False else 'n/a'}")
        m = re.match(r">RPV(\d{5})([+-]\d{2})(\d{5})([+-]\d{3})(\d{5})"
                     r"(\d{3})(\d{3})(\d)(\d)", line)
        if m:
            tod, lad, laf, lod, lof, spd, hdg, fix, age = m.groups()
            lat = float(f"{lad}.{laf}")
            lon = float(f"{lod}.{lof}")
            fixtxt = {"0": "2D", "1": "3D", "9": "no-fix"}.get(fix, fix)
            bits.append(f"lat={lat:.5f} lon={lon:.5f} speed={int(spd)}mph "
                        f"heading={int(hdg)} fix={fixtxt}")
        vid = re.search(r";ID=([^;]+)", line)
        if vid:
            bits.append(f"ID={vid.group(1)}")
        volt = re.search(r";VOLT=([^;*]+)", line)
        if volt:
            bits.append(f"VOLT={volt.group(1)}  <-- voltage carried alongside position")
    elif line.startswith("$"):
        ok = _validate_nmea_checksum(line)
        kind = line[1:6]
        bits.append(f"NMEA {kind}  checksum={'OK' if ok else 'BAD' if ok is False else 'n/a'}")
        if line.startswith("$PDGIVLT"):
            v = line[9:].split("*")[0]
            bits.append(f"VOLT={v}  <-- voltage carried alongside position")
    return "  ".join(bits) if bits else "(unrecognised)"


def show(addr, data):
    stamp = time.strftime("%H:%M:%S")
    text = data.decode("ascii", "replace")
    print(f"\n[{stamp}] {len(data)} bytes from {addr[0]}:{addr[1]}")
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        print(f"  RAW : {line}")
        print(f"  DEC : {decode(line)}")
    sys.stdout.flush()


def serve_udp(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    print(f"UDP location test server listening on {host}:{port} (Ctrl-C to stop)")
    while True:
        data, addr = sock.recvfrom(65535)
        show(addr, data)


def serve_tcp(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(5)
    print(f"TCP location test server listening on {host}:{port} (Ctrl-C to stop)")
    while True:
        conn, addr = sock.accept()
        with conn:
            data = conn.recv(65535)
            if data:
                show(addr, data)


def main():
    ap = argparse.ArgumentParser(description="Location-report test server.")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--proto", choices=("udp", "tcp"), default="udp")
    args = ap.parse_args()
    try:
        (serve_tcp if args.proto == "tcp" else serve_udp)(args.host, args.port)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
