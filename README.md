# Digi TX40 — Power Voltage + GPS Location Reporter

Publish a Digi TX40's internal (power-input) voltage to an MQTT broker, and
optionally forward the device's **GPS position with the voltage attached** to a
location server as **TAIP** or **NMEA**.

The router's built-in location forwarding (DAL OS `service location forward`)
can stream standard TAIP/NMEA position, but there is **no standard field for
supply voltage** in either protocol — so this script builds the sentences itself
and carries the voltage in a non-standard extension field.

Runs on the device (DAL OS, via the `digidevice`/`runt` API) and also off-device
using `FAKE_*` environment variables for testing.

---

## Contents

| File | Purpose |
|------|---------|
| `mqtt_power.py` | **The device script.** Reads voltage + GPS on the TX40, publishes voltage over MQTT, and (optionally) sends a position+voltage report over UDP/TCP. |
| `gps_dashboard.py` | **Location server + live dashboard.** Binds a UDP/TCP port, decodes the incoming TAIP/NMEA reports and paints position, voltage, a street map of the track and the city name. |
| `loc_test_server.py` | Minimal UDP/TCP listener that *prints* and decodes incoming TAIP/NMEA reports (checksum check, voltage highlighted). Same decode as the dashboard, no UI. |
| `exemple_power.py` | Live **MQTT** voltage dashboard: subscribes to the voltage topic (and can publish demo samples) and paints the values that came back through the broker. |

Two ways to watch the same data: `gps_dashboard.py` for the TAIP/NMEA side
(position + voltage off the wire), `exemple_power.py` for the MQTT side
(voltage only).

---

## What `mqtt_power.py` does

1. **Reads the internal voltage** from the runtime database (`runt`) at
   `system.supply_voltage` (already in volts, e.g. `12.1`). If a future firmware
   renames the key, it falls back to auto-detecting a voltage-like key.
2. **Publishes it over MQTT** as JSON to the configured broker, on topic
   `‹TOPIC_BASE›/‹serial›/voltage`. Latitude/longitude are added to the payload
   when a GPS fix is present.
3. **Optionally sends a location report** (only when `LOC_HOST` is set) to a
   location server over UDP or TCP, in TAIP (default) or NMEA, with the voltage
   attached in a non-standard field.

If `LOC_HOST` is not set, step 3 is skipped and behavior is identical to the
original voltage-only script.

### MQTT payload

```json
{"internal_voltage": 12.1, "timestamp": 1786635580, "latitude": 19.4326, "longitude": -99.1332}
```

(`latitude`/`longitude` are included only when the GPS has a valid fix.)

---

## What `gps_dashboard.py` does

It **is** the location server: it binds the port itself, so nothing else needs to
be running. Every datagram is decoded and painted live.

```sh
python3 gps_dashboard.py                      # UDP on 0.0.0.0:5000
python3 gps_dashboard.py --port 5005
python3 gps_dashboard.py --proto tcp --port 5005
python3 gps_dashboard.py --demo               # simulated drive, no device needed
python3 gps_dashboard.py --offline            # no OSM lookups
python3 gps_dashboard.py --altscreen          # draw on the alternate screen
```

What it shows:

- **Voltage** — gauge on a fixed 9–15 V scale, colour-coded, with a sparkline and
  min/avg/max. The value comes from the `;VOLT=` field of the sentence.
- **Position** — latitude/longitude, speed (mph + km/h), heading with compass
  point, fix mode and age, vehicle ID, TAIP time of day in UTC (flagged if it
  drifts from this host's clock), checksum status and a maps link. Without a
  valid fix, coordinates are dimmed and marked as not a real position.
- **Place** — the city/region the fix is in, via OSM Nominatim reverse geocoding.
- **Track** — a braille mini map: OSM streets underneath (through roads bright,
  side streets faint) with up to four street names, your track drawn over them in
  cyan, `○` where the track starts and `◉` where the device is now, plus a scale
  bar and distance driven.
- **Sentence log + raw datagram** — the last sentences with source address, and
  the `RAW:`/`DEC:` pair of the latest datagram.

Both OSM lookups are **on by default** and send the coordinates to
openstreetmap.org (the header says so at startup). Requests are cached,
throttled, and repeated only after the device moves; any network failure shows as
dim text instead of taking the panel down. Disable with `--no-geocode`,
`--no-basemap` or `--offline`.

| Flag | Default | Description |
|------|---------|-------------|
| `--host` / `--port` | `0.0.0.0` / `5000` | Local bind address for the listener |
| `--proto` | `udp` | `udp` or `tcp` |
| `--interval` | `10` | Expected seconds between reports (staleness + demo pacing) |
| `--demo` | off | Simulated drive around Hopkins, MN; never binds a port |
| `--no-geocode` | on | Do not name the position via Nominatim |
| `--no-basemap` | on | Do not fetch OSM streets under the track |
| `--offline` | off | Both of the above |
| `--geocode-interval` | `15` | Minimum seconds between geocoding requests |
| `--no-fallback` | off | Exit instead of falling back to demo mode |
| `--altscreen` | off | Draw on the alternate screen instead of inline |

If the port cannot be bound (usually: something else has it), the dashboard falls
back to DEMO mode with a yellow banner instead of dying; `--no-fallback` makes it
exit with an error.

The layout is measured against the terminal before drawing, so panels appear only
when they fit: the bottom row (log, map, raw datagram) needs about 36 rows, the
`Track` map about 100 columns, and the raw-datagram panel joins them from about
144 columns. In a smaller window you keep the voltage/position table and lose the
extras rather than getting a scrolling, flickering screen. Demo mode drives a real street loop in Hopkins,
Minnesota (Mainstreet → Blake Rd → Minnetonka Blvd → Shady Oak Rd), so the map,
the heading and the speed all agree with each other.

---

## Requirements

- **On-device:** a Digi TX40 running DAL OS (provides `digidevice`/`runt` and
  Python 3). `paho-mqtt` is available on the device.
- **Off-device (testing):** Python 3, plus
  - `mqtt_power.py`, `exemple_power.py`: `pip install paho-mqtt`
  - `gps_dashboard.py`, `exemple_power.py`: `pip install rich`
  - `loc_test_server.py`: standard library only

```sh
pip install paho-mqtt rich --break-system-packages
```

`gps_dashboard.py` needs no MQTT broker; `exemple_power.py` does (or `--demo`).

---

## Configuration

All configuration of the device script is via environment variables.

### MQTT (voltage publishing)

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_HOST` | `10.10.65.214` | Broker address |
| `MQTT_PORT` | `1883` | Broker port |
| `MQTT_TOPIC_BASE` | `router` | Topic prefix (`‹base›/‹serial›/voltage`) |
| `TX40_VOLTAGE_KEY` | *(auto)* | Pin the exact runt voltage key if auto-detection ever fails |
| `DISCOVER_FULL` | *(unset)* | If set, a missing-key report scans the whole runt tree |

### Location forwarding (optional — active only when `LOC_HOST` is set)

| Variable | Default | Description |
|----------|---------|-------------|
| `LOC_HOST` | *(unset)* | Location server address. **Unset = feature off.** |
| `LOC_PORT` | `0` | Location server port (required when `LOC_HOST` is set) |
| `LOC_FORMAT` | `taip` | `taip` or `nmea` |
| `LOC_PROTO` | `udp` | `udp` or `tcp` |
| `LOC_VEHICLE_ID` | `0000` | TAIP vehicle ID (4 characters) |

### Off-device test values (used only when `digidevice` is absent)

| Variable | Default | Description |
|----------|---------|-------------|
| `FAKE_VOLTAGE` | *(none)* | Voltage to publish/send |
| `FAKE_LAT` / `FAKE_LON` | *(none)* | Position in decimal degrees |
| `FAKE_SPEED_MS` | `0` | Ground speed in **m/s** |
| `FAKE_HEADING` | `0` | Heading in degrees |
| `FAKE_ALT` | `0` | Altitude in meters |
| `FAKE_SATS` | `0` | Satellite count |
| `FAKE_HDOP` | `0` | HDOP |
| `FAKE_QUALITY` | `3D` | Fix quality string |
| `MQTT_DEVICE` | `tx40-test` | Device id used in the topic |

---

## Usage

### On the TX40

Deploy `mqtt_power.py` to the router and run it (typically on a schedule — the
script is one-shot by design; the router's scheduler runs it at intervals):

```sh
# Voltage over MQTT only (original behavior)
python3 mqtt_power.py

# Voltage over MQTT + TAIP position/voltage to a location server over UDP
LOC_HOST=203.0.113.10 LOC_PORT=5000 LOC_FORMAT=taip python3 mqtt_power.py

# NMEA over TCP instead
LOC_HOST=203.0.113.10 LOC_PORT=5000 LOC_FORMAT=nmea LOC_PROTO=tcp python3 mqtt_power.py
```

### Testing off-device

Terminal 1 — start a receiver. Either the dashboard:

```sh
python3 gps_dashboard.py --port 5000              # UDP (default)
python3 gps_dashboard.py --port 5000 --proto tcp
```

…or the plain-text listener:

```sh
python3 loc_test_server.py --port 5000
python3 loc_test_server.py --port 5000 --proto tcp
```

Run only **one** of them per port. Both bind the same UDP port successfully on
Linux (`SO_REUSEADDR`), but each datagram is delivered to only one of the two, so
the other looks like it is losing reports.

Terminal 2 — run the publisher against it with fake data:

```sh
LOC_HOST=127.0.0.1 LOC_PORT=5000 LOC_FORMAT=taip \
FAKE_VOLTAGE=12.3 FAKE_LAT=37.39438 FAKE_LON=-122.03846 \
FAKE_SPEED_MS=13.4 FAKE_HEADING=126 FAKE_SATS=9 FAKE_HDOP=0.9 FAKE_ALT=52 \
python3 mqtt_power.py
```

`loc_test_server.py` prints and decodes each report:

```
[10:35:37] 59 bytes from 127.0.0.1:43556
  RAW : >RPV56137+3739438-1220384603012612;ID=0000;VOLT=12.3;*65<
  DEC : TAIP PV  checksum=OK  lat=37.39438 lon=-122.03846 speed=30mph heading=126 fix=3D  ID=0000  VOLT=12.3  <-- voltage carried alongside position
```

`gps_dashboard.py` paints the same decode, plus the map:

```
╭───────────── Track · Hopkins, Minnesota, US ──────────────╮
│    ⠘⣼ ⢸     ⡇     ⡇     ⡇     ⡇    ⢠⠃    ⢸     ⡇         │
│ ⡠⠊⠉ ⡏⡆⡜     ⡇    ⢀⠇    ⢠⠃    ⢀⠇    ⢸     ⡸     ⡇         │
│    ○⣀⣀⣀⣀⣀⣀⣀⣀⡇    ⢸     ⢸     ⢸     ⡸     ⡇     ⡇         │
│     ⢸Shady Oak Road⠉⠉⠉⠉⠉⠉⠉⠉⠉14th Avenue North⠉⠉⠉⠒⠒⠒◉      │
│      ⣿Main Street⠒⠒⠒⠒⠒⠒⠒⠒⠒⠒⠒⠒⡞Mainstreet⠒⠒⠒⠒⠒⠒⠒⠒⠒⠒⠒⠒⠒⠒⠒⠒  │
│ ├────────┤ 173 m  ·  18 fixes, 859 m driven  ·  270 OSM r │
╰───────────────────────────────────────────────────────────╯
```

### Watching the MQTT side

```sh
mosquitto -p 1883 -v                                            # a broker
python3 exemple_power.py --topic 'router/+/voltage'             # real device
python3 exemple_power.py --demo                                 # no broker needed
```

`exemple_power.py` publishes demo samples itself unless you pass `--no-publish`
(implied by a wildcard topic) and falls back to DEMO mode when the broker is
unreachable.

---

## Message formats

### TAIP (default)

A standard PV (position/velocity) report with the voltage appended as a
non-standard `;VOLT=` field, kept inside the checksummed range so the sentence
stays checksum-valid:

```
>RPV56137+3739438-1220384603012612;ID=0000;VOLT=12.3;*65<
```

| Segment | Meaning |
|---------|---------|
| `RPV` | Position/velocity report |
| `56137` | GPS time of day (seconds since UTC midnight) |
| `+3739438` | Latitude `+37.39438°` (sign + 2 int + 5 dec) |
| `-12203846` | Longitude `-122.03846°` (sign + 3 int + 5 dec) |
| `030` | Speed in mph |
| `126` | Heading in degrees |
| `1` | Fix mode (`0`=2D, `1`=3D, `9`=no fix) |
| `2` | Data age (`2`=<10s, `1`=>10s, `0`=n/a) |
| `;ID=0000` | Vehicle ID |
| `;VOLT=12.3` | **Non-standard** supply-voltage field |
| `*65` | Checksum: XOR of every byte from `>` through `*` inclusive |

The position part is fully standard, so any TAIP server parses it; only a server
taught to look for `;VOLT=` will read the voltage.

### NMEA

Standard `$GPRMC` + `$GPGGA` position sentences plus a proprietary `$PDGIVLT`
sentence for the voltage. Unaware servers ignore the proprietary sentence and
still parse position:

```
$GPRMC,153551.00,A,3723.6628,N,12202.3076,W,26.0,126.0,130826,,,A*7B
$GPGGA,153551.00,3723.6628,N,12202.3076,W,1,09,0.9,52.0,M,0.0,M,,*71
$PDGIVLT,12.3*66
```

NMEA checksum is the XOR of the bytes between `$` and `*`.

Because position and voltage arrive in *separate* NMEA sentences,
`gps_dashboard.py` merges the fields into one running report: a `$PDGIVLT` does
not erase the position that came in the `$GPRMC` right before it.
`loc_test_server.py` does no merging — it decodes and prints each sentence on its
own line.

---

## Notes & caveats

- **Voltage is not a standard TAIP/NMEA field.** It is carried in a non-standard
  extension. The *position* is understood by any server; the *voltage* is only
  usable by a server configured to read the extension. This mirrors the common
  pattern of shipping non-standard TAIP messages for requirements the protocol
  doesn't cover.
- **Strict TAIP parsers:** if your server rejects extra fields inside the RPV
  sentence, send the voltage as a **separate** TAIP message instead so the
  position sentence stays 100% standard. (Small change in `build_taip`.)
- **No GPS fix → still sends.** With no satellite fix, the report is sent with a
  `0/0` position and the invalid-fix flag set, so the voltage still gets through.
  A real fix requires a GNSS antenna with sky view. The dashboard shows those
  reports with the voltage intact and the coordinates marked as invalid, and does
  not plot them on the map.
- **One-shot by design.** `mqtt_power.py` has no internal loop; schedule it on
  the router at your desired interval.
- **Units:** `runt` reports ground speed in m/s; the script converts to mph
  (TAIP) and knots (NMEA) automatically.
- **The dashboard's map is vector, not tiles.** Streets and names come from the
  OSM Overpass API, drawn as braille lines — there is no satellite or raster
  imagery. Overpass is a shared public service and occasionally answers `504`; the
  dashboard shows the failure in the map footer and retries.
- **Nothing is faked silently.** Every value on the dashboard comes off the
  socket; simulated data only ever appears in demo mode, which says so in a
  yellow banner and marks the device as `● simulated`.
