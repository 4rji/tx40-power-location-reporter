#!/usr/bin/env python3
"""
MQTT demo #2 -- live voltage table.

Reads the supply-voltage messages off a topic and paints them live. The expected
payload is what the router's power script publishes:

    {"internal_voltage": 12.1, "timestamp": 1786631554}

    router/TX40060023202338/voltage  (source key: system.supply_voltage)

A publisher thread pushes samples in that same shape every N seconds, and a
separate subscriber thread listens to the same topic, so the table shows what
actually came back through the broker and not the values generated in memory.
When a real device is publishing, use --no-publish and only subscribe. A --topic
with a wildcard (+ or #) implies --no-publish: you cannot publish to a filter.

Any other JSON payload still renders (generic key/value table), so a stray
message on the topic will not take the dashboard down.

Broker: localhost:1883, no username, no TLS, no certificates.

Requirements:
    pip install paho-mqtt rich --break-system-packages

Start a broker first (either one):
    mosquitto -p 1883 -v
    docker run --rm -p 1883:1883 eclipse-mosquitto

Usage:
    python3 exemple_power.py
    python3 exemple_power.py --interval 2 --topic router/TX400/voltage
    python3 exemple_power.py --topic 'router/+/voltage'    # a real device is publishing
    python3 exemple_power.py --topic router/TX40060023202338/voltage --no-publish
    python3 exemple_power.py --demo            # simulated data, no broker needed
    python3 exemple_power.py --altscreen       # draw on the alternate screen instead

If the broker is unreachable the script falls back to DEMO mode (values are
simulated locally and the header says so) so a presentation never dies on a
missing broker. Use --no-fallback to fail loudly instead.

No flicker, by design:
  * the screen is repainted only when the data really changed (about once per
    message), never on a timer;
  * bursts are coalesced, so publishing and receiving the same sample cost one
    repaint instead of two;
  * every column has a fixed width and the height is measured before drawing,
    so the layout never resizes or overflows into scrolling;
  * each frame is wrapped in synchronized output (DEC 2026) so the terminal
    swaps it in atomically instead of showing a half-drawn screen.
"""

import argparse
import json
import random
import socket
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime

import paho.mqtt.client as mqtt
from rich.console import Console, Group
from rich.live import Live
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 1883
DEFAULT_TOPIC = "router/TX40060023202338/voltage"
HISTORY_LEN = 32
RECENT_LEN = 6

# the only field this dashboard charts, and the range the gauge is drawn against
VOLTAGE_KEY = "internal_voltage"
VOLTAGE_MIN = 9.0
VOLTAGE_MAX = 15.0

# what the built-in publisher/demo pretends to be, mirroring a real sample
DEMO_DEVICE = "TX40060023202338"
DEMO_VOLTAGE = 12.1

# DEC private mode 2026: "hold the display until I'm done writing this frame".
# Terminals that don't know it ignore both sequences.
SYNC_BEGIN = "\x1b[?2026h"
SYNC_END = "\x1b[?2026l"

state_lock = threading.Lock()
shared_state = {
    "last_payload": None,
    "last_topic": None,
    "last_receive": None,
    "connected_pub": False,
    "connected_sub": False,
    "msgs_sent": 0,
    "msgs_recv": 0,
    "last_error": None,
    "demo": False,
    "history": {},        # metric name -> deque of values
    "recent": deque(maxlen=RECENT_LEN),
    "revision": 0,        # bumped on every visible change, drives the redraw
}


def bump(**changes):
    """Apply state changes and mark the screen as needing a redraw."""
    with state_lock:
        shared_state.update(changes)
        shared_state["revision"] += 1


@dataclass
class VoltageSample:
    """One supply-voltage sample, field for field as the device publishes it."""
    internal_voltage: float
    timestamp: int

    @classmethod
    def random_walk(cls, prev=None, elapsed_s=4):
        base = prev.internal_voltage if prev else DEMO_VOLTAGE
        value = round(min(14.4, max(10.6, base + random.uniform(-0.15, 0.15))), 2)
        return cls(internal_voltage=value, timestamp=int(time.time()))

    def payload(self):
        """Exactly the 2 fields the device sends, in the same order."""
        return {
            "internal_voltage": self.internal_voltage,
            "timestamp": self.timestamp,
        }


# --------------------------------------------------------------------------- #
# state updates
# --------------------------------------------------------------------------- #

def numeric_fields(data, prefix=""):
    """Flatten numbers out of a payload, so a nested dict still gets charted."""
    for key, value in data.items():
        name = f"{prefix}{key}"
        if isinstance(value, bool):
            continue                      # bools are ints in Python, not metrics
        elif isinstance(value, (int, float)):
            yield name, value
        elif isinstance(value, dict):
            yield from numeric_fields(value, f"{name}.")


def record_message(topic, data):
    """Single entry point for "a message arrived", used by MQTT and demo mode.

    data is the decoded object when the payload is a JSON dict, and the raw text
    otherwise: a device publishing plain text still belongs in the log.
    """
    with state_lock:
        shared_state["last_payload"] = data
        shared_state["last_topic"] = topic
        shared_state["last_receive"] = datetime.now()
        shared_state["msgs_recv"] += 1
        if isinstance(data, dict):
            for key, value in numeric_fields(data):
                hist = shared_state["history"].setdefault(key, deque(maxlen=HISTORY_LEN))
                hist.append(value)
            text = json.dumps(data, separators=(",", ":"))
        else:
            # one log row is one line: fold newlines and tabs into spaces
            text = " ".join(str(data).split())
        shared_state["recent"].appendleft(
            (datetime.now().strftime("%H:%M:%S"), topic, text)
        )
        shared_state["revision"] += 1


# --------------------------------------------------------------------------- #
# MQTT
# --------------------------------------------------------------------------- #

def make_client(client_id):
    """Plain TCP client: no username, no password, no TLS."""
    return mqtt.Client(
        client_id=client_id,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )


def connect_with_retry(client, args, who, stop_event):
    while not stop_event.is_set():
        try:
            client.connect(args.host, args.port, keepalive=30)
            return True
        except Exception as exc:
            bump(last_error=f"{who} connect: {exc}")
            stop_event.wait(2.0)
    return False


def publisher_loop(args, stop_event):
    client = make_client(f"voltage-pub-{random.randint(1000, 9999)}")

    def on_connect(c, u, flags, reason_code, properties=None):
        bump(connected_pub=reason_code == 0 or str(reason_code) == "Success")

    def on_disconnect(c, u, flags, reason_code, properties=None):
        bump(connected_pub=False)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    if not connect_with_retry(client, args, "publisher", stop_event):
        return

    client.loop_start()
    topic = concrete_topic(args.topic)
    sample = None
    try:
        while not stop_event.is_set():
            sample = VoltageSample.random_walk(sample, args.interval)
            client.publish(topic, json.dumps(sample.payload()), qos=1)
            with state_lock:
                shared_state["msgs_sent"] += 1
                shared_state["revision"] += 1
            stop_event.wait(args.interval)
    finally:
        client.loop_stop()
        client.disconnect()


def subscriber_loop(args, stop_event):
    client = make_client(f"voltage-sub-{random.randint(1000, 9999)}")

    def on_connect(c, u, flags, reason_code, properties=None):
        bump(connected_sub=reason_code == 0 or str(reason_code) == "Success")
        c.subscribe(args.topic, qos=1)

    def on_disconnect(c, u, flags, reason_code, properties=None):
        bump(connected_sub=False)

    def on_message(c, u, msg):
        try:
            text = msg.payload.decode()
        except UnicodeDecodeError:
            text = repr(msg.payload)          # binary payload: show the bytes
        try:
            data = json.loads(text)
        except ValueError:
            data = text
        if not isinstance(data, dict):        # a bare string/number is not a sample,
            data = text                       # but it is still a message: log it raw
        record_message(msg.topic, data)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    if not connect_with_retry(client, args, "subscriber", stop_event):
        return

    client.loop_start()
    stop_event.wait()
    client.loop_stop()
    client.disconnect()


def is_filter(topic):
    """True for a subscription filter: you can subscribe to it but not publish to it."""
    return "+" in topic or "#" in topic


def concrete_topic(topic):
    """Turn a filter into something publishable, for the demo/publisher threads."""
    parts = [DEMO_DEVICE if part == "+" else part for part in topic.split("/")]
    if parts and parts[-1] == "#":
        parts[-1] = "voltage"
    return "/".join(parts)


def device_from_topic(topic):
    """router/<device>/voltage -> <device>: the payload does not name the device."""
    parts = [part for part in str(topic or "").split("/") if part]
    if len(parts) >= 3:
        return parts[-2]
    return parts[0] if parts else "router"


def demo_loop(args, stop_event):
    """No broker: generate the samples and feed them straight into the table."""
    topic = concrete_topic(args.topic)
    sample = None
    while not stop_event.is_set():
        sample = VoltageSample.random_walk(sample, args.interval)
        with state_lock:
            shared_state["msgs_sent"] += 1
            shared_state["revision"] += 1
        record_message(topic, sample.payload())
        stop_event.wait(args.interval)


# --------------------------------------------------------------------------- #
# rendering
# --------------------------------------------------------------------------- #

SPARK = "▁▂▃▄▅▆▇█"


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


def stamp_str(value):
    """Unix epoch -> local time, with the raw number kept in sight."""
    if is_number(value):
        try:
            when = datetime.fromtimestamp(value)
        except (OSError, OverflowError, ValueError):
            return str(value)
        return f"{when.strftime('%Y-%m-%d %H:%M:%S')}  [dim]({int(value)})[/dim]"
    return str(value) if value else "—"


def cell(value):
    """Fallback formatting for a payload field we know nothing about."""
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, dict):
        return escape(" · ".join(f"{k}={v}" for k, v in value.items()))
    if isinstance(value, list):
        return escape(", ".join(str(v) for v in value))
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
        pub_ok = shared_state["connected_pub"]
        sub_ok = shared_state["connected_sub"]
        sent = shared_state["msgs_sent"]
        recv = shared_state["msgs_recv"]
        demo = shared_state["demo"]
        last_topic = shared_state["last_topic"]

    def status(ok):
        return "● connected" if ok else "● offline  "

    pub_status = status(True) if demo else status(pub_ok)
    sub_status = status(True) if demo else status(sub_ok)
    broker_line = "SIMULATED (demo)" if demo else f"{args.host}:{args.port}"

    # with --no-publish the device is the publisher, so report it instead of us
    if args.no_publish and not demo:
        device = device_from_topic(last_topic or args.topic)[:15]
        pub_box = ["PUBLISHER", device, "script.power", f"seen: {recv}"]
    else:
        pub_box = ["PUBLISHER", "voltage-pub", pub_status, f"sent: {sent}"]

    rows = box_row(
        boxes=[
            (19, pub_box),
            (26, ["BROKER", broker_line, "no auth · no TLS", f"topic: {args.topic}"]),
            (19, ["SUBSCRIBER", "voltage-sub", sub_status, f"received: {recv}"]),
        ],
        gap=11,
        arrow_row=2,
        arrow_labels=[("publish", "qos 1"), ("deliver", "qos 1")],
    )

    text = Text("\n".join(rows))
    text.highlight_regex(r"[┌┐└┘─│▶]", "grey42")
    text.highlight_words(["● connected"], "bold green")
    text.highlight_words(["● offline"], "bold red")
    text.highlight_words(["PUBLISHER", "BROKER", "SUBSCRIBER"], "bold white")
    text.highlight_words(["publish", "deliver", "qos 1"], "cyan")
    text.highlight_words([args.topic, broker_line], "yellow")
    return text


def metrics_table(args):
    with state_lock:
        data = shared_state["last_payload"]
        last_topic = shared_state["last_topic"]
        history = {k: list(v) for k, v in shared_state["history"].items()}

    table = Table(title="Current data (received from the broker)", expand=True, title_style="bold")
    table.add_column("Metric", style="bold", width=18, no_wrap=True)
    table.add_column("Value", width=30, no_wrap=True)
    table.add_column("Last samples", style="cyan", ratio=1, no_wrap=True, overflow="crop")

    if data is None:
        table.add_row("—", "waiting for the broker...", "")
        return table

    # Plain text (or a bare number) on the topic: there is nothing to chart, so
    # just show what arrived.
    if not isinstance(data, dict):
        table.add_row("payload", escape(str(data)), "")
        return table

    # Unknown payload on the topic: show it as it is instead of crashing on a
    # field that is not there.
    if VOLTAGE_KEY not in data:
        for key, value in list(data.items())[:12]:
            hist = history.get(key, [])
            lo, hi = (min(hist), max(hist)) if hist else (0, 1)
            table.add_row(escape(str(key)), cell(value), spark(hist, lo, hi) if hist else "")
        return table

    value = data.get(VOLTAGE_KEY)
    hist = history.get(VOLTAGE_KEY, [])
    lo, hi = voltage_window(hist)

    table.add_row("Internal voltage", voltage_bar(value), spark(hist, lo, hi))
    if hist:
        table.add_row(
            "Min · avg · max",
            f"[dim]{min(hist):.2f} · {sum(hist) / len(hist):.2f} · {max(hist):.2f} V[/dim]",
            f"[dim]scale {lo:.2f} – {hi:.2f} V, {len(hist)} samples[/dim]",
        )
    table.add_row("Scale", f"[dim]{VOLTAGE_MIN:.0f} V ─── {VOLTAGE_MAX:.0f} V[/dim]", "")
    table.add_row("Device", f"[bold]{escape(device_from_topic(last_topic or args.topic))}[/bold]", "")
    table.add_row("Source key", "[dim]system.supply_voltage[/dim]", "")
    table.add_row("Sample timestamp", stamp_str(data.get("timestamp")), "")

    # anything else the device decides to add to the payload still shows up
    for key, extra in data.items():
        if key in (VOLTAGE_KEY, "timestamp"):
            continue
        table.add_row(escape(str(key)), cell(extra), "")
    return table


def detail_tables(log_rows, json_lines):
    """Message log and raw payload, each capped to the rows it was given."""
    with state_lock:
        recent = list(shared_state["recent"])[:log_rows]
        data = shared_state["last_payload"]

    log = Table(title="Last messages delivered", expand=True, title_style="bold")
    log.add_column("Time", style="dim", width=8, no_wrap=True)
    log.add_column("Topic", style="yellow", width=16, no_wrap=True, overflow="ellipsis")
    log.add_column("Payload", style="white", ratio=1, no_wrap=True, overflow="ellipsis")
    if recent:
        for when, topic, payload in recent:
            # payload and topic come off the wire: brackets in them are not markup
            log.add_row(when, escape(topic), escape(payload))
    else:
        log.add_row("—", "—", "no messages yet")

    if isinstance(data, dict):
        body, title = json.dumps(data, indent=2), "Raw JSON payload"
    elif data is None:
        body, title = "{}", "Raw JSON payload"
    else:
        body, title = str(data), "Raw payload (not JSON)"
    lines = body.splitlines() or [""]
    if len(lines) > json_lines:
        lines = lines[:json_lines - 1] + ["  …"]
    # no_wrap keeps one JSON line on one screen row, so the height is predictable
    raw = Text("\n".join(lines), style="green", no_wrap=True, overflow="crop")
    return log, Panel(raw, title=title, border_style="grey42")


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
        sent = shared_state["msgs_sent"]
        recv = shared_state["msgs_recv"]

    parts = [topology_panel(args), "", metrics_table(args)]
    if demo:
        parts.insert(0, Text(
            "DEMO MODE — no broker on "
            f"{args.host}:{args.port}, data is simulated locally",
            style="bold black on yellow",
        ))

    if last_receive:
        stamp = last_receive.strftime("%H:%M:%S")
        if is_stale(args, last_receive):
            stamp += "  [bold red](stale — nothing arriving)[/bold red]"
    else:
        stamp = "—"
    footer = f"sent: {sent}   received: {recv}   last message: {stamp}   ctrl-c to quit"
    if err and not demo:
        footer += f"\n[bold red]error: {err}[/bold red]"

    def wrap(sections):
        return Panel(
            Group(*sections),
            title=f"[bold]MQTT voltage dashboard[/bold] · {args.host}:{args.port} · {args.topic}",
            subtitle=footer,
            border_style="blue",
        )

    # Measure what the essential part costs, then spend whatever rows are left on
    # the message log and the raw payload. Guessing the height instead of
    # measuring it is what makes a dashboard overflow and scroll (= flicker).
    # one row spare: if the render fills the window exactly it scrolls, and a
    # scrolling live region is the flicker
    base = wrap(parts)
    room = console.size.height - 1 - rendered_height(console, base)
    if room < 6:
        return base

    # a log table costs 5 rows of chrome, the JSON panel costs 2
    log, raw = detail_tables(max(1, min(RECENT_LEN, room - 5)), max(1, room - 2))
    grid = Table.grid(expand=True, padding=(0, 1))
    grid.add_column(ratio=2)
    grid.add_column(ratio=1)
    grid.add_row(log, raw)
    return wrap(parts + [grid])


def state_signature(args, console):
    """Cheap fingerprint of everything the screen shows: redraw only when it changes."""
    with state_lock:
        revision = shared_state["revision"]
        last_receive = shared_state["last_receive"]
    size = console.size
    return (revision, is_stale(args, last_receive), size.width, size.height)


# --------------------------------------------------------------------------- #

def broker_reachable(host, port, timeout=1.5):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def main():
    parser = argparse.ArgumentParser(
        description="MQTT voltage publisher (background) + live dashboard fed by a real subscription"
    )
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--interval", type=float, default=2.0, help="seconds between publishes")
    parser.add_argument("--demo", action="store_true", help="simulate data, never touch a broker")
    parser.add_argument("--no-publish", action="store_true",
                        help="only subscribe: the data comes from a real device")
    parser.add_argument("--no-fallback", action="store_true",
                        help="exit instead of falling back to demo mode")
    parser.add_argument("--altscreen", action="store_true",
                        help="draw on the alternate screen instead of inline")
    args = parser.parse_args()

    console = Console()

    # A wildcard topic is a subscription filter: publishing to it is illegal, so
    # asking for one means "listen to whoever is publishing under it".
    if is_filter(args.topic) and not args.no_publish and not args.demo:
        args.no_publish = True
        console.print(f"[yellow]{args.topic} es un filtro con wildcard → "
                      f"solo escucho (--no-publish)[/yellow]")

    demo = args.demo
    if not demo and not broker_reachable(args.host, args.port):
        if args.no_fallback:
            console.print(f"[bold red]no MQTT broker on {args.host}:{args.port}[/bold red]")
            console.print("start one with:  mosquitto -p 1883 -v")
            raise SystemExit(1)
        console.print(f"[yellow]no broker on {args.host}:{args.port} → demo mode[/yellow]")
        demo = True
        time.sleep(1.0)

    with state_lock:
        shared_state["demo"] = demo

    stop_event = threading.Event()
    if demo:
        threading.Thread(target=demo_loop, args=(args, stop_event), daemon=True).start()
    else:
        if not args.no_publish:
            threading.Thread(target=publisher_loop, args=(args, stop_event), daemon=True).start()
        threading.Thread(target=subscriber_loop, args=(args, stop_event), daemon=True).start()

    # No auto-refresh and no timed redraws: the screen is repainted only when the
    # state actually changed, and each repaint is wrapped in synchronized output
    # so the terminal shows the new frame in one go instead of mid-redraw.
    try:
        with Live(console=console, auto_refresh=False, screen=args.altscreen,
                  vertical_overflow="crop") as live:
            pending = drawn = None
            while True:
                current = state_signature(args, console)
                # repaint once the state has settled for a tick: publishing and
                # receiving happen milliseconds apart, this coalesces both into
                # a single repaint instead of two in a row
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
