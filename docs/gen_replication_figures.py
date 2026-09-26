#!/usr/bin/env python3
"""Regenerate the replication figures in docs/img from the numbers in docs/replication.md.
Dependency-free (hand-written SVG) so the figures are reproducible anywhere."""
import os, textwrap

OUT = os.path.join(os.path.dirname(__file__), "img")
FONT = "font-family='JetBrains Mono, SFMono-Regular, Menlo, monospace'"
INK, MUTED, GRID, PANEL = "#1f2328", "#6a737d", "#d0d7de", "#f6f8fa"
RING, RING_FILL = "#c0392b", "#fdecea"
NET, NET_FILL = "#1d4ed8", "#e8efff"
OK, OK_FILL = "#2e7d32", "#e8f5e9"
OTHER = "#8fa3b8"

def esc(t):
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def para(x, y, text, width=95, color=None, size=12, lh=17):
    col = color or MUTED
    return "".join(f"<text x='{x}' y='{y + i * lh}' fill='{col}' font-size='{size}'>{esc(line)}</text>"
                   for i, line in enumerate(textwrap.wrap(text, width)))

def head(W, H, title, sub=None):
    s = [f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {W} {H}' width='{W}' height='{H}' {FONT} font-size='12'>",
         "<defs>"
         f"<marker id='a' markerWidth='8' markerHeight='8' refX='7' refY='4' orient='auto'><path d='M0,0 L8,4 L0,8 z' fill='{INK}'/></marker>"
         f"<marker id='n' markerWidth='8' markerHeight='8' refX='7' refY='4' orient='auto'><path d='M0,0 L8,4 L0,8 z' fill='{NET}'/></marker>"
         f"<marker id='r' markerWidth='8' markerHeight='8' refX='7' refY='4' orient='auto'><path d='M0,0 L8,4 L0,8 z' fill='{RING}'/></marker>"
         "</defs>",
         f"<rect width='{W}' height='{H}' fill='white'/>",
         f"<text x='16' y='24' font-size='15' font-weight='bold' fill='{INK}'>{esc(title)}</text>"]
    if sub:
        s.append(f"<text x='16' y='42' fill='{MUTED}'>{esc(sub)}</text>")
    return s

def box(x, y, w, h, text, sub=None, fill="white", stroke=INK, bold=True, size=12):
    t = f"<rect x='{x}' y='{y}' width='{w}' height='{h}' rx='6' fill='{fill}' stroke='{stroke}' stroke-width='1.5'/>"
    t += f"<text x='{x + w / 2}' y='{y + h / 2 + (-2 if sub else 5)}' text-anchor='middle' fill='{INK}' font-size='{size}' font-weight='{'bold' if bold else 'normal'}'>{esc(text)}</text>"
    if sub:
        t += f"<text x='{x + w / 2}' y='{y + h / 2 + 14}' text-anchor='middle' fill='{MUTED}' font-size='11'>{esc(sub)}</text>"
    return t

def ring(x, y, w, h, text="ring", sub="/dev/shm"):
    return box(x, y, w, h, text, sub, fill=RING_FILL, stroke=RING)

def panel(x, y, w, h, title, fill=PANEL):
    return (f"<rect x='{x}' y='{y}' width='{w}' height='{h}' rx='10' fill='{fill}' stroke='{GRID}'/>"
            f"<text x='{x + 12}' y='{y + 20}' fill='{INK}' font-weight='bold' font-size='13'>{esc(title)}</text>")

def arrow(x1, y1, x2, y2, label=None, color=INK, marker="a", dash=None, above=True, size=11):
    d = f" stroke-dasharray='{dash}'" if dash else ""
    t = f"<line x1='{x1}' y1='{y1}' x2='{x2}' y2='{y2}' stroke='{color}' stroke-width='1.5' marker-end='url(#{marker})'{d}/>"
    if label:
        ly = min(y1, y2) - 7 if above else max(y1, y2) + 15
        t += f"<text x='{(x1 + x2) / 2}' y='{ly}' text-anchor='middle' fill='{color}' font-size='{size}'>{esc(label)}</text>"
    return t

def write(name, s):
    s.append("</svg>")
    open(os.path.join(OUT, name), "w").write("\n".join(s))

# 1. The pipeline of one record: source ring to mirror ring, with measured latencies ----------
W, H = 960, 340
s = head(W, H, "One record's path: source ring to mirror ring, same sequence number everywhere",
         "push on the source host → read on the mirror host, measured, 64-byte records")
s.append(panel(16, 56, 330, 190, "source host"))
s.append(box(30, 96, 86, 44, "producer", "push()"))
s.append(arrow(116, 118, 142, 118))
s.append(ring(142, 90, 96, 56, "ring", "seq 1, 2, 3 …"))
s.append(arrow(238, 104, 262, 104))
s.append(box(262, 86, 74, 36, "serve", "raw reader", size=11))
s.append(arrow(238, 132, 262, 160))
s.append(box(262, 150, 74, 36, "readers", "0.1 µs", size=11))
s.append(f"<text x='181' y='222' text-anchor='middle' fill='{MUTED}' font-size='11'>fixed slots, or descriptors + arena</text>")
# network band
s.append(f"<rect x='356' y='66' width='250' height='170' rx='10' fill='{NET_FILL}' stroke='{NET}' stroke-dasharray='4 3'/>")
s.append(f"<text x='481' y='86' text-anchor='middle' fill='{NET}' font-weight='bold'>network</text>")
s.append(arrow(336, 118, 616, 118, "DATA: raw slot bytes + seq", NET, "n"))
s.append(f"<text x='481' y='146' text-anchor='middle' fill='{NET}' font-size='11'>UDP multicast · UDP unicast · TCP</text>")
s.append(arrow(616, 186, 336, 186, "NAK / GAP over TCP", NET, "n", dash="4 3", above=False))
s.append(f"<text x='481' y='214' text-anchor='middle' fill='{MUTED}' font-size='11'>the source ring is</text>")
s.append(f"<text x='481' y='228' text-anchor='middle' fill='{MUTED}' font-size='11'>the retransmission buffer</text>")
s.append(panel(616, 56, 328, 190, "mirror host"))
s.append(box(630, 100, 84, 36, "mirror", "one writer", size=11))
s.append(arrow(714, 118, 736, 118))
s.append(ring(736, 90, 96, 56, "ring", "same seq"))
s.append(arrow(832, 118, 852, 118))
s.append(box(852, 96, 80, 44, "readers", "as local", size=11))
s.append(f"<text x='780' y='222' text-anchor='middle' fill='{MUTED}' font-size='11'>written in order, never duplicated</text>")
y = 276
for x, label, val in [(30, "same ring", "0.1 µs"), (250, "mirror on the same host", "3.8 µs"),
                      (490, "mirror across a 1 GbE LAN", "30 µs"), (730, "Tokyo → Los Angeles", "51.7 ms, p99 +50 µs")]:
    s.append(f"<text x='{x}' y='{y}' fill='{INK}' font-weight='bold' font-size='13'>{esc(val)}</text>")
    s.append(f"<text x='{x}' y='{y + 16}' fill='{MUTED}' font-size='11'>{esc(label)}</text>")
s.append(f"<text x='30' y='{y + 40}' fill='{MUTED}' font-size='11'>push → read, p50, measured on each host; remote hosts corrected for clock offset</text>")
write("mirror-pipeline.svg", s)

# 2. LAN: multicast fan-out --------------------------------------------------------------------
W, H = 900, 400
s = head(W, H, "On a LAN with a switch: one datagram, every mirror",
         "UDP multicast; the source's cost does not grow with the number of mirrors")
s.append(panel(16, 60, 250, 150, "source host"))
s.append(box(30, 100, 80, 40, "producer", size=11))
s.append(arrow(110, 120, 138, 120))
s.append(ring(138, 92, 100, 56, "ring"))
s.append(arrow(238, 120, 300, 120, "1 sendto", NET, "n"))
s.append(box(300, 96, 90, 48, "switch", "IGMP snooping", fill=NET_FILL, stroke=NET))
hosts = ["mirror host 1", "mirror host 2", "mirror host 3", "… host 16"]
for i, name in enumerate(hosts):
    y = 60 + i * 62
    s.append(arrow(390, 120, 440, y + 26, None, NET, "n"))
    s.append(panel(440, y, 440, 52, name))
    s.append(box(560, y + 10, 70, 32, "mirror", size=11))
    s.append(arrow(630, y + 26, 660, y + 26))
    s.append(ring(660, y + 8, 80, 36, "ring", None))
    s.append(arrow(740, y + 26, 770, y + 26))
    s.append(box(770, y + 10, 100, 32, "readers", size=11))
s.append(para(20, 330, "Measured with 16 extra mirrors on the second host, one message every 100 µs: round trip p50 63.7 µs, p99 72.1 µs, max 84 µs, every mirror complete. The same with a TCP stream per mirror: p99 235 µs, max 11 ms, two mirrors behind. Inside one host the mirror costs 3.8 µs; each ring hand-off 0.1 µs.", 105))
write("topology-lan.svg", s)

# 3. WAN into a cloud: unicast, NAT punch, dup, site hub ---------------------------------------
W, H = 920, 420
s = head(W, H, "Between sites and into a cloud: cross the network once per site",
         "UDP unicast with NAT punching and duplicated datagrams over the WAN; a hub mirror serves the site")
s.append(panel(16, 60, 230, 150, "Tokyo: source"))
s.append(box(30, 100, 74, 40, "producer", size=11))
s.append(arrow(104, 120, 128, 120))
s.append(ring(128, 92, 100, 56, "ring"))
s.append(f"<text x='128' y='168' fill='{MUTED}' font-size='11'>serve --udp 7403</text>")
s.append(f"<text x='128' y='184' fill='{MUTED}' font-size='11'>      --dup 2</text>")
# ocean
s.append(f"<rect x='256' y='70' width='170' height='150' rx='10' fill='{NET_FILL}' stroke='{NET}' stroke-dasharray='4 3'/>")
s.append(f"<text x='341' y='92' text-anchor='middle' fill='{NET}' font-weight='bold'>internet, 100 ms</text>")
s.append(arrow(228, 112, 436, 112, "every datagram twice", NET, "n"))
s.append(arrow(436, 150, 228, 150, None, NET, "n", dash="4 3"))
s.append(f"<text x='341' y='168' text-anchor='middle' fill='{NET}' font-size='11'>PUNCH opens the NAT; NAK</text>")
s.append(f"<text x='341' y='206' text-anchor='middle' fill='{MUTED}' font-size='11'>one copy per site</text>")
s.append(panel(436, 60, 468, 270, "AWS: hub + instances (no multicast in a VPC)"))
s.append(box(452, 92, 70, 36, "mirror", "--unicast", size=11))
s.append(arrow(522, 110, 548, 110))
s.append(ring(548, 92, 84, 36, "ring", None))
s.append(f"<text x='590' y='146' text-anchor='middle' fill='{MUTED}' font-size='11'>serve --udp 7403</text>")
for i in range(3):
    y = 170 + i * 50
    s.append(arrow(632, 110, 690, y + 18, None, NET, "n"))
    s.append(box(690, y, 60, 36, "mirror", size=11))
    s.append(arrow(750, y + 18, 774, y + 18))
    s.append(ring(774, y + 3, 56, 30, "ring", None))
    s.append(arrow(830, y + 18, 848, y + 18))
    s.append(box(848, y + 3, 48, 30, "readers", size=10))
s.append(para(20, 356, "Measured Tokyo → Los Angeles, 1,000 msg/s: TCP one-way p50 50.4 ms but p99 99.6 ms (a full extra round trip for one record in a hundred); UDP unicast p50 51.7 ms, p99 51.7 ms, max 61 ms; sent twice, max 58 ms. Through a hub on the LAN: 52.9 µs end to end against 10.3 µs direct, i.e. the hub costs its two hops and nothing of its own.", 118))
write("topology-wan-hub.svg", s)

# 4. Protocol sequence ------------------------------------------------------------------------------
W, H = 900, 480
s = head(W, H, "The wire protocol: handshake over TCP, live records by UDP, repairs over TCP",
         "16-byte frame header: kind · flags · count · len · seq; records are raw slot bytes")
lx, rx, top, bottom = 180, 720, 70, 466
for x, name in [(lx, "mirror"), (rx, "source")]:
    s.append(box(x - 60, top - 14, 120, 32, name, size=12))
    s.append(f"<line x1='{x}' y1='{top + 18}' x2='{x}' y2='{bottom}' stroke='{GRID}' stroke-width='2'/>")
def msg(y, text, to_source, tcp=True, lost=False, color=None):
    col = color or (INK if tcp else NET)
    marker = "a" if tcp else "n"
    x1, x2 = (lx, rx) if to_source else (rx, lx)
    if lost:
        mid = (x1 + x2) / 2
        s.append(f"<line x1='{x1}' y1='{y}' x2='{mid}' y2='{y}' stroke='{RING}' stroke-width='1.5' stroke-dasharray='4 3'/>")
        s.append(f"<text x='{mid + 8}' y='{y + 4}' fill='{RING}' font-weight='bold'>✕ lost</text>")
        s.append(f"<text x='{(x1 + x2) / 2}' y='{y - 7}' text-anchor='middle' fill='{RING}' font-size='11'>{esc(text)}</text>")
        return
    s.append(arrow(x1, y, x2, y, text, col, marker, None if tcp else "6 3"))
def note(y, text):
    w = 6.7 * len(text) + 16
    s.append(f"<rect x='{(lx + rx) / 2 - w / 2:.1f}' y='{y - 13}' width='{w:.1f}' height='19' fill='white'/>")
    s.append(f"<text x='{(lx + rx) / 2}' y='{y}' text-anchor='middle' fill='{MUTED}' font-size='11'>{esc(text)}</text>")
msg(110, "HELLO: wanted seq, magic, version, flags (UDP capable / prefers unicast)   [TCP]", True)
msg(140, "GEOMETRY: capacity, slot size, schema, arena; first seq that will come   [TCP]", False)
msg(170, "MULTICAST: group or 0.0.0.0 = unicast, port, MTU, session byte, token   [TCP]", False)
msg(200, "PUNCH: token, session   [UDP, unicast only; repeats until data arrives, then every 5 s]", True, tcp=False)
msg(240, "DATA seq 1–26   [UDP, session byte in flags]", False, tcp=False)
msg(270, "DATA seq 27–52", False, tcp=False, lost=True)
msg(300, "DATA seq 53–78   [UDP]  → held back: 27 is missing", False, tcp=False)
msg(330, "NAK 27–52   [TCP]", True)
msg(360, "DATA seq 27–52   [TCP, from the source ring]  → 27–78 written in order", False)
note(392, "GAP instead of DATA if the ring no longer holds the range: readers see a lapped count, never a reorder")
msg(420, "HEARTBEAT: last seq sent   [UDP, every 1 ms while idle: reveals a lost last datagram]", False, tcp=False)
note(452, "a datagram from another session byte is ignored; a mirror can be served again as a source")
write("protocol-sequence.svg", s)

# 5. Latency by stage (LAN) ---------------------------------------------------------------------------
def hbars(name, title, rows, unit, width=940, fmt="{:g}"):
    rowh, top, left, right = 30, 54, 400, 80
    h = top + rowh * len(rows) + 24
    vmax = max(v for _, v, _ in rows)
    scale = (width - left - right) / vmax
    s = head(width, h, title, unit)
    for i, (label, v, col) in enumerate(rows):
        y = top + i * rowh
        s.append(f"<text x='{left - 10}' y='{y + 17}' text-anchor='end' fill='{INK}'>{esc(label)}</text>")
        s.append(f"<rect x='{left}' y='{y + 4}' width='{max(2, v * scale):.1f}' height='{rowh - 10}' fill='{col}' rx='2'/>")
        s.append(f"<text x='{left + v * scale + 6:.1f}' y='{y + 17}' fill='{INK}'>{esc(fmt.format(v))}</text>")
    write(name, s)

hbars("latency-stages.svg", "push on the source → read by a consumer, p50, 1,000 msg/s",
      [("consumer on the source host, same ring", 0.1, RING),
       ("mirror on the same host, multicast", 3.8, NET),
       ("mirror on the same host, TCP", 9.0, OTHER),
       ("mirror on the same host, UDP unicast", 10.3, NET),
       ("mirror on another LAN host, multicast (each of 6)", 30.0, NET),
       ("mirror on another LAN host, TCP", 32.0, OTHER),
       ("mirror on another LAN host, UDP unicast (each of 6)", 43.0, NET),
       ("leaf behind a hub on another host, unicast twice", 52.9, NET)],
      "microseconds; two Ryzen 9 7950X hosts on a 1 GbE LAN, kernel network stack, busy-polling")

# 6. WAN percentiles ------------------------------------------------------------------------------------
def grouped(name, title, groups, series, unit, width=900, h=340):
    top, bottom, left, right = 58, 62, 70, 20
    plot_h = h - top - bottom
    vmax = max(v for _, vals, _ in series for v in vals) * 1.08
    n, m = len(groups), len(series)
    gw = (width - left - right) / n
    bw = gw / (m + 1)
    s = head(width, h, title, unit)
    step = 1
    while vmax / step > 7:
        step = step * 2 if str(step)[0] == "1" else (step * 5 // 2 if str(step)[0] == "2" else step * 2)
    g = 0
    while g <= vmax:
        y = top + plot_h - g / vmax * plot_h
        s.append(f"<line x1='{left}' x2='{width - right}' y1='{y:.1f}' y2='{y:.1f}' stroke='{GRID}'/>")
        s.append(f"<text x='{left - 6}' y='{y + 4:.1f}' text-anchor='end' fill='{MUTED}'>{g:g}</text>")
        g += step
    for gi, glabel in enumerate(groups):
        x0 = left + gi * gw + bw / 2
        for si, (sname, vals, col) in enumerate(series):
            v = vals[gi]
            bh = v / vmax * plot_h
            x = x0 + si * bw
            s.append(f"<rect x='{x:.1f}' y='{top + plot_h - bh:.1f}' width='{bw - 4:.1f}' height='{bh:.1f}' fill='{col}' rx='2'/>")
            s.append(f"<text x='{x + (bw - 4) / 2:.1f}' y='{top + plot_h - bh - 4:.1f}' text-anchor='middle' fill='{INK}' font-size='11'>{v:g}</text>")
        s.append(f"<text x='{left + gi * gw + gw / 2:.1f}' y='{top + plot_h + 18}' text-anchor='middle' fill='{INK}'>{esc(glabel)}</text>")
    lx = left
    for sname, _, col in series:
        s.append(f"<rect x='{lx}' y='{h - 24}' width='12' height='12' fill='{col}' rx='2'/>")
        s.append(f"<text x='{lx + 16}' y='{h - 14}' fill='{INK}'>{esc(sname)}</text>")
        lx += 18 + 7.5 * len(sname) + 24
    write(name, s)

grouped("wan-percentiles.svg", "Tokyo → Los Angeles, one-way latency by percentile, 1,000 msg/s for 5 s",
        ["p50", "p90", "p99", "p99.9", "max"],
        [("TCP", [50.4, 50.5, 99.6, 127.2, 132.2], OTHER),
         ("UDP unicast", [51.7, 51.7, 51.7, 56.2, 61.2], NET),
         ("UDP unicast, every datagram twice", [51.5, 51.5, 51.6, 53.6, 57.6], OK)],
        "milliseconds; 100 ms ping; TCP recovers about one record in a hundred with a full extra round trip")

# 7. Pacing under load -------------------------------------------------------------------------------------
grouped("pacing.svg", "Why frames are paced: round trip p50 by publish rate, multicast",
        ["20,000/s", "50,000/s", "100,000/s"],
        [("one record per datagram", [51, 830, 1200], OTHER),
         ("frames paced / lingered", [88, 213, 221], NET)],
        "microseconds; above ~20,000 datagrams/s the kernel path queues up, so frames are held up to 50 µs unless full",
        h=320)
print("figures written:", sorted(os.listdir(OUT)))
