# Mirroring a shared-memory ring across hosts

*How ringfire replicates a ring to other machines with the same sequence numbers,
what a stress test taught us on the way, and what it costs on a LAN, across an
ocean and through a cloud hub.*

## The problem

A market-data process on one machine parses exchange frames into a ring in
`/dev/shm`. Strategies, testers and recorders on that machine read the ring at
~100 ns per hand-off and never see the network. Then the fleet grows: a 128-core
box for testers, a cloud region next to an exchange, a second site. Every one of
those wants the same stream, in the same order, as close to "local" as physics
allows, and none of them may slow the source down.

Message brokers were ruled out on day one: an extra process, an extra serialization,
tens of microseconds per hop before any network, and a queue whose ordering and
retention semantics are not the ring's. What we wanted was simpler: **the ring
itself, on the other machine.**

## The design

A ring is a fixed-size array of slots, each carrying a sequence number and a payload
(for `BlobProducer` rings, the payload is a descriptor pointing into a byte arena).
The writer publishes sequence `n` into slot `n mod capacity`. Readers each keep their
own cursor and never block the writer. That structure is what gets copied.

```text
source host                                   mirror host
producer ─▶ ring ◀── serve (raw reader) ══▶ mirror (single writer) ─▶ ring ◀── readers
                       one per mirror,          same geometry,
                       or one UDP sender        same sequence numbers
```

* **Serve** maps the ring read-only and reads records exactly like a `RingConsumer`
  does, slot protocol and all, but as raw bytes: it does not know the element type.
  For arena rings it copies the blob too and re-checks the arena's reservation counter,
  so a payload overwritten underneath it is reported lost rather than shipped torn.
* **Mirror** creates a ring from the geometry the source sends (capacity, slot size,
  schema signature, registry size, arena) and writes each record under its original
  sequence number with the same slot protocol writers use. Blobs go into the mirror's
  own arena and the descriptor is rewritten to point there; readers cannot tell.
* A mirror ring carries `FLAG_SPARSE`: its sequence numbers may have holes, because a
  mirror that joins mid-stream starts at the source's current sequence. `RingConsumer`
  and `BlobConsumer` on a sparse ring skip to the next record present instead of
  waiting for one that will never come. Rings that are not mirrors are unchanged.
* Mirror rings are always single-writer, latest-wins broadcast rings. The mirror writer
  never waits for local readers: a slow reader on a mirror host is that reader's
  problem, exactly as on the source host.

## The protocol

Little-endian, a 16-byte header per frame: `kind u8 | flags u8 | count u16 | len u32 |
seq u64`, payload follows.

| kind | direction | seq | payload |
|---|---|---|---|
| `HELLO` 1 | mirror → source | first wanted sequence (`0` new only, `u64::MAX` oldest retained, else resume there) | magic, version, flags (can receive UDP unicast, prefers it) |
| `GEOMETRY` 2 | source → mirror | first sequence that will be sent | capacity, element size, flags, schema signature, registry size, slots offset, arena offset and size |
| `MULTICAST` 7 | source → mirror | | group (`0.0.0.0` = unicast), port, MTU, TTL, session byte, token |
| `DATA` 3 | source → mirror | first record's sequence | records: slot payload, then blob bytes for arena rings |
| `GAP` 4 | source → mirror | next sequence that will come | |
| `HEARTBEAT` 5 | source → mirror | last sequence sent | |
| `NAK` 6 | mirror → source | first missing | last missing |
| `PUNCH` 8 | mirror → source (UDP) | mirror's token | |

Three transports carry `DATA`:

1. **TCP**: one connection per mirror carries everything. Ordered and reliable by
   construction; the source pays a thread and a `write` per mirror per frame.
2. **UDP multicast**: the source sends each `DATA` frame once to a group; every mirror
   on the LAN receives it. The TCP connection stays for the handshake and for
   retransmission: a mirror that sees a sequence jump sends `NAK`, the source answers
   with `DATA` from its ring, or `GAP` for what it no longer retains. Frames carry a
   one-byte session so datagrams from an earlier incarnation of the source are ignored.
   A 1 ms heartbeat while idle is how a lost *last* datagram is noticed.
3. **UDP unicast**: for routes without multicast (between sites, into clouds, from behind
   NAT). The mirror announces the capability in `HELLO`, learns the source's UDP port and
   a token, and punches to it; the source replies to whatever address the punch came
   from. `--dup N` sends every datagram N times; mirrors drop the copies by sequence, so
   a single loss on a long link costs no round trip.

Whatever the transport, a mirror writes its ring only at `next_seq`. Datagrams that
overtake a hole are held back (up to 8,192 of them) while one `NAK` (up to 65,535
records, repeated after 20 ms) fills it; retransmissions that arrive after multicast
already delivered the tail are dropped as stale. **Reordering and duplication cannot
happen.** What can happen, only under overload, is a visible gap.

### Gaps, and why they are rare

The source ring is circular: `capacity` records after `n`, slot `n` is overwritten.
That is the whole retention model. A mirror gets a `GAP` in exactly one situation: it
needs a record the source has already overwritten. That takes one of:

* the source's own sender lapped, because the producer sustained more than the sender
  can drain (about 1 M records/s at 64 bytes on a kernel network stack) for longer than
  the ring holds;
* a mirror was gone (link down, process stalled) for longer than the ring holds:
  262,144 slots is 262 ms at 1 M msg/s, 26 s at 10 k msg/s; an ordinary network loss is
  repaired in ~50 µs and cannot come close;
* for arena rings, an arena smaller than `capacity × typical payload`, so bytes are
  overwritten before their descriptor.

Every stress run below finished with zero gaps; the only gaps in the test suite come
from rings deliberately built with 16 slots or a 64 KiB arena to exercise the path.

## What the stress test found

An open-loop stress (`examples/replication_stress.rs`: publish at a fixed rate for a
fixed time, never wait, match echoes by sequence) found three real bugs before any
number was worth reporting:

1. **A heartbeat race.** The multicast heartbeat announced the ring's `write_seq`. A
   record published between the sender's last look at the ring and that load was
   announced before its datagram went out, and every mirror asked for it. Heartbeats now
   carry the sender's last *sent* sequence.
2. **An unbounded drain.** `Mirror::step` processed every queued datagram before
   returning, so a single-threaded caller that interleaves its own work never got control
   back at 500 k msg/s. It now handles at most 32 datagrams per call.
3. **No batching while keeping up.** A sender that keeps pace with the producer sends one
   record per datagram. Above ~20,000 msg/s that is a system call and a packet per record,
   and this kernel path sustains about 40,000 datagrams/s: latency went from 50 µs to over
   a millisecond. Frames are now paced: they leave at most once per 50 µs unless full,
   and a lone record arriving later than that after the previous frame goes out at once,
   so quiet and bursty streams pay nothing. Three pacing rules were tried; the two that
   waited "whenever the previous frame was recent" or "whenever a backlog was seen"
   delayed steady 20 k/s streams or the tails of bursts, and were measured out.

Two measurement artifacts are worth passing on: a mirror that connects with `latest`
130 ms after the producer started looks like a constant 2.5 % loss (it is history, not
loss); and a mirror process given one core for its two busy-polling threads reports
millisecond latencies quantized by the scheduler. Two cores per mirror.

## Measurements

All on Linux 6.8, kernel network stack, 64-byte records, everything busy-polling.
Two Ryzen 9 7950X hosts (`booster`, `ram9`) on a 1 GbE LAN, shared with other work,
so treat ±10 µs at the median as noise between runs.

### Per stage, on every host at once

`examples/replication_stages.rs`: the master stamps each record on push; a consumer
on the master and one on each of eight slaves (six on the second host, two on the
master's own host) stamp the read. Slave clocks are translated into the master's with
a PTP-style offset from the minimum-round-trip probe, so cross-host figures carry a
few microseconds of systematic uncertainty; same-host figures are exact.

| Stage, 1,000 msg/s | multicast | UDP unicast | TCP |
| :--- | ---: | ---: | ---: |
| push → read by a consumer on the master | 0.1 µs | 0.1 µs | 0.1 µs |
| push → read on a mirror on the same host | 3.8–4.1 µs | 8.8–21 µs | 9 µs |
| push → read on each of six mirrors on the other host | 29.6–32.4 µs | 38–48 µs | 31–34 µs |

Multicast is one `sendto` per frame regardless of mirrors; unicast is one per mirror,
about 1.5 µs each, so later mirrors in the list wait longer.

### More mirrors

Round trip between the two hosts (two network hops, four ring hand-offs), one message
every 100 µs, 20,000 samples, with 16 extra mirrors of the source ring on the second
host. All 16 held every record afterwards.

| Extra mirrors | Transport | p50 | p90 | p99 | max |
| ---: | :--- | ---: | ---: | ---: | ---: |
| 0 | TCP | 54.4 µs | 56.1 µs | 60.3 µs | 82 µs |
| 16 | TCP | 53.5 µs | 102.2 µs | 235.1 µs | 11.0 ms |
| 0 | multicast | 52.5 µs | 53.9 µs | 58.5 µs | 68 µs |
| 16 | multicast | 63.7 µs | 66.2 µs | 72.1 µs | 84 µs |

With TCP the source pays a thread and a `write` per mirror per message, and two of the
sixteen were still behind when the run ended.

### Sustained rate

Open loop, 5 s per point, every record echoed back and matched by sequence.

| Rate | Transport, frame linger | Delivered | RTT p50 | RTT p99 |
| ---: | :--- | ---: | ---: | ---: |
| 1,000/s | multicast, adaptive | 100 % | 55 µs | 62 µs |
| 5,000/s | multicast, adaptive | 100 % | 55 µs | 62 µs |
| bursts of 4 at 4,000/s | multicast, adaptive | 100 % | 62–69 µs | 117–130 µs |
| 20,000/s | multicast, none | 100 % | 51 µs | 65 µs |
| 20,000/s | multicast, adaptive | 100 % | 88 µs | 115 µs |
| 50,000/s | multicast, none | 100 % | 830 µs | 1.5 ms |
| 50,000/s | multicast, 100 µs | 100 % | 213 µs | 268 µs |
| 100,000/s | multicast, 100 µs | 100 % | 221 µs | 272 µs |
| 500,000/s | multicast, 100 µs | 100 % | 122 µs | 3.5 ms |
| 1,000,000/s | multicast, 300 µs | 100 % | 0.93 ms | 1.8 ms |
| 100,000/s | TCP | 100 % | 1.2 ms | 4.0 ms |

No record lost or reordered anywhere, 5 million records at 1 M/s included. At 100 k/s
and above the numbers were bimodal between identical runs on these shared hosts
(90 µs in one run, 1.8 ms in the next); the cause was not isolated and is not a protocol
property. Near 1 M/s the 1500-byte MTU (26 records per datagram) is the limit.

### Across an ocean

Source in Tokyo, mirror in Los Angeles behind a home NAT, 100 ms ping, 1,000 msg/s for
5 s. One-way figures use a clock offset whose error over such a path is a few
milliseconds, so compare spreads, not medians; the echo round trips are on one clock.

| Transport | one-way p50 | p90 | p99 | p99.9 | max |
| :--- | ---: | ---: | ---: | ---: | ---: |
| TCP | 50.4 ms | 50.5 ms | 99.6 ms | 127 ms | 132 ms |
| UDP unicast | 51.7 ms | 51.7 ms | 51.7 ms | 56.2 ms | 61.2 ms |
| UDP unicast, every datagram twice | 51.5 ms | 51.5 ms | 51.6 ms | 53.6 ms | 57.6 ms |

TCP spends a full round trip recovering about one record in a hundred; the UDP path's
99th percentile sits 50 µs above its median across the Pacific, no `NAK` was needed, and
sending twice trims the last of the tail. The NAT was punched on the first try.

### Through a site hub

Master on the first host, hub on the second (`ringfire mirror --unicast` and
`ringfire serve --udp` on the same ring), leaf back on the first host, unicast on both
hops; a direct mirror on the first host measured the same records for reference.

| Path | p50 | p99 | max |
| :--- | ---: | ---: | ---: |
| direct mirror | 10.3 µs | 11.1 µs | 17.5 µs |
| via the hub | 52.9 µs | 57.3 µs | 87 µs |

The hub costs its two network hops and nothing measurable of its own.

## Deployment recipes

**A LAN with its own switch**: `serve --multicast` on the source, `mirror --iface` on
every host, jumbo frames (`--mtu 8972`) once the switch and NICs allow.

**A second site or a cloud region**: `serve --udp 7403 --dup 2` on the source; one
`mirror --unicast` per site, then `serve` on that mirror ring for local readers, by
multicast if the site has it, by `--udp` otherwise. Clouds do not route multicast
(AWS only through Transit Gateway multicast domains, at a cost and an extra hop), so
inside a VPC the hub sends unicast to each instance: about 1.5 µs per instance per
frame. Put the instances in a cluster placement group.

**Processes on the hub itself** need no network at all: they read the mirror ring.

## Verification

Nineteen integration tests: exact byte-for-byte copies, start modes, hole skipping by
readers, resume without duplicates, restart of the source, constant lapping without torn
records, multicast with injected datagram loss and injected reordering, tail loss caught
by heartbeat, datagrams from another session ignored, blob rings over TCP and multicast
with payloads beyond a datagram, arena lapping without corruption, unicast with
duplicates, and a mirror served again as a source. Plus the three examples used above
(`replication_stress`, `replication_stages`, `replication_pingpong`) and a loopback
`replication_latency`.

## What is next

Kernel bypass (`AF_XDP`, DPDK, Onload) for the multicast path: the two kernel stacks are
nearly all of the 27 µs a LAN hop costs. `sendmmsg` for unicast fan-out to many peers.
Jumbo frames for the 1 M/s regime. Mirror telemetry (lag, NAK and gap counts reported
back to the source) so a slow site is visible before it gaps.
