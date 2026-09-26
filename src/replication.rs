//! # Ring replication over the network
//!
//! Mirrors a ring published on one host (the *source*) into rings with the same geometry
//! and the same sequence numbers on other hosts (*mirrors*). A mirror is an ordinary ring
//! in `/dev/shm`: local readers attach with [`RingConsumer`](crate::RingConsumer) exactly
//! as they would on the source host, and never touch the network themselves.
//!
//! ```text
//!   source host                                  mirror host
//!   producer -> ring <- ReplicaServer  ==TCP==>  Mirror -> ring <- readers
//!                       one raw reader           single writer
//!                       per mirror
//! ```
//!
//! * Records travel as raw slot payloads tagged with the source's sequence numbers, so one
//!   `ringfire serve` / `ringfire mirror` pair works for any element type without
//!   recompiling, and a message keeps the same sequence number on every host.
//! * Delivery is in order and gap-free while the server-side reader keeps up with the
//!   source ring. If it is lapped (a stalled link, a mirror that stopped reading), it
//!   resynchronizes at the oldest retained message and the mirror receives a `GAP`. Local
//!   readers then observe a lapping, just as a slow reader on the source host would.
//! * A mirror ring is a single-writer broadcast ring (`FLAG_MODE_SPMC`,
//!   `FLAG_POLICY_LATEST_WINS`, `FLAG_SPARSE`): the mirror writer never waits for local
//!   readers, or the whole host would fall behind the source. `FLAG_SPARSE` tells readers
//!   that sequence numbers may have holes (a mirror that joined mid-stream starts at the
//!   source's current sequence), so they skip to the next message present instead of
//!   waiting for one that will never arrive.
//! * Fixed-size records (`RingProducer`) and variable-length ones
//!   ([`BlobProducer`](crate::BlobProducer), a descriptor ring plus a payload arena) are
//!   both mirrored. A blob keeps its bytes, length, flags and sequence; only its position
//!   in the mirror's arena differs, which no reader can observe.
//!
//! ## Wire protocol (version 1)
//!
//! Little-endian. Every frame starts with a 16-byte header:
//!
//! ```text
//! kind u8 | flags u8 | count u16 | len u32 | seq u64 | payload[len]
//! ```
//!
//! | kind | direction | `seq` | payload |
//! |---|---|---|---|
//! | `HELLO` 1 | mirror → source | first wanted sequence: `0` = only new messages, `u64::MAX` = oldest retained, otherwise resume from there | magic u64, version u32, pad u32 |
//! | `GEOMETRY` 2 | source → mirror | first sequence that will be sent; flag bit 0 = the wanted sequence was ahead of the source (restart), bit 1 = a `MULTICAST` frame follows | capacity u64, element_size u32, flags u32, schema_sig u64, registry_count u32, slots_offset u32, arena_offset u64, arena_size u64 |
//! | `DATA` 3 | source → mirror | sequence of the first record | `count` records: the slot payload (`element_size - 8` bytes), followed for arena rings by the blob bytes the descriptor names |
//! | `GAP` 4 | source → mirror | next sequence that will be sent | none |
//! | `HEARTBEAT` 5 | source → mirror | last sequence published by the source | none |
//!
//! `HEARTBEAT` is sent while the ring is idle so a mirror can tell a quiet source from a
//! dead link.
//!
//! ## UDP multicast (optional)
//!
//! With [`ReplicaServer::multicast`] the source sends every `DATA` frame once, as one UDP
//! datagram to a multicast group, and every mirror receives it: the network cost no longer
//! grows with the number of mirrors and no per-mirror TCP send sits between the ring and
//! the wire. The TCP session stays for the handshake and for **retransmission**: a mirror
//! that sees a sequence jump (a lost datagram, or history it asked for) sends `NAK` and
//! the source answers with `DATA` from its ring, or `GAP` for what it no longer retains.
//! Nothing is written to a mirror ring out of order.
//!
//! | kind | direction | `seq` | payload |
//! |---|---|---|---|
//! | `NAK` 6 | mirror → source (TCP) | first missing sequence | last missing sequence u64 |
//! | `MULTICAST` 7 | source → mirror (TCP, after `GEOMETRY`) | none | group ipv4 [u8; 4], port u16, mtu u16, ttl u8, session u8, pad [u8; 6] |
//!
//! Multicast `DATA` and `HEARTBEAT` frames carry the source's one-byte `session` in
//! `flags`, so datagrams from a previous incarnation of the source are ignored.
//! `HEARTBEAT` datagrams are sent every millisecond while the ring is idle, which is how a
//! lost *last* datagram is noticed.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::net::{
    Ipv4Addr, Shutdown, SocketAddr, SocketAddrV4, TcpListener, TcpStream, ToSocketAddrs, UdpSocket,
};
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering, fence};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use memmap2::{Mmap, MmapMut};

use crate::arena::{ArenaHeader, BlobRef, PayloadArena};
use crate::error::{Result, RingfireError};
use crate::header::{
    FLAG_MODE_SPMC, FLAG_POLICY_LATEST_WINS, FLAG_SPARSE, FLAG_WITH_ARENA, FLAG_WITH_REGISTRY,
    ReaderSlot, RingHeader, RingLayout, RingView, SLOT_WRITING, validate_ring,
};
use crate::registry::ReaderRegistry;
use crate::shm::create_backing_file;
use crate::spmc::oldest_retained;
use crate::wait::wake_futex;

/// Magic carried by `HELLO` ("RINGMIRR").
pub const REPLICATION_MAGIC: u64 = 0x5249_4E47_4D49_5252;
/// Wire protocol version.
pub const REPLICATION_VERSION: u32 = 1;
/// `HELLO` sequence asking for new messages only.
pub const HELLO_LATEST: u64 = 0;
/// `HELLO` sequence asking for everything the source still retains.
pub const HELLO_OLDEST: u64 = u64::MAX;
/// Datagram budget for a 1500-byte MTU (IPv4 + UDP headers subtracted).
pub const DEFAULT_MTU: usize = 1472;

const FRAME_HEADER_LEN: usize = 16;
const HELLO_LEN: usize = 16;
const GEOMETRY_LEN: usize = 48;
const NAK_LEN: usize = 8;
/// Size of the [`BlobRef`] that ends every arena-ring descriptor.
const BLOB_REF_LEN: usize = 16;
/// Largest UDP payload the kernel will send (IPv4, after fragmentation).
const UDP_MAX_PAYLOAD: usize = 65_507;
/// Frame budget on a TCP stream.
const TCP_FRAME_BYTES: usize = 1 << 20;
const MULTICAST_LEN: usize = 16;
const KIND_HELLO: u8 = 1;
const KIND_GEOMETRY: u8 = 2;
const KIND_DATA: u8 = 3;
const KIND_GAP: u8 = 4;
const KIND_HEARTBEAT: u8 = 5;
const KIND_NAK: u8 = 6;
const KIND_MULTICAST: u8 = 7;
/// `GEOMETRY` flag: the mirror asked for a sequence the source has not reached.
const GEOMETRY_RESET: u8 = 0x01;
/// `GEOMETRY` flag: a `MULTICAST` frame follows; live data arrives by multicast.
const GEOMETRY_MULTICAST: u8 = 0x02;
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_BATCH: usize = 256;
/// Largest datagram a mirror accepts (jumbo frames).
const MAX_DATAGRAM: usize = 65_536;
/// Most records one `NAK` asks for.
const NAK_MAX: u64 = u16::MAX as u64;
/// Out-of-order datagrams a mirror keeps while a hole is being filled.
const PENDING_MAX: usize = 8192;
/// How long a mirror sleeps in `poll` between checks when not busy-polling.
const POLL_SLICE: Duration = Duration::from_millis(10);
const _: () = assert!(
    cfg!(target_endian = "little"),
    "the replication protocol carries native slot bytes and assumes little-endian hosts"
);

/// Datagrams one `Mirror::step` processes before returning: small enough that a caller
/// interleaving its own work sees new records within a few frames.
const DATAGRAMS_PER_STEP: usize = 32;
/// Adaptive linger: when the previous frame went out less than this long ago, the sender
/// keeps collecting for up to the same time before sending the next one.
const ADAPTIVE_LINGER: Duration = Duration::from_micros(50);

/// Geometry of a ring as exchanged during the handshake: everything a mirror needs to
/// create an identical ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Geometry {
    pub capacity: u64,
    pub element_size: u32,
    pub flags: u32,
    pub schema_sig: u64,
    pub registry_count: u32,
    pub slots_offset: u32,
    /// Byte offset of the payload arena (0 = fixed-size records only).
    pub arena_offset: u64,
    /// Capacity of the payload arena in bytes (0 = none).
    pub arena_size: u64,
}

impl Geometry {
    fn from_header(header: &RingHeader, view: &RingView) -> Self {
        Self {
            capacity: view.capacity,
            element_size: view.slot_size as u32,
            flags: header.flags,
            schema_sig: header.schema_sig,
            registry_count: header.reader_registry_count,
            slots_offset: view.slots_offset as u32,
            arena_offset: header.arena_offset,
            arena_size: header.arena_size,
        }
    }

    /// Bytes of slot payload per record (the slot minus its sequence word). For arena
    /// rings this is the descriptor; the blob bytes follow it on the wire.
    pub fn payload_len(&self) -> usize {
        self.element_size as usize - 8
    }

    /// Whether records carry a variable-length payload in an arena (`BlobProducer` rings).
    pub fn has_arena(&self) -> bool {
        self.arena_offset != 0
    }

    /// Same slot layout: a ring with this layout can hold this geometry's records.
    fn same_layout(&self, other: &Geometry) -> bool {
        self.capacity == other.capacity
            && self.element_size == other.element_size
            && self.schema_sig == other.schema_sig
            && self.registry_count == other.registry_count
            && self.slots_offset == other.slots_offset
            && self.arena_offset == other.arena_offset
            && self.arena_size == other.arena_size
    }

    fn mirror_flags(&self) -> u32 {
        FLAG_MODE_SPMC
            | FLAG_POLICY_LATEST_WINS
            | FLAG_SPARSE
            | if self.registry_count > 0 {
                FLAG_WITH_REGISTRY
            } else {
                0
            }
            | if self.has_arena() { FLAG_WITH_ARENA } else { 0 }
    }

    /// Bytes the whole mapping takes.
    fn total_len(&self) -> usize {
        if self.has_arena() {
            self.arena_offset as usize
                + std::mem::size_of::<ArenaHeader>()
                + self.arena_size as usize
        } else {
            self.slots_offset as usize + self.capacity as usize * self.element_size as usize
        }
    }

    fn encode(&self, out: &mut [u8; GEOMETRY_LEN]) {
        out[0..8].copy_from_slice(&self.capacity.to_le_bytes());
        out[8..12].copy_from_slice(&self.element_size.to_le_bytes());
        out[12..16].copy_from_slice(&self.flags.to_le_bytes());
        out[16..24].copy_from_slice(&self.schema_sig.to_le_bytes());
        out[24..28].copy_from_slice(&self.registry_count.to_le_bytes());
        out[28..32].copy_from_slice(&self.slots_offset.to_le_bytes());
        out[32..40].copy_from_slice(&self.arena_offset.to_le_bytes());
        out[40..48].copy_from_slice(&self.arena_size.to_le_bytes());
    }

    fn decode(b: &[u8; GEOMETRY_LEN]) -> Result<Self> {
        let g = Self {
            capacity: u64::from_le_bytes(b[0..8].try_into().unwrap()),
            element_size: u32::from_le_bytes(b[8..12].try_into().unwrap()),
            flags: u32::from_le_bytes(b[12..16].try_into().unwrap()),
            schema_sig: u64::from_le_bytes(b[16..24].try_into().unwrap()),
            registry_count: u32::from_le_bytes(b[24..28].try_into().unwrap()),
            slots_offset: u32::from_le_bytes(b[28..32].try_into().unwrap()),
            arena_offset: u64::from_le_bytes(b[32..40].try_into().unwrap()),
            arena_size: u64::from_le_bytes(b[40..48].try_into().unwrap()),
        };
        if !g.capacity.is_power_of_two() {
            return Err(RingfireError::Protocol(
                "geometry capacity is not a power of two",
            ));
        }
        if g.element_size < 8 || !g.element_size.is_multiple_of(8) {
            return Err(RingfireError::Protocol(
                "geometry element size is not a multiple of 8",
            ));
        }
        let min_offset = std::mem::size_of::<RingHeader>()
            + g.registry_count as usize * std::mem::size_of::<ReaderSlot>();
        if (g.slots_offset as usize) < min_offset || !g.slots_offset.is_multiple_of(64) {
            return Err(RingfireError::Protocol(
                "geometry slots offset is misplaced",
            ));
        }
        let Some(slots_end) = (g.capacity as usize)
            .checked_mul(g.element_size as usize)
            .and_then(|b| b.checked_add(g.slots_offset as usize))
        else {
            return Err(RingfireError::Protocol("geometry does not fit in memory"));
        };
        if g.has_arena() {
            if (g.arena_offset as usize) < slots_end || !g.arena_offset.is_multiple_of(64) {
                return Err(RingfireError::Protocol(
                    "geometry arena offset is misplaced",
                ));
            }
            if !g.arena_size.is_power_of_two() || g.arena_size < 64 {
                return Err(RingfireError::Protocol(
                    "geometry arena size is not a power of two",
                ));
            }
            if g.payload_len() < BLOB_REF_LEN {
                return Err(RingfireError::Protocol(
                    "geometry arena ring has no blob descriptor",
                ));
            }
            if (g.arena_offset as usize)
                .checked_add(std::mem::size_of::<ArenaHeader>())
                .and_then(|b| b.checked_add(g.arena_size as usize))
                .is_none()
            {
                return Err(RingfireError::Protocol("geometry does not fit in memory"));
            }
        } else if g.arena_size != 0 {
            return Err(RingfireError::Protocol(
                "geometry arena size without an arena",
            ));
        }
        Ok(g)
    }
}

/// Multicast delivery settings for a [`ReplicaServer`].
#[derive(Debug, Clone, Copy)]
pub struct MulticastConfig {
    /// Multicast group, e.g. `239.255.0.1`.
    pub group: Ipv4Addr,
    pub port: u16,
    /// Address of the local interface to send on; unspecified = the default route.
    pub interface: Ipv4Addr,
    /// Datagram budget including the 16-byte frame header: [`DEFAULT_MTU`] for a
    /// 1500-byte link MTU, `8972` with jumbo frames.
    pub mtu: usize,
    pub ttl: u8,
    /// `HEARTBEAT` interval while the ring is idle: how quickly a lost last datagram is
    /// noticed.
    pub heartbeat: Duration,
    /// Fault injection for tests: skip every n-th datagram (0 = never).
    #[doc(hidden)]
    pub drop_every: u64,
    /// Fault injection for tests: send every n-th datagram after the one that follows it
    /// (0 = never), so mirrors see datagrams out of order.
    #[doc(hidden)]
    pub swap_every: u64,
}

impl MulticastConfig {
    pub fn new(group: Ipv4Addr, port: u16) -> Self {
        Self {
            group,
            port,
            interface: Ipv4Addr::UNSPECIFIED,
            mtu: DEFAULT_MTU,
            ttl: 1,
            heartbeat: Duration::from_millis(1),
            drop_every: 0,
            swap_every: 0,
        }
    }

    pub fn interface(mut self, interface: Ipv4Addr) -> Self {
        self.interface = interface;
        self
    }

    pub fn mtu(mut self, mtu: usize) -> Self {
        self.mtu = mtu.clamp(FRAME_HEADER_LEN + 8, MAX_DATAGRAM);
        self
    }

    pub fn ttl(mut self, ttl: u8) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn heartbeat(mut self, heartbeat: Duration) -> Self {
        self.heartbeat = heartbeat;
        self
    }

    #[doc(hidden)]
    pub fn drop_every(mut self, n: u64) -> Self {
        self.drop_every = n;
        self
    }

    #[doc(hidden)]
    pub fn swap_every(mut self, n: u64) -> Self {
        self.swap_every = n;
        self
    }

    fn encode(&self, session: u8, out: &mut [u8; MULTICAST_LEN]) {
        out[0..4].copy_from_slice(&self.group.octets());
        out[4..6].copy_from_slice(&self.port.to_le_bytes());
        out[6..8].copy_from_slice(&(self.mtu.min(u16::MAX as usize) as u16).to_le_bytes());
        out[8] = self.ttl;
        out[9] = session;
        out[10..16].fill(0);
    }
}

/// What a mirror learns from the `MULTICAST` frame.
#[derive(Debug, Clone, Copy)]
struct MulticastInfo {
    group: Ipv4Addr,
    port: u16,
    session: u8,
}

impl MulticastInfo {
    fn decode(b: &[u8; MULTICAST_LEN]) -> Result<Self> {
        let group = Ipv4Addr::new(b[0], b[1], b[2], b[3]);
        if !group.is_multicast() {
            return Err(RingfireError::Protocol(
                "MULTICAST group is not a multicast address",
            ));
        }
        Ok(Self {
            group,
            port: u16::from_le_bytes(b[4..6].try_into().unwrap()),
            session: b[9],
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Frame {
    kind: u8,
    flags: u8,
    count: u16,
    len: u32,
    seq: u64,
}

impl Frame {
    fn control(kind: u8, seq: u64) -> Self {
        Self {
            kind,
            flags: 0,
            count: 0,
            len: 0,
            seq,
        }
    }

    fn encode(&self) -> [u8; FRAME_HEADER_LEN] {
        let mut b = [0u8; FRAME_HEADER_LEN];
        b[0] = self.kind;
        b[1] = self.flags;
        b[2..4].copy_from_slice(&self.count.to_le_bytes());
        b[4..8].copy_from_slice(&self.len.to_le_bytes());
        b[8..16].copy_from_slice(&self.seq.to_le_bytes());
        b
    }

    fn decode(b: &[u8; FRAME_HEADER_LEN]) -> Self {
        Self {
            kind: b[0],
            flags: b[1],
            count: u16::from_le_bytes(b[2..4].try_into().unwrap()),
            len: u32::from_le_bytes(b[4..8].try_into().unwrap()),
            seq: u64::from_le_bytes(b[8..16].try_into().unwrap()),
        }
    }
}

fn protocol(what: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what)
}

// ---------------------------------------------------------------------------------------
// Sockets
// ---------------------------------------------------------------------------------------

/// Blocks until one of `fds` is readable or `timeout` passes.
fn wait_readable(fds: &[i32], timeout: Duration) {
    let mut polls: Vec<libc::pollfd> = fds
        .iter()
        .map(|&fd| libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        })
        .collect();
    let ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    unsafe { libc::poll(polls.as_mut_ptr(), polls.len() as libc::nfds_t, ms) };
}

/// `read_exact` that also works on a non-blocking socket: between chunks it busy-polls
/// when `spin` is set and sleeps in `poll` otherwise.
fn read_full(stream: &mut TcpStream, buf: &mut [u8], spin: bool) -> io::Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        match stream.read(&mut buf[filled..]) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                if spin {
                    core::hint::spin_loop();
                } else {
                    wait_readable(&[stream.as_raw_fd()], POLL_SLICE);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// `write_all` that also works on a non-blocking socket.
fn write_full(stream: &mut TcpStream, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        match stream.write(buf) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
            Ok(n) => buf = &buf[n..],
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                let mut poll = libc::pollfd {
                    fd: stream.as_raw_fd(),
                    events: libc::POLLOUT,
                    revents: 0,
                };
                unsafe { libc::poll(&mut poll, 1, POLL_SLICE.as_millis() as i32) };
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn set_sockopt<T>(fd: i32, level: i32, name: i32, value: &T) -> io::Result<()> {
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            (value as *const T).cast(),
            std::mem::size_of::<T>() as libc::socklen_t,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Sending socket for the multicast group described by `cfg`.
fn multicast_sender(cfg: &MulticastConfig) -> io::Result<UdpSocket> {
    let sock = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))?;
    sock.set_multicast_ttl_v4(u32::from(cfg.ttl))?;
    sock.set_multicast_loop_v4(true)?;
    if !cfg.interface.is_unspecified() {
        let addr = libc::in_addr {
            s_addr: u32::from(cfg.interface).to_be(),
        };
        set_sockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IP,
            libc::IP_MULTICAST_IF,
            &addr,
        )?;
    }
    Ok(sock)
}

/// Non-blocking receiving socket joined to `group` on `interface`, sharing the port with
/// other mirrors on the same host.
fn multicast_receiver(
    group: Ipv4Addr,
    port: u16,
    interface: Ipv4Addr,
    rcvbuf: usize,
) -> io::Result<UdpSocket> {
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a fresh descriptor we own.
    let sock = unsafe { UdpSocket::from_raw_fd(fd) };
    unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    set_sockopt(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, &1i32)?;
    #[cfg(not(target_os = "linux"))]
    set_sockopt(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, &1i32)?;
    if rcvbuf > 0 {
        let bytes = rcvbuf.min(i32::MAX as usize) as i32;
        set_sockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &bytes)?;
    }
    let mut addr: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    addr.sin_family = libc::AF_INET as libc::sa_family_t;
    addr.sin_port = port.to_be();
    addr.sin_addr.s_addr = u32::from(Ipv4Addr::UNSPECIFIED).to_be();
    let rc = unsafe {
        libc::bind(
            fd,
            (&addr as *const libc::sockaddr_in).cast(),
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    sock.join_multicast_v4(&group, &interface)?;
    sock.set_nonblocking(true)?;
    Ok(sock)
}

fn send_datagram(sock: &UdpSocket, dest: SocketAddrV4, bytes: &[u8]) -> io::Result<()> {
    loop {
        match sock.send_to(bytes, dest) {
            Ok(_) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => core::hint::spin_loop(),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

// ---------------------------------------------------------------------------------------
// Raw ring access (element type unknown)
// ---------------------------------------------------------------------------------------

/// Outcome of reading one record.
enum RawRead {
    Item,
    Pending,
    Overwritten(u64),
    /// The descriptor was intact but its payload had already been overwritten in the
    /// arena (or pointed outside it): the record is gone for good.
    Lost,
}

/// What one `collect` gathered.
struct Collected {
    count: usize,
    lapped: Option<u64>,
    /// The record after the collected ones is lost (see [`RawRead::Lost`]).
    lost: bool,
}

/// The blob descriptor stored at the end of a record's slot payload.
fn blob_ref_at(payload: &[u8]) -> BlobRef {
    let b = &payload[payload.len() - BLOB_REF_LEN..];
    BlobRef {
        offset: u64::from_le_bytes(b[0..8].try_into().unwrap()),
        len: u32::from_le_bytes(b[8..12].try_into().unwrap()),
        flags: u32::from_le_bytes(b[12..16].try_into().unwrap()),
    }
}

fn put_blob_ref(payload: &mut [u8], r: BlobRef) {
    let n = payload.len();
    let b = &mut payload[n - BLOB_REF_LEN..];
    b[0..8].copy_from_slice(&r.offset.to_le_bytes());
    b[8..12].copy_from_slice(&r.len.to_le_bytes());
    b[12..16].copy_from_slice(&r.flags.to_le_bytes());
}

/// Read-only view of the source ring's payload arena.
struct ArenaView {
    header: *const ArenaHeader,
    data: *const u8,
    capacity: u64,
    mask: u64,
}

impl ArenaView {
    /// # Safety
    /// `base` must map a validated ring whose header names an arena.
    unsafe fn open(base: *const u8, geometry: &Geometry) -> Result<Self> {
        let header = unsafe { base.add(geometry.arena_offset as usize) }.cast::<ArenaHeader>();
        let (capacity, mask) = unsafe { ((*header).capacity, (*header).mask) };
        if capacity != geometry.arena_size || mask != capacity - 1 {
            return Err(RingfireError::CorruptLayout(
                "arena header disagrees with ring header",
            ));
        }
        Ok(Self {
            header,
            data: unsafe { header.cast::<u8>().add(std::mem::size_of::<ArenaHeader>()) },
            capacity,
            mask,
        })
    }

    fn in_bounds(&self, r: BlobRef) -> bool {
        (r.offset & self.mask) + r.len as u64 <= self.capacity
    }

    /// Appends the bytes `r` names to `out`. The copy is only trustworthy if
    /// [`ArenaView::is_lapped`] is false afterwards.
    fn copy_into(&self, r: BlobRef, out: &mut Vec<u8>) {
        let len = r.len as usize;
        out.reserve(len);
        unsafe {
            ptr::copy_nonoverlapping(
                self.data.add((r.offset & self.mask) as usize),
                out.as_mut_ptr().add(out.len()),
                len,
            );
            out.set_len(out.len() + len);
        }
    }

    fn is_lapped(&self, r: BlobRef) -> bool {
        fence(Ordering::Acquire);
        let reserved = unsafe { (*self.header).reserved.load(Ordering::Relaxed) };
        reserved.saturating_sub(r.offset) > self.capacity
    }
}

/// Read-only view of the source ring; one per mirror connection.
struct SourceRing {
    base: *const u8,
    view: RingView,
    geometry: Geometry,
    arena: Option<ArenaView>,
    _mmap: Mmap,
}

unsafe impl Send for SourceRing {}

impl SourceRing {
    fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let view = unsafe { validate_ring(mmap.as_ptr(), mmap.len(), None, 8)? };
        let header = unsafe { &*mmap.as_ptr().cast::<RingHeader>() };
        let geometry = Geometry::from_header(header, &view);
        let arena = if header.flags & FLAG_WITH_ARENA != 0 {
            if geometry.payload_len() < BLOB_REF_LEN {
                return Err(RingfireError::Unsupported(
                    "arena ring without a blob descriptor",
                ));
            }
            Some(unsafe { ArenaView::open(mmap.as_ptr(), &geometry)? })
        } else {
            None
        };
        Ok(Self {
            base: mmap.as_ptr(),
            view,
            geometry,
            arena,
            _mmap: mmap,
        })
    }

    fn header(&self) -> &RingHeader {
        unsafe { &*self.base.cast::<RingHeader>() }
    }

    fn write_seq(&self) -> u64 {
        self.header().write_seq.load(Ordering::Acquire)
    }

    /// Copies the slot payload of message `want` into `out` using the v2 slot protocol.
    fn read(&self, want: u64, out: &mut [u8]) -> RawRead {
        let slot = unsafe {
            self.base.add(
                self.view.slots_offset + (want & self.view.mask) as usize * self.view.slot_size,
            )
        };
        let seq = unsafe { &*slot.cast::<AtomicU64>() };
        let s1 = seq.load(Ordering::Acquire);
        if s1 == want {
            unsafe { ptr::copy_nonoverlapping(slot.add(8), out.as_mut_ptr(), out.len()) };
            fence(Ordering::Acquire);
            let s2 = seq.load(Ordering::Relaxed);
            if s2 == want {
                RawRead::Item
            } else {
                RawRead::Overwritten(if s2 == SLOT_WRITING { 0 } else { s2 })
            }
        } else if s1 == SLOT_WRITING || s1 < want {
            RawRead::Pending
        } else {
            RawRead::Overwritten(s1)
        }
    }

    /// Appends record `want` to `out`: its slot payload, then its blob bytes for arena
    /// rings. Leaves `out` unchanged unless it returns [`RawRead::Item`].
    fn read_record(&self, want: u64, out: &mut Vec<u8>) -> RawRead {
        let payload_len = self.geometry.payload_len();
        let start = out.len();
        out.resize(start + payload_len, 0);
        match self.read(want, &mut out[start..]) {
            RawRead::Item => {}
            other => {
                out.truncate(start);
                return other;
            }
        }
        if let Some(arena) = &self.arena {
            let r = blob_ref_at(&out[start..]);
            if r.len > 0 {
                if !arena.in_bounds(r) {
                    out.truncate(start);
                    return RawRead::Lost;
                }
                arena.copy_into(r, out);
                if arena.is_lapped(r) {
                    out.truncate(start);
                    return RawRead::Lost;
                }
            }
        }
        RawRead::Item
    }

    /// Appends up to `max_records` consecutive records from `cursor` to `out`, which
    /// already holds the frame header, stopping before `max_bytes` would be exceeded
    /// unless the frame is still empty (a single record is never split).
    fn collect(
        &self,
        cursor: u64,
        max_records: usize,
        max_bytes: usize,
        out: &mut Vec<u8>,
    ) -> Collected {
        let mut count = 0usize;
        while count < max_records {
            let before = out.len();
            match self.read_record(cursor + count as u64, out) {
                RawRead::Item => {
                    if count > 0 && out.len() > max_bytes {
                        out.truncate(before);
                        break;
                    }
                    count += 1;
                    if out.len() >= max_bytes {
                        break;
                    }
                }
                RawRead::Pending => break,
                RawRead::Overwritten(seen) => {
                    return Collected {
                        count,
                        lapped: Some(seen),
                        lost: false,
                    };
                }
                RawRead::Lost => {
                    return Collected {
                        count,
                        lapped: None,
                        lost: true,
                    };
                }
            }
        }
        Collected {
            count,
            lapped: None,
            lost: false,
        }
    }

    /// Where a lapped reader continues.
    fn resync(&self, seen: u64) -> u64 {
        oldest_retained(self.write_seq().max(seen), self.view.capacity)
    }

    /// [`SourceRing::collect`] that, once it has something, keeps collecting for up to
    /// `linger` until the frame is full. Trades a few microseconds for far fewer sends
    /// under load: without it a sender that keeps up with the producer puts one or two
    /// records in every frame and pays a system call for each.
    fn collect_lingering(
        &self,
        cursor: u64,
        max_records: usize,
        max_bytes: usize,
        out: &mut Vec<u8>,
        linger: Duration,
    ) -> Collected {
        let mut got = self.collect(cursor, max_records, max_bytes, out);
        let full = |got: &Collected, out: &Vec<u8>| {
            got.count >= max_records || out.len() >= max_bytes || got.lapped.is_some() || got.lost
        };
        if got.count == 0 || full(&got, out) || linger.is_zero() {
            return got;
        }
        let until = Instant::now() + linger;
        while !full(&got, out) && Instant::now() < until {
            let more = self.collect(
                cursor + got.count as u64,
                max_records - got.count,
                max_bytes,
                out,
            );
            got.count += more.count;
            got.lapped = more.lapped;
            got.lost = more.lost;
            if more.count == 0 {
                core::hint::spin_loop();
            }
        }
        got
    }
}

/// Fills in the `DATA` header of a frame built by `collect` and returns the whole frame.
fn data_frame(out: &mut [u8], flags: u8, count: usize, seq: u64) -> &[u8] {
    let frame = Frame {
        kind: KIND_DATA,
        flags,
        count: count as u16,
        len: (out.len() - FRAME_HEADER_LEN) as u32,
        seq,
    };
    out[..FRAME_HEADER_LEN].copy_from_slice(&frame.encode());
    out
}

/// Single writer of a mirror ring, addressing slots by the source's sequence numbers.
struct MirrorRing {
    base: *mut u8,
    view: RingView,
    geometry: Geometry,
    /// Sequence the next record must carry.
    next_seq: u64,
    arena: Option<PayloadArena>,
    _mmap: MmapMut,
    _file: std::fs::File,
}

unsafe impl Send for MirrorRing {}

impl MirrorRing {
    /// Creates a fresh mirror ring with `geometry`, replacing whatever file is at `path`.
    fn create(path: &Path, geometry: &Geometry, mode: u32) -> Result<Self> {
        let header_size = std::mem::size_of::<RingHeader>();
        let registry_count = geometry.registry_count as usize;
        let slot_size = geometry.element_size as usize;
        let slots_offset = geometry.slots_offset as usize;
        let capacity = geometry.capacity;
        let total = geometry.total_len();

        let file = create_backing_file(path, mode, true, total as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let base = mmap.as_mut_ptr();
        let arena = unsafe {
            RingHeader::initialize(
                base.cast(),
                &RingLayout {
                    capacity,
                    slot_size,
                    flags: geometry.mirror_flags(),
                    schema_sig: geometry.schema_sig,
                    claim_seq: 0,
                    read_seq: 0,
                    registry_offset: if registry_count > 0 { header_size } else { 0 },
                    registry_count,
                    slots_offset,
                    arena_offset: geometry.arena_offset as usize,
                    arena_size: geometry.arena_size as usize,
                },
            );
            if registry_count > 0 {
                let _ = ReaderRegistry::init(base.add(header_size), registry_count);
            }
            for i in 0..capacity as usize {
                let seq = &*base.add(slots_offset + i * slot_size).cast::<AtomicU64>();
                seq.store(0, Ordering::Relaxed);
            }
            let arena = if geometry.has_arena() {
                Some(PayloadArena::init(
                    base.add(geometry.arena_offset as usize),
                    geometry.arena_size as usize,
                )?)
            } else {
                None
            };
            RingHeader::publish(base.cast());
            arena
        };
        Ok(Self {
            base,
            view: RingView {
                capacity,
                mask: capacity - 1,
                slot_size,
                slots_offset,
            },
            geometry: *geometry,
            next_seq: 1,
            arena,
            _mmap: mmap,
            _file: file,
        })
    }

    /// Takes over an existing mirror ring at `path` (its previous writer must be gone).
    /// `Ok(None)` when there is nothing usable there.
    fn adopt(path: &Path) -> Result<Option<Self>> {
        let file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(RingfireError::ProducerAlreadyExists);
        }
        let Ok(mut mmap) = (unsafe { MmapMut::map_mut(&file) }) else {
            return Ok(None);
        };
        let base = mmap.as_mut_ptr();
        let Ok(view) = (unsafe { validate_ring(base, mmap.len(), None, 8) }) else {
            return Ok(None);
        };
        let header = unsafe { &*base.cast::<RingHeader>() };
        let geometry = Geometry::from_header(header, &view);
        let arena = if geometry.has_arena() {
            match unsafe { PayloadArena::from_ptr(base.add(geometry.arena_offset as usize)) } {
                Ok(arena) if arena.capacity() as u64 == geometry.arena_size => Some(arena),
                _ => return Ok(None),
            }
        } else {
            None
        };
        let next_seq = header.write_seq.load(Ordering::Acquire) + 1;
        Ok(Some(Self {
            base,
            view,
            geometry,
            next_seq,
            arena,
            _mmap: mmap,
            _file: file,
        }))
    }

    fn header(&self) -> &RingHeader {
        unsafe { &*self.base.cast::<RingHeader>() }
    }

    fn slot(&self, seq: u64) -> *mut u8 {
        unsafe {
            self.base
                .add(self.view.slots_offset + (seq & self.view.mask) as usize * self.view.slot_size)
        }
    }

    /// Writes fixed-size record `seq` (which must be `next_seq`) without publishing it.
    fn write(&mut self, seq: u64, payload: &[u8]) {
        debug_assert_eq!(seq, self.next_seq);
        debug_assert_eq!(payload.len(), self.view.slot_size - 8);
        let slot = self.slot(seq);
        let word = unsafe { &*slot.cast::<AtomicU64>() };
        word.store(SLOT_WRITING, Ordering::Relaxed);
        fence(Ordering::Release);
        unsafe { ptr::copy_nonoverlapping(payload.as_ptr(), slot.add(8), payload.len()) };
        word.store(seq, Ordering::Release);
        self.next_seq = seq + 1;
    }

    /// Writes arena record `seq`: the blob goes into this ring's arena and the descriptor
    /// is stored pointing at it, keeping the source's length and flags.
    fn write_blob(&mut self, seq: u64, descriptor: &[u8], blob: &[u8]) -> Result<()> {
        debug_assert_eq!(seq, self.next_seq);
        debug_assert_eq!(descriptor.len(), self.view.slot_size - 8);
        let arena = self.arena.as_ref().expect("arena ring");
        let source = blob_ref_at(descriptor);
        let placed = arena.write_blob(blob, source.flags)?;
        let slot = self.slot(seq);
        let word = unsafe { &*slot.cast::<AtomicU64>() };
        word.store(SLOT_WRITING, Ordering::Relaxed);
        fence(Ordering::Release);
        unsafe {
            ptr::copy_nonoverlapping(descriptor.as_ptr(), slot.add(8), descriptor.len());
            put_blob_ref(
                std::slice::from_raw_parts_mut(slot.add(8), descriptor.len()),
                placed,
            );
        }
        word.store(seq, Ordering::Release);
        self.next_seq = seq + 1;
        Ok(())
    }

    /// Publishes everything written so far and wakes sleeping readers.
    fn publish(&self) {
        let header = self.header();
        header.write_seq.store(self.next_seq - 1, Ordering::Release);
        wake_futex(header, i32::MAX);
    }

    /// Moves the write position forward, leaving a hole.
    fn skip_to(&mut self, seq: u64) {
        if seq > self.next_seq {
            self.next_seq = seq;
        }
    }
}

// ---------------------------------------------------------------------------------------
// Source side
// ---------------------------------------------------------------------------------------

/// Serves a ring to network mirrors. Each accepted connection gets its own thread and its
/// own reader over the ring, so mirrors never wait for each other. With
/// [`ReplicaServer::multicast`], live records go out once by UDP multicast and the
/// connections only serve handshakes and retransmissions.
pub struct ReplicaServer {
    ring_path: PathBuf,
    listener: TcpListener,
    batch: usize,
    spin: bool,
    linger: Option<Duration>,
    multicast: Option<MulticastConfig>,
    session: u8,
}

/// Decides how long the next frame may be held to fill up: a fixed setting, or the
/// adaptive rule (batch only while frames go out back to back).
#[derive(Clone, Copy)]
struct Linger {
    fixed: Option<Duration>,
    last_send: Instant,
}

impl Linger {
    fn new(fixed: Option<Duration>) -> Self {
        Self {
            fixed,
            last_send: Instant::now() - ADAPTIVE_LINGER,
        }
    }

    fn current(&self) -> Duration {
        match self.fixed {
            Some(fixed) => fixed,
            None if self.last_send.elapsed() < ADAPTIVE_LINGER => ADAPTIVE_LINGER,
            None => Duration::ZERO,
        }
    }

    fn sent(&mut self) {
        self.last_send = Instant::now();
    }
}

impl ReplicaServer {
    /// Binds `addr` for the ring at `ring_path`. The ring must exist already.
    pub fn bind<P: AsRef<Path>, A: ToSocketAddrs>(ring_path: P, addr: A) -> Result<Self> {
        let ring_path = ring_path.as_ref().to_path_buf();
        // Fail early on a missing or unsupported ring rather than at the first connection.
        SourceRing::open(&ring_path)?;
        let listener = TcpListener::bind(addr)?;
        let session = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| (d.subsec_nanos() ^ d.as_secs() as u32) as u8)
            .unwrap_or(1);
        Ok(Self {
            ring_path,
            listener,
            batch: DEFAULT_BATCH,
            spin: false,
            linger: None,
            multicast: None,
            session,
        })
    }

    /// After finding new records, keep collecting for up to this long until a frame is
    /// full before sending it. Zero sends every record as soon as it is seen. The default
    /// (`None`) is adaptive: no waiting while frames are sparse, up to 50 µs while they
    /// go out back to back. Each frame costs a system call and a packet, so at
    /// 100,000 messages/s unbatched frames alone push latency past a millisecond on a
    /// kernel network stack; see the stress example.
    pub fn linger(mut self, linger: Option<Duration>) -> Self {
        self.linger = linger;
        self
    }

    /// Maximum records per `DATA` frame (default 256, at most 65535). With multicast the
    /// datagram budget (`mtu`) caps it further.
    pub fn batch(mut self, batch: usize) -> Self {
        self.batch = batch.clamp(1, u16::MAX as usize);
        self
    }

    /// Busy-poll the ring when idle instead of backing off to sleeps: lowest latency, one
    /// core per connected mirror (one core in total with multicast).
    pub fn spin(mut self, spin: bool) -> Self {
        self.spin = spin;
        self
    }

    /// Send live records by UDP multicast; TCP connections then only handle handshakes
    /// and `NAK` retransmissions.
    pub fn multicast(mut self, config: MulticastConfig) -> Self {
        self.multicast = Some(config);
        self
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn ring_path(&self) -> &Path {
        &self.ring_path
    }

    /// Starts the multicast sender thread when configured.
    fn start_multicast(&self) -> Result<()> {
        let Some(cfg) = self.multicast else {
            return Ok(());
        };
        let ring = SourceRing::open(&self.ring_path)?;
        let (batch, spin, session, linger) = (self.batch, self.spin, self.session, self.linger);
        thread::Builder::new()
            .name("ringfire-multicast".into())
            .spawn(move || {
                if let Err(e) = multicast_loop(ring, cfg, session, batch, spin, linger) {
                    eprintln!("ringfire multicast sender stopped: {}", e);
                }
            })?;
        Ok(())
    }

    /// Accepts mirrors forever, serving each on its own thread.
    pub fn run(&self) -> Result<()> {
        self.start_multicast()?;
        loop {
            let (stream, peer) = self.listener.accept()?;
            let ring = SourceRing::open(&self.ring_path)?;
            let (batch, spin, session, linger) = (self.batch, self.spin, self.session, self.linger);
            let multicast = self.multicast;
            thread::Builder::new()
                .name(format!("ringfire-serve-{}", peer))
                .spawn(move || {
                    let _ = serve_client(ring, stream, batch, spin, linger, multicast, session);
                })?;
        }
    }

    /// Accepts exactly one mirror and serves it on the calling thread until it disconnects
    /// (TCP delivery only; the multicast sender is not started).
    pub fn serve_one(&self) -> Result<()> {
        let (stream, _) = self.listener.accept()?;
        let ring = SourceRing::open(&self.ring_path)?;
        serve_client(
            ring,
            stream,
            self.batch,
            self.spin,
            self.linger,
            None,
            self.session,
        )?;
        Ok(())
    }

    /// Runs [`ReplicaServer::run`] on a new thread.
    pub fn spawn(self) -> io::Result<JoinHandle<Result<()>>> {
        thread::Builder::new()
            .name("ringfire-serve".into())
            .spawn(move || self.run())
    }
}

/// Sends every new record once, to the multicast group, as it appears in the ring.
fn multicast_loop(
    ring: SourceRing,
    cfg: MulticastConfig,
    session: u8,
    batch: usize,
    spin: bool,
    linger: Option<Duration>,
) -> io::Result<()> {
    let mut linger = Linger::new(linger);
    let sock = multicast_sender(&cfg)?;
    let dest = SocketAddrV4::new(cfg.group, cfg.port);
    let batch = batch.clamp(1, u16::MAX as usize);
    let mut max_bytes = cfg.mtu.max(FRAME_HEADER_LEN + 1);
    let mut wire: Vec<u8> = Vec::with_capacity(MAX_DATAGRAM);
    let mut cursor = ring.write_seq() + 1;
    let mut sent = 0u64;
    let mut idle = 0u32;
    let mut last_beat = Instant::now();
    // Fault injection: a datagram held back to go out after its successor.
    let mut held: Option<Vec<u8>> = None;
    loop {
        wire.clear();
        wire.resize(FRAME_HEADER_LEN, 0);
        let got = ring.collect_lingering(cursor, batch, max_bytes, &mut wire, linger.current());
        if got.count > 0 {
            if wire.len() > UDP_MAX_PAYLOAD {
                // One record larger than a datagram can carry: mirrors notice the jump
                // and fetch it over TCP.
                cursor += got.count as u64;
                continue;
            }
            sent += 1;
            let frame = data_frame(&mut wire, session, got.count, cursor);
            if cfg.swap_every != 0 && sent.is_multiple_of(cfg.swap_every) && held.is_none() {
                held = Some(frame.to_vec());
            } else if cfg.drop_every == 0 || !sent.is_multiple_of(cfg.drop_every) {
                match send_datagram(&sock, dest, frame) {
                    Ok(()) => {
                        if let Some(late) = held.take() {
                            send_datagram(&sock, dest, &late)?;
                        }
                    }
                    Err(e) if e.raw_os_error() == Some(libc::EMSGSIZE) => {
                        // Larger than this host lets a datagram be (macOS stops at
                        // 9 KiB by default): remember the limit and let mirrors fetch
                        // these records over TCP.
                        max_bytes = max_bytes
                            .min(wire.len().saturating_sub(1))
                            .max(FRAME_HEADER_LEN + 1);
                    }
                    Err(e) => return Err(e),
                }
            }
            linger.sent();
            cursor += got.count as u64;
            idle = 0;
            last_beat = Instant::now();
        }
        if let Some(seen) = got.lapped {
            // Mirrors notice the jump and NAK; the TCP side answers with GAP for what is
            // no longer retained.
            let resync = ring.resync(seen);
            if resync > cursor {
                cursor = resync;
            }
            continue;
        }
        if got.lost {
            // Its payload is gone from the arena: mirrors NAK and get a GAP over TCP.
            cursor += 1;
            continue;
        }
        if got.count == 0 {
            if spin {
                core::hint::spin_loop();
            } else {
                idle = idle.saturating_add(1);
                if idle < 2000 {
                    core::hint::spin_loop();
                } else if idle < 4000 {
                    thread::yield_now();
                } else {
                    thread::sleep(Duration::from_micros(50));
                }
            }
            if last_beat.elapsed() >= cfg.heartbeat {
                // Announce what this sender has put on the wire, not the ring's write_seq:
                // a record published between `collect` and this load would otherwise be
                // announced before its datagram and make every mirror NAK it.
                let mut beat = Frame::control(KIND_HEARTBEAT, cursor - 1);
                beat.flags = session;
                send_datagram(&sock, dest, &beat.encode())?;
                last_beat = Instant::now();
            }
        }
    }
}

fn serve_client(
    ring: SourceRing,
    mut stream: TcpStream,
    batch: usize,
    spin: bool,
    linger: Option<Duration>,
    multicast: Option<MulticastConfig>,
    session: u8,
) -> io::Result<()> {
    let mut linger = Linger::new(linger);
    stream.set_nodelay(true)?;
    stream.set_write_timeout(Some(WRITE_TIMEOUT))?;

    let mut hdr = [0u8; FRAME_HEADER_LEN];
    stream.read_exact(&mut hdr)?;
    let hello = Frame::decode(&hdr);
    if hello.kind != KIND_HELLO || hello.len as usize != HELLO_LEN {
        return Err(protocol("expected HELLO"));
    }
    let mut body = [0u8; HELLO_LEN];
    stream.read_exact(&mut body)?;
    let magic = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let version = u32::from_le_bytes(body[8..12].try_into().unwrap());
    if magic != REPLICATION_MAGIC {
        return Err(protocol("bad HELLO magic"));
    }
    if version != REPLICATION_VERSION {
        return Err(protocol("unsupported replication protocol version"));
    }

    let capacity = ring.view.capacity;
    let write_seq = ring.write_seq();
    let oldest = oldest_retained(write_seq, capacity);
    let (mut cursor, mut flags) = match hello.seq {
        HELLO_LATEST => (write_seq + 1, 0),
        HELLO_OLDEST => (oldest, 0),
        wanted if wanted > write_seq + 1 => (write_seq + 1, GEOMETRY_RESET),
        wanted => (wanted.max(oldest), 0),
    };
    if multicast.is_some() {
        flags |= GEOMETRY_MULTICAST;
    }

    let mut geometry = [0u8; GEOMETRY_LEN];
    ring.geometry.encode(&mut geometry);
    let mut out =
        Vec::with_capacity(FRAME_HEADER_LEN + GEOMETRY_LEN + FRAME_HEADER_LEN + MULTICAST_LEN);
    out.extend_from_slice(
        &Frame {
            kind: KIND_GEOMETRY,
            flags,
            count: 0,
            len: GEOMETRY_LEN as u32,
            seq: cursor,
        }
        .encode(),
    );
    out.extend_from_slice(&geometry);
    if let Some(cfg) = multicast {
        let mut info = [0u8; MULTICAST_LEN];
        cfg.encode(session, &mut info);
        out.extend_from_slice(
            &Frame {
                kind: KIND_MULTICAST,
                flags: 0,
                count: 0,
                len: MULTICAST_LEN as u32,
                seq: 0,
            }
            .encode(),
        );
        out.extend_from_slice(&info);
    }
    stream.write_all(&out)?;

    let batch = batch.clamp(1, u16::MAX as usize);
    let mut wire: Vec<u8> = Vec::with_capacity(1 << 16);

    if multicast.is_some() {
        // Live data goes by multicast; answer retransmission requests until the mirror
        // hangs up.
        loop {
            stream.read_exact(&mut hdr)?;
            let frame = Frame::decode(&hdr);
            match frame.kind {
                KIND_NAK if frame.len as usize == NAK_LEN => {
                    let mut to = [0u8; NAK_LEN];
                    stream.read_exact(&mut to)?;
                    let to = u64::from_le_bytes(to);
                    serve_range(&ring, &mut stream, frame.seq, to, batch, &mut wire)?;
                }
                _ => return Err(protocol("expected NAK")),
            }
        }
    }

    let mut idle = 0u32;
    let mut last_beat = Instant::now();
    loop {
        wire.clear();
        wire.resize(FRAME_HEADER_LEN, 0);
        let got =
            ring.collect_lingering(cursor, batch, TCP_FRAME_BYTES, &mut wire, linger.current());
        if got.count > 0 {
            stream.write_all(data_frame(&mut wire, 0, got.count, cursor))?;
            linger.sent();
            cursor += got.count as u64;
            idle = 0;
            last_beat = Instant::now();
        }
        if let Some(seen) = got.lapped {
            // Resynchronize at the oldest message still retained and tell the mirror.
            let resync = ring.resync(seen);
            if resync > cursor {
                stream.write_all(&Frame::control(KIND_GAP, resync).encode())?;
                cursor = resync;
            }
            continue;
        }
        if got.lost {
            cursor += 1;
            stream.write_all(&Frame::control(KIND_GAP, cursor).encode())?;
            continue;
        }
        if got.count == 0 {
            if spin {
                core::hint::spin_loop();
            } else {
                idle = idle.saturating_add(1);
                if idle < 2000 {
                    core::hint::spin_loop();
                } else if idle < 4000 {
                    thread::yield_now();
                } else {
                    thread::sleep(Duration::from_micros(50));
                }
            }
            if last_beat.elapsed() >= HEARTBEAT_INTERVAL {
                stream.write_all(&Frame::control(KIND_HEARTBEAT, ring.write_seq()).encode())?;
                last_beat = Instant::now();
            }
        }
    }
}

/// Answers a `NAK` for `[from, to]` from the ring: `GAP` for what is gone, `DATA` for the
/// rest, stopping at anything not published yet (that part arrives by multicast).
fn serve_range(
    ring: &SourceRing,
    stream: &mut TcpStream,
    from: u64,
    to: u64,
    batch: usize,
    wire: &mut Vec<u8>,
) -> io::Result<()> {
    let mut cursor = from.max(1);
    let oldest = oldest_retained(ring.write_seq(), ring.view.capacity);
    if cursor < oldest {
        stream.write_all(&Frame::control(KIND_GAP, oldest).encode())?;
        cursor = oldest;
    }
    while cursor <= to {
        let max = batch.min((to - cursor + 1) as usize);
        wire.clear();
        wire.resize(FRAME_HEADER_LEN, 0);
        let got = ring.collect(cursor, max, TCP_FRAME_BYTES, wire);
        if got.count > 0 {
            stream.write_all(data_frame(wire, 0, got.count, cursor))?;
            cursor += got.count as u64;
        }
        if let Some(seen) = got.lapped {
            let resync = ring.resync(seen);
            if resync > cursor {
                stream.write_all(&Frame::control(KIND_GAP, resync).encode())?;
                cursor = resync;
            }
            continue;
        }
        if got.lost {
            cursor += 1;
            stream.write_all(&Frame::control(KIND_GAP, cursor).encode())?;
            continue;
        }
        if got.count == 0 {
            break;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Mirror side
// ---------------------------------------------------------------------------------------

/// Where a mirror starts in the source's stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MirrorStart {
    /// Only messages published after the connection.
    #[default]
    Latest,
    /// Everything the source still retains, then live.
    Oldest,
    /// Continue after the last sequence in the existing mirror ring at the path (falls
    /// back to `Oldest` when there is none).
    Resume,
    /// From this sequence (clamped to what the source retains).
    Sequence(u64),
}

/// Configures a [`Mirror`].
#[derive(Debug, Clone, Copy)]
pub struct MirrorBuilder {
    start: MirrorStart,
    spin: bool,
    file_mode: u32,
    interface: Ipv4Addr,
    rcvbuf: usize,
    nak_timeout: Duration,
}

impl Default for MirrorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MirrorBuilder {
    pub fn new() -> Self {
        Self {
            start: MirrorStart::Latest,
            spin: false,
            file_mode: 0o660,
            interface: Ipv4Addr::UNSPECIFIED,
            rcvbuf: 8 << 20,
            nak_timeout: Duration::from_millis(20),
        }
    }

    pub fn start(mut self, start: MirrorStart) -> Self {
        self.start = start;
        self
    }

    /// Busy-poll the sockets instead of blocking in the kernel: lowest latency, one core.
    pub fn spin(mut self, spin: bool) -> Self {
        self.spin = spin;
        self
    }

    /// Permission bits of the mirror ring file (default `0o660`).
    pub fn file_mode(mut self, mode: u32) -> Self {
        self.file_mode = mode;
        self
    }

    /// Local interface address to receive multicast on (default: any).
    pub fn interface(mut self, interface: Ipv4Addr) -> Self {
        self.interface = interface;
        self
    }

    /// Kernel receive buffer for the multicast socket (default 8 MiB).
    pub fn rcvbuf(mut self, bytes: usize) -> Self {
        self.rcvbuf = bytes;
        self
    }

    /// How long to wait for a retransmission before repeating the `NAK` (default 20 ms).
    pub fn nak_timeout(mut self, timeout: Duration) -> Self {
        self.nak_timeout = timeout;
        self
    }

    /// Connects to a [`ReplicaServer`] at `source` and creates (or resumes) the mirror
    /// ring at `ring_path`. Returns once the ring exists, before any record is copied.
    pub fn connect<A: ToSocketAddrs, P: AsRef<Path>>(
        self,
        source: A,
        ring_path: P,
    ) -> Result<Mirror> {
        let ring_path = ring_path.as_ref().to_path_buf();
        let adopted = match self.start {
            MirrorStart::Resume => MirrorRing::adopt(&ring_path)?,
            _ => None,
        };
        let wanted = match self.start {
            MirrorStart::Latest => HELLO_LATEST,
            MirrorStart::Oldest => HELLO_OLDEST,
            MirrorStart::Sequence(seq) => seq.max(1),
            MirrorStart::Resume => adopted.as_ref().map_or(HELLO_OLDEST, |ring| ring.next_seq),
        };

        let mut stream = TcpStream::connect(source)?;
        stream.set_nodelay(true)?;
        let mut hello = Vec::with_capacity(FRAME_HEADER_LEN + HELLO_LEN);
        hello.extend_from_slice(
            &Frame {
                kind: KIND_HELLO,
                flags: 0,
                count: 0,
                len: HELLO_LEN as u32,
                seq: wanted,
            }
            .encode(),
        );
        hello.extend_from_slice(&REPLICATION_MAGIC.to_le_bytes());
        hello.extend_from_slice(&REPLICATION_VERSION.to_le_bytes());
        hello.extend_from_slice(&[0u8; 4]);
        stream.write_all(&hello)?;

        let mut hdr = [0u8; FRAME_HEADER_LEN];
        stream.read_exact(&mut hdr)?;
        let frame = Frame::decode(&hdr);
        if frame.kind != KIND_GEOMETRY || frame.len as usize != GEOMETRY_LEN {
            return Err(RingfireError::Protocol("expected GEOMETRY"));
        }
        let mut body = [0u8; GEOMETRY_LEN];
        stream.read_exact(&mut body)?;
        let geometry = Geometry::decode(&body)?;
        let reset = frame.flags & GEOMETRY_RESET != 0;

        let mut udp = None;
        let mut session = 0u8;
        if frame.flags & GEOMETRY_MULTICAST != 0 {
            stream.read_exact(&mut hdr)?;
            let mc = Frame::decode(&hdr);
            if mc.kind != KIND_MULTICAST || mc.len as usize != MULTICAST_LEN {
                return Err(RingfireError::Protocol("expected MULTICAST"));
            }
            let mut info = [0u8; MULTICAST_LEN];
            stream.read_exact(&mut info)?;
            let info = MulticastInfo::decode(&info)?;
            udp = Some(multicast_receiver(
                info.group,
                info.port,
                self.interface,
                self.rcvbuf,
            )?);
            session = info.session;
        }

        let (ring, resumed) = match adopted {
            Some(ring) if !reset && ring.geometry.same_layout(&geometry) => (ring, true),
            other => {
                drop(other);
                let mut ring = MirrorRing::create(&ring_path, &geometry, self.file_mode)?;
                ring.skip_to(frame.seq);
                (ring, false)
            }
        };
        if self.spin || udp.is_some() {
            stream.set_nonblocking(true)?;
        }
        Ok(Mirror {
            stream,
            udp,
            session,
            ring: Some(ring),
            ring_path,
            geometry,
            spin: self.spin,
            file_mode: self.file_mode,
            nak_timeout: self.nak_timeout,
            first_seq: frame.seq,
            resumed,
            source_seq: frame.seq.saturating_sub(1),
            frames: 0,
            gaps: 0,
            datagrams: 0,
            naks: 0,
            retransmitted: 0,
            nak: None,
            last_nak: None,
            pending: BTreeMap::new(),
            buf: Vec::new(),
            dgram: vec![0u8; MAX_DATAGRAM],
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct Nak {
    from: u64,
    to: u64,
    sent: Instant,
}

/// Maintains a mirror ring: receives records from a [`ReplicaServer`] and writes them
/// into a local ring under the source's sequence numbers.
pub struct Mirror {
    stream: TcpStream,
    udp: Option<UdpSocket>,
    session: u8,
    ring: Option<MirrorRing>,
    ring_path: PathBuf,
    geometry: Geometry,
    spin: bool,
    file_mode: u32,
    nak_timeout: Duration,
    first_seq: u64,
    resumed: bool,
    source_seq: u64,
    frames: u64,
    gaps: u64,
    datagrams: u64,
    naks: u64,
    retransmitted: u64,
    nak: Option<Nak>,
    last_nak: Option<(u64, u64)>,
    /// Multicast datagrams that arrived ahead of a hole, by first sequence: record
    /// count and frame payload.
    pending: BTreeMap<u64, (usize, Vec<u8>)>,
    buf: Vec<u8>,
    dgram: Vec<u8>,
}

/// Lets another thread stop a running [`Mirror`] by shutting its connection down.
#[derive(Debug)]
pub struct MirrorHandle {
    stream: TcpStream,
}

impl MirrorHandle {
    pub fn shutdown(&self) -> io::Result<()> {
        self.stream.shutdown(Shutdown::Both)
    }
}

impl Mirror {
    pub fn builder() -> MirrorBuilder {
        MirrorBuilder::new()
    }

    /// [`MirrorBuilder::connect`] with default options (start at the latest message).
    pub fn connect<A: ToSocketAddrs, P: AsRef<Path>>(source: A, ring_path: P) -> Result<Self> {
        MirrorBuilder::new().connect(source, ring_path)
    }

    pub fn handle(&self) -> io::Result<MirrorHandle> {
        Ok(MirrorHandle {
            stream: self.stream.try_clone()?,
        })
    }

    pub fn geometry(&self) -> Geometry {
        self.geometry
    }

    pub fn path(&self) -> &Path {
        &self.ring_path
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.peer_addr()
    }

    /// Whether live records arrive by multicast (with TCP only for retransmission).
    pub fn is_multicast(&self) -> bool {
        self.udp.is_some()
    }

    /// Session byte the source stamps on its multicast datagrams (tests).
    #[doc(hidden)]
    pub fn session(&self) -> u8 {
        self.session
    }

    /// Range of the last `NAK` sent, if any (tests).
    #[doc(hidden)]
    pub fn last_nak(&self) -> Option<(u64, u64)> {
        self.last_nak
    }

    /// First sequence the source agreed to send.
    pub fn first_sequence(&self) -> u64 {
        self.first_seq
    }

    /// Whether an existing mirror ring was continued instead of created.
    pub fn resumed(&self) -> bool {
        self.resumed
    }

    /// Last sequence written into the mirror ring.
    pub fn sequence(&self) -> u64 {
        self.ring().next_seq - 1
    }

    /// Last sequence the source reported (from data or heartbeats).
    pub fn source_sequence(&self) -> u64 {
        self.source_seq
    }

    /// Number of times the stream jumped forward (source lapped the server-side reader,
    /// or messages were missed while disconnected).
    pub fn gaps(&self) -> u64 {
        self.gaps
    }

    /// Frames received over TCP.
    pub fn frames(&self) -> u64 {
        self.frames
    }

    /// Multicast datagrams received.
    pub fn datagrams(&self) -> u64 {
        self.datagrams
    }

    /// Retransmission requests sent.
    pub fn naks(&self) -> u64 {
        self.naks
    }

    /// Records received by retransmission over TCP.
    pub fn retransmitted(&self) -> u64 {
        self.retransmitted
    }

    fn ring(&self) -> &MirrorRing {
        self.ring.as_ref().expect("mirror ring")
    }

    fn ring_mut(&mut self) -> &mut MirrorRing {
        self.ring.as_mut().expect("mirror ring")
    }

    /// Copies records until the source closes the connection (`Ok`) or fails.
    pub fn run(&mut self) -> Result<()> {
        while self.step()? {}
        Ok(())
    }

    /// Processes what is available (one TCP frame, or every queued datagram).
    /// `Ok(false)` once the source has closed the connection.
    pub fn step(&mut self) -> Result<bool> {
        if self.udp.is_some() {
            return self.step_multicast();
        }
        let mut hdr = [0u8; FRAME_HEADER_LEN];
        match read_full(&mut self.stream, &mut hdr, self.spin) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e.into()),
        }
        let frame = Frame::decode(&hdr);
        self.frames += 1;
        match frame.kind {
            KIND_DATA => {
                let (count, len) = match self.read_data_payload(frame) {
                    Ok(sizes) => sizes,
                    Err(RingfireError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                };
                let next = self.ring().next_seq;
                if frame.seq < next {
                    // The source restarted its numbering underneath us: start over.
                    self.ring = None;
                    let mut ring =
                        MirrorRing::create(&self.ring_path, &self.geometry, self.file_mode)?;
                    ring.skip_to(frame.seq);
                    self.ring = Some(ring);
                    self.gaps += 1;
                } else if frame.seq > next {
                    self.gaps += 1;
                    self.ring_mut().skip_to(frame.seq);
                }
                let bytes = std::mem::take(&mut self.buf);
                let result = self.write_records(frame.seq, count, &bytes[..len]);
                self.buf = bytes;
                result?;
            }
            KIND_GAP => {
                if frame.seq > self.ring().next_seq {
                    self.gaps += 1;
                    self.ring_mut().skip_to(frame.seq);
                }
            }
            KIND_HEARTBEAT => {
                self.source_seq = self.source_seq.max(frame.seq);
            }
            _ => return Err(RingfireError::Protocol("unexpected frame kind")),
        }
        Ok(true)
    }

    /// Reads a `DATA` frame's records into `self.buf`; returns the record count and the
    /// byte length.
    fn read_data_payload(&mut self, frame: Frame) -> Result<(usize, usize)> {
        let count = frame.count as usize;
        let len = frame.len as usize;
        if !self.frame_sizes_ok(count, len) {
            return Err(RingfireError::Protocol(
                "DATA length does not match its record count",
            ));
        }
        if self.buf.len() < len {
            self.buf.resize(len, 0);
        }
        read_full(&mut self.stream, &mut self.buf[..len], self.spin)?;
        Ok((count, len))
    }

    /// Whether `count` records can be `len` bytes long for this ring.
    fn frame_sizes_ok(&self, count: usize, len: usize) -> bool {
        let descriptors = count * self.geometry.payload_len();
        count > 0
            && if self.geometry.has_arena() {
                len >= descriptors
            } else {
                len == descriptors
            }
    }

    /// Writes the records of a `DATA` frame into the ring, skipping those below the
    /// write position, and publishes. Records are consecutive from `seq`.
    fn write_records(&mut self, seq: u64, count: usize, bytes: &[u8]) -> Result<()> {
        let payload_len = self.geometry.payload_len();
        let arena = self.geometry.has_arena();
        let ring = self.ring.as_mut().expect("mirror ring");
        let next = ring.next_seq;
        let mut pos = 0usize;
        for i in 0..count {
            let s = seq + i as u64;
            if pos + payload_len > bytes.len() {
                return Err(RingfireError::Protocol(
                    "DATA frame shorter than its records",
                ));
            }
            let descriptor = &bytes[pos..pos + payload_len];
            pos += payload_len;
            if arena {
                let len = blob_ref_at(descriptor).len as usize;
                if pos + len > bytes.len() {
                    return Err(RingfireError::Protocol("DATA frame shorter than its blobs"));
                }
                let blob = &bytes[pos..pos + len];
                pos += len;
                if s >= next {
                    ring.write_blob(s, descriptor, blob)?;
                }
            } else if s >= next {
                ring.write(s, descriptor);
            }
        }
        if pos != bytes.len() {
            return Err(RingfireError::Protocol(
                "DATA frame longer than its records",
            ));
        }
        ring.publish();
        self.source_seq = self.source_seq.max(seq + count as u64 - 1);
        Ok(())
    }

    // --- multicast mode -----------------------------------------------------------------

    fn step_multicast(&mut self) -> Result<bool> {
        let mut progressed = false;
        // Bounded, so a caller that interleaves other work with `step` gets control back
        // even when datagrams arrive faster than it drains them.
        let mut drained = 0;
        while drained < DATAGRAMS_PER_STEP {
            drained += 1;
            let received = {
                let udp = self.udp.as_ref().expect("multicast socket");
                udp.recv(&mut self.dgram)
            };
            match received {
                Ok(n) => {
                    progressed = true;
                    self.datagrams += 1;
                    let dgram = std::mem::take(&mut self.dgram);
                    let result = self.handle_datagram(&dgram[..n]);
                    self.dgram = dgram;
                    result?;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e.into()),
            }
        }
        match self.try_read_frame() {
            Ok(Some(frame)) => {
                progressed = true;
                self.frames += 1;
                match self.handle_control(frame) {
                    Ok(()) => {}
                    Err(RingfireError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(None) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e.into()),
        }
        if let Some(nak) = self.nak
            && nak.sent.elapsed() >= self.nak_timeout
        {
            self.send_nak(nak.from, nak.to)?;
            progressed = true;
        }
        if !progressed {
            if self.spin {
                core::hint::spin_loop();
            } else {
                let udp_fd = self.udp.as_ref().expect("multicast socket").as_raw_fd();
                wait_readable(
                    &[udp_fd, self.stream.as_raw_fd()],
                    self.nak_timeout.min(POLL_SLICE),
                );
            }
        }
        Ok(true)
    }

    /// A frame header from the control connection if one is available now.
    fn try_read_frame(&mut self) -> io::Result<Option<Frame>> {
        let mut hdr = [0u8; FRAME_HEADER_LEN];
        let n = match self.stream.read(&mut hdr) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(n) => n,
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::Interrupted =>
            {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        if n < FRAME_HEADER_LEN {
            read_full(&mut self.stream, &mut hdr[n..], self.spin)?;
        }
        Ok(Some(Frame::decode(&hdr)))
    }

    fn handle_control(&mut self, frame: Frame) -> Result<()> {
        match frame.kind {
            KIND_DATA => {
                let (count, len) = self.read_data_payload(frame)?;
                self.retransmitted += count as u64;
                let payload = std::mem::take(&mut self.buf);
                let result = self.apply(frame.seq, count, &payload[..len]);
                self.buf = payload;
                result
            }
            KIND_GAP => {
                if frame.seq > self.ring().next_seq {
                    self.gaps += 1;
                    self.ring_mut().skip_to(frame.seq);
                    self.after_advance()
                } else {
                    Ok(())
                }
            }
            KIND_HEARTBEAT => self.on_source_seq(frame.seq),
            KIND_MULTICAST => Ok(()),
            _ => Err(RingfireError::Protocol("unexpected frame kind")),
        }
    }

    fn handle_datagram(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() < FRAME_HEADER_LEN {
            return Ok(());
        }
        let frame = Frame::decode(bytes[..FRAME_HEADER_LEN].try_into().unwrap());
        if frame.flags != self.session {
            return Ok(());
        }
        match frame.kind {
            KIND_DATA => {
                let count = frame.count as usize;
                let len = frame.len as usize;
                if !self.frame_sizes_ok(count, len) || bytes.len() < FRAME_HEADER_LEN + len {
                    return Ok(());
                }
                self.apply(
                    frame.seq,
                    count,
                    &bytes[FRAME_HEADER_LEN..FRAME_HEADER_LEN + len],
                )
            }
            KIND_HEARTBEAT => self.on_source_seq(frame.seq),
            _ => Ok(()),
        }
    }

    /// Writes `count` records starting at `seq` if they continue the ring; keeps them
    /// aside and asks for the missing range otherwise.
    fn apply(&mut self, seq: u64, count: usize, payload: &[u8]) -> Result<()> {
        let next = self.ring().next_seq;
        let end = seq + count as u64;
        if end <= next {
            return Ok(());
        }
        if seq > next {
            if self.pending.len() < PENDING_MAX {
                self.pending
                    .entry(seq)
                    .or_insert_with(|| (count, payload.to_vec()));
            }
            return self.request(next, seq - 1);
        }
        self.write_records(seq, count, payload)?;
        self.after_advance()
    }

    /// After the write position moved: drop a satisfied `NAK` and apply queued datagrams
    /// that now continue the ring.
    fn after_advance(&mut self) -> Result<()> {
        loop {
            let next = self.ring().next_seq;
            if let Some(nak) = self.nak
                && next > nak.to
            {
                self.nak = None;
            }
            let Some((&seq, _)) = self.pending.first_key_value() else {
                return Ok(());
            };
            let (count, payload) = self.pending.remove(&seq).expect("first pending entry");
            let end = seq + count as u64;
            if end <= next {
                continue;
            }
            if seq > next {
                self.pending.insert(seq, (count, payload));
                return self.request(next, seq - 1);
            }
            self.write_records(seq, count, &payload)?;
        }
    }

    fn on_source_seq(&mut self, seq: u64) -> Result<()> {
        self.source_seq = self.source_seq.max(seq);
        let next = self.ring().next_seq;
        if seq >= next {
            self.request(next, seq)?;
        }
        Ok(())
    }

    /// Makes sure a `NAK` for `[from, to]` is outstanding (one at a time, bounded).
    fn request(&mut self, from: u64, to: u64) -> Result<()> {
        let to = to.min(from + NAK_MAX - 1);
        if let Some(nak) = self.nak
            && nak.from == from
            && nak.to >= to
            && nak.sent.elapsed() < self.nak_timeout
        {
            return Ok(());
        }
        self.send_nak(from, to)
    }

    fn send_nak(&mut self, from: u64, to: u64) -> Result<()> {
        let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + NAK_LEN);
        frame.extend_from_slice(
            &Frame {
                kind: KIND_NAK,
                flags: 0,
                count: 0,
                len: NAK_LEN as u32,
                seq: from,
            }
            .encode(),
        );
        frame.extend_from_slice(&to.to_le_bytes());
        write_full(&mut self.stream, &frame)?;
        self.nak = Some(Nak {
            from,
            to,
            sent: Instant::now(),
        });
        self.last_nak = Some((from, to));
        self.naks += 1;
        Ok(())
    }
}

impl std::fmt::Debug for Mirror {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mirror")
            .field("path", &self.ring_path)
            .field("multicast", &self.udp.is_some())
            .field("sequence", &self.sequence())
            .field("source_sequence", &self.source_seq)
            .field("gaps", &self.gaps)
            .field("naks", &self.naks)
            .finish()
    }
}
