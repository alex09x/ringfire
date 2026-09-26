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
//! * Rings with a payload arena ([`BlobProducer`](crate::BlobProducer)) are not supported.
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
//! | `GEOMETRY` 2 | source → mirror | first sequence that will be sent; flag bit 0 = the wanted sequence was ahead of the source (restart) | capacity u64, element_size u32, flags u32, schema_sig u64, registry_count u32, slots_offset u32 |
//! | `DATA` 3 | source → mirror | sequence of the first record | `count` records of `element_size - 8` bytes |
//! | `GAP` 4 | source → mirror | next sequence that will be sent | none |
//! | `HEARTBEAT` 5 | source → mirror | last sequence published by the source | none |
//!
//! `HEARTBEAT` is sent while the ring is idle so a mirror can tell a quiet source from a
//! dead link.

use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering, fence};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use memmap2::{Mmap, MmapMut};

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

const FRAME_HEADER_LEN: usize = 16;
const HELLO_LEN: usize = 16;
const GEOMETRY_LEN: usize = 32;
const KIND_HELLO: u8 = 1;
const KIND_GEOMETRY: u8 = 2;
const KIND_DATA: u8 = 3;
const KIND_GAP: u8 = 4;
const KIND_HEARTBEAT: u8 = 5;
/// `GEOMETRY` flag: the mirror asked for a sequence the source has not reached.
const GEOMETRY_RESET: u8 = 0x01;
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_BATCH: usize = 256;

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
        }
    }

    /// Bytes of payload per record (the slot minus its sequence word).
    pub fn payload_len(&self) -> usize {
        self.element_size as usize - 8
    }

    /// Same slot layout: a ring with this layout can hold this geometry's records.
    fn same_layout(&self, other: &Geometry) -> bool {
        self.capacity == other.capacity
            && self.element_size == other.element_size
            && self.schema_sig == other.schema_sig
            && self.registry_count == other.registry_count
            && self.slots_offset == other.slots_offset
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
    }

    fn encode(&self, out: &mut [u8; GEOMETRY_LEN]) {
        out[0..8].copy_from_slice(&self.capacity.to_le_bytes());
        out[8..12].copy_from_slice(&self.element_size.to_le_bytes());
        out[12..16].copy_from_slice(&self.flags.to_le_bytes());
        out[16..24].copy_from_slice(&self.schema_sig.to_le_bytes());
        out[24..28].copy_from_slice(&self.registry_count.to_le_bytes());
        out[28..32].copy_from_slice(&self.slots_offset.to_le_bytes());
    }

    fn decode(b: &[u8; GEOMETRY_LEN]) -> Result<Self> {
        let g = Self {
            capacity: u64::from_le_bytes(b[0..8].try_into().unwrap()),
            element_size: u32::from_le_bytes(b[8..12].try_into().unwrap()),
            flags: u32::from_le_bytes(b[12..16].try_into().unwrap()),
            schema_sig: u64::from_le_bytes(b[16..24].try_into().unwrap()),
            registry_count: u32::from_le_bytes(b[24..28].try_into().unwrap()),
            slots_offset: u32::from_le_bytes(b[28..32].try_into().unwrap()),
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
        if (g.capacity as usize)
            .checked_mul(g.element_size as usize)
            .and_then(|b| b.checked_add(g.slots_offset as usize))
            .is_none()
        {
            return Err(RingfireError::Protocol("geometry does not fit in memory"));
        }
        Ok(g)
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

/// `read_exact` that busy-polls a non-blocking socket when `spin` is set.
fn read_full(stream: &mut TcpStream, buf: &mut [u8], spin: bool) -> io::Result<()> {
    if !spin {
        return stream.read_exact(buf);
    }
    let mut filled = 0;
    while filled < buf.len() {
        match stream.read(&mut buf[filled..]) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => core::hint::spin_loop(),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Raw ring access (element type unknown)
// ---------------------------------------------------------------------------------------

enum RawRead {
    Item,
    Pending,
    Overwritten(u64),
}

/// Read-only view of the source ring; one per mirror connection.
struct SourceRing {
    base: *const u8,
    view: RingView,
    geometry: Geometry,
    _mmap: Mmap,
}

unsafe impl Send for SourceRing {}

impl SourceRing {
    fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let view = unsafe { validate_ring(mmap.as_ptr(), mmap.len(), None, 8)? };
        let header = unsafe { &*mmap.as_ptr().cast::<RingHeader>() };
        if header.flags & FLAG_WITH_ARENA != 0 {
            return Err(RingfireError::Unsupported(
                "rings with a payload arena cannot be mirrored",
            ));
        }
        let geometry = Geometry::from_header(header, &view);
        Ok(Self {
            base: mmap.as_ptr(),
            view,
            geometry,
            _mmap: mmap,
        })
    }

    fn header(&self) -> &RingHeader {
        unsafe { &*self.base.cast::<RingHeader>() }
    }

    fn write_seq(&self) -> u64 {
        self.header().write_seq.load(Ordering::Acquire)
    }

    /// Copies the payload of message `want` into `out` using the v2 slot protocol.
    fn read(&self, want: u64, out: &mut [u8]) -> RawRead {
        let slot = unsafe {
            self.base.add(
                self.view.slots_offset + (want & self.view.mask) as usize * self.view.slot_size,
            )
        };
        let seq = unsafe { &*slot.cast::<AtomicU64>() };
        let s1 = seq.load(Ordering::Acquire);
        if s1 == want {
            unsafe { std::ptr::copy_nonoverlapping(slot.add(8), out.as_mut_ptr(), out.len()) };
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
}

/// Single writer of a mirror ring, addressing slots by the source's sequence numbers.
struct MirrorRing {
    base: *mut u8,
    view: RingView,
    geometry: Geometry,
    /// Sequence the next record must carry.
    next_seq: u64,
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
        let total = slots_offset + capacity as usize * slot_size;

        let file = create_backing_file(path, mode, true, total as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let base = mmap.as_mut_ptr();
        unsafe {
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
                    arena_offset: 0,
                    arena_size: 0,
                },
            );
            if registry_count > 0 {
                let _ = ReaderRegistry::init(base.add(header_size), registry_count);
            }
            for i in 0..capacity as usize {
                let seq = &*base.add(slots_offset + i * slot_size).cast::<AtomicU64>();
                seq.store(0, Ordering::Relaxed);
            }
            RingHeader::publish(base.cast());
        }
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
        if header.flags & FLAG_WITH_ARENA != 0 {
            return Ok(None);
        }
        let geometry = Geometry::from_header(header, &view);
        let next_seq = header.write_seq.load(Ordering::Acquire) + 1;
        Ok(Some(Self {
            base,
            view,
            geometry,
            next_seq,
            _mmap: mmap,
            _file: file,
        }))
    }

    fn header(&self) -> &RingHeader {
        unsafe { &*self.base.cast::<RingHeader>() }
    }

    /// Writes record `seq` (which must be `next_seq`) without publishing it.
    fn write(&mut self, seq: u64, payload: &[u8]) {
        debug_assert_eq!(seq, self.next_seq);
        debug_assert_eq!(payload.len(), self.view.slot_size - 8);
        let slot = unsafe {
            self.base
                .add(self.view.slots_offset + (seq & self.view.mask) as usize * self.view.slot_size)
        };
        let word = unsafe { &*slot.cast::<AtomicU64>() };
        word.store(SLOT_WRITING, Ordering::Relaxed);
        fence(Ordering::Release);
        unsafe { std::ptr::copy_nonoverlapping(payload.as_ptr(), slot.add(8), payload.len()) };
        word.store(seq, Ordering::Release);
        self.next_seq = seq + 1;
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
/// own reader over the ring, so mirrors never wait for each other.
pub struct ReplicaServer {
    ring_path: PathBuf,
    listener: TcpListener,
    batch: usize,
    spin: bool,
}

impl ReplicaServer {
    /// Binds `addr` for the ring at `ring_path`. The ring must exist already.
    pub fn bind<P: AsRef<Path>, A: ToSocketAddrs>(ring_path: P, addr: A) -> Result<Self> {
        let ring_path = ring_path.as_ref().to_path_buf();
        // Fail early on a missing or unsupported ring rather than at the first connection.
        SourceRing::open(&ring_path)?;
        let listener = TcpListener::bind(addr)?;
        Ok(Self {
            ring_path,
            listener,
            batch: DEFAULT_BATCH,
            spin: false,
        })
    }

    /// Maximum records per `DATA` frame (default 256, at most 65535).
    pub fn batch(mut self, batch: usize) -> Self {
        self.batch = batch.clamp(1, u16::MAX as usize);
        self
    }

    /// Busy-poll the ring when idle instead of backing off to sleeps: lowest latency, one
    /// core per connected mirror.
    pub fn spin(mut self, spin: bool) -> Self {
        self.spin = spin;
        self
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn ring_path(&self) -> &Path {
        &self.ring_path
    }

    /// Accepts mirrors forever, serving each on its own thread.
    pub fn run(&self) -> Result<()> {
        loop {
            let (stream, peer) = self.listener.accept()?;
            let ring = SourceRing::open(&self.ring_path)?;
            let (batch, spin) = (self.batch, self.spin);
            thread::Builder::new()
                .name(format!("ringfire-serve-{}", peer))
                .spawn(move || {
                    let _ = serve_client(ring, stream, batch, spin);
                })?;
        }
    }

    /// Accepts exactly one mirror and serves it on the calling thread until it disconnects.
    pub fn serve_one(&self) -> Result<()> {
        let (stream, _) = self.listener.accept()?;
        let ring = SourceRing::open(&self.ring_path)?;
        serve_client(ring, stream, self.batch, self.spin)?;
        Ok(())
    }

    /// Runs [`ReplicaServer::run`] on a new thread.
    pub fn spawn(self) -> io::Result<JoinHandle<Result<()>>> {
        thread::Builder::new()
            .name("ringfire-serve".into())
            .spawn(move || self.run())
    }
}

fn serve_client(
    ring: SourceRing,
    mut stream: TcpStream,
    batch: usize,
    spin: bool,
) -> io::Result<()> {
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
    let (mut cursor, flags) = match hello.seq {
        HELLO_LATEST => (write_seq + 1, 0),
        HELLO_OLDEST => (oldest, 0),
        wanted if wanted > write_seq + 1 => (write_seq + 1, GEOMETRY_RESET),
        wanted => (wanted.max(oldest), 0),
    };

    let mut geometry = [0u8; GEOMETRY_LEN];
    ring.geometry.encode(&mut geometry);
    let mut out = Vec::with_capacity(FRAME_HEADER_LEN + GEOMETRY_LEN);
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
    stream.write_all(&out)?;

    let payload_len = ring.geometry.payload_len();
    let batch = batch.clamp(1, u16::MAX as usize);
    let mut buf = vec![0u8; FRAME_HEADER_LEN + batch * payload_len];
    let mut idle = 0u32;
    let mut last_beat = Instant::now();
    loop {
        let mut count = 0usize;
        let mut lapped = None;
        while count < batch {
            let start = FRAME_HEADER_LEN + count * payload_len;
            match ring.read(cursor + count as u64, &mut buf[start..start + payload_len]) {
                RawRead::Item => count += 1,
                RawRead::Pending => break,
                RawRead::Overwritten(seen) => {
                    lapped = Some(seen);
                    break;
                }
            }
        }
        if count > 0 {
            let frame = Frame {
                kind: KIND_DATA,
                flags: 0,
                count: count as u16,
                len: (count * payload_len) as u32,
                seq: cursor,
            };
            buf[..FRAME_HEADER_LEN].copy_from_slice(&frame.encode());
            stream.write_all(&buf[..FRAME_HEADER_LEN + count * payload_len])?;
            cursor += count as u64;
            idle = 0;
            last_beat = Instant::now();
        }
        if let Some(seen) = lapped {
            // Resynchronize at the oldest message still retained and tell the mirror.
            let resync = oldest_retained(ring.write_seq().max(seen), capacity);
            if resync > cursor {
                stream.write_all(&Frame::control(KIND_GAP, resync).encode())?;
                cursor = resync;
            }
            continue;
        }
        if count == 0 {
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
        }
    }

    pub fn start(mut self, start: MirrorStart) -> Self {
        self.start = start;
        self
    }

    /// Busy-poll the socket instead of blocking in the kernel: lowest latency, one core.
    pub fn spin(mut self, spin: bool) -> Self {
        self.spin = spin;
        self
    }

    /// Permission bits of the mirror ring file (default `0o660`).
    pub fn file_mode(mut self, mode: u32) -> Self {
        self.file_mode = mode;
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

        let (ring, resumed) = match adopted {
            Some(ring) if !reset && ring.geometry.same_layout(&geometry) => (ring, true),
            other => {
                drop(other);
                let mut ring = MirrorRing::create(&ring_path, &geometry, self.file_mode)?;
                ring.skip_to(frame.seq);
                (ring, false)
            }
        };
        if self.spin {
            stream.set_nonblocking(true)?;
        }
        Ok(Mirror {
            stream,
            ring: Some(ring),
            ring_path,
            geometry,
            spin: self.spin,
            file_mode: self.file_mode,
            first_seq: frame.seq,
            resumed,
            source_seq: frame.seq.saturating_sub(1),
            frames: 0,
            gaps: 0,
            buf: Vec::new(),
        })
    }
}

/// Maintains a mirror ring: receives records from a [`ReplicaServer`] and writes them
/// into a local ring under the source's sequence numbers.
pub struct Mirror {
    stream: TcpStream,
    ring: Option<MirrorRing>,
    ring_path: PathBuf,
    geometry: Geometry,
    spin: bool,
    file_mode: u32,
    first_seq: u64,
    resumed: bool,
    source_seq: u64,
    frames: u64,
    gaps: u64,
    buf: Vec<u8>,
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

    /// Frames received.
    pub fn frames(&self) -> u64 {
        self.frames
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

    /// Processes one frame. `Ok(false)` once the source has closed the connection.
    pub fn step(&mut self) -> Result<bool> {
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
                let payload_len = self.geometry.payload_len();
                let count = frame.count as usize;
                if count == 0 || frame.len as usize != count * payload_len {
                    return Err(RingfireError::Protocol(
                        "DATA length does not match its record count",
                    ));
                }
                let total = count * payload_len;
                if self.buf.len() < total {
                    self.buf.resize(total, 0);
                }
                read_full(&mut self.stream, &mut self.buf[..total], self.spin)?;
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
                let ring = self.ring.as_mut().expect("mirror ring");
                for i in 0..count {
                    ring.write(
                        frame.seq + i as u64,
                        &self.buf[i * payload_len..(i + 1) * payload_len],
                    );
                }
                ring.publish();
                self.source_seq = self.source_seq.max(ring.next_seq - 1);
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
}

impl std::fmt::Debug for Mirror {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mirror")
            .field("path", &self.ring_path)
            .field("sequence", &self.sequence())
            .field("source_sequence", &self.source_seq)
            .field("gaps", &self.gaps)
            .finish()
    }
}
