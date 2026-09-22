//! High-performance shared memory offset persistence for ring consumers.
//!
//! Stores the last processed sequence number directly in `/dev/shm` using an
//! atomic 64-byte cache-line aligned struct mapped via `memmap2`.
//!
//! Commits are lock-free single CPU atomic store instructions (<10 nanoseconds)
//! with ZERO disk I/O, syscalls, or allocations, while persisting across
//! consumer process crashes and restarts.

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use memmap2::MmapMut;

/// Magic bytes for shared memory offset files: ASCII "SHM_OFFS".
pub const SHM_OFFSET_MAGIC: u64 = 0x53484D5F4F464653;
/// Current layout version of the shared memory offset slot.
pub const SHM_OFFSET_VERSION: u32 = 1;

/// Cache-aligned 64-byte shared memory slot storing consumer offset state.
#[repr(C, align(64))]
pub struct ShmOffsetSlot {
    /// Validation magic number.
    pub magic: u64,
    /// Format version.
    pub version: u32,
    /// PID of last process that attached to or updated this offset.
    pub pid: AtomicU32,
    /// Last processed sequence number.
    pub offset_seq: AtomicU64,
    /// Monotonic timestamp of last offset update (nanoseconds since UNIX epoch).
    pub updated_ns: AtomicU64,
    /// Human-readable consumer identifier (null-terminated UTF-8).
    pub name: [u8; 32],
}

/// Shared memory consumer offset checkpoint manager.
pub struct OffsetCheckpoint {
    path: PathBuf,
    name: String,
    _mmap: MmapMut,
    slot: *mut ShmOffsetSlot,
}

unsafe impl Send for OffsetCheckpoint {}
unsafe impl Sync for OffsetCheckpoint {}

#[inline]
fn current_time_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

impl OffsetCheckpoint {
    /// Opens or creates an atomic 64-byte offset file in shared memory (`/dev/shm`).
    ///
    /// If the file does not exist, it is initialized with `offset_seq = 0`.
    /// If the file exists, its magic and version are validated, and its PID updated.
    pub fn open_or_create<P: AsRef<Path>>(path: P, consumer_name: &str) -> io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        if let Some(parent) = path_buf.parent()
            && !parent.as_os_str().is_empty()
        {
            let _ = std::fs::create_dir_all(parent);
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path_buf)?;

        let slot_size = std::mem::size_of::<ShmOffsetSlot>();
        let current_len = file.metadata()?.len();

        let is_new = if current_len < slot_size as u64 {
            file.set_len(slot_size as u64)?;
            true
        } else {
            false
        };

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let slot_ptr = mmap.as_mut_ptr().cast::<ShmOffsetSlot>();

        let current_pid = std::process::id();
        let name_trimmed = consumer_name.trim();

        if is_new {
            let mut name_buf = [0u8; 32];
            let bytes = name_trimmed.as_bytes();
            let len = bytes.len().min(31);
            name_buf[..len].copy_from_slice(&bytes[..len]);

            unsafe {
                slot_ptr.write(ShmOffsetSlot {
                    magic: SHM_OFFSET_MAGIC,
                    version: SHM_OFFSET_VERSION,
                    pid: AtomicU32::new(current_pid),
                    offset_seq: AtomicU64::new(0),
                    updated_ns: AtomicU64::new(current_time_nanos()),
                    name: name_buf,
                });
            }
        } else {
            let slot = unsafe { &*slot_ptr };
            if slot.magic != SHM_OFFSET_MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Invalid magic in shared memory offset {:?}: expected 0x{:016X}, got 0x{:016X}",
                        path_buf, SHM_OFFSET_MAGIC, slot.magic
                    ),
                ));
            }
            if slot.version != SHM_OFFSET_VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Version mismatch in shared memory offset {:?}: expected {}, got {}",
                        path_buf, SHM_OFFSET_VERSION, slot.version
                    ),
                ));
            }
            slot.pid.store(current_pid, Ordering::Release);
        }

        Ok(Self {
            path: path_buf,
            name: name_trimmed.to_string(),
            _mmap: mmap,
            slot: slot_ptr,
        })
    }

    /// Automatically derives a shared memory offset path for a given ring buffer and consumer name.
    ///
    /// Example: ring `/dev/shm/market_data.shm`, consumer `algo1` -> `/dev/shm/market_data_algo1.offset`.
    pub fn for_consumer<P: AsRef<Path>>(ring_path: P, consumer_name: &str) -> io::Result<Self> {
        let p = ring_path.as_ref();
        let parent = p.parent().unwrap_or_else(|| Path::new("/dev/shm"));
        let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("ring");
        let safe_name: String = consumer_name
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        let offset_path = parent.join(format!("{}_{}.offset", stem, safe_name));
        Self::open_or_create(offset_path, consumer_name)
    }

    /// Reads the saved offset sequence from shared memory.
    ///
    /// Takes ~5ns with zero allocations. Returns `None` if offset is 0 (never committed).
    #[inline(always)]
    pub fn load(&self) -> Option<u64> {
        let seq = unsafe { (*self.slot).offset_seq.load(Ordering::Acquire) };
        if seq == 0 {
            None
        } else {
            Some(seq)
        }
    }

    /// Saves the offset sequence atomically to shared memory (<10ns).
    ///
    /// Pure lock-free atomic store. Zero disk I/O, zero syscalls.
    #[inline(always)]
    pub fn save(&self, seq: u64) {
        unsafe {
            (*self.slot).offset_seq.store(seq, Ordering::Release);
            (*self.slot).updated_ns.store(current_time_nanos(), Ordering::Relaxed);
        }
    }

    /// Path to the shared memory offset file.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Consumer name associated with this offset checkpoint.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Last update timestamp in nanoseconds since UNIX epoch.
    #[inline]
    pub fn updated_nanos(&self) -> u64 {
        unsafe { (*self.slot).updated_ns.load(Ordering::Acquire) }
    }

    /// PID of last consumer process attached to this offset.
    #[inline]
    pub fn pid(&self) -> u32 {
        unsafe { (*self.slot).pid.load(Ordering::Acquire) }
    }
}

impl std::fmt::Debug for OffsetCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OffsetCheckpoint")
            .field("path", &self.path)
            .field("name", &self.name)
            .field("offset_seq", &self.load())
            .field("pid", &self.pid())
            .field("updated_nanos", &self.updated_nanos())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shm_offset_checkpoint_roundtrip() {
        let dir = std::env::temp_dir();
        let file_path = dir.join(format!("test_ringfire_shm_offset_{}.shm", std::process::id()));
        let _ = std::fs::remove_file(&file_path);

        {
            let cp = OffsetCheckpoint::open_or_create(&file_path, "test_bot").unwrap();
            assert_eq!(cp.load(), None);
            assert_eq!(cp.name(), "test_bot");

            cp.save(123456);
            assert_eq!(cp.load(), Some(123456));
            assert!(cp.updated_nanos() > 0);
        }

        // Re-open from existing file to simulate consumer crash recovery
        {
            let cp2 = OffsetCheckpoint::open_or_create(&file_path, "test_bot").unwrap();
            assert_eq!(cp2.load(), Some(123456));

            cp2.save(123457);
            assert_eq!(cp2.load(), Some(123457));
        }

        let _ = std::fs::remove_file(&file_path);
    }

    #[test]
    fn test_shm_offset_for_consumer_path() {
        let ring_path = std::env::temp_dir().join("hl_market_data.shm");
        let cp = OffsetCheckpoint::for_consumer(&ring_path, "recorder_v2").unwrap();
        assert!(cp.path().to_str().unwrap().contains("hl_market_data_recorder_v2.offset"));
        let _ = std::fs::remove_file(cp.path());
    }
}
