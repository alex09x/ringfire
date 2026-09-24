//! Creation of shared memory backing files.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::io::AsRawFd;
use std::path::Path;

use crate::error::{Result, RingfireError};

/// Creates (or takes over) the backing file at `path` and sizes it to `size` bytes.
///
/// With `exclusive_lock`, the file is `flock`ed *before* anything is modified, so a second
/// producer fails with [`RingfireError::ProducerAlreadyExists`] without touching the live
/// ring. A stale file left by a dead producer is unlinked and replaced by a fresh inode:
/// readers still mapping the old ring keep a consistent (orphaned) view instead of seeing
/// it truncated underneath them.
///
/// Without `exclusive_lock` the file is truncated in place (legacy behaviour).
pub(crate) fn create_backing_file(path: &Path, mode: u32, exclusive_lock: bool, size: u64) -> Result<File> {
    crate::wait::register_producer_barrier();
    if !exclusive_lock {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .mode(mode)
            .open(path)?;
        file.set_len(size)?;
        return Ok(file);
    }

    for _ in 0..8 {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(mode)
            .open(path)?;

        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(RingfireError::ProducerAlreadyExists);
        }

        // The path may have been replaced between open() and flock(): make sure the inode
        // we locked is still the one the path names.
        let locked = file.metadata()?;
        match std::fs::metadata(path) {
            Ok(current) if current.dev() == locked.dev() && current.ino() == locked.ino() => {}
            Ok(_) => continue,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        }

        if locked.len() != 0 {
            // Stale ring from a producer that is gone (we hold its lock): replace the inode.
            std::fs::remove_file(path)?;
            continue;
        }

        file.set_len(size)?;
        return Ok(file);
    }

    Err(RingfireError::ProducerAlreadyExists)
}
