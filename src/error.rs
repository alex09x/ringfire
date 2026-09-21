use std::fmt;
use std::io;

/// Error types for `ringfire` operations.
#[derive(Debug)]
pub enum RingfireError {
    /// An underlying I/O error occurred.
    Io(io::Error),
    /// Ring capacity is not a power of two.
    InvalidCapacity(u64),
    /// Shared memory magic bytes did not match expected value.
    InvalidMagic { expected: u64, actual: u64 },
    /// Version mismatch between binary and shared memory header.
    VersionMismatch { expected: u32, actual: u32 },
    /// Element size mismatch between struct definition and ring header.
    ElementSizeMismatch { expected: usize, actual: usize },
    /// Producer already exists and holds exclusive lock.
    ProducerAlreadyExists,
    /// Key out of range for blackboard.
    KeyOutOfRange { key: usize, capacity: usize },
    /// Value size mismatch for blackboard.
    ValueSizeMismatch { expected: usize, actual: usize },
}

impl fmt::Display for RingfireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {}", err),
            Self::InvalidCapacity(cap) => {
                write!(f, "Capacity {} is invalid: must be a power of two", cap)
            }
            Self::InvalidMagic { expected, actual } => write!(
                f,
                "Invalid magic header: expected 0x{:016X}, got 0x{:016X}",
                expected, actual
            ),
            Self::VersionMismatch { expected, actual } => {
                write!(f, "Version mismatch: expected {}, got {}", expected, actual)
            }
            Self::ElementSizeMismatch { expected, actual } => write!(
                f,
                "Element size mismatch: expected {} bytes, got {} bytes",
                expected, actual
            ),
            Self::ProducerAlreadyExists => {
                write!(f, "Exclusive producer already exists for this shared memory path")
            }
            Self::KeyOutOfRange { key, capacity } => {
                write!(f, "Blackboard key {} out of range (capacity: {})", key, capacity)
            }
            Self::ValueSizeMismatch { expected, actual } => write!(
                f,
                "Blackboard value size mismatch: expected {} bytes, got {} bytes",
                expected, actual
            ),
        }
    }
}

impl std::error::Error for RingfireError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for RingfireError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

pub type Result<T> = std::result::Result<T, RingfireError>;
