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
    /// Layout signature / schema fingerprint mismatch across processes.
    SchemaMismatch {
        expected: u64,
        actual: u64,
        type_name: &'static str,
    },
    /// Payload exceeds maximum arena capacity.
    ArenaPayloadTooLarge { len: usize, max_capacity: usize },
    /// Provided output buffer is too small for payload.
    BufferTooSmall { required: usize, provided: usize },
    /// Reader registry is full (all slots occupied).
    NoAvailableReaderSlots,
    /// No offset file was configured for this consumer.
    NoOffsetFileConfigured,
    /// Ring buffer is full under lossless backpressure flow control.
    BackpressureBufferFull,
    /// Shared memory region is truncated, foreign, or its header points outside the mapping.
    CorruptLayout(&'static str),
    /// A blackboard slot stayed mid-write for too long (writer stalled or crashed).
    WriterStalled { key: usize },
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
            Self::SchemaMismatch { expected, actual, type_name } => write!(
                f,
                "Schema signature mismatch for type '{}': expected 0x{:016X}, got 0x{:016X}",
                type_name, expected, actual
            ),
            Self::ArenaPayloadTooLarge { len, max_capacity } => write!(
                f,
                "Payload size ({} bytes) exceeds arena capacity ({} bytes)",
                len, max_capacity
            ),
            Self::BufferTooSmall { required, provided } => write!(
                f,
                "Output buffer too small: required {} bytes, provided {} bytes",
                required, provided
            ),
            Self::NoAvailableReaderSlots => {
                write!(f, "Reader registry full: no free slots available")
            }
            Self::NoOffsetFileConfigured => {
                write!(f, "No offset file configured for this consumer")
            }
            Self::BackpressureBufferFull => {
                write!(f, "Ring buffer is full: slowest active reader has not caught up")
            }
            Self::CorruptLayout(what) => write!(f, "Corrupt shared memory layout: {}", what),
            Self::WriterStalled { key } => {
                write!(f, "Blackboard key {} stayed mid-write: writer stalled or crashed", key)
            }
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
