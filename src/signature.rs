//! Layout signature verification for cross-process shared memory structures.
//!
//! Generates a 64-bit fingerprint from struct memory layout (size, alignment,
//! and type name) to prevent silent memory corruption when sender and receiver
//! are compiled with divergent struct definitions.

use std::any::type_name;
use std::mem::{align_of, size_of};

/// 64-bit FNV-1a hash over bytes.
pub const fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
        i += 1;
    }
    hash
}

/// Compute a 64-bit layout signature for a given type `T`.
pub fn compute_layout_signature<T: 'static>() -> u64 {
    let name = type_name::<T>();
    let size = size_of::<T>();
    let align = align_of::<T>();

    let mut buf = Vec::with_capacity(name.len() + 16);
    buf.extend_from_slice(name.as_bytes());
    buf.extend_from_slice(&size.to_le_bytes());
    buf.extend_from_slice(&align.to_le_bytes());

    fnv1a64(&buf)
}

/// Trait providing a layout signature for cross-process IPC validation.
pub trait LayoutSignature: 'static {
    fn layout_signature() -> u64;
}

impl<T: 'static> LayoutSignature for T {
    #[inline]
    fn layout_signature() -> u64 {
        compute_layout_signature::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C)]
    struct StructA {
        a: u64,
        b: u32,
    }

    #[repr(C)]
    struct StructB {
        a: u64,
        b: u64,
    }

    #[test]
    fn test_signature_uniqueness() {
        let sig_a = StructA::layout_signature();
        let sig_b = StructB::layout_signature();
        let sig_u64 = u64::layout_signature();

        assert_ne!(sig_a, sig_b);
        assert_ne!(sig_a, sig_u64);
        assert_eq!(sig_a, StructA::layout_signature());
    }
}
