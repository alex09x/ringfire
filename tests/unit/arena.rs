use super::*;

#[test]
fn test_arena_contiguous_wraparound() {
    let capacity = 1024;
    let total_size = std::mem::size_of::<ArenaHeader>() + capacity;
    let mut buffer = vec![0u8; total_size + 128];
    let ptr = buffer.as_mut_ptr();
    let offset = (64 - (ptr as usize % 64)) % 64;
    let aligned_ptr = unsafe { ptr.add(offset) };

    let arena = unsafe { PayloadArena::init(aligned_ptr, capacity).unwrap() };

    // Write 900 bytes
    let data1 = vec![0xAAu8; 900];
    let ref1 = arena.write_blob(&data1, 1).unwrap();
    assert_eq!(ref1.len, 900);
    assert_eq!(ref1.offset, 0);

    let mut read_buf = vec![0u8; 900];
    arena.read_blob(ref1, &mut read_buf).unwrap();
    assert_eq!(read_buf, data1);

    // Writing 200 bytes would overflow 900 + 200 > 1024 -> must wrap to index 0!
    let data2 = vec![0xBBu8; 200];
    let ref2 = arena.write_blob(&data2, 2).unwrap();
    assert_eq!(ref2.len, 200);
    // Offset must be aligned to next cycle (1024)
    assert_eq!(ref2.offset, 1024);

    let mut read_buf2 = vec![0u8; 200];
    arena.read_blob(ref2, &mut read_buf2).unwrap();
    assert_eq!(read_buf2, data2);
}
