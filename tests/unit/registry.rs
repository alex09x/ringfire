use super::*;

fn with_registry(count: usize, f: impl FnOnce(&ReaderRegistry)) {
    let total_size = count * std::mem::size_of::<ReaderSlot>();
    let mut memory = vec![0u8; total_size + 128];
    let ptr = memory.as_mut_ptr();
    let offset = (64 - (ptr as usize % 64)) % 64;
    let registry = unsafe { ReaderRegistry::init(ptr.add(offset), count) };
    f(&registry);
}

#[test]
fn test_reader_registry_lifecycle() {
    with_registry(4, |registry| {
        assert_eq!(registry.min_reader_seq(), None);

        let reg1 = registry.register("worker-1", 100).unwrap();
        assert_eq!(registry.min_reader_seq(), Some(100));

        let reg2 = registry.register("worker-2", 150).unwrap();
        assert_eq!(registry.min_reader_seq(), Some(100));

        reg1.update_cursor(120);
        assert_eq!(registry.min_reader_seq(), Some(120));

        // cursor 120 = next to read, so 119 is consumed: 200 - 119 = 81 unread
        assert_eq!(registry.reader_lag(200), 81);
        assert_eq!(registry.headroom(200, 1024), 1024 - 81);

        drop(reg1);
        assert_eq!(registry.min_reader_seq(), Some(150));
        drop(reg2);
        assert_eq!(registry.min_reader_seq(), None);
    });
}

#[test]
fn test_dead_reader_reclaimed_without_touching_new_owner() {
    with_registry(1, |registry| {
        // Simulate a slot abandoned by a dead process.
        let slot = registry.slot(0);
        slot.pid.store(i32::MAX as u32 - 7, Ordering::Release);
        slot.active.store(1, Ordering::Release);
        slot.cursor_seq.store(5, Ordering::Release);

        let reg = registry.register("fresh", 42).unwrap();
        assert_eq!(reg.slot_index(), 0);
        // A scanner that still believes the dead PID owns the slot must not free it.
        assert!(!registry.reclaim(0, i32::MAX as u32 - 7));
        assert_eq!(registry.min_reader_seq(), Some(42));
    });
}

#[test]
fn test_registry_full() {
    with_registry(1, |registry| {
        let _a = registry.register("a", 1).unwrap();
        assert!(matches!(
            registry.register("b", 1),
            Err(RingfireError::NoAvailableReaderSlots)
        ));
    });
}
