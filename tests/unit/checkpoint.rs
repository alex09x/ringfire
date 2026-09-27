use super::*;

#[test]
fn test_shm_offset_checkpoint_roundtrip() {
    let dir = std::env::temp_dir();
    let file_path = dir.join(format!(
        "test_ringfire_shm_offset_{}.shm",
        std::process::id()
    ));
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
    assert!(
        cp.path()
            .to_str()
            .unwrap()
            .contains("hl_market_data_recorder_v2.offset")
    );
    let _ = std::fs::remove_file(cp.path());
}
