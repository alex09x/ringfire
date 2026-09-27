use super::*;

#[test]
fn test_blob_producer_consumer_roundtrip() {
    let tmp_path = std::env::temp_dir().join("test_ringfire_blob.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let mut producer = BlobProducer::<u64>::create(&tmp_path, 64, 65536).unwrap();
    let mut consumer = BlobConsumer::<u64>::attach_with_name(&tmp_path, "test_cons").unwrap();

    let mut out = [0u8; 1024];
    let mut meta = 0u64;

    assert_eq!(consumer.recv(&mut meta, &mut out).unwrap(), None);

    let payload1 = b"Hello from Ringfire PayloadArena!";
    producer.push(&101, payload1).unwrap();

    let payload2 = vec![0xFEu8; 512];
    producer.push(&102, &payload2).unwrap();

    let len1 = consumer.recv(&mut meta, &mut out).unwrap().unwrap();
    assert_eq!(meta, 101);
    assert_eq!(&out[..len1], payload1);

    let len2 = consumer.recv(&mut meta, &mut out).unwrap().unwrap();
    assert_eq!(meta, 102);
    assert_eq!(&out[..len2], &payload2[..]);

    assert_eq!(consumer.recv(&mut meta, &mut out).unwrap(), None);
}

#[test]
fn test_blob_view_in_place() {
    let tmp_path = std::env::temp_dir().join("test_ringfire_blob_view.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let mut producer = BlobProducer::<()>::create(&tmp_path, 64, 65536).unwrap();
    let mut consumer = BlobConsumer::<()>::attach(&tmp_path).unwrap();

    let payload = b"In-place inspection test data";
    producer.push_payload(payload).unwrap();

    let inspected_len = consumer
        .view(|_, slice| {
            assert_eq!(slice, payload);
            slice.len()
        })
        .unwrap()
        .unwrap();

    assert_eq!(inspected_len, payload.len());
}
