use ringfire::{BlackboardProducer, BlobProducer, RingConsumer, RingProducer};
use std::process::Command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct PyTrade {
    timestamp_ns: u64,
    price: u64,
    quantity: u64,
    side: u8,
    _pad: [u8; 7],
}

#[test]
fn test_rust_producer_to_python_consumer() {
    let tmp_path = std::env::temp_dir().join("test_rust_to_py.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let capacity = 1024;
    let mut producer = RingProducer::<PyTrade>::create(&tmp_path, capacity).unwrap();

    let t1 = PyTrade {
        timestamp_ns: 1_000_000_000,
        price: 8_500_050,
        quantity: 150,
        side: b'B',
        _pad: [0; 7],
    };
    let t2 = PyTrade {
        timestamp_ns: 1_000_000_100,
        price: 8_500_100,
        quantity: 200,
        side: b'S',
        _pad: [0; 7],
    };

    producer.push(&t1);
    producer.push(&t2);

    let python_code = format!(
        r#"
import sys, ctypes
sys.path.insert(0, 'python')
from ringfire import RingConsumer

class PyTrade(ctypes.Structure):
    _fields_ = [
        ("timestamp_ns", ctypes.c_uint64),
        ("price", ctypes.c_uint64),
        ("quantity", ctypes.c_uint64),
        ("side", ctypes.c_uint8),
        ("_pad", ctypes.c_uint8 * 7),
    ]

consumer = RingConsumer('{}', PyTrade)
m1 = consumer.try_recv()
assert m1 is not None
assert m1.timestamp_ns == 1000000000
assert m1.price == 8500050
assert m1.quantity == 150
assert m1.side == ord('B')

m2 = consumer.try_recv()
assert m2 is not None
assert m2.timestamp_ns == 1000000100
assert m2.price == 8500100
assert m2.quantity == 200
assert m2.side == ord('S')

assert consumer.try_recv() is None
print("PYTHON_CONSUMER_OK")
"#,
        tmp_path.to_str().unwrap()
    );

    let output = Command::new("python3")
        .arg("-c")
        .arg(&python_code)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute python3");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Python consumer failed:\nSTDOUT:\n{}\nSTDERR:\n{}",
        stdout,
        stderr
    );
    assert!(stdout.contains("PYTHON_CONSUMER_OK"));
}

#[test]
fn test_python_producer_to_rust_consumer() {
    let tmp_path = std::env::temp_dir().join("test_py_to_rust.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let python_code = format!(
        r#"
import sys, ctypes
sys.path.insert(0, 'python')
from ringfire import RingProducer

class PyTrade(ctypes.Structure):
    _fields_ = [
        ("timestamp_ns", ctypes.c_uint64),
        ("price", ctypes.c_uint64),
        ("quantity", ctypes.c_uint64),
        ("side", ctypes.c_uint8),
        ("_pad", ctypes.c_uint8 * 7),
    ]

prod = RingProducer('{}', PyTrade, 1024)
m1 = PyTrade(123456, 77700, 50, ord('B'), (ctypes.c_uint8 * 7)(*([0]*7)))
prod.push(m1)
m2 = PyTrade(123499, 77750, 60, ord('S'), (ctypes.c_uint8 * 7)(*([0]*7)))
prod.push(m2)
print("PYTHON_PRODUCER_OK")
"#,
        tmp_path.to_str().unwrap()
    );

    let output = Command::new("python3")
        .arg("-c")
        .arg(&python_code)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute python3");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Python producer failed:\nSTDOUT:\n{}\nSTDERR:\n{}",
        stdout,
        stderr
    );

    let mut consumer = RingConsumer::<PyTrade>::attach(&tmp_path).unwrap();
    let m1 = consumer.try_recv().unwrap();
    assert_eq!(m1.timestamp_ns, 123456);
    assert_eq!(m1.price, 77700);
    assert_eq!(m1.quantity, 50);
    assert_eq!(m1.side, b'B');

    let m2 = consumer.try_recv().unwrap();
    assert_eq!(m2.timestamp_ns, 123499);
    assert_eq!(m2.price, 77750);
    assert_eq!(m2.quantity, 60);
    assert_eq!(m2.side, b'S');

    assert_eq!(consumer.try_recv(), None);
}

#[test]
fn test_rust_blackboard_to_python() {
    let tmp_path = std::env::temp_dir().join("test_bb_rust_to_py.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let mut producer = BlackboardProducer::<PyTrade>::create(&tmp_path, 64).unwrap();

    let trade = PyTrade {
        timestamp_ns: 999999,
        price: 91000,
        quantity: 42,
        side: b'B',
        _pad: [0; 7],
    };

    producer.write(7, &trade).unwrap();

    let python_code = format!(
        r#"
import sys, ctypes
sys.path.insert(0, 'python')
from ringfire import BlackboardConsumer

class PyTrade(ctypes.Structure):
    _fields_ = [
        ("timestamp_ns", ctypes.c_uint64),
        ("price", ctypes.c_uint64),
        ("quantity", ctypes.c_uint64),
        ("side", ctypes.c_uint8),
        ("_pad", ctypes.c_uint8 * 7),
    ]

bb = BlackboardConsumer('{}', PyTrade)
assert bb.read(0) is None
v = bb.read(7)
assert v is not None
assert v.timestamp_ns == 999999
assert v.price == 91000
assert v.quantity == 42
assert v.side == ord('B')
print("PYTHON_BLACKBOARD_OK")
"#,
        tmp_path.to_str().unwrap()
    );

    let output = Command::new("python3")
        .arg("-c")
        .arg(&python_code)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute python3");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let _stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("PYTHON_BLACKBOARD_OK"));
}

#[test]
fn test_python_consumer_shm_offset_resume() {
    let dir = std::env::temp_dir();
    let ring_path = dir.join(format!("test_py_resume_ring_{}.shm", std::process::id()));
    let offset_path = dir.join(format!("test_py_resume_offset_{}.offset", std::process::id()));
    let _ = std::fs::remove_file(&ring_path);
    let _ = std::fs::remove_file(&offset_path);

    let mut producer = RingProducer::<PyTrade>::create(&ring_path, 1024).unwrap();
    for i in 1..=20 {
        producer.push(&PyTrade {
            timestamp_ns: i * 1000,
            price: i * 100,
            quantity: 1,
            side: b'B',
            _pad: [0; 7],
        });
    }

    let python_code = format!(
        r#"
import sys, ctypes
sys.path.insert(0, 'python')
from ringfire import RingConsumer

class PyTrade(ctypes.Structure):
    _fields_ = [
        ("timestamp_ns", ctypes.c_uint64),
        ("price", ctypes.c_uint64),
        ("quantity", ctypes.c_uint64),
        ("side", ctypes.c_uint8),
        ("_pad", ctypes.c_uint8 * 7),
    ]

# 1. First run: read 10 items, commit offset, exit
cons1 = RingConsumer('{ring}', PyTrade, offset_file='{offset}', consumer_name='py_bot')
for expected in range(1, 11):
    item = cons1.try_recv()
    assert item is not None, f"Expected item {{expected}}, got None"
    assert item.price == expected * 100
cons1.commit_offset()
assert cons1.last_processed_sequence() == 10
cons1.close()

# 2. Second run: resume from offset file. Must receive 11..20
cons2 = RingConsumer('{ring}', PyTrade, offset_file='{offset}', consumer_name='py_bot')
for expected in range(11, 21):
    item = cons2.try_recv()
    assert item is not None, f"Expected item {{expected}}, got None"
    assert item.price == expected * 100
assert cons2.try_recv() is None
assert cons2.last_processed_sequence() == 20
cons2.commit_offset()
cons2.close()
print("PYTHON_SHM_OFFSET_RESUME_OK")
"#,
        ring = ring_path.to_str().unwrap(),
        offset = offset_path.to_str().unwrap()
    );

    let output = Command::new("python3")
        .arg("-c")
        .arg(&python_code)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute python3");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Python offset resume failed:\nSTDOUT:\n{}\nSTDERR:\n{}",
        stdout,
        stderr
    );
    assert!(stdout.contains("PYTHON_SHM_OFFSET_RESUME_OK"));

    let _ = std::fs::remove_file(&ring_path);
    let _ = std::fs::remove_file(&offset_path);
}

#[test]
fn test_python_consumer_sanitized_offset_path() {
    let dir = std::env::temp_dir();
    let ring_path = dir.join(format!("test_py_sanitize_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&ring_path);

    let mut producer = RingProducer::<PyTrade>::create(&ring_path, 1024).unwrap();
    producer.push(&PyTrade {
        timestamp_ns: 1000,
        price: 100,
        quantity: 1,
        side: b'B',
        _pad: [0; 7],
    });

    let python_code = format!(
        r#"
import sys, ctypes, os
sys.path.insert(0, 'python')
from ringfire import RingConsumer

class PyTrade(ctypes.Structure):
    _fields_ = [
        ("timestamp_ns", ctypes.c_uint64),
        ("price", ctypes.c_uint64),
        ("quantity", ctypes.c_uint64),
        ("side", ctypes.c_uint8),
        ("_pad", ctypes.c_uint8 * 7),
    ]

# Unsafe consumer name containing directory traversal and invalid chars
unsafe_name = "../../unsafe/worker:1#tag"
consumer = RingConsumer('{ring}', PyTrade, consumer_name=unsafe_name)
expected_stem = os.path.splitext(os.path.basename('{ring}'))[0]
expected_safe = "______unsafe_worker_1_tag"
expected_filename = f"{{expected_stem}}_{{expected_safe}}.offset"
actual_filename = os.path.basename(consumer.offset_file)
assert actual_filename == expected_filename, f"Expected {{expected_filename}}, got {{actual_filename}}"
assert os.path.dirname(consumer.offset_file) == os.path.dirname('{ring}')
consumer.close()
if os.path.exists(consumer.offset_file):
    os.remove(consumer.offset_file)
print("PYTHON_SANITIZATION_OK")
"#,
        ring = ring_path.to_str().unwrap()
    );

    let output = Command::new("python3")
        .arg("-c")
        .arg(&python_code)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute python3");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Python sanitization failed:\nSTDOUT:\n{}\nSTDERR:\n{}",
        stdout,
        stderr
    );
    assert!(stdout.contains("PYTHON_SANITIZATION_OK"));

    let _ = std::fs::remove_file(&ring_path);
}

#[test]
fn test_python_consumer_start_mode_sequence_validation() {
    let dir = std::env::temp_dir();
    let ring_path = dir.join(format!("test_py_start_mode_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&ring_path);

    let mut producer = RingProducer::<PyTrade>::create(&ring_path, 1024).unwrap();
    for i in 1..=10 {
        producer.push(&PyTrade {
            timestamp_ns: i * 1000,
            price: i * 100,
            quantity: 1,
            side: b'B',
            _pad: [0; 7],
        });
    }

    let python_code = format!(
        r#"
import sys, ctypes
sys.path.insert(0, 'python')
from ringfire import RingConsumer

class PyTrade(ctypes.Structure):
    _fields_ = [
        ("timestamp_ns", ctypes.c_uint64),
        ("price", ctypes.c_uint64),
        ("quantity", ctypes.c_uint64),
        ("side", ctypes.c_uint8),
        ("_pad", ctypes.c_uint8 * 7),
    ]

# 1. sequence mode without start_seq must raise ValueError
try:
    RingConsumer('{ring}', PyTrade, start_mode="sequence")
    assert False, "Should have raised ValueError for sequence mode without start_seq"
except ValueError as e:
    assert "start_seq" in str(e), f"Unexpected message: {{e}}"

# 2. sequence mode with start_seq=5 starts at 5
c = RingConsumer('{ring}', PyTrade, start_mode="sequence", start_seq=5)
item = c.try_recv()
assert item is not None and item.price == 500, f"Expected price 500, got {{item.price if item else None}}"
c.close()

# 3. invalid start mode raises ValueError
try:
    RingConsumer('{ring}', PyTrade, start_mode="nonexistent_mode")
    assert False, "Should have raised ValueError for unknown start_mode"
except ValueError as e:
    assert "Unknown start_mode" in str(e)

print("PYTHON_START_MODE_VALIDATION_OK")
"#,
        ring = ring_path.to_str().unwrap()
    );

    let output = Command::new("python3")
        .arg("-c")
        .arg(&python_code)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute python3");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Python start mode validation failed:\nSTDOUT:\n{}\nSTDERR:\n{}",
        stdout,
        stderr
    );
    assert!(stdout.contains("PYTHON_START_MODE_VALIDATION_OK"));

    let _ = std::fs::remove_file(&ring_path);
}

#[test]
fn test_rust_blob_producer_to_python_blob_consumer() {
    let dir = std::env::temp_dir();
    let ring_path = dir.join(format!("test_py_blob_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&ring_path);

    let capacity = 64;
    let arena_capacity = 65536; // 64 KB arena
    let mut producer = BlobProducer::<PyTrade>::create(&ring_path, capacity, arena_capacity).unwrap();

    let t1 = PyTrade {
        timestamp_ns: 1_000_000,
        price: 8_800_050,
        quantity: 10,
        side: b'B',
        _pad: [0; 7],
    };
    let payload1 = b"Hello from Rust BlobProducer variable payload!";

    let t2 = PyTrade {
        timestamp_ns: 2_000_000,
        price: 8_805_000,
        quantity: 25,
        side: b'S',
        _pad: [0; 7],
    };
    let payload2 = vec![0xABu8; 1024]; // 1 KB binary chunk

    producer.push(&t1, payload1).unwrap();
    producer.push(&t2, &payload2).unwrap();

    let python_code = format!(
        r#"
import sys, ctypes
sys.path.insert(0, 'python')
from ringfire import BlobConsumer

class PyTrade(ctypes.Structure):
    _fields_ = [
        ("timestamp_ns", ctypes.c_uint64),
        ("price", ctypes.c_uint64),
        ("quantity", ctypes.c_uint64),
        ("side", ctypes.c_uint8),
        ("_pad", ctypes.c_uint8 * 7),
    ]

consumer = BlobConsumer('{ring}', PyTrade)

# Packet 1
res1 = consumer.try_recv()
assert res1 is not None, "Expected packet 1"
meta1, view1 = res1
assert meta1.timestamp_ns == 1000000
assert meta1.price == 8800050
assert meta1.quantity == 10
assert meta1.side == ord('B')
assert view1.tobytes() == b"Hello from Rust BlobProducer variable payload!"

# Packet 2
res2 = consumer.try_recv()
assert res2 is not None, "Expected packet 2"
meta2, view2 = res2
assert meta2.timestamp_ns == 2000000
assert meta2.price == 8805000
assert meta2.quantity == 25
assert meta2.side == ord('S')
assert len(view2) == 1024
assert view2[0] == 0xAB and view2[1023] == 0xAB

# Empty
assert consumer.try_recv() is None

consumer.close()
print("PYTHON_BLOB_CONSUMER_OK")
"#,
        ring = ring_path.to_str().unwrap()
    );

    let output = Command::new("python3")
        .arg("-c")
        .arg(&python_code)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute python3");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Python BlobConsumer failed:\nSTDOUT:\n{}\nSTDERR:\n{}",
        stdout,
        stderr
    );
    assert!(stdout.contains("PYTHON_BLOB_CONSUMER_OK"));

    let _ = std::fs::remove_file(&ring_path);
}
