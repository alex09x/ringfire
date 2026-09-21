use ringfire::{BlackboardProducer, RingConsumer, RingProducer};
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
        price: 85000_50,
        quantity: 150,
        side: b'B',
        _pad: [0; 7],
    };
    let t2 = PyTrade {
        timestamp_ns: 1_000_000_100,
        price: 85001_00,
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
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "Python blackboard read failed:\nSTDOUT:\n{}\nSTDERR:\n{}",
        stdout,
        stderr
    );
    assert!(stdout.contains("PYTHON_BLACKBOARD_OK"));
}
