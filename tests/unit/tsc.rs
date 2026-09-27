use super::*;

#[test]
fn test_cycle_stamp_progress() {
    let t1 = CycleStamp::now();
    for _ in 0..1000 {
        core::hint::spin_loop();
    }
    let t2 = CycleStamp::now();
    assert!(t2.tsc >= t1.tsc);
    assert!(t2.diff_cycles(&t1) > 0);
}
