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

#[test]
fn test_signature_ignores_module_paths() {
    assert_eq!(short_type_name("app_a::types::Trade"), "Trade");
    assert_eq!(
        short_type_name("alloc::vec::Vec<core::option::Option<my::T>>"),
        "Vec<Option<T>>"
    );
    assert_eq!(short_type_name("[u8; 32]"), "[u8; 32]");
}
