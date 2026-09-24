//! # ringfire CLI
//!
//! Diagnostic, inspection, and real-time monitoring tool for `ringfire` shared memory
//! ring buffers and blackboards.
//!
//! Subcommands:
//! - `stat <path> [--json]`: Dump header, configuration, sequence counters, and reader status.
//! - `top <path> [--interval-ms <ms>]`: Live terminal dashboard showing throughput and consumer lag.
//! - `dump <path> [--tail <n>] [--hex]`: Inspect recent slots and payloads.
//! - `prune <path>`: Clean up dead reader slots whose processes have terminated.

use std::fs::OpenOptions;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use memmap2::{Mmap, MmapMut, MmapOptions};
use ringfire::header::{
    validate_ring, BlackboardHeader, ReaderSlot, RingHeader, RingView, BLACKBOARD_MAGIC,
    FLAG_MODE_MPMC, FLAG_MODE_SPMC, FLAG_POLICY_LOSSLESS_BACKPRESSURE, FLAG_WITH_ARENA,
    FLAG_WITH_REGISTRY, RINGFIRE_MAGIC, SLOT_WRITING,
};
use ringfire::registry::is_process_alive;

/// JSON string literal for `s` (quotes, backslashes and control characters escaped).
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Maps `path` read-only and validates it as a ring.
fn map_ring(path: &str) -> Result<(Mmap, RingView), Box<dyn std::error::Error>> {
    let file = OpenOptions::new().read(true).open(path)?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let view = unsafe { validate_ring(mmap.as_ptr(), mmap.len(), None, 8)? };
    Ok((mmap, view))
}

/// Retained window `[oldest, newest]` of a ring, or `None` if nothing was published.
fn retained_window(write_seq: u64, capacity: u64) -> Option<(u64, u64)> {
    if write_seq == 0 {
        None
    } else {
        Some((write_seq.saturating_sub(capacity - 1).max(1), write_seq))
    }
}

fn print_usage() {
    eprintln!(
        r#"ringfire {} - IPC Shared Memory Monitoring & Diagnostics

USAGE:
    ringfire <SUBCOMMAND> <PATH> [OPTIONS]

SUBCOMMANDS:
    stat <PATH> [--json]            Inspect buffer header, capacity, sequences, and readers
    top  <PATH> [--interval-ms <N>] Live terminal monitor with throughput and consumer lag
    dump <PATH> [--tail <N>]        Dump recent slots and payload data
    prune <PATH>                    Reclaim inactive/dead reader slots

OPTIONS:
    --json                          Output in JSON format (stat only)
    --interval-ms <N>               Refresh interval in milliseconds for top (default: 500)
    --tail <N>                      Number of recent slots to inspect in dump (default: 10)
    --hex                           Print slot payload in hex format
    -h, --help                      Show help information
    -V, --version                   Show version
"#,
        env!("CARGO_PKG_VERSION")
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    match args[1].as_str() {
        "-h" | "--help" | "help" => {
            print_usage();
        }
        "-V" | "--version" | "version" => {
            println!("ringfire {}", env!("CARGO_PKG_VERSION"));
        }
        "stat" => {
            if args.len() < 3 {
                eprintln!("Error: 'stat' requires a path to a shared memory file.");
                std::process::exit(1);
            }
            let path = &args[2];
            let json = args.iter().any(|a| a == "--json");
            if let Err(e) = cmd_stat(path, json) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        "top" => {
            if args.len() < 3 {
                eprintln!("Error: 'top' requires a path to a shared memory file.");
                std::process::exit(1);
            }
            let path = &args[2];
            let mut interval_ms = 500u64;
            for i in 3..args.len() {
                if args[i] == "--interval-ms" && i + 1 < args.len() {
                    interval_ms = args[i + 1].parse().unwrap_or(500);
                }
            }
            if let Err(e) = cmd_top(path, interval_ms) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        "dump" => {
            if args.len() < 3 {
                eprintln!("Error: 'dump' requires a path to a shared memory file.");
                std::process::exit(1);
            }
            let path = &args[2];
            let mut tail = 10usize;
            let mut hex = false;
            let mut i = 3;
            while i < args.len() {
                if args[i] == "--tail" && i + 1 < args.len() {
                    tail = args[i + 1].parse().unwrap_or(10);
                    i += 2;
                } else if args[i] == "--hex" {
                    hex = true;
                    i += 1;
                } else {
                    i += 1;
                }
            }
            if let Err(e) = cmd_dump(path, tail, hex) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        "prune" => {
            if args.len() < 3 {
                eprintln!("Error: 'prune' requires a path to a shared memory file.");
                std::process::exit(1);
            }
            let path = &args[2];
            if let Err(e) = cmd_prune(path) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        other => {
            eprintln!("Unknown subcommand: '{}'", other);
            print_usage();
            std::process::exit(1);
        }
    }
}

struct ReaderSnapshot {
    slot_index: usize,
    pid: u32,
    name: String,
    cursor_seq: u64,
    lag: u64,
    alive: bool,
}

fn read_readers(mmap: &Mmap, header: &RingHeader, write_seq: u64) -> Vec<ReaderSnapshot> {
    let mut readers = Vec::new();
    if header.reader_registry_offset == 0 || header.reader_registry_count == 0 {
        return readers;
    }

    let reg_offset = header.reader_registry_offset as usize;
    let reg_count = header.reader_registry_count as usize;
    let slot_size = std::mem::size_of::<ReaderSlot>();

    if mmap.len() < reg_offset + reg_count * slot_size {
        return readers;
    }

    let base_ptr = unsafe { mmap.as_ptr().add(reg_offset) as *const ReaderSlot };

    for i in 0..reg_count {
        let slot = unsafe { &*base_ptr.add(i) };
        let active = slot.active.load(Ordering::Acquire);
        let pid = slot.pid.load(Ordering::Acquire);

        if pid != 0 && active != 0 {
            let cursor = slot.cursor_seq.load(Ordering::Acquire);
            let name_len = slot.name.iter().position(|&b| b == 0).unwrap_or(32);
            let name = String::from_utf8_lossy(&slot.name[..name_len]).into_owned();
            let alive = if pid != 0 { is_process_alive(pid) } else { false };
            // cursor = next sequence to read, so everything from it to write_seq is unread
            let lag = write_seq.saturating_sub(cursor.saturating_sub(1));

            readers.push(ReaderSnapshot {
                slot_index: i,
                pid,
                name,
                cursor_seq: cursor,
                lag,
                alive,
            });
        }
    }

    readers
}

fn cmd_stat(path_str: &str, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(path_str);
    let file = OpenOptions::new().read(true).open(path)?;
    let metadata = file.metadata()?;
    let file_len = metadata.len();

    if file_len < std::mem::size_of::<RingHeader>() as u64 {
        return Err(format!("File too small ({} bytes) to contain RingHeader", file_len).into());
    }

    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let magic = unsafe { *(mmap.as_ptr() as *const u64) };

    if magic == BLACKBOARD_MAGIC {
        return print_blackboard_stat(&mmap, path_str, file_len, json);
    }

    if magic != RINGFIRE_MAGIC {
        return Err(format!(
            "Invalid magic signature 0x{:016X} (expected RINGFIRE 0x{:016X} or BLACKBOARD 0x{:016X})",
            magic, RINGFIRE_MAGIC, BLACKBOARD_MAGIC
        )
        .into());
    }

    drop(mmap);
    let (mmap, view) = map_ring(path_str)?;
    let header = unsafe { &*(mmap.as_ptr() as *const RingHeader) };
    let write_seq = header.write_seq.load(Ordering::Acquire);
    let claim_seq = header.claim_seq.load(Ordering::Acquire);
    let read_seq = header.read_seq.load(Ordering::Acquire);
    let waiting = header.waiting_consumers.load(Ordering::Relaxed);
    let futex = header.futex_word.load(Ordering::Relaxed);

    let window = retained_window(write_seq, view.capacity);
    let (oldest_seq, active_messages) = match window {
        Some((oldest, newest)) => (oldest, newest - oldest + 1),
        None => (0, 0),
    };
    let utilization_pct = (active_messages as f64 / view.capacity as f64) * 100.0;

    let is_mpmc = (header.flags & FLAG_MODE_MPMC) != 0;
    let is_spmc = (header.flags & FLAG_MODE_SPMC) != 0;
    let is_lossless = (header.flags & FLAG_POLICY_LOSSLESS_BACKPRESSURE) != 0;
    let has_arena = (header.flags & FLAG_WITH_ARENA) != 0;
    let has_registry = (header.flags & FLAG_WITH_REGISTRY) != 0;

    let readers = read_readers(&mmap, header, write_seq);

    // Arena stats
    let (arena_cap, arena_reserved) = if has_arena && header.arena_offset > 0 {
        let arena_off = header.arena_offset as usize;
        if mmap.len() >= arena_off + 24 {
            let cap = unsafe { *(mmap.as_ptr().add(arena_off) as *const u64) };
            let res = unsafe {
                (*(mmap.as_ptr().add(arena_off + 16) as *const std::sync::atomic::AtomicU64))
                    .load(Ordering::Relaxed)
            };
            (cap, res)
        } else {
            (0, 0)
        }
    } else {
        (0, 0)
    };

    if json {
        println!("{{");
        println!("  \"path\": {},", json_str(path_str));
        println!("  \"file_size\": {},", file_len);
        println!("  \"magic\": \"0x{:016X}\",", header.magic);
        println!("  \"version\": {},", header.version);
        println!(
            "  \"mode\": \"{}\",",
            if is_mpmc {
                "MPMC"
            } else if is_spmc {
                "SPMC"
            } else {
                "UNKNOWN"
            }
        );
        println!(
            "  \"flow_control\": \"{}\",",
            if is_lossless {
                "LosslessBackpressure"
            } else {
                "LossyLatestWins"
            }
        );
        println!("  \"capacity\": {},", header.capacity);
        println!("  \"element_size\": {},", header.element_size);
        println!("  \"write_seq\": {},", write_seq);
        println!("  \"claim_seq\": {},", claim_seq);
        println!("  \"read_seq\": {},", read_seq);
        println!("  \"oldest_seq\": {},", oldest_seq);
        println!("  \"utilization_pct\": {:.2},", utilization_pct);
        println!("  \"waiting_consumers\": {},", waiting);
        println!("  \"futex_word\": {},", futex);
        println!("  \"has_arena\": {},", has_arena);
        if has_arena {
            println!("  \"arena_capacity\": {},", arena_cap);
            println!("  \"arena_reserved\": {},", arena_reserved);
        }
        println!("  \"has_registry\": {},", has_registry);
        println!("  \"readers\": [");
        for (idx, r) in readers.iter().enumerate() {
            println!("    {{");
            println!("      \"slot\": {},", r.slot_index);
            println!("      \"pid\": {},", r.pid);
            println!("      \"name\": {},", json_str(&r.name));
            println!("      \"cursor\": {},", r.cursor_seq);
            println!("      \"lag\": {},", r.lag);
            println!("      \"alive\": {}", r.alive);
            if idx + 1 < readers.len() {
                println!("    }},");
            } else {
                println!("    }}");
            }
        }
        println!("  ]");
        println!("}}");
        return Ok(());
    }

    println!("================================================================================");
    println!(" ringfire Shared Memory Ring Buffer Status");
    println!("================================================================================");
    println!(" File:                {}", path_str);
    println!(" Size:                {} bytes ({:.2} MB)", file_len, file_len as f64 / (1024.0 * 1024.0));
    println!(
        " Protocol / Mode:     Version {} | {} | {}",
        header.version,
        if is_mpmc { "MPMC Queue" } else { "SPMC Broadcast" },
        if is_lossless { "LosslessBackpressure" } else { "LossyLatestWins" }
    );
    println!(" Capacity (Slots):    {} (Mask: 0x{:X})", header.capacity, header.mask);
    println!(" Element Size:        {} bytes", header.element_size);
    println!(" Total Slots Memory:  {:.2} MB", (header.capacity * header.element_size as u64) as f64 / (1024.0 * 1024.0));
    println!("--------------------------------------------------------------------------------");
    println!(" Sequence State:");
    println!("   Write Sequence:    {}", write_seq);
    if is_mpmc {
        println!("   Claim Sequence:    {}", claim_seq);
        println!("   Read Sequence:     {}", read_seq);
    }
    match window {
        Some((oldest, newest)) => println!(
            "   Retained Window:   [{} .. {}] ({} slots, {:.1}% filled)",
            oldest, newest, active_messages, utilization_pct
        ),
        None => println!("   Retained Window:   (empty: nothing published yet)"),
    }
    println!("   Sleeping Readers:  {} (Futex word: {})", waiting, futex);

    if has_arena {
        println!("--------------------------------------------------------------------------------");
        println!(" Variable-Length Payload Arena:");
        println!("   Capacity:          {} bytes ({:.2} MB)", arena_cap, arena_cap as f64 / (1024.0 * 1024.0));
        let util = if arena_cap > 0 { (arena_reserved % arena_cap) as f64 / arena_cap as f64 * 100.0 } else { 0.0 };
        let cycles = arena_reserved.checked_div(arena_cap).unwrap_or(0);
        println!("   Reserved Bytes:    {} ({:.1}% wrapped cycles: {})", arena_reserved, util, cycles);
    }

    println!("--------------------------------------------------------------------------------");
    println!(
        " Registered Readers ({} active / {} max):",
        readers.len(),
        header.reader_registry_count
    );

    if readers.is_empty() {
        println!("   (No active reader processes registered)");
    } else {
        println!(
            "   {:<4}  {:<8}  {:<20}  {:<12}  {:<10}  {:<8}",
            "Slot", "PID", "Name", "Cursor", "Lag", "Status"
        );
        println!("   -------------------------------------------------------------------");
        for r in &readers {
            println!(
                "   {:<4}  {:<8}  {:<20}  {:<12}  {:<10}  {:<8}",
                r.slot_index,
                r.pid,
                r.name,
                r.cursor_seq,
                r.lag,
                if r.alive { "ALIVE" } else { "DEAD" }
            );
        }
    }
    println!("================================================================================");

    Ok(())
}

fn print_blackboard_stat(
    mmap: &Mmap,
    path_str: &str,
    file_len: u64,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let header = unsafe { &*(mmap.as_ptr() as *const BlackboardHeader) };
    if json {
        println!("{{");
        println!("  \"type\": \"Blackboard\",");
        println!("  \"path\": {},", json_str(path_str));
        println!("  \"file_size\": {},", file_len);
        println!("  \"version\": {},", header.version);
        println!("  \"slot_count\": {},", header.slot_count);
        println!("  \"value_size\": {},", header.value_size);
        println!("  \"slot_size\": {}", header.slot_size);
        println!("}}");
        return Ok(());
    }

    println!("================================================================================");
    println!(" ringfire Blackboard (O(1) Seqlock State Table) Status");
    println!("================================================================================");
    println!(" File:         {}", path_str);
    println!(" Size:         {} bytes", file_len);
    println!(" Version:      {}", header.version);
    println!(" Slots Count:  {}", header.slot_count);
    println!(" Value Size:   {} bytes", header.value_size);
    println!(" Slot Stride:  {} bytes (cache-line aligned)", header.slot_size);
    println!("================================================================================");
    Ok(())
}

fn cmd_top(path_str: &str, interval_ms: u64) -> Result<(), Box<dyn std::error::Error>> {
    let (mmap, view) = map_ring(path_str)?;
    let header = unsafe { &*(mmap.as_ptr() as *const RingHeader) };
    let is_lossless = (header.flags & FLAG_POLICY_LOSSLESS_BACKPRESSURE) != 0;
    let mode = if (header.flags & FLAG_MODE_MPMC) != 0 {
        "MPMC"
    } else {
        "SPMC"
    };

    let interval = Duration::from_millis(interval_ms);
    let mut last_time = Instant::now();
    let mut last_seq = header.write_seq.load(Ordering::Acquire);

    println!("\x1B[2J"); // Clear screen

    loop {
        std::thread::sleep(interval);
        let now = Instant::now();
        let dt = now.duration_since(last_time).as_secs_f64();
        last_time = now;

        let write_seq = header.write_seq.load(Ordering::Acquire);
        let delta_seq = write_seq.saturating_sub(last_seq);
        last_seq = write_seq;

        let rate_msg_sec = if dt > 0.0 { delta_seq as f64 / dt } else { 0.0 };
        let rate_mb_sec = (rate_msg_sec * header.element_size as f64) / (1024.0 * 1024.0);

        let (oldest_seq, active_slots) = match retained_window(write_seq, view.capacity) {
            Some((oldest, newest)) => (oldest, newest - oldest + 1),
            None => (0, 0),
        };
        let utilization = (active_slots as f64 / view.capacity as f64) * 100.0;

        let readers = read_readers(&mmap, header, write_seq);

        print!("\x1B[H"); // Cursor to home (0,0)
        println!("ringfire top - {} [{}] | Flow: {}", path_str, mode, if is_lossless { "Backpressure" } else { "LatestWins" });
        println!("Write Seq: {:<12} | Throughput: {:>10.1} msg/s ({:>7.2} MB/s)", write_seq, rate_msg_sec, rate_mb_sec);
        println!("Buffer:    {:<12} / {} slots ({:>5.1}%) | Window: [{} .. {}]", active_slots, header.capacity, utilization, oldest_seq, write_seq);
        println!("Futex:     {} sleeping | Readers: {} active", header.waiting_consumers.load(Ordering::Relaxed), readers.len());
        println!("--------------------------------------------------------------------------------");
        println!("{:<4}  {:<8}  {:<20}  {:<12}  {:<10}  {:<8}", "Slot", "PID", "Name", "Cursor", "Lag", "Status");
        println!("--------------------------------------------------------------------------------");

        if readers.is_empty() {
            println!("(No active readers registered)");
        } else {
            for r in &readers {
                println!(
                    "{:<4}  {:<8}  {:<20}  {:<12}  {:<10}  {:<8}",
                    r.slot_index,
                    r.pid,
                    r.name,
                    r.cursor_seq,
                    r.lag,
                    if r.alive { "ALIVE" } else { "DEAD" }
                );
            }
        }
        println!("--------------------------------------------------------------------------------");
        println!("Press Ctrl+C to exit.");
    }
}

fn cmd_dump(path_str: &str, tail: usize, hex: bool) -> Result<(), Box<dyn std::error::Error>> {
    let (mmap, view) = map_ring(path_str)?;
    let header = unsafe { &*(mmap.as_ptr() as *const RingHeader) };
    let write_seq = header.write_seq.load(Ordering::Acquire);
    let element_size = view.slot_size;

    let Some((oldest, newest)) = retained_window(write_seq, view.capacity) else {
        println!("Ring is empty: nothing published yet (capacity {}).", view.capacity);
        return Ok(());
    };
    let start_seq = newest.saturating_sub(tail.max(1) as u64 - 1).max(oldest);

    println!("Dumping slots from seq {} to seq {} (capacity {}):", start_seq, newest, view.capacity);
    println!("{:<8}  {:<8}  {:<10}  Payload", "Seq", "SlotIdx", "SlotSeq");
    println!("--------------------------------------------------------------------------------");

    for seq in start_seq..=newest {
        let slot_idx = (seq & view.mask) as usize;
        let slot_byte_offset = view.slots_offset + slot_idx * element_size;
        let slot_seq = unsafe {
            (*(mmap.as_ptr().add(slot_byte_offset) as *const std::sync::atomic::AtomicU64))
                .load(Ordering::Acquire)
        };
        let slot_seq_str = if slot_seq == SLOT_WRITING {
            "WRITING".to_string()
        } else {
            slot_seq.to_string()
        };

        let data_slice = &mmap[slot_byte_offset + 8..slot_byte_offset + element_size];

        let payload_str = if hex {
            let hex_preview = data_slice.iter().take(16).map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(" ");
            format!("hex: [{}]{}", hex_preview, if data_slice.len() > 16 { "..." } else { "" })
        } else {
            let printable = data_slice.iter().take(32).map(|&b| if (32..=126).contains(&b) { b as char } else { '.' }).collect::<String>();
            format!("\"{}\" ({} bytes)", printable, data_slice.len())
        };

        println!("{:<8}  {:<8}  {:<10}  {}", seq, slot_idx, slot_seq_str, payload_str);
    }

    Ok(())
}

fn cmd_prune(path_str: &str) -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(path_str);
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut mmap = unsafe { MmapMut::map_mut(&file)? };
    unsafe { validate_ring(mmap.as_ptr(), mmap.len(), None, 8)? };

    let header = unsafe { &*(mmap.as_ptr() as *const RingHeader) };
    if header.reader_registry_offset == 0 || header.reader_registry_count == 0 {
        println!("No ReaderRegistry configured for '{}'", path_str);
        return Ok(());
    }

    let reg_offset = header.reader_registry_offset as usize;
    let reg_count = header.reader_registry_count as usize;
    let base_ptr = unsafe { mmap.as_mut_ptr().add(reg_offset) as *mut ReaderSlot };

    let mut pruned = 0;
    for i in 0..reg_count {
        let slot = unsafe { &*base_ptr.add(i) };
        let pid = slot.pid.load(Ordering::Acquire);
        if pid != 0 && !is_process_alive(pid) {
            let name_len = slot.name.iter().position(|&b| b == 0).unwrap_or(32);
            let name = String::from_utf8_lossy(&slot.name[..name_len]).into_owned();
            // CAS so a reader that re-registered in this slot meanwhile is left alone.
            if slot
                .pid
                .compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                println!("Pruning dead reader slot {}: PID {} ({})", i, pid, name);
                pruned += 1;
            }
        }
    }

    println!("Successfully pruned {} dead reader slot(s).", pruned);
    Ok(())
}
