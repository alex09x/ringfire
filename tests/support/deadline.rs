//! A last-resort bound for tests entering blocking library/network calls.
//! Normal completion cancels and joins the watchdog; timeout fails the test binary.
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub struct Deadline {
    cancel: Option<Sender<()>>,
    worker: Option<JoinHandle<()>>,
}
impl Deadline {
    pub fn new() -> Self {
        let name = thread::current()
            .name()
            .unwrap_or("network test")
            .to_owned();
        let (tx, rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            if matches!(
                rx.recv_timeout(Duration::from_secs(45)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                eprintln!("test deadline exceeded (45 s): {name}");
                std::process::abort();
            }
        });
        Self {
            cancel: Some(tx),
            worker: Some(worker),
        }
    }
}
impl Drop for Deadline {
    fn drop(&mut self) {
        drop(self.cancel.take());
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}
