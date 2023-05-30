use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{fmt, thread};

use tracing::warn;

pub struct AutoJoinHandle {
    pub(crate) join_handle: Option<JoinHandle<()>>,
}

impl AutoJoinHandle {
    pub fn spawn<F>(f: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self::from(thread::spawn(|| {
            f();
        }))
    }

    pub fn spawn_res<F, E>(f: F) -> Self
    where
        F: FnOnce() -> std::result::Result<(), E>,
        F: Send + 'static,
        E: Send + fmt::Debug + 'static,
    {
        Self::from(thread::spawn(|| f().expect("Task failed")))
    }
}

impl From<JoinHandle<()>> for AutoJoinHandle {
    fn from(value: JoinHandle<()>) -> Self {
        Self {
            join_handle: Some(value),
        }
    }
}

impl Drop for AutoJoinHandle {
    fn drop(&mut self) {
        self.join_handle
            .take()
            .expect("Missing join handle")
            .join()
            .expect("Task thread panicked");
    }
}

pub struct PanicGuard {
    name: &'static str,
    flag: Arc<AtomicBool>,
    done: bool,
}

impl PanicGuard {
    pub fn new(name: &'static str, flag: Arc<AtomicBool>) -> Self {
        Self {
            name,
            flag,
            done: false,
        }
    }

    pub fn done(mut self) {
        self.done = true;
    }
}

impl Drop for PanicGuard {
    fn drop(&mut self) {
        if !self.done {
            warn!("Task {} panic detected via PanicGuard", self.name);
            self.flag.store(true, Ordering::SeqCst);
        }
    }
}
