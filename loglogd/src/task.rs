use std::thread::JoinHandle;
use std::{fmt, thread};

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
