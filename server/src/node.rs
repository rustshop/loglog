use binrw::{BinRead, BinWrite};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use tokio::sync::Mutex;
use typed_builder::TypedBuilder;

use crate::segment::OpenSegment;
use crate::{AllocationId, EntrySize, LogOffset};

#[derive(Copy, Clone, Debug, BinRead, PartialEq, Eq)]
#[br(big)]
pub struct NodeId(pub u8);

#[derive(Copy, Clone, Debug, BinRead, BinWrite, PartialEq, Eq)]
#[br(big)]
#[bw(big)]
pub struct TermId(pub u16);

/// Some parameters of runtime operation
#[derive(TypedBuilder, Debug, Clone)]
pub struct Parameters {
    /// Base size that a segment file will be allocated with
    #[builder(default = 16*1024)]
    pub base_segment_file_size: u64,

    pub db_path: PathBuf,
}

pub struct Node {
    params: Parameters,
    #[allow(unused)]
    id: NodeId,
    pub term: TermId,
    /// What position in the stream the next entry goes into
    stream_next_pos: AtomicU64,
    // TODO: split into buckets by size
    entry_buffer_pool: Mutex<Vec<Vec<u8>>>,
    pub segment: OpenSegment,
}

impl Node {
    pub async fn new(params: Parameters) -> io::Result<Self> {
        Ok(Self {
            segment: Self::get_segment_file(0, &params.db_path, params.base_segment_file_size)
                .await?,
            params,
            stream_next_pos: AtomicU64::from(128),
            id: NodeId(0),
            term: TermId(0),
            entry_buffer_pool: Mutex::new(vec![]),
        })
    }

    pub async fn get_segment_file(idx: usize, db_path: &Path, size: u64) -> io::Result<OpenSegment> {
        // TODO: this allocs
        OpenSegment::new(&db_path.join(&format!("{idx}.loglog")), size).await
    }

    pub async fn pop_entry_buffer(self: &Arc<Self>) -> Vec<u8> {
        self.entry_buffer_pool
            .lock()
            .await
            .pop()
            .unwrap_or_default()
    }

    pub async fn put_entry_buffer(self: &Arc<Self>, mut buf: Vec<u8>) {
        // keep capacity, clear content
        buf.clear();
        self.entry_buffer_pool.lock().await.push(buf)
    }

    /// Allocate a space in the event stream and return allocation id
    pub fn advance_log_pos(self: &Arc<Self>, len: EntrySize) -> AllocationId {
        AllocationId {
            pos: LogOffset(self.stream_next_pos.fetch_add(u64::from(len.0), SeqCst)),
            term: self.term,
        }
    }
}
