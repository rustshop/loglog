mod request_handler;
mod segment_preallocator;
mod segment_sealer;
mod segment_writer;

use crate::db::{load_db, ScanError};
use crate::node::request_handler::RequestHandler;
use crate::node::segment_preallocator::SegmentPreallocator;
use crate::node::segment_sealer::SegmentSealer;
use crate::node::segment_writer::SegmentWriter;
use crate::segment::{OpenSegment, PreallocatedSegment, SegmentMeta};
use loglogd_api::{AllocationId, EntryHeader, EntrySize, EntryTrailer, LogOffset, NodeId, TermId};
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, RwLock};
use std::{io, ops};
use thiserror::Error;
use tracing::{debug, info, trace};
use typed_builder::TypedBuilder;

/// Some parameters of runtime operation
#[derive(TypedBuilder, Debug, Clone)]
pub struct Parameters {
    /// Base path where segment files are stored
    pub data_dir: PathBuf,

    /// Base size that a segment file will be allocated with
    #[builder(default = Parameters::DEFAULT_BASE_SEGMENT_SIZE)]
    pub base_segment_file_size: u64,
}

impl Parameters {
    pub const DEFAULT_BASE_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
}

/// Actively open segments
///
/// Since `future_segments.recv()` needs exclusive access,
/// we must use the same lock for both fields.
#[derive(Debug)]
pub struct OpenSegments {
    /// Sorted list of all open segements
    ///
    /// There're `Arc`ed so we can just clone and return them.
    /// IO operations don't need uniq reference anyway.
    ///
    /// The outter lock is only for adding/removing elements
    /// from this vector, and `future_rx`.
    inner: BTreeMap<LogOffset, Arc<OpenSegment>>,
    /// Segment pre-allocator will keep sending us new threads through this
    preallocated_segments_rx: flume::Receiver<PreallocatedSegment>,
}

impl OpenSegments {
    /// Fine the segment containing given `offset`
    fn get_containing_offset(&self, offset: LogOffset) -> Option<&Arc<OpenSegment>> {
        self.inner.range(..=offset).next_back().map(|s| s.1)
    }

    /// Fine the segment right after one containing given `offset`
    fn get_after_containing_offset(&self, offset: LogOffset) -> Option<&Arc<OpenSegment>> {
        self.inner.range((offset + 1)..).next().map(|s| s.1)
    }
}

pub struct SealedSegments {
    inner: BTreeMap<LogOffset, SegmentMeta>,
}

impl ops::Deref for SealedSegments {
    type Target = BTreeMap<LogOffset, SegmentMeta>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ops::DerefMut for SealedSegments {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl FromIterator<SegmentMeta> for SealedSegments {
    fn from_iter<T: IntoIterator<Item = SegmentMeta>>(iter: T) -> Self {
        Self {
            inner: BTreeMap::from_iter(
                iter.into_iter()
                    .map(|s| (s.content_meta.start_log_offset, s)),
            ),
        }
    }
}

impl SealedSegments {
    fn get_containing_offset(&self, offset: LogOffset) -> Option<&SegmentMeta> {
        self.inner.range(..=offset).next_back().map(|s| s.1)
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}
#[derive(Debug)]
pub struct EntriesInFlight {
    /// Next log offset to give out to incoming entry
    next_available_log_offset: LogOffset,

    /// Entries that were already allocated but were not yet written to storage
    unwritten: BTreeSet<LogOffset>,
}

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("log store error")]
    LogStore(#[from] ScanError),
    #[error("io error")]
    Io(#[from] io::Error),
}

pub type NodeResult<T> = std::result::Result<T, NodeError>;

pub struct NodeShared {
    params: Parameters,

    is_node_shutting_down: Arc<AtomicBool>,

    is_segment_writer_done: AtomicBool,

    #[allow(unused)]
    id: NodeId,
    pub term: TermId,
    // TODO: split into buckets by size?
    entry_buffer_pool: Mutex<Vec<Vec<u8>>>,

    /// Entries already received and allocated, but not yet written
    entries_in_flight: RwLock<EntriesInFlight>,

    open_segments: RwLock<OpenSegments>,

    /// Known segments, sorted by `stream_offset`
    sealed_segments: RwLock<SealedSegments>,

    fsynced_log_offset: AtomicU64,
    last_fsynced_log_offset_tx: tokio::sync::watch::Sender<LogOffset>,
}

impl NodeShared {
    pub fn fsynced_log_offset(&self) -> LogOffset {
        LogOffset(self.fsynced_log_offset.load(Ordering::Relaxed))
    }

    /// Get a vector from a pool of vectors
    ///
    /// This is to avoid allocating all the time.
    /// TODO: Optimize. Is it even worth it? Should we spread
    /// accross multiple size buckets and to spread the contention?
    pub fn pop_entry_buffer(self: &Arc<Self>) -> Vec<u8> {
        self.entry_buffer_pool
            .lock()
            .expect("locking failed")
            .pop()
            .unwrap_or_default()
    }

    // Return back a vector to a pool of vectors. See [`Self::pop_entry_buffer`].
    pub fn put_entry_buffer(self: &Arc<Self>, mut buf: Vec<u8>) {
        // keep capacity, clear content
        buf.clear();
        self.entry_buffer_pool
            .lock()
            .expect("locking failed")
            .push(buf)
    }

    /// Allocate a space in the event stream and return allocation id
    pub fn allocate_new_entry(self: &Arc<Self>, len: EntrySize) -> AllocationId {
        let mut write_in_flight = self.entries_in_flight.write().expect("locking failed");

        let offset = write_in_flight.next_available_log_offset;
        let was_inserted = write_in_flight.unwritten.insert(offset);
        debug_assert!(was_inserted);
        let alloc = AllocationId {
            offset,
            term: self.term,
        };

        write_in_flight.next_available_log_offset = LogOffset(
            offset.0 + EntryHeader::BYTE_SIZE_U64 + u64::from(len.0) + EntryTrailer::BYTE_SIZE_U64,
        );

        trace!(offset = alloc.offset.0, "Allocated new entry");

        alloc
    }

    pub fn get_first_unwritten_log_offset(&self) -> LogOffset {
        let read_in_flight = self.entries_in_flight.read().expect("Locking failed");
        read_in_flight
            .unwritten
            .iter()
            .next()
            .copied()
            .unwrap_or(read_in_flight.next_available_log_offset)
    }

    pub fn get_first_unwritten_entry_offset_ge(
        &self,
        offset_inclusive: LogOffset,
    ) -> Option<LogOffset> {
        self.entries_in_flight
            .read()
            .expect("Locking failed")
            .unwritten
            .range(offset_inclusive..)
            .next()
            .copied()
    }
    pub fn get_sealed_segments_end_log_offset(&self) -> Option<LogOffset> {
        self.sealed_segments
            .read()
            .expect("Locking failed")
            .last_key_value()
            .map(|(_, last_sealed_segment)| last_sealed_segment.content_meta.end_log_offset)
    }
}

pub struct NodeCtrl {
    is_node_shutting_down: Arc<AtomicBool>,
    local_addr: SocketAddr,
}

impl NodeCtrl {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn stop(&self) {
        self.is_node_shutting_down.store(true, Ordering::SeqCst);
    }
    pub fn install_signal_handler(&self) -> anyhow::Result<()> {
        debug!("Installing signal handler");
        for sig in TERM_SIGNALS {
            trace!(sig, "Installing signal handler");
            flag::register(*sig, Arc::clone(&self.is_node_shutting_down))?;
        }
        Ok(())
    }
}

pub struct Node {
    is_node_shutting_down: Arc<AtomicBool>,
    stop_on_drop: bool,
    local_addr: SocketAddr,
    /// Thread preallocating new segments in parallel
    #[allow(unused)]
    segment_preallocator: SegmentPreallocator,
    /// Tokio executor running a tcp connection handling
    #[allow(unused)]
    request_handler: RequestHandler,
    /// Thread(s) handling writting out received entries to storage
    #[allow(unused)]
    segment_writer: SegmentWriter,
    #[allow(unused)]
    segment_sealer: SegmentSealer,
}

impl Drop for Node {
    fn drop(&mut self) {
        if self.stop_on_drop {
            self.is_node_shutting_down.store(true, Ordering::SeqCst);
        }
    }
}

impl Node {
    pub fn new(listen_addr: SocketAddr, params: Parameters) -> anyhow::Result<Self> {
        info!(
            listen = %listen_addr,
            "data-dir" = %params.data_dir.display(),
            "Starting loglogd"
        );

        let segments = load_db(&params.data_dir)?;

        // TODO: what to do with this check? It should not happen if
        // there was no data loss or bugs. Should it delete segments past
        // the inconsistency? That risks automatic loss of data. But
        // any data recovery issues kind of do already...
        for segments in segments.windows(2) {
            if segments[0].content_meta.end_log_offset != segments[1].content_meta.start_log_offset
            {
                panic!(
                    "offset inconsistency detected: {} {} != {} {}",
                    segments[0].file_meta.id,
                    segments[0].content_meta.end_log_offset,
                    segments[1].file_meta.id,
                    segments[1].content_meta.start_log_offset
                );
            }
        }

        let next_segment_id = segments
            .last()
            .map(|segment| segment.file_meta.id.next())
            .unwrap_or_default();
        let (entry_writer_tx, entry_writer_rx) = flume::bounded(16);
        let (future_segments_tx, future_segments_rx) = flume::bounded(4);
        let (last_fsynced_log_offset_tx, last_fsynced_log_offset_rx) =
            tokio::sync::watch::channel(LogOffset(0));
        let (last_written_entry_log_offset_tx, last_written_entry_log_offset_rx) =
            watch::channel(LogOffset(0));

        let is_node_shutting_down = Arc::new(AtomicBool::new(false));
        let shared = Arc::new(NodeShared {
            is_segment_writer_done: AtomicBool::new(false),
            is_node_shutting_down: is_node_shutting_down.clone(),
            id: NodeId(0),
            term: TermId(0),
            entry_buffer_pool: Mutex::new(vec![]),
            params: params.clone(),
            entries_in_flight: RwLock::new(EntriesInFlight {
                next_available_log_offset: segments
                    .last()
                    .map(|s| s.content_meta.end_log_offset)
                    .unwrap_or(LogOffset(0)),
                unwritten: BTreeSet::new(),
            }),
            sealed_segments: RwLock::new(SealedSegments::from_iter(segments)),
            open_segments: RwLock::new(OpenSegments {
                inner: BTreeMap::new(),
                preallocated_segments_rx: future_segments_rx,
            }),
            fsynced_log_offset: AtomicU64::new(0),
            last_fsynced_log_offset_tx,
        });

        let request_handler = RequestHandler::new(
            shared.clone(),
            listen_addr,
            entry_writer_tx,
            last_fsynced_log_offset_rx,
        )?;
        let local_addr = request_handler.local_addr();

        let segment_writer = SegmentWriter::new(
            shared.clone(),
            entry_writer_rx,
            last_written_entry_log_offset_tx,
        );

        let segment_sealer = SegmentSealer::new(shared, last_written_entry_log_offset_rx);

        let segment_preallocator =
            SegmentPreallocator::new(next_segment_id, params, future_segments_tx);

        Ok(Node {
            stop_on_drop: true,
            local_addr,
            segment_preallocator,
            request_handler,
            segment_writer,
            segment_sealer,
            is_node_shutting_down,
        })
    }

    pub fn get_ctrl(&self) -> NodeCtrl {
        NodeCtrl {
            is_node_shutting_down: Arc::clone(&self.is_node_shutting_down),
            local_addr: self.local_addr,
        }
    }

    pub fn wait(mut self) {
        self.stop_on_drop = false;
        drop(self);
        info!("Node finished");
    }
}
