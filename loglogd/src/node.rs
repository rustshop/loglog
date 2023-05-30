#![allow(unused)]
mod peer_handler;
mod rpc_handler;
mod segment_preallocator;
mod segment_sealer;
mod segment_writer;

use crate::db::{load_db, ScanError};
use crate::ioutil::send_file_to_fd;
use crate::node::peer_handler::PeerHandler;
use crate::node::rpc_handler::RpcHandler;
use crate::node::segment_preallocator::SegmentPreallocator;
use crate::node::segment_sealer::SegmentSealer;
use crate::node::segment_writer::SegmentWriter;
use crate::raft::{self, PeerState};
use crate::segment::{OpenSegment, PreallocatedSegment, SegmentFileHeader, SegmentId, SegmentMeta};
use crate::task::PanicGuard;
use convi::ExpectFrom;
use loglogd_api::{AllocationId, EntryHeader, EntrySize, EntryTrailer, LogOffset, NodeId, TermId};
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv6Addr, SocketAddr};
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, RwLock};
use std::{cmp, io, ops};
use thiserror::Error;
use tokio::net::{TcpSocket, TcpStream};
use tracing::{debug, info, trace};
use typed_builder::TypedBuilder;

/// Some parameters of runtime operation
#[derive(TypedBuilder, Debug, Clone)]
pub struct Parameters {
    #[builder(default = Parameters::DEFAULT_NODE_ID)]
    pub id: NodeId,

    #[builder(default = Parameters::DEFAULT_BIND_ADDR)]
    pub rpc_bind: SocketAddr,
    #[builder(default = Parameters::DEFAULT_BIND_ADDR)]
    pub peer_bind: SocketAddr,

    #[builder(default)]
    pub peers: Vec<SocketAddr>,

    /// Base path where segment files are stored
    pub data_dir: PathBuf,

    /// Base size that a segment file will be allocated with
    #[builder(default = Parameters::DEFAULT_BASE_SEGMENT_SIZE)]
    pub base_segment_file_size: u64,
}

impl Parameters {
    pub const DEFAULT_NODE_ID: NodeId = NodeId(0);
    pub const DEFAULT_BASE_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
    pub const DEFAULT_BIND_ADDR: SocketAddr =
        SocketAddr::new(std::net::IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
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
    pub fn next_segment_id(&self) -> SegmentId {
        self.inner
            .range(..)
            .next_back()
            .map(|segment| segment.1.file_meta.id.next())
            .unwrap_or_default()
    }

    /// `end_log_offset` of the last sealed segment
    pub fn end_log_offset(&self) -> Option<LogOffset> {
        self.inner
            .range(..)
            .next_back()
            .map(|s| s.1.content_meta.end_log_offset)
    }

    fn get_containing_offset(&self, offset: LogOffset) -> Option<&SegmentMeta> {
        self.inner.range(..=offset).next_back().map(|s| s.1)
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}
#[derive(Debug)]
pub struct PendingEntries {
    /// Next log offset to give out to incoming entry
    next_available_log_offset: LogOffset,

    /// Entries that were already allocated but were not yet written to storage
    entries: BTreeSet<LogOffset>,
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

    persistent_state: raft::PersistentState,

    // TODO: split into buckets by size?
    entry_buffer_pool: Mutex<Vec<Vec<u8>>>,

    /// Entries already received and allocated, but not yet written
    pending_entries: RwLock<PendingEntries>,

    /// Segment currently being written to
    open_segments: RwLock<OpenSegments>,

    /// Known segments, sorted by `stream_offset`
    sealed_segments: RwLock<SealedSegments>,

    /// Up to which log_offset was the log fsynced to disk (all entries before already written at the time of fsync)
    fsynced_log_offset: AtomicU64,
    /// Used to notify threads waiting for `fsynced_log_offset` to advance
    fsynced_log_offset_tx: tokio::sync::watch::Sender<LogOffset>,
    fsynced_log_offset_rx: tokio::sync::watch::Receiver<LogOffset>,

    /// Last commited (globaly) log offset
    #[allow(unused)] // TODO
    commited_log_offset: AtomicU64,

    // TODO: move or `Option` to allow disconnecting
    raft_state_change_tx: tokio::sync::watch::Sender<raft::PeerState>,
    raft_state_change_rx: tokio::sync::watch::Receiver<raft::PeerState>,
}

impl NodeShared {
    pub fn panic_guard(&self, name: &'static str) -> PanicGuard {
        PanicGuard::new(name, self.is_node_shutting_down.clone())
    }

    pub fn fsynced_log_offset(&self) -> LogOffset {
        LogOffset::new(self.fsynced_log_offset.load(Ordering::SeqCst))
    }

    pub fn is_node_shutting_down(&self) -> bool {
        self.is_node_shutting_down.load(Ordering::SeqCst)
    }

    pub fn update_fsynced_log_offset(&self, log_offset: LogOffset) {
        self.fsynced_log_offset
            .store(log_offset.as_u64(), Ordering::SeqCst);
        // we don't care if everyone disconnected
        let _ = self.fsynced_log_offset_tx.send(log_offset);
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
        let mut write_in_flight = self.pending_entries.write().expect("locking failed");

        let offset = write_in_flight.next_available_log_offset;
        let was_inserted = write_in_flight.entries.insert(offset);
        debug_assert!(was_inserted);
        let alloc = AllocationId {
            offset,
            term: self.persistent_state.current_term,
        };

        write_in_flight.next_available_log_offset =
            offset + EntryHeader::BYTE_SIZE_U64 + u64::from(len.0) + EntryTrailer::BYTE_SIZE_U64;

        trace!(offset = %alloc.offset, "Allocated new entry");

        alloc
    }

    pub fn get_first_pending_log_offset(&self) -> LogOffset {
        let read_in_flight = self.pending_entries.read().expect("Locking failed");
        read_in_flight
            .entries
            .iter()
            .next()
            .copied()
            .unwrap_or(read_in_flight.next_available_log_offset)
    }

    pub fn get_first_pending_entry_offset_ge(
        &self,
        offset_inclusive: LogOffset,
    ) -> Option<LogOffset> {
        self.pending_entries
            .read()
            .expect("Locking failed")
            .entries
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

    pub fn find_log_data_from_sealed(
        &self,
        log_offset: LogOffset,
        len: u64,
    ) -> Option<(Arc<PathBuf>, u64, u64)> {
        let segment = {
            let sealed_segments = &self.sealed_segments.read().expect("Locking failed");
            let Some(segment) = sealed_segments.get_containing_offset(log_offset) else {
                        // if we couldn't find any segments below `log_offset`, that must
                        // mean there are no sealed segments at all, otherwise there wouldn't
                        // be any commited bytes to send and we wouldn't be here
                        debug_assert!(sealed_segments.is_empty());
                        return None;
                    };
            segment.clone()
        };

        if segment.content_meta.end_log_offset <= log_offset {
            // Offset after last sealed segments. `log_offset` must be in an opened segment then.
            return None;
        }

        debug_assert!(segment.content_meta.start_log_offset <= log_offset);
        let bytes_available_in_segment = segment.content_meta.end_log_offset - log_offset;
        let file_offset =
            log_offset - segment.content_meta.start_log_offset + SegmentFileHeader::BYTE_SIZE_U64;

        debug_assert!(0 < bytes_available_in_segment);

        let bytes_to_send = cmp::min(bytes_available_in_segment, len);
        Some((segment.file_meta.path(), file_offset, bytes_to_send))
    }

    pub fn find_log_data_from_open(
        &self,
        log_offset: LogOffset,
        len: u64,
    ) -> Option<(Arc<OpenSegment>, u64, u64)> {
        let (segment, segment_end_log_offset) = {
            let read_open_segments = self.open_segments.read().expect("Locking failed");

            let Some(segment) = read_open_segments.get_containing_offset(log_offset).cloned() else {
                    // This means we couldn't find a matching open segment. This
                    // must be because we missed a segment that was moved between
                    // opened and sealed group.
                    return None;
                };

            // Since segments are closed in order, from lowest offset upward,
            // if we were able to find an open segment starting just before the
            // requested offset, the whole data must be in this segment. The question
            // is only how much of it can we serve, before switching
            // to next segment.

            let segment_end_log_offset =
                if let Some(segment) = read_open_segments.get_after_containing_offset(log_offset) {
                    segment.start_log_offset
                } else {
                    // No open segments after current one (at least yet).
                    // Just use request data as the end pointer
                    log_offset + len
                };

            (segment, segment_end_log_offset)
        };

        debug_assert!(segment.start_log_offset <= log_offset);
        let bytes_available_in_segment = segment_end_log_offset - log_offset;
        let file_offset = log_offset - segment.start_log_offset + SegmentFileHeader::BYTE_SIZE_U64;

        debug_assert!(0 < bytes_available_in_segment);

        let bytes_to_send = cmp::min(bytes_available_in_segment, len);

        Some((segment, file_offset, bytes_to_send))
    }

    pub fn find_log_data(&self, log_offset: LogOffset, len: u64) -> LogData {
        // Should never request data is not yet durable
        debug_assert!(log_offset + len <= self.fsynced_log_offset());

        if let Some((path, offset, len)) = self.find_log_data_from_sealed(log_offset, len) {
            return LogData {
                source: LogDataSource::Sealed(path),
                offset,
                len,
            };
        }
        if let Some((segment, offset, len)) = self.find_log_data_from_open(log_offset, len) {
            return LogData {
                source: LogDataSource::Open(segment),
                offset,
                len,
            };
        }

        panic!("Could not find data for {log_offset} of size {len}. Writer was supposed to guarantee we can't miss a chunk");
    }
}

#[derive(Debug, Clone)]
pub struct LogData {
    source: LogDataSource,
    offset: u64,
    len: u64,
}

impl LogData {
    fn write_to_fd(&self, dst_fd: RawFd) -> io::Result<u64> {
        match self.source {
            LogDataSource::Sealed(ref sealed) => {
                // TODO(perf): we should cache FDs to open sealed files somewhere in some LRU
                let file = std::fs::File::open(sealed.as_ref())?;
                let src_fd = file.as_raw_fd();
                send_file_to_fd(src_fd, self.offset, dst_fd, self.len)
            }
            LogDataSource::Open(ref open) => {
                let src_fd = open.fd.as_raw_fd();

                send_file_to_fd(src_fd, self.offset, dst_fd, self.len)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum LogDataSource {
    Sealed(Arc<PathBuf>),
    Open(Arc<OpenSegment>),
}

pub struct NodeCtrl {
    is_node_shutting_down: Arc<AtomicBool>,
    rpc_addr: SocketAddr,
    peer_addr: SocketAddr,
}

impl NodeCtrl {
    pub fn rpc_addr(&self) -> SocketAddr {
        self.rpc_addr
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
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
    /// Thread preallocating new segments in parallel
    #[allow(unused)]
    segment_preallocator: SegmentPreallocator,
    /// Tokio executor running a tcp connection handling
    #[allow(unused)]
    rpc_handler: RpcHandler,
    /// Thread(s) handling writting out received entries to storage
    #[allow(unused)]
    segment_writer: SegmentWriter,
    #[allow(unused)]
    segment_sealer: SegmentSealer,
    #[allow(unused)]
    peer_handler: PeerHandler,
}

impl Drop for Node {
    fn drop(&mut self) {
        if self.stop_on_drop {
            self.is_node_shutting_down.store(true, Ordering::SeqCst);
        }
    }
}

impl Node {
    pub fn new(params: Parameters) -> anyhow::Result<Self> {
        info!(
            rpc_bind = %params.rpc_bind,
            peer_bind = %params.peer_bind,
            "data-dir" = %params.data_dir.display(),
            "Starting loglogd"
        );

        let segments = load_segments(&params)?;
        let fsynced_log_offset = segments.end_log_offset().unwrap_or_default();
        let next_segment_id = segments.next_segment_id();

        let is_node_shutting_down = Arc::new(AtomicBool::new(false));
        let (entry_writer_tx, entry_writer_rx) = flume::bounded(16);
        let (future_segments_tx, future_segments_rx) = flume::bounded(4);
        let (fsynced_log_offset_tx, fsynced_log_offset_rx) =
            tokio::sync::watch::channel(LogOffset::zero());
        let (last_written_entry_log_offset_tx, last_written_entry_log_offset_rx) =
            watch::channel(LogOffset::zero());

        let (raft_state_change_tx, raft_state_change_rx) =
            tokio::sync::watch::channel(PeerState::default());

        let shared = Arc::new(NodeShared {
            is_segment_writer_done: AtomicBool::new(false),
            is_node_shutting_down: is_node_shutting_down.clone(),
            persistent_state: raft::PersistentState {
                id: params.id,
                current_term: TermId(0),
                voted_for: None,
            },
            entry_buffer_pool: Mutex::new(vec![]),
            params: params.clone(),
            pending_entries: RwLock::new(PendingEntries {
                next_available_log_offset: segments.end_log_offset().unwrap_or_default(),
                entries: BTreeSet::new(),
            }),
            sealed_segments: RwLock::new(segments),
            open_segments: RwLock::new(OpenSegments {
                inner: BTreeMap::new(),
                preallocated_segments_rx: future_segments_rx,
            }),
            fsynced_log_offset: AtomicU64::new(fsynced_log_offset.as_u64()),
            fsynced_log_offset_tx,
            fsynced_log_offset_rx: fsynced_log_offset_rx.clone(),
            commited_log_offset: AtomicU64::new(0),
            raft_state_change_tx,
            raft_state_change_rx,
        });

        let rpc_handler = RpcHandler::new(
            shared.clone(),
            params.rpc_bind,
            entry_writer_tx,
            fsynced_log_offset_rx,
        )?;

        let segment_preallocator =
            SegmentPreallocator::new(next_segment_id, params.clone(), future_segments_tx);

        let segment_writer = SegmentWriter::new(
            shared.clone(),
            entry_writer_rx,
            last_written_entry_log_offset_tx,
        );

        let segment_sealer = SegmentSealer::new(shared.clone(), last_written_entry_log_offset_rx);

        let peer_handler = PeerHandler::new(shared, params.peer_bind, params.peers)?;
        Ok(Node {
            stop_on_drop: true,
            segment_preallocator,
            rpc_handler,
            segment_writer,
            segment_sealer,
            peer_handler,
            is_node_shutting_down,
        })
    }

    pub fn get_ctrl(&self) -> NodeCtrl {
        NodeCtrl {
            is_node_shutting_down: Arc::clone(&self.is_node_shutting_down),
            rpc_addr: self.rpc_handler.local_addr(),
            peer_addr: self.peer_handler.local_addr(),
        }
    }

    pub fn wait(mut self) {
        self.stop_on_drop = false;
        drop(self);
        info!("Node finished");
    }
}

fn load_segments(params: &Parameters) -> Result<SealedSegments, anyhow::Error> {
    let segments = load_db(&params.data_dir)?;
    for segments in segments.windows(2) {
        if segments[0].content_meta.end_log_offset != segments[1].content_meta.start_log_offset {
            panic!(
                "offset inconsistency detected: {} {} != {} {}",
                segments[0].file_meta.id,
                segments[0].content_meta.end_log_offset,
                segments[1].file_meta.id,
                segments[1].content_meta.start_log_offset
            );
        }
    }
    Ok(SealedSegments::from_iter(segments))
}
