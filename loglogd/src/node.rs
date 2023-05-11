// mod request_handling;
// mod segment_preloading;

mod request_handler;
mod write_loop;

use crate::db::{load_db, ScanError};
use crate::node::request_handler::RequestHandler;
use crate::node::write_loop::WriteLoop;
use crate::segment::{OpenSegment, SegmentMeta};
use loglogd_api::{AllocationId, EntryHeader, EntrySize, EntryTrailer, LogOffset, NodeId, TermId};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, RwLock};
use thiserror::Error;
use tokio::sync::watch;
use tracing::{info, trace};
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
    /// Segment pre-writing thread will keep sending us new threads through this
    future_rx: flume::Receiver<OpenSegment>,
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

    is_node_shutting_down: AtomicBool,

    is_writting_loop_done: AtomicBool,

    #[allow(unused)]
    id: NodeId,
    pub term: TermId,
    // TODO: split into buckets by size?
    entry_buffer_pool: Mutex<Vec<Vec<u8>>>,

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
}

pub struct Node {
    is_node_shutting_down: Arc<AtomicBool>,
    local_addr: SocketAddr,
    request_handler: RequestHandler,
    // local_addr: SocketAddr,
    // write_loop: tokio::task::JoinHandle<()>,
    // segment_preloading_loop: tokio::task::JoinHandle<()>,
    // fsync_loop: tokio::task::JoinHandle<()>,
    // request_handling_loop: tokio::task::JoinHandle<TcpListener>,
}

impl Node {
    pub async fn new(listen_addr: SocketAddr, params: Parameters) -> anyhow::Result<Self> {
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
            is_writting_loop_done: AtomicBool::new(false),
            is_node_shutting_down: AtomicBool::new(false),
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
                future_rx: future_segments_rx,
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

        let write_loop = WriteLoop::new(
            shared.clone(),
            entry_writer_rx,
            last_written_entry_log_offset_tx,
        );
        let local_addr = request_handler.local_addr();
        Ok(Node {
            request_handler,
            local_addr,
            // write_loop: tokio_uring::spawn(
            //     shared
            //         .clone()
            //         .run_entry_write_loop(entry_writer_rx, last_written_entry_log_offset_tx),
            // ),
            // segment_preloading_loop: tokio_uring::spawn(
            //     SegmentPreloading {
            //         params,
            //         tx: future_segments_tx,
            //     }
            //     .run_segment_preloading_loop(next_segment_id),
            // ),

            // fsync_loop: tokio_uring::spawn(
            //     shared
            //         .clone()
            //         .run_fsync_loop(last_written_entry_log_offset_rx),
            // ),
            // request_handling_loop: tokio_uring::spawn(
            //     RequestHandler {
            //         shared,
            //         last_fsynced_log_offset_rx,
            //         is_node_shutting_down: is_node_shutting_down.clone(),
            //     }
            //     .run_request_handling_loop(listener, entry_writer_tx),
            // ),
            is_node_shutting_down,
        })
    }

    /*
        pub fn get_ctrl(&self) -> NodeCtrl {
            NodeCtrl {
                is_node_shutting_down: Arc::clone(&self.is_node_shutting_down),
                local_addr: self.local_addr,
            }
        }

        pub async fn wait(self) -> Result<TcpListener, JoinError> {
            let Self {
                write_loop,
                segment_preloading_loop,
                fsync_loop,
                request_handling_loop,
                ..
            } = self;

            write_loop.await?;
            segment_preloading_loop.await?;
            fsync_loop.await?;
            let res = request_handling_loop.await;

            info!("Node finished");

            res
        }
    }

    #[derive(Error, Debug)]
    pub enum NodeError {
        #[error("log store error")]
        LogStore(#[from] ScanError),
        #[error("io error")]
        Io(#[from] io::Error),
    }

    pub type NodeResult<T> = std::result::Result<T, NodeError>;

    impl NodeShared {
        /// Get a vector from a pool of vectors
        ///
        /// This is to avoid allocating all the time.
        /// TODO: Optimize. Is it even worth it? Should we spread
        /// accross multiple size buckets and to spread the contention?
        pub async fn pop_entry_buffer(self: &Arc<Self>) -> Vec<u8> {
            self.entry_buffer_pool
                .lock()
                .await
                .pop()
                .unwrap_or_default()
        }

        // Return back a vector to a pool of vectors. See [`Self::pop_entry_buffer`].
        pub async fn put_entry_buffer(self: &Arc<Self>, mut buf: Vec<u8>) {
            // keep capacity, clear content
            buf.clear();
            self.entry_buffer_pool.lock().await.push(buf)
        }

        /// Allocate a space in the event stream and return allocation id
        pub async fn allocate_new_entry(self: &Arc<Self>, len: EntrySize) -> AllocationId {
            let mut write_in_flight = self.entries_in_flight.write().await;

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


        pub async fn get_first_unwritten_log_offset(self: &Arc<Self>) -> LogOffset {
            let read_in_flight = self.entries_in_flight.read().await;
            read_in_flight
                .unwritten
                .iter()
                .next()
                .copied()
                .unwrap_or(read_in_flight.next_available_log_offset)
        }

        pub async fn get_first_unwritten_entry_offset_ge(
            self: &Arc<Self>,
            offset_inclusive: LogOffset,
        ) -> Option<LogOffset> {
            self.entries_in_flight
                .read()
                .await
                .unwritten
                .range(offset_inclusive..)
                .next()
                .copied()
        }

        pub async fn get_sealed_segments_end_log_offset(self: &Arc<Self>) -> Option<LogOffset> {
            self.sealed_segments
                .read()
                .await
                .last_key_value()
                .map(|(_, last_sealed_segment)| last_sealed_segment.content_meta.end_log_offset)
        }

        pub async fn run_fsync_loop(
            self: Arc<Self>,
            mut last_written_entry_log_offset_rx: watch::Receiver<LogOffset>,
        ) {
            let _guard = scopeguard::guard((), |_| {
                info!("fsync loop is done");
            });
            loop {
                // we don't care if the other side dropped - we will just keep fsyncing
                // until done
                let _ = last_written_entry_log_offset_rx.changed().await;

                let first_unwritten_log_offset = self.get_first_unwritten_log_offset().await;
                let fsynced_log_offset = self.fsynced_log_offset.load(Ordering::SeqCst);

                if fsynced_log_offset == first_unwritten_log_offset.0 {
                    // if we're done with all the pending work, and the writting loop
                    // is done too, we can finish
                    if self.is_writting_loop_done.load(Ordering::Relaxed) {
                        return;
                    }
                    continue;
                }

                'inner: loop {
                    let read_open_segments = self.open_segments.read().await;

                    let mut segments_iter = read_open_segments.inner.iter();
                    let first_segment = segments_iter
                        .next()
                        .map(|(offset, segment)| (*offset, Arc::clone(segment)));
                    let second_segment_start = segments_iter.next().map(|s| s.0).cloned();
                    drop(segments_iter);
                    drop(read_open_segments);

                    if let Some((first_segment_start_log_offset, first_segment)) = first_segment {
                        debug!(
                            segment_id = %first_segment.id,
                            segment_start_log_offset = first_segment_start_log_offset.0,
                            first_unwritten_log_offset = first_unwritten_log_offset.0,
                            "fsync"
                        );
                        if let Err(e) = first_segment.file.sync_data().await {
                            warn!(error = %e, "Could not fsync opened segment file");
                            break 'inner;
                        }

                        let first_segment_end_log_offset = match second_segment_start {
                            Some(v) => v,
                            // Until we have at lest two open segments, we won't close the first one.
                            // It's not a big deal, and avoids having to calculate the end of first
                            // segment.
                            None => break 'inner,
                        };

                        // Are all the pending writes to the first open segment complete? If so,
                        // we can close and seal it.
                        if first_segment_end_log_offset <= first_unwritten_log_offset {
                            let first_segment_offset_size =
                                first_segment_end_log_offset.0 - first_segment_start_log_offset.0;
                            // Note: we append to sealed segments first, and remove from open segments second. This way
                            // any readers looking for their data can't miss it between removal and addition.
                            {
                                let mut write_sealed_segments = self.sealed_segments.write().await;
                                write_sealed_segments.insert(
                                    first_segment_start_log_offset,
                                    SegmentMeta {
                                        file_meta: SegmentFileMeta::new(
                                            first_segment.id,
                                            first_segment_offset_size
                                                + SegmentFileHeader::BYTE_SIZE_U64,
                                            SegmentFileMeta::get_path(
                                                &self.params.data_dir,
                                                first_segment.id,
                                            ),
                                        ),
                                        content_meta: segment::SegmentContentMeta {
                                            start_log_offset: first_segment_start_log_offset,
                                            end_log_offset: first_segment_end_log_offset,
                                        },
                                    },
                                );
                                drop(write_sealed_segments);
                            }
                            {
                                let mut write_open_segments = self.open_segments.write().await;
                                let removed = write_open_segments
                                    .inner
                                    .remove(&first_segment_start_log_offset);
                                debug_assert!(removed.is_some());
                            }
                        } else {
                            break 'inner;
                        }

                        // continue 'inner - there might be more
                    } else {
                        break 'inner;
                    }
                }

                // Only after successfully looping and fsyncing and/or closing all matching segments
                // we update `last_fsynced_log_offset`
                self.fsynced_log_offset
                    .store(first_unwritten_log_offset.0, Ordering::SeqCst);
                // we don't care if everyone disconnected
                let _ = self
                    .last_fsynced_log_offset_tx
                    .send(first_unwritten_log_offset);
            }
        }
        */
}
