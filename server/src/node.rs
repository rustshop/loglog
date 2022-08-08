use self::segment_preloading::*;
use crate::ioutil::file_write_all;
use crate::segment::{
    self, EntryWrite, LogStore, OpenSegment, ScanError, SegmentFileHeader, SegmentFileMeta,
    SegmentMeta,
};
use loglogd_api::{AllocationId, EntryHeader, EntrySize, EntryTrailer, LogOffset, NodeId, TermId};
use request_handling::*;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::{watch, Mutex, RwLock};
use tokio::task::JoinError;
use tokio_uring::buf::IoBuf;
use tokio_uring::net::TcpListener;
use tracing::{debug, info, trace, warn};
use typed_builder::TypedBuilder;
mod request_handling;
mod segment_preloading;

/// Some parameters of runtime operation
#[derive(TypedBuilder, Debug, Clone)]
pub struct Parameters {
    /// Base path where segment files are stored
    pub db_path: PathBuf,

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
    future_rx: Receiver<OpenSegment>,
}

#[derive(Debug)]
pub struct EntriesInFlight {
    /// Next log offset to give out to incoming entry
    next_available_log_offset: LogOffset,

    /// Entries that were already allocated but were not yet written to storage
    unwritten: BTreeSet<LogOffset>,
}

#[derive(Debug)]
pub struct NodeShared {
    params: Parameters,

    is_writting_loop_done: AtomicBool,

    #[allow(unused)]
    id: NodeId,
    pub term: TermId,
    // TODO: split into buckets by size?
    entry_buffer_pool: Mutex<Vec<Vec<u8>>>,

    entries_in_flight: RwLock<EntriesInFlight>,

    open_segments: RwLock<OpenSegments>,

    /// Known segments, sorted by `stream_offset`
    sealed_segments: RwLock<BTreeMap<LogOffset, SegmentMeta>>,

    fsynced_log_offset: AtomicU64,
    last_fsynced_log_offset_tx: watch::Sender<LogOffset>,
}

pub struct NodeCtrl {
    is_node_shutting_down: Arc<AtomicBool>,
}

impl NodeCtrl {
    pub fn stop(&self) {
        self.is_node_shutting_down.store(true, Ordering::SeqCst);
    }
}

pub struct Node {
    is_node_shutting_down: Arc<AtomicBool>,
    write_loop: tokio::task::JoinHandle<()>,
    segment_preloading_loop: tokio::task::JoinHandle<()>,
    fsync_loop: tokio::task::JoinHandle<()>,
    request_handling_loop: tokio::task::JoinHandle<TcpListener>,
}

impl Node {
    pub async fn new(listener: TcpListener, params: Parameters) -> NodeResult<Self> {
        let segments = tokio::task::spawn_blocking({
            let db_path = params.db_path.clone();
            move || -> NodeResult<Vec<SegmentMeta>> {
                let log_store = LogStore::open_or_create(db_path)?;
                Ok(log_store.load_db()?)
            }
        })
        .await
        .expect("log store thread panicked")?;

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
            .map(|segment| segment.file_meta.id + 1)
            .unwrap_or(0);
        let (entry_writer_tx, entry_writer_rx) = channel(16);
        let (future_segments_tx, future_segments_rx) = channel(4);
        let (last_fsynced_log_offset_tx, last_fsynced_log_offset_rx) = watch::channel(LogOffset(0));
        let (last_written_entry_log_offset_tx, last_written_entry_log_offset_rx) =
            watch::channel(LogOffset(0));

        let is_node_shutting_down = Arc::new(AtomicBool::new(false));
        let shared = Arc::new(NodeShared {
            is_writting_loop_done: AtomicBool::new(false),
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
            sealed_segments: RwLock::new(
                segments
                    .into_iter()
                    .map(|s| (s.content_meta.start_log_offset, s))
                    .collect(),
            ),
            open_segments: RwLock::new(OpenSegments {
                inner: BTreeMap::new(),
                future_rx: future_segments_rx,
            }),
            fsynced_log_offset: AtomicU64::new(0),
            last_fsynced_log_offset_tx,
        });

        Ok(Node {
            write_loop: tokio_uring::spawn(
                shared
                    .clone()
                    .run_entry_write_loop(entry_writer_rx, last_written_entry_log_offset_tx),
            ),
            segment_preloading_loop: tokio_uring::spawn(
                SegmentPreloading {
                    params,
                    tx: future_segments_tx,
                }
                .run_segment_preloading_loop(next_segment_id),
            ),

            fsync_loop: tokio_uring::spawn(
                shared
                    .clone()
                    .run_fsync_loop(last_written_entry_log_offset_rx),
            ),
            request_handling_loop: tokio_uring::spawn(
                RequestHandler {
                    shared,
                    last_fsynced_log_offset_rx,
                    is_node_shutting_down: is_node_shutting_down.clone(),
                }
                .run_request_handling_loop(listener, entry_writer_tx),
            ),
            is_node_shutting_down,
        })
    }

    pub fn get_ctrl(&self) -> NodeCtrl {
        NodeCtrl {
            is_node_shutting_down: Arc::clone(&self.is_node_shutting_down),
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

    pub async fn run_entry_write_loop(
        self: Arc<Self>,
        mut rx: Receiver<EntryWrite>,
        last_written_entry_log_offset_tx: watch::Sender<LogOffset>,
    ) {
        let _guard = scopeguard::guard((), |_| {
            info!("writting loop is done");
            self.is_writting_loop_done.store(true, Ordering::SeqCst);
        });

        while let Some(entry) = rx.recv().await {
            self.handle_entry_write(entry, &last_written_entry_log_offset_tx)
                .await;
        }
    }

    /// Write an entry to the log
    async fn handle_entry_write(
        self: &Arc<Self>,
        entry: EntryWrite,
        last_written_entry_log_offset_tx: &watch::Sender<LogOffset>,
    ) {
        let EntryWrite { offset, entry } = entry;

        debug!(
            offset = offset.0,
            size = entry.len(),
            "Received new entry write"
        );

        let (segment_start_log_offset, segment) = self.get_segment_for_write(offset).await;

        // TODO: check if we didn't already have this chunk, and if the offset seems valid (keep track in memory)
        let file_offset = offset.0 - segment_start_log_offset.0 + SegmentFileHeader::BYTE_SIZE_U64;

        debug!(
            offset = offset.0,
            size = entry.len(),
            segment_id = segment.id,
            segment_offset = file_offset,
            "Writing entry to segment"
        );
        let entry_len = entry.len();
        let (res, res_buf) =
            // Note: `slice` here seem to surrisingly use `capacity`, not `len`
            file_write_all(&segment.file, entry.slice(..entry_len), file_offset).await;

        if let Err(e) = res {
            panic!("IO Error when writing log: {}, crashing immediately", e);
        }

        self.mark_entry_written(offset).await;
        // we don't care if the other side is still there
        let _ = last_written_entry_log_offset_tx.send(offset);

        self.put_entry_buffer(res_buf).await;
    }

    async fn get_segment_for_write<'a>(
        self: &'a Arc<Self>,
        entry_log_offset: LogOffset,
    ) -> (LogOffset, Arc<OpenSegment>) {
        fn get_segment_for_offset(
            open_segments: &BTreeMap<LogOffset, Arc<OpenSegment>>,
            log_offset: LogOffset,
        ) -> Option<(LogOffset, Arc<OpenSegment>)> {
            let mut iter = open_segments.range(..=log_offset);

            if let Some(segment) = iter.next_back() {
                // It's one of the not-last ones, we can return right away
                if segment.0
                    != open_segments
                        .last_key_value()
                        .expect("has at least one element")
                        .0
                {
                    return Some((*segment.0, segment.1.clone()));
                }
            }

            // It's the last one or we need a new one
            if let Some((last_start_log_offset, last)) = open_segments.last_key_value() {
                debug_assert!(*last_start_log_offset <= log_offset);
                let write_offset = log_offset.0 - last_start_log_offset.0;
                if write_offset + SegmentFileHeader::BYTE_SIZE_U64 < last.allocated_size {
                    return Some((*last_start_log_offset, last.clone()));
                }
            }
            None
        }

        #[derive(Copy, Clone)]
        struct LastSegmentInfo {
            start_log_offset: LogOffset,
            end_of_allocation_log_offset: LogOffset,
        }

        loop {
            let last_segment_info = {
                // Usually the segment to use should be one of the existing one,
                // so lock the list for reading and clone the match if so.
                let read_open_segments = self.open_segments.read().await;

                if let Some(segment) =
                    get_segment_for_offset(&read_open_segments.inner, entry_log_offset)
                {
                    trace!(%entry_log_offset, ?segment, "found segment for entry write");
                    return segment;
                }
                // Well.. seems like we need a new one... . Record the ending offset
                let last_segment_info =
                    read_open_segments
                        .inner
                        .last_key_value()
                        .map(|s| LastSegmentInfo {
                            start_log_offset: *s.0,
                            end_of_allocation_log_offset: LogOffset(
                                s.0 .0 + s.1.allocated_size - SegmentFileHeader::BYTE_SIZE_U64,
                            ),
                        });
                drop(read_open_segments);
                last_segment_info
            };

            let next_segment_start_log_offset = if let Some(last_segment_info) = last_segment_info {
                // It would be a mistake to consider current `entry_log_offset` as a beginning of
                // next segment as we might have arrived here out of order. Instead - we can use
                // `unwritten` to find first unwritten entry for new segment, and thus its starting
                // byte.
                self.get_first_unwritten_entry_offset_ge(
                    last_segment_info.end_of_allocation_log_offset,
                )
                .await
                .expect("at very least this entry should be in `unwritten`")
            } else {
                self.get_sealed_segments_end_log_offset()
                    .await
                    .unwrap_or(LogOffset(0))
            };

            // It wasn't one of the existing open segments, so we drop the lock fo reading
            // so potentially other threads can make write their stuff, and try to switch
            // to writes.
            let (new_segment_start_log_offset, new_segment) = {
                let mut write_open_segments = self.open_segments.write().await;

                // Check again if we still need to create new open segment. Things might have changed between unlock & write lock - some
                // other thread might have done it before us. In such a case the last_segment we recorded, will be different now.
                if write_open_segments.inner.last_key_value().map(|s| *s.0)
                    != last_segment_info.map(|s| s.start_log_offset)
                {
                    // Some other thread(s) must have appened new open segment(s) - we can posibly just use it
                    //  but we need to check again.
                    continue;
                }

                // This recv while holding a write lock looks a bit meh, but
                // for this thread to ever have to wait here, would
                // mean that pre-allocating segments is slower than filling
                // them with actual data.
                let new_segment = Arc::new(
                    write_open_segments
                        .future_rx
                        .recv()
                        .await
                        .expect("segment pre-writing thread must never disconnect"),
                );
                let prev = write_open_segments
                    .inner
                    .insert(next_segment_start_log_offset, new_segment.clone());

                debug_assert!(prev.is_none());
                // we can drop the write lock already, since the segment is already usable
                // However, we are still responsible for writing the segment file header,
                // which pre-writer couldn't do, as it didn't know the starting log offset
                drop(write_open_segments);
                debug!(
                    %next_segment_start_log_offset,
                    ?new_segment, "opened new segment"
                );
                (next_segment_start_log_offset, new_segment)
            };

            self.put_entry_buffer(
                new_segment
                    .write_header(new_segment_start_log_offset, self.pop_entry_buffer().await)
                    .await,
            )
            .await;

            // It would be tempting to just return the `new_segment` as the segment we need. But that would be
            // a mistake - this entry might theoretically be so far ahead, that it needs even more segments opened.
            // Because of this we just let the loop run again.
        }
    }

    pub async fn mark_entry_written(self: &Arc<NodeShared>, log_offset: LogOffset) {
        let mut write_in_flight = self.entries_in_flight.write().await;
        let was_removed = write_in_flight.unwritten.remove(&log_offset);
        debug_assert!(was_removed);
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
                        segment_id = first_segment.id,
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
                                            &self.params.db_path,
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
}
