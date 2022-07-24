use binrw::{BinRead, BinWrite};
use std::collections::BTreeSet;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, RwLock};
use tokio_uring::buf::IoBuf;
use tracing::{debug, trace};
use typed_builder::TypedBuilder;

use crate::ioutil::file_write_all;
use crate::segment::{
    EntryHeader, EntryTrailer, EntryWrite, OpenSegment, SegmentFileHeader, SegmentFileMeta,
    SegmentMeta,
};
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

/// Actively open segments
///
/// Since `future_segments.recv()` needs exclusive access,
/// we must use the same lock for both fields.
pub struct OpenSegments {
    /// Sorted list of all open segements
    ///
    /// There're `Arc`ed so we can just clone and return them.
    /// IO operations don't need uniq reference anyway.
    ///
    /// The outter lock is only for adding/removing elements
    /// from this vector, and `future_rx`.
    inner: Vec<Arc<OpenSegment>>,
    /// Segment pre-writing thread will keep sending us new threads through this
    future_rx: Receiver<OpenSegment>,
}

pub struct EntriesInFlight {
    /// Next log offset to give out to incoming entry
    next_available_log_offset: LogOffset,

    /// Entries that were already allocated but were not yet written to storage
    unwritten: BTreeSet<LogOffset>,
}

pub struct Node {
    params: Parameters,
    #[allow(unused)]
    id: NodeId,
    pub term: TermId,
    // TODO: split into buckets by size
    entry_buffer_pool: Mutex<Vec<Vec<u8>>>,

    entries_in_flight: RwLock<EntriesInFlight>,

    open_segments: RwLock<OpenSegments>,

    /// Known segments, sorted by `stream_offset`
    sealed_segments: RwLock<Vec<SegmentMeta>>,
    pub entry_writer_tx: Sender<EntryWrite>,
}

impl Node {
    pub fn new(
        params: Parameters,
        sealed_segments: Vec<SegmentMeta>,
        future_segments: Receiver<OpenSegment>,
        entry_writer_tx: Sender<EntryWrite>,
    ) -> io::Result<Self> {
        Ok(Self {
            id: NodeId(0),
            term: TermId(0),
            entry_buffer_pool: Mutex::new(vec![]),
            params,
            entries_in_flight: RwLock::new(EntriesInFlight {
                next_available_log_offset: sealed_segments
                    .last()
                    .map(|s| s.content_meta.end_log_offset)
                    .unwrap_or(LogOffset(0)),
                unwritten: BTreeSet::new(),
            }),
            sealed_segments: RwLock::new(sealed_segments),
            open_segments: RwLock::new(OpenSegments {
                inner: vec![],
                future_rx: future_segments,
            }),
            entry_writer_tx,
        })
    }

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

    pub async fn run_entry_write_loop(self: Arc<Self>, mut rx: Receiver<EntryWrite>) {
        while let Some(entry) = rx.recv().await {
            self.handle_entry_write(entry).await;
        }
    }

    /// Write an entry to the log
    async fn handle_entry_write(self: &Arc<Self>, entry: EntryWrite) {
        let EntryWrite { offset, entry } = entry;

        debug!(
            offset = offset.0,
            size = entry.len(),
            "Received new entry write"
        );

        let segment = self.get_segment_for(offset).await;

        // TODO: check if we didn't already have this chunk, and if the offset seems valid (keep track in memory)
        let file_offset =
            offset.0 - segment.file_stream_start_pos.0 + SegmentFileHeader::BYTE_SIZE_U64;

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

        self.put_entry_buffer(res_buf).await;
    }

    async fn get_segment_for<'a>(self: &'a Arc<Self>, log_offset: LogOffset) -> Arc<OpenSegment> {
        fn find_index_for_offset(
            open_segments: &Vec<Arc<OpenSegment>>,
            log_offset: LogOffset,
        ) -> Option<usize> {
            let index = match open_segments
                .binary_search_by_key(&log_offset, |segment| segment.file_stream_start_pos)
            {
                Ok(i) => i,
                Err(i) => i,
            };

            // It's one of the not-last ones
            if index < open_segments.len() {
                return Some(index);
            }
            assert_eq!(index, open_segments.len());

            // It's the last one or we need a new one
            if let Some(last) = open_segments.last() {
                debug_assert!(last.file_stream_start_pos < log_offset);
                let write_offset = log_offset.0 - last.file_stream_start_pos.0;
                if write_offset + SegmentFileHeader::BYTE_SIZE_U64 < last.allocated_size {
                    return Some(open_segments.len() - 1);
                }
            }
            None
        }

        // Usually the segment to use should be one of the existing one,
        // so lock the list for reading and clone the match if so.
        let read_open_segments = self.open_segments.read().await;

        if let Some(idx) = find_index_for_offset(&read_open_segments.inner, log_offset) {
            return read_open_segments.inner[idx].clone();
        }
        let last_segment_pos = read_open_segments
            .inner
            .last()
            .map(|s| s.file_stream_start_pos);
        drop(read_open_segments);

        // It wasn't one of the existing open segments, so we drop the lock fo reading
        // so potentially other threads can make write their stuff, and try to switch
        // to writes.
        let mut write_open_segments = self.open_segments.write().await;

        // Check again if still needed (things might have changed between unlock & write lock)

        if write_open_segments
            .inner
            .last()
            .map(|s| s.file_stream_start_pos)
            != last_segment_pos
        {
            // Some other thread(s) must have appened new open segment(s) - we can just use it

            if let Some(idx) = find_index_for_offset(&write_open_segments.inner, log_offset) {
                return write_open_segments.inner[idx].clone();
            } else {
                panic!("Expected to see forward progress");
            }
        }

        // Receive a new pre-written segment from pre-writer
        let mut new_segment = write_open_segments
            .future_rx
            .recv()
            .await
            .expect("segment pre-writing thread must never disconnect");
        new_segment.file_stream_start_pos = log_offset;
        let new_segment = Arc::new(new_segment);
        write_open_segments.inner.push(new_segment.clone());
        // we can drop the write lock already, since the segment is already usable
        // However, we are still responsible for writing the segment file header,
        // which pre-writer couldn't do, as it didn't know the starting log offset
        drop(write_open_segments);

        self.put_entry_buffer(
            new_segment
                .write_header(log_offset, self.pop_entry_buffer().await)
                .await,
        )
        .await;

        new_segment
    }

    pub async fn mark_entry_written(self: &Arc<Node>, log_offset: LogOffset) {
        let mut write_in_flight = self.entries_in_flight.write().await;
        let was_removed = write_in_flight.unwritten.remove(&log_offset);
        debug_assert!(was_removed);
    }

    pub async fn run_segment_preloading_loop(
        self: Arc<Self>,
        start_id: u64,
        tx: Sender<OpenSegment>,
    ) {
        let mut id = start_id;
        loop {
            let segment = self
                .preload_segment_file(id)
                .await
                .expect("Could not preload next segment file");

            id += 1;

            if let Err(_e) = tx.send(segment).await {
                // on disconnect, just finish
                return;
            }
        }
    }

    async fn preload_segment_file(self: &Arc<Self>, id: u64) -> io::Result<OpenSegment> {
        let file_path =
            self.params
                .db_path
                .join(format!("{:#016}{}", id, SegmentFileMeta::FILE_SUFFIX));

        OpenSegment::create_and_fallocate(&file_path, id, self.params.base_segment_file_size).await
    }
}
