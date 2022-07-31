use binrw::{BinRead, BinWrite};
use convi::ExpectFrom;
use loglogd_api::{
    AllocationId, AppendRequestHeader, EntryHeader, EntrySize, EntryTrailer, FillRequestHeader,
    LogOffset, NodeId, ReadDataSize, ReadRequestHeader, RequestHeaderCmd, TermId,
};
use std::cmp;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Cursor};
use std::os::unix::prelude::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use tokio_uring::buf::IoBuf;
use tokio_uring::net::TcpStream;
use tracing::{debug, trace, warn};
use typed_builder::TypedBuilder;

use crate::ioutil::{
    file_write_all, tcpstream_read_fill, tcpstream_write_all, vec_extend_to_at_least,
};
use crate::segment::{
    self, EntryWrite, OpenSegment, SegmentFileHeader, SegmentFileMeta, SegmentMeta,
};
use crate::{uring_try_rec, ConnectionError, ConnectionResult, RingConnectionResult};

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
    inner: BTreeMap<LogOffset, Arc<OpenSegment>>,
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
    // TODO: split into buckets by size?
    entry_buffer_pool: Mutex<Vec<Vec<u8>>>,

    entries_in_flight: RwLock<EntriesInFlight>,

    open_segments: RwLock<OpenSegments>,

    /// Known segments, sorted by `stream_offset`
    sealed_segments: RwLock<BTreeMap<LogOffset, SegmentMeta>>,
    pub entry_writer_tx: Sender<EntryWrite>,

    fsynced_log_offset: AtomicU64,
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
            sealed_segments: RwLock::new(
                sealed_segments
                    .into_iter()
                    .map(|s| (s.content_meta.start_log_offset, s))
                    .collect(),
            ),
            open_segments: RwLock::new(OpenSegments {
                inner: BTreeMap::new(),
                future_rx: future_segments,
            }),
            entry_writer_tx,
            fsynced_log_offset: AtomicU64::new(0),
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
            let mut iter = open_segments.range(..log_offset);

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
                    return segment;
                }
                // Well.. seems like we need a new one... . Record the ending offset
                let last_segment_info =
                    read_open_segments
                        .inner
                        .last_key_value()
                        .map(|s| LastSegmentInfo {
                            start_log_offset: *s.0,
                            end_of_allocation_log_offset: LogOffset(s.0 .0 + s.1.allocated_size),
                        });
                drop(read_open_segments);
                last_segment_info
            };

            let next_segment_start_log_offset = if let Some(last_segment_info) = last_segment_info {
                // TODO: make `entries_in_flight` their own type, and make a method on that type
                // It would be a mistake to consider current `entry_log_offset` as a beginning of
                // next segment as we might have arrived here out of order. Instead - we can use
                // `unwritten` to find first
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
            let new_segment = {
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

                // Receive a new pre-written segment from pre-writer
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
                new_segment
            };

            self.put_entry_buffer(
                new_segment
                    .write_header(entry_log_offset, self.pop_entry_buffer().await)
                    .await,
            )
            .await;

            // It would be tempting to just return the `new_segment` as the segment we need. But that would be
            // a mistake - this entry might theoretically be so far ahead, that it needs even more segments opened.
            // Because of this we just let the loop run again.
        }
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

    /// Handle connection
    pub async fn handle_connection(
        self: &Arc<Node>,
        stream: &mut TcpStream,
    ) -> ConnectionResult<()> {
        // Header breakdown:
        // * 1B - cmd + basic args
        // * if Append
        //   * 3B event size
        // * if Fill:
        //   * 3B size
        //   * 10B allocation id
        // * if Read:
        //   * 8B - stream offset
        // * if Peer commands
        //   * TBD: something else, but short
        //
        // Max: 14B of constant header, so we can read constant header once
        // and move straight to action.
        // let mut header_buf = [0u8; 14];

        // TODO: add timeouts?
        loop {
            let mut buf = self.pop_entry_buffer().await;
            vec_extend_to_at_least(&mut buf, 14);

            let (res, res_buf) = tcpstream_read_fill(stream, buf.slice(0..14)).await;

            if res.is_err() {
                self.put_entry_buffer(res_buf).await;
                return res;
            } else {
                buf = res_buf;
            }

            let cursor = &mut Cursor::new(&buf[..14]);
            let cmd = match RequestHeaderCmd::read(cursor) {
                Ok(cmd) => cmd,
                Err(e) => {
                    self.put_entry_buffer(buf).await;
                    return Err(e.into());
                }
            };

            match cmd {
                RequestHeaderCmd::Peer => {
                    todo!();
                }
                RequestHeaderCmd::Append => {
                    let args = match AppendRequestHeader::read(cursor) {
                        Ok(args) => args,
                        Err(e) => {
                            self.put_entry_buffer(buf).await;
                            return Err(e.into());
                        }
                    };
                    debug!(cmd = ?cmd, args = ?args);

                    self.handle_append_request(stream, buf, args.size).await?;
                }
                RequestHeaderCmd::Fill => {
                    let args = match FillRequestHeader::read(cursor) {
                        Ok(args) => args,
                        Err(e) => {
                            self.put_entry_buffer(buf).await;
                            return Err(e.into());
                        }
                    };
                    debug!(cmd = ?cmd, args = ?args);

                    self.handle_fill_request(stream, buf, args.allocation_id, args.size)
                        .await?;
                }
                RequestHeaderCmd::Read => {
                    let args = match ReadRequestHeader::read(cursor) {
                        Ok(args) => args,
                        Err(e) => {
                            self.put_entry_buffer(buf).await;
                            return Err(e.into());
                        }
                    };
                    debug!(cmd = ?cmd, args = ?args);

                    self.handle_read_request(stream, args.offset, args.limit)
                        .await?;
                }
                RequestHeaderCmd::Other => Err(ConnectionError::Invalid)?,
            }
        }
    }

    async fn handle_append_request(
        self: &Arc<Node>,
        stream: &mut TcpStream,
        buf: Vec<u8>,
        entry_size: EntrySize,
    ) -> ConnectionResult<()> {
        let allocation_id = self.allocate_new_entry(entry_size).await;

        // TODO: send the allocation id right away, in parallel, flush it, so the client can
        // start uploading to other nodes immediately

        self.handle_fill_request(stream, buf, allocation_id, entry_size)
            .await?;

        debug!("Sending response to append request");
        let mut resp_buf = self.pop_entry_buffer().await;
        // 0-byte == 0 -> success
        let res_size = 1 + AllocationId::BYTE_SIZE;
        vec_extend_to_at_least(&mut resp_buf, res_size);

        resp_buf[1..res_size].copy_from_slice(&allocation_id.to_bytes());

        let (res, res_buf) = tcpstream_write_all(stream, resp_buf.slice(..res_size)).await;

        self.put_entry_buffer(res_buf).await;

        res
    }

    async fn handle_fill_request(
        self: &Arc<Node>,
        stream: &mut TcpStream,
        mut buf: Vec<u8>,
        allocation_id: AllocationId,
        payload_size: EntrySize,
    ) -> ConnectionResult<()> {
        async fn read_payload(
            stream: &mut TcpStream,
            mut entry_buf: Vec<u8>,
            payload_size: EntrySize,
        ) -> RingConnectionResult<()> {
            let entry_header_size = EntryHeader::BYTE_SIZE;

            debug!(size = payload_size.0, "Reading payload");
            uring_try_rec!(
                entry_buf,
                tcpstream_read_fill(
                    stream,
                    entry_buf.slice(
                        entry_header_size..(entry_header_size + usize::expect_from(payload_size.0))
                    )
                )
                .await
            );

            (Ok(()), entry_buf)
        }

        let entry_header_size = EntryHeader::BYTE_SIZE;
        let entry_trailer_size = EntryTrailer::BYTE_SIZE;
        // TODO: allocation_id.term vs node.term
        let header = EntryHeader {
            term: allocation_id.term,
            payload_size,
        };

        let total_entry_size =
            entry_header_size + usize::expect_from(payload_size.0) + entry_trailer_size;
        vec_extend_to_at_least(&mut buf, total_entry_size);

        header
            .write_to(&mut Cursor::new(&mut buf[0..entry_header_size]))
            .expect("Can't fail");

        let (read_res, res_buf) = read_payload(stream, buf, payload_size).await;
        buf = res_buf;

        let trailer = match &read_res {
            Ok(()) => EntryTrailer::valid(),
            Err(e) => {
                warn!("Failed to read payload from client: {}", e);
                EntryTrailer::invalid()
            }
        };

        trailer
            .write_to(&mut Cursor::new(
                &mut buf[total_entry_size - entry_trailer_size..total_entry_size],
            ))
            .expect("Can't fail");

        buf.truncate(total_entry_size);

        // The other side should never be disconnected, but if it is,
        // we just move on without complaining.
        let _ = self
            .entry_writer_tx
            .send(EntryWrite {
                offset: allocation_id.offset,
                entry: buf,
            })
            .await;

        read_res
    }

    async fn write_read_response_size(
        self: &Arc<Node>,
        stream: &mut TcpStream,
        response_size: u32,
    ) -> ConnectionResult<()> {
        use std::io::Write as _;
        let mut buf = self.pop_entry_buffer().await;
        vec_extend_to_at_least(&mut buf, 4);
        (&mut buf[0..4]).write_all(&response_size.to_be_bytes())?;
        let (res, res_buf) = tcpstream_write_all(stream, buf.slice(0..4)).await;
        self.put_entry_buffer(res_buf).await;

        res
    }

    /// Send as much data (under `data_to_send` limit) from the log at `log_offset` as possible
    async fn handle_read_request_send_data(
        self: &Arc<Node>,
        stream: &mut TcpStream,
        log_offset: &mut LogOffset,
        num_bytes_to_send: &mut u32,
    ) -> ConnectionResult<()> {
        // first try serving from sealed files
        while 0 < *num_bytes_to_send {
            let read_sealed_segments = self.sealed_segments.read().await;

            let (segment_start_log_offset, segment) =
                if let Some(segment) = read_sealed_segments.range(..=*log_offset).next_back() {
                    (segment.0.clone(), segment.1.clone())
                } else {
                    // if we couldn't find any segments below `log_offset`, that must
                    // mean there are no sealed segments at all, otherwise there wouldn't
                    // be any commited bytes to send and we wouldn't be here
                    debug_assert!(read_sealed_segments.is_empty());
                    break;
                };

            if segment.content_meta.end_log_offset <= *log_offset {
                // Offset after last sealed segments. `log_offset` must be in an opened segment then.
                // Break into the open segment loop search.
                break;
            }
            // TODO(perf): allocates
            let path = segment.file_meta.path.clone();
            drop(read_sealed_segments);

            debug_assert!(segment_start_log_offset <= *log_offset);
            let bytes_available_in_segment = segment.content_meta.end_log_offset.0 - log_offset.0;
            let mut file_offset = log_offset.0 - segment.content_meta.start_log_offset.0
                + SegmentFileHeader::BYTE_SIZE_U64;

            debug_assert!(0 < bytes_available_in_segment);

            // TODO(perf): should we cache these somewhere in some LRU or something?
            let file = tokio_uring::fs::File::open(path).await?;

            let mut bytes_to_send =
                cmp::min(bytes_available_in_segment, u64::from(*num_bytes_to_send));
            let stream_fd = stream.as_raw_fd();
            let file_fd = file.as_raw_fd();
            let bytes_written_total = tokio::task::spawn_blocking(move || -> io::Result<u64> {
                let mut bytes_written_total = 0u64;
                while 0 < bytes_to_send {
                    trace!(
                        bytes_to_send,
                        file_offset,
                        segment_id = segment.file_meta.id,
                        "sending sealed segment data"
                    );
                    let mut file_offset_mut: i64 = i64::expect_from(file_offset);
                    // TODO: fallback to `send`?
                    let bytes_sent = nix::sys::sendfile::sendfile64(
                        stream_fd,
                        file_fd,
                        Some(&mut file_offset_mut),
                        usize::expect_from(bytes_to_send),
                    )
                    .map_err(io::Error::from)?;

                    let bytes_sent = u64::expect_from(bytes_sent);

                    bytes_to_send -= bytes_sent;
                    file_offset += u64::expect_from(bytes_sent);
                    bytes_written_total += bytes_sent;
                }
                Ok(u64::expect_from(bytes_written_total))
            })
            .await??;
            *num_bytes_to_send -= u32::expect_from(bytes_written_total);
            log_offset.0 += u64::from(bytes_written_total);
        }

        // if more data is still needed, it's probably in the still opened buffers
        while 0 < *num_bytes_to_send {
            let read_open_segments = self.open_segments.read().await;

            let (segment_start_log_offset, segment) =
                if let Some(segment) = read_open_segments.inner.range(..=*log_offset).next_back() {
                    (segment.0.clone(), segment.1.clone())
                } else {
                    // This means we couldn't find a matching open segment. This
                    // must be because we missed a segment that was moved between
                    // opened and sealed group.
                    break;
                };

            // Since segments are closed in order, from lowest offset upward,
            // if we were able to find an open segment starting just before the
            // requested offset, the data must be in this segment. The question
            // is only how much of it can we serve, before switching
            // to next segment.

            let segment_end_log_offset = if let Some(segment) = read_open_segments
                .inner
                .range((*log_offset + LogOffset(1))..)
                .next()
            {
                *segment.0
            } else {
                // No open segments after current one (at least yet).
                // Just use request data as the end pointer
                LogOffset(log_offset.0 + u64::from(*num_bytes_to_send))
            };

            drop(read_open_segments);

            debug_assert!(segment_start_log_offset <= *log_offset);
            let bytes_available_in_segment = segment_end_log_offset.0 - log_offset.0;
            let mut file_offset =
                log_offset.0 - segment_start_log_offset.0 + SegmentFileHeader::BYTE_SIZE_U64;

            debug_assert!(0 < bytes_available_in_segment);

            let mut bytes_to_send =
                cmp::min(bytes_available_in_segment, u64::from(*num_bytes_to_send));
            let stream_fd = stream.as_raw_fd();
            let file_fd = segment.file.as_raw_fd();
            let segment_id = segment.id;
            // TODO: move to a function, dedup with the block above
            let bytes_written_total = tokio::task::spawn_blocking(move || -> io::Result<u64> {
                let mut bytes_written_total = 0u64;
                while 0 < bytes_to_send {
                    trace!(
                        bytes_to_send,
                        file_offset,
                        segment_id,
                        "sending open segment data"
                    );
                    let mut file_offset_mut: i64 = i64::expect_from(file_offset);
                    // TODO: fallback to `send`?
                    let bytes_sent = nix::sys::sendfile::sendfile64(
                        stream_fd,
                        file_fd,
                        Some(&mut file_offset_mut),
                        usize::expect_from(bytes_to_send),
                    )
                    .map_err(io::Error::from)?;

                    let bytes_sent = u64::expect_from(bytes_sent);

                    bytes_to_send -= bytes_sent;
                    file_offset += u64::expect_from(bytes_sent);
                    bytes_written_total += bytes_sent;
                }
                Ok(u64::expect_from(bytes_written_total))
            })
            .await??;
            *num_bytes_to_send -= u32::expect_from(bytes_written_total);
            log_offset.0 += u64::from(bytes_written_total);
        }
        Ok(())
    }

    async fn handle_read_request(
        self: &Arc<Node>,
        stream: &mut TcpStream,
        mut log_offset: LogOffset,
        limit: ReadDataSize,
    ) -> ConnectionResult<()> {
        // TODO: change this to `commited_log_offset` when Raft is implemented
        let last_fsynced_log_offset = LogOffset(self.fsynced_log_offset.load(SeqCst));

        let commited_data_available = if log_offset < last_fsynced_log_offset {
            last_fsynced_log_offset.0 - log_offset.0
        } else {
            0
        };

        let mut num_bytes_to_send =
            u32::expect_from(cmp::min(u64::from(limit.0), commited_data_available));

        trace!(
            %last_fsynced_log_offset,
            commited_data_available,
            %log_offset,
            limit = limit.0,
            num_bytes_to_send,
            "read request"
        );
        self.write_read_response_size(stream, num_bytes_to_send)
            .await?;

        // Because we have two sources of data (sealed and opened segments), and
        // no good (desireable) way to lock both of the them at the same time,
        // we possibly can miss some data between when the segment is closed
        // and added to sealed segments. But that's not a big issue, we can
        // loop and try again.
        while 0 < num_bytes_to_send {
            self.handle_read_request_send_data(stream, &mut log_offset, &mut num_bytes_to_send)
                .await?;
            trace!(
                %last_fsynced_log_offset,
                commited_data_available,
                %log_offset,
                limit = limit.0,
                num_bytes_to_send,
                "read request progress"
            );
        }

        Ok(())
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

    pub async fn run_fsync_loop(self: Arc<Self>) {
        let mut last_fsync = Instant::now();
        loop {
            let now = Instant::now();
            let till_next_fsync =
                Duration::from_millis(300).saturating_sub(now.duration_since(last_fsync));

            sleep(till_next_fsync).await;
            last_fsync = Instant::now();

            let first_unwritten_log_offset = self.get_first_unwritten_log_offset().await;

            if self.fsynced_log_offset.load(SeqCst) == first_unwritten_log_offset.0 {
                continue;
            }

            'inner: loop {
                let read_open_segments = self.open_segments.read().await;

                let mut segments_iter = read_open_segments.inner.iter();
                let first_segment = segments_iter.next().clone();
                let second_segment_start = segments_iter.next().map(|s| s.0).cloned();
                drop(segments_iter);

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
                                *first_segment_start_log_offset,
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
                                        start_log_offset: *first_segment_start_log_offset,
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
                                .remove(first_segment_start_log_offset);
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
                .store(first_unwritten_log_offset.0, SeqCst);
        }
    }
}
