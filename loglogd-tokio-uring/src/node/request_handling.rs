use super::NodeShared;
use crate::{
    ioutil::{tcpstream_read_fill, tcpstream_write_all, vec_extend_to_at_least},
    segment::{EntryWrite, SegmentFileHeader},
    uring_try_rec, ConnectionError, ConnectionResult, RingConnectionResult,
};
use binrw::{BinRead, BinWrite};
use convi::ExpectFrom;
use loglogd_api::{
    AllocationId, AppendRequestHeader, ConnectionHello, EntryHeader, EntrySize, EntryTrailer,
    FillRequestHeader, GetEndResponse, LogOffset, ReadDataSize, ReadRequestHeader,
    RequestHeaderCmd, LOGLOGD_VERSION_0,
};
use std::{
    cmp,
    io::{self, Cursor},
    os::unix::prelude::AsRawFd,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    join,
    sync::{mpsc, watch},
    time::{sleep, timeout},
};
use tokio_uring::{
    buf::IoBuf,
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error as log_err, info, trace, warn};

#[derive(Debug, Clone)]
pub struct RequestHandler {
    pub shared: Arc<NodeShared>,
    pub last_fsynced_log_offset_rx: watch::Receiver<LogOffset>,
    pub is_node_shutting_down: Arc<AtomicBool>,
}

impl RequestHandler {
    pub async fn run_request_handling_loop(
        self,
        listener: TcpListener,

        entry_writer_tx: mpsc::Sender<EntryWrite>,
    ) -> TcpListener {
        let _guard = scopeguard::guard((), |_| {
            info!("request handling loop is done");
        });

        let entry_writer_tx = Arc::new(entry_writer_tx);

        while !self.is_node_shutting_down.load(Ordering::Relaxed) {
            let (mut stream, _peer_addr) =
                // bound by a timeout, so we can exit after `is_stopped` is set in a reasonable time
                match timeout(Duration::from_millis(500), listener.accept()).await {
                    Ok(Ok(o)) => o,
                    Ok(Err(e)) => {
                        log_err!(%e, "request handling listener accept error");
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    Err(_) => {
                        // just a timeout
                        continue;
                    }
                };

            tokio_uring::spawn({
                let node = self.clone();
                let entry_writer_tx = Arc::clone(&entry_writer_tx);
                async move {
                    if let Err(e) = node.handle_connection(&mut stream, &entry_writer_tx).await {
                        info!("Connection error: {}", e);
                    }
                }
            });
        }

        listener
    }

    pub async fn handle_connection(
        &self,
        stream: &mut TcpStream,
        entry_writer_tx: &mpsc::Sender<EntryWrite>,
    ) -> ConnectionResult<()> {
        self.handle_connection_init(stream).await?;
        self.handle_connection_loop(stream, entry_writer_tx).await?;
        Ok(())
    }

    /// Handle connection
    pub async fn handle_connection_init(&self, stream: &mut TcpStream) -> ConnectionResult<()> {
        let hello = ConnectionHello {
            version: LOGLOGD_VERSION_0,
            // current: self.shared.fsynced_log_offset(),
        };

        let mut buf = self.shared.pop_entry_buffer().await;
        hello.write(&mut binrw::io::NoSeek::new(&mut buf))?;
        let (res, res_buf) = tcpstream_write_all(stream, buf.slice(..)).await;
        self.shared.put_entry_buffer(res_buf).await;
        res
    }

    /// Handle connection
    pub async fn handle_connection_loop(
        &self,
        stream: &mut TcpStream,
        entry_writer_tx: &mpsc::Sender<EntryWrite>,
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
        while !self.is_node_shutting_down.load(Ordering::SeqCst) {
            let mut buf = self.shared.pop_entry_buffer().await;
            vec_extend_to_at_least(&mut buf, 14);

            let (res, res_buf) = tcpstream_read_fill(stream, buf.slice(0..14)).await;

            if res.is_err() {
                self.shared.put_entry_buffer(res_buf).await;
                return res;
            } else {
                buf = res_buf;
            }

            let cursor = &mut Cursor::new(&buf[..14]);
            let cmd = match RequestHeaderCmd::read(cursor) {
                Ok(cmd) => cmd,
                Err(e) => {
                    self.shared.put_entry_buffer(buf).await;
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
                            self.shared.put_entry_buffer(buf).await;
                            return Err(e.into());
                        }
                    };
                    debug!(cmd = ?cmd, args = ?args);

                    self.handle_append_request(stream, entry_writer_tx, buf, args.size)
                        .await?;
                }
                RequestHeaderCmd::AppendWait => {
                    unimplemented!();
                }
                RequestHeaderCmd::Fill => {
                    let args = match FillRequestHeader::read(cursor) {
                        Ok(args) => args,
                        Err(e) => {
                            self.shared.put_entry_buffer(buf).await;
                            return Err(e.into());
                        }
                    };
                    debug!(cmd = ?cmd, args = ?args);

                    self.handle_fill_request(
                        stream,
                        entry_writer_tx,
                        buf,
                        args.allocation_id,
                        args.size,
                    )
                    .await?;
                }
                cmd @ (RequestHeaderCmd::Read | RequestHeaderCmd::ReadWait) => {
                    let args = match ReadRequestHeader::read(cursor) {
                        Ok(args) => args,
                        Err(e) => {
                            self.shared.put_entry_buffer(buf).await;
                            return Err(e.into());
                        }
                    };
                    debug!(cmd = ?cmd, args = ?args);

                    self.handle_read_request(
                        stream,
                        args.offset,
                        args.limit,
                        cmd == RequestHeaderCmd::ReadWait,
                    )
                    .await?;
                }
                RequestHeaderCmd::GetEnd => {
                    debug!(cmd = ?cmd);

                    self.handle_get_end_request(stream).await?;
                }
                RequestHeaderCmd::Other => Err(ConnectionError::Invalid)?,
            }
        }

        Ok(())
    }

    async fn handle_append_request(
        &self,
        stream: &TcpStream,
        entry_writer_tx: &mpsc::Sender<EntryWrite>,
        buf: Vec<u8>,
        entry_size: EntrySize,
    ) -> ConnectionResult<()> {
        let allocation_id = self.shared.allocate_new_entry(entry_size).await;

        let mut resp_buf = self.shared.pop_entry_buffer().await;

        let (fill_res, (send_res, res_buf)) = join!(
            self.handle_fill_request(stream, entry_writer_tx, buf, allocation_id, entry_size),
            async {
                debug!("Sending response to append request");
                let res_size = AllocationId::BYTE_SIZE;
                vec_extend_to_at_least(&mut resp_buf, res_size);

                resp_buf[0..res_size].copy_from_slice(&allocation_id.to_bytes());

                tcpstream_write_all(stream, resp_buf.slice(..res_size)).await
            },
        );

        self.shared.put_entry_buffer(res_buf).await;

        fill_res?;

        send_res
    }

    async fn handle_fill_request(
        &self,
        stream: &TcpStream,
        entry_writer_tx: &mpsc::Sender<EntryWrite>,
        mut buf: Vec<u8>,
        allocation_id: AllocationId,
        payload_size: EntrySize,
    ) -> ConnectionResult<()> {
        async fn read_payload(
            stream: &TcpStream,
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
            .write(&mut Cursor::new(&mut buf[0..entry_header_size]))
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
            .write(&mut Cursor::new(
                &mut buf[total_entry_size - entry_trailer_size..total_entry_size],
            ))
            .expect("Can't fail");

        buf.truncate(total_entry_size);

        // The other side should never be disconnected, but if it is,
        // we just move on without complaining.
        let _ = entry_writer_tx
            .send(EntryWrite {
                offset: allocation_id.offset,
                entry: buf,
            })
            .await;

        read_res
    }

    async fn write_read_response_size(
        &self,
        stream: &mut TcpStream,
        response_size: u32,
    ) -> ConnectionResult<()> {
        use std::io::Write as _;
        let mut buf = self.shared.pop_entry_buffer().await;
        vec_extend_to_at_least(&mut buf, 4);
        (&mut buf[0..4]).write_all(&response_size.to_be_bytes())?;
        let (res, res_buf) = tcpstream_write_all(stream, buf.slice(0..4)).await;
        self.shared.put_entry_buffer(res_buf).await;

        res
    }

    /// Send as much data (under `data_to_send` limit) from the log at `log_offset` as possible
    async fn handle_read_request_send_data(
        &self,
        stream: &mut TcpStream,
        log_offset: &mut LogOffset,
        num_bytes_to_send: &mut u32,
    ) -> ConnectionResult<()> {
        // first try serving from sealed files
        while 0 < *num_bytes_to_send {
            let read_sealed_segments = self.shared.sealed_segments.read().await;

            let (segment_start_log_offset, segment) =
                if let Some(segment) = read_sealed_segments.range(..=*log_offset).next_back() {
                    (*segment.0, segment.1.clone())
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
            let path = segment.file_meta.path();
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
                        segment_id = %segment.file_meta.id,
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
            log_offset.0 += bytes_written_total;
        }

        // if more data is still needed, it's probably in the still opened buffers
        while 0 < *num_bytes_to_send {
            let read_open_segments = self.shared.open_segments.read().await;

            let (segment_start_log_offset, segment) =
                if let Some(segment) = read_open_segments.inner.range(..=*log_offset).next_back() {
                    (*segment.0, segment.1.clone())
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

            let segment_end_log_offset =
                if let Some(segment) = read_open_segments.inner.range((*log_offset + 1)..).next() {
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
                        %segment_id,
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
            log_offset.0 += bytes_written_total;
        }
        Ok(())
    }

    async fn handle_read_request(
        &self,
        stream: &mut TcpStream,
        mut log_offset: LogOffset,
        limit: ReadDataSize,
        wait: bool,
    ) -> ConnectionResult<()> {
        // TODO: change this to `commited_log_offset` when Raft is implemented
        let mut last_fsynced_log_offset_rx = self.last_fsynced_log_offset_rx.clone();

        let mut num_bytes_to_send = loop {
            let last_fsynced_log_offset = *last_fsynced_log_offset_rx.borrow_and_update();

            let commited_data_available = if log_offset < last_fsynced_log_offset {
                last_fsynced_log_offset.0 - log_offset.0
            } else {
                0
            };

            let num_bytes_to_send =
                u32::expect_from(cmp::min(u64::from(limit.0), commited_data_available));

            trace!(
                %last_fsynced_log_offset,
                commited_data_available,
                %log_offset,
                limit = limit.0,
                num_bytes_to_send,
                "read request"
            );

            if num_bytes_to_send == 0 {
                if !wait {
                    break num_bytes_to_send;
                }
                if last_fsynced_log_offset_rx.changed().await.is_err() {
                    break num_bytes_to_send;
                }
            } else {
                break num_bytes_to_send;
            }
        };

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
                %log_offset,
                limit = limit.0,
                num_bytes_to_send,
                "read request progress"
            );
        }

        Ok(())
    }

    async fn handle_get_end_request(&self, stream: &mut TcpStream) -> ConnectionResult<()> {
        let resp = GetEndResponse {
            offset: self.shared.fsynced_log_offset(),
        };

        let mut buf = self.shared.pop_entry_buffer().await;
        resp.write(&mut binrw::io::NoSeek::new(&mut buf))?;

        let (res, res_buf) = tcpstream_write_all(stream, buf.slice(..)).await;
        self.shared.put_entry_buffer(res_buf).await;
        res
    }
}
