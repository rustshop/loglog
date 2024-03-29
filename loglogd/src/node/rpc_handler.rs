use binrw::{BinRead, BinWrite};
use convi::ExpectFrom;
use loglogd_api::{
    AllocationId, ConnectionHello, EntryHeader, EntrySize, EntryTrailer, GetEndResponse, LogOffset,
    ReadDataSize, Request, LOGLOGD_VERSION_0,
};
use std::{
    cmp,
    io::{self, Cursor},
    mem,
    net::SocketAddr,
    os::fd::{AsRawFd, RawFd},
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::{sleep, timeout},
};
use tracing::{debug, error, info, trace};

use crate::{
    ioutil::vec_extend_to_at_least,
    segment::{EntryWrite, SegmentFileHeader},
    task::AutoJoinHandle,
};

use super::NodeShared;

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("invalid data: {0}")]
    ParseError(#[from] binrw::Error),
    #[error("io: {0}")]
    IO(#[from] io::Error),
    #[error("join: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

pub type ConnectionResult<T> = std::result::Result<T, ConnectionError>;

pub struct RpcHandler {
    local_addr: SocketAddr,
    #[allow(unused)]
    join_handle: AutoJoinHandle,
}

impl RpcHandler {
    pub fn new(
        shared: Arc<NodeShared>,
        listen_addr: SocketAddr,
        entry_writer_tx: flume::Sender<EntryWrite>,
        last_fsynced_log_offset_rx: watch::Receiver<LogOffset>,
    ) -> anyhow::Result<Self> {
        let inner = Arc::new(RpcHandlerInner {
            shared,
            entry_writer_tx,
            last_fsynced_log_offset_rx,
        });

        let rt = tokio::runtime::Runtime::new()?;

        let (tx, rx) = flume::bounded(1);

        let join_handle = AutoJoinHandle::spawn_res(move || -> Result<(), io::Error> {
            let _guard = scopeguard::guard((), |_| {
                info!("RequestHandler is done");
            });
            let res: Result<(), io::Error> = rt.block_on(async {
                let listener = tokio::net::TcpListener::bind(listen_addr).await?;

                tx.send(listener.local_addr()?)
                    .expect("local_addr rx not there?");

                inner.handle_requests(listener).await;

                Ok(())
            });

            res?;

            info!("Waiting for RequestHandler to complete all connections...");
            rt.shutdown_timeout(Duration::from_secs(60));

            Ok(())
        });

        let local_addr = rx.recv()?;

        Ok(Self {
            join_handle,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

pub struct RpcHandlerInner {
    shared: Arc<NodeShared>,
    pub last_fsynced_log_offset_rx: watch::Receiver<LogOffset>,
    entry_writer_tx: flume::Sender<EntryWrite>,
}

impl RpcHandlerInner {
    async fn handle_requests(self: &Arc<Self>, listener: TcpListener) {
        while !self.shared.is_node_shutting_down.load(Ordering::Relaxed) {
            let (mut stream, peer_addr) =
                // bound by a timeout, so we can exit after `is_stopped` is set in a reasonable time
                match timeout(Duration::from_millis(500), listener.accept()).await {
                    Ok(Ok(o)) => o,
                    Ok(Err(e)) => {
                        error!(%e, "request handling listener accept error");
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    Err(_) => {
                        // just a timeout
                        continue;
                    }
                };

            info!(%peer_addr, "New peer connection");

            let self_copy = self.clone();
            tokio::spawn({
                async move {
                    let guard = self_copy.shared.panic_guard("peer-outgoing");
                    let mut buf = self_copy.shared.pop_entry_buffer();
                    if let Err(e) = self_copy.handle_connection(&mut stream, &mut buf).await {
                        info!("Connection error: {}", e);
                    }
                    self_copy.shared.put_entry_buffer(buf);
                    guard.done();
                }
            });
        }
    }

    pub async fn handle_connection(
        &self,
        stream: &mut TcpStream,
        buf: &mut Vec<u8>,
    ) -> ConnectionResult<()> {
        // We always prepare exact buffers to be sent immediately
        stream.set_nodelay(true)?;
        self.handle_connection_init(stream, buf).await?;
        self.handle_connection_loop(stream, buf).await?;
        Ok(())
    }

    /// Handle connection
    pub async fn handle_connection_init(
        &self,
        stream: &mut TcpStream,
        buf: &mut Vec<u8>,
    ) -> ConnectionResult<()> {
        let hello = ConnectionHello {
            version: LOGLOGD_VERSION_0,
        };
        vec_extend_to_at_least(buf, ConnectionHello::BYTE_SIZE);

        hello.write(&mut binrw::io::NoSeek::new(
            &mut buf[..ConnectionHello::BYTE_SIZE],
        ))?;
        stream.write_all(buf).await?;
        Ok(())
    }

    /// Handle connection
    pub async fn handle_connection_loop(
        &self,
        stream: &mut TcpStream,
        buf: &mut Vec<u8>,
    ) -> ConnectionResult<()> {
        // TODO: add timeouts for idle connections?
        while !self.shared.is_node_shutting_down.load(Ordering::SeqCst) {
            self.handle_connection_loop_inner(stream, buf).await?;
        }

        Ok(())
    }

    async fn handle_connection_loop_inner(
        &self,
        stream: &mut TcpStream,
        buf: &mut Vec<u8>,
    ) -> Result<(), ConnectionError> {
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

        debug_assert_eq!(Request::BYTE_SIZE, 14);
        vec_extend_to_at_least(buf, Request::BYTE_SIZE);
        stream.read_exact(&mut buf[..Request::BYTE_SIZE]).await?;
        let cmd = Request::read(&mut Cursor::new(&buf))?;
        match cmd {
            cmd @ (Request::Append(args) | Request::AppendWait(args)) => {
                debug!(cmd = ?cmd, args = ?args);

                self.handle_append_request(
                    stream,
                    buf,
                    args.size,
                    matches!(cmd, Request::AppendWait(_)),
                )
                .await?;
            }
            cmd @ (Request::Read(args) | Request::ReadWait(args)) => {
                debug!(cmd = ?cmd, args = ?args);

                self.handle_read_request(
                    stream,
                    args.offset,
                    args.limit,
                    matches!(cmd, Request::ReadWait(_)),
                )
                .await?;
            }
            Request::GetEnd => {
                debug!(cmd = ?cmd);

                self.handle_get_end_request(stream, buf).await?;
            }
        }
        Ok(())
    }

    async fn handle_append_request(
        &self,
        stream: &mut TcpStream,
        buf: &mut Vec<u8>,
        entry_size: EntrySize,
        wait: bool,
    ) -> ConnectionResult<()> {
        let allocation_id = self.handle_fill_request(stream, buf, entry_size).await?;

        debug!("Sending response to append request");
        let res_size = AllocationId::BYTE_SIZE + 1;
        vec_extend_to_at_least(buf, res_size);

        buf[0..AllocationId::BYTE_SIZE].copy_from_slice(&allocation_id.to_bytes());

        if wait {
            stream.write_all(&buf[..AllocationId::BYTE_SIZE]).await?;
            let mut last_fsynced_log_offset_rx = self.last_fsynced_log_offset_rx.clone();

            loop {
                let last_fsynced_log_offset = *last_fsynced_log_offset_rx.borrow_and_update();

                if last_fsynced_log_offset <= allocation_id.offset {
                    if last_fsynced_log_offset_rx.changed().await.is_err() {
                        return Ok(());
                    }
                } else {
                    buf[AllocationId::BYTE_SIZE] = 0xff;
                    stream
                        .write_all(&buf[AllocationId::BYTE_SIZE..res_size])
                        .await?;
                    break;
                }
            }
        } else {
            buf[AllocationId::BYTE_SIZE] = 0xff;
            stream.write_all(&buf[..res_size]).await?;
        }

        Ok(())
    }

    async fn handle_fill_request(
        &self,
        stream: &mut (impl AsyncRead + Unpin),
        buf: &mut Vec<u8>,
        payload_size: EntrySize,
    ) -> ConnectionResult<AllocationId> {
        async fn read_payload(
            mut stream: impl AsyncRead + Unpin,
            entry_buf: &mut [u8],
            payload_size: EntrySize,
        ) -> ConnectionResult<()> {
            let entry_header_size = EntryHeader::BYTE_SIZE;

            debug!(size = payload_size.0, "Reading payload");
            stream
                .read_exact(
                    &mut entry_buf[entry_header_size
                        ..(entry_header_size + usize::expect_from(payload_size.0))],
                )
                .await?;

            Ok(())
        }

        let entry_header_size = EntryHeader::BYTE_SIZE;
        let entry_trailer_size = EntryTrailer::BYTE_SIZE;

        let total_entry_size =
            entry_header_size + usize::expect_from(payload_size.0) + entry_trailer_size;
        vec_extend_to_at_least(buf, total_entry_size);

        read_payload(stream, buf, payload_size).await?;

        EntryTrailer::valid()
            .write(&mut Cursor::new(
                &mut buf[total_entry_size - entry_trailer_size..total_entry_size],
            ))
            .expect("Can't fail");

        buf.truncate(total_entry_size);

        let allocation_id = self.shared.allocate_new_entry(payload_size);

        // TODO: allocation_id.term vs node.term
        let header = EntryHeader {
            term: allocation_id.term,
            payload_size,
        };

        header
            .write(&mut Cursor::new(&mut buf[0..entry_header_size]))
            .expect("Can't fail");
        // The other side should never be disconnected, but if it is,
        // we just move on without complaining.
        let _ = self
            .entry_writer_tx
            .send_async(EntryWrite {
                offset: allocation_id.offset,
                entry: mem::replace(buf, self.shared.pop_entry_buffer()),
            })
            .await;

        Ok(allocation_id)
    }

    async fn write_read_response_size(
        &self,
        stream: &mut TcpStream,
        response_size: u32,
    ) -> ConnectionResult<()> {
        use std::io::Write as _;
        let mut buf = self.shared.pop_entry_buffer();
        vec_extend_to_at_least(&mut buf, 4);
        (&mut buf[0..4]).write_all(&response_size.to_be_bytes())?;
        stream.write_all(&buf[0..4]).await?;
        self.shared.put_entry_buffer(buf);

        Ok(())
    }

    // /// Send as much data (under `data_to_send` limit) from the log at `log_offset` as possible
    // async fn handle_read_request_send_data(
    //     &self,
    //     stream: &mut TcpStream,
    //     log_offset: &mut LogOffset,
    //     num_bytes_to_send: &mut u32,
    // ) -> ConnectionResult<()> {
    //     // first try serving from sealed files
    //     while 0 < *num_bytes_to_send {
    //         let segment = {
    //             let sealed_segments = &self.shared.sealed_segments.read().expect("Locking failed");
    //             let Some(segment) = sealed_segments.get_containing_offset(*log_offset) else {
    //                     // if we couldn't find any segments below `log_offset`, that must
    //                     // mean there are no sealed segments at all, otherwise there wouldn't
    //                     // be any commited bytes to send and we wouldn't be here
    //                     debug_assert!(sealed_segments.is_empty());
    //                     break;
    //                 };
    //             segment.clone()
    //         };

    //         if segment.content_meta.end_log_offset <= *log_offset {
    //             // Offset after last sealed segments. `log_offset` must be in an opened segment then.
    //             // Break into the open segment loop search.
    //             break;
    //         }

    //         debug_assert!(segment.content_meta.start_log_offset <= *log_offset);
    //         let bytes_available_in_segment = segment.content_meta.end_log_offset - *log_offset;
    //         let file_offset = *log_offset - segment.content_meta.start_log_offset
    //             + SegmentFileHeader::BYTE_SIZE_U64;

    //         debug_assert!(0 < bytes_available_in_segment);

    //         let bytes_to_send = cmp::min(bytes_available_in_segment, u64::from(*num_bytes_to_send));
    //         trace!(
    //             bytes_to_send,
    //             file_offset,
    //             segment_id = %segment.file_meta.id,
    //             "sending sealed segment data"
    //         );

    //         let stream_fd = stream.as_raw_fd();
    //         let bytes_written = tokio::task::spawn_blocking(move || -> io::Result<u64> {
    //             // TODO(perf): we should cache FDs to open sealed files somewhere in some LRU
    //             let file = std::fs::File::open(segment.file_meta.path_ref())?;
    //             let file_fd = file.as_raw_fd();
    //             send_file_to_stream(file_fd, file_offset, stream_fd, bytes_to_send)
    //         })
    //         .await??;

    //         *num_bytes_to_send -= u32::expect_from(bytes_written);
    //         *log_offset += bytes_written;
    //     }

    //     // if more data is still needed, it's probably in the still opened buffers
    //     while 0 < *num_bytes_to_send {
    //         let (segment, segment_end_log_offset) = {
    //             let read_open_segments = self.shared.open_segments.read().expect("Locking failed");

    //             let Some(segment) = read_open_segments.get_containing_offset(*log_offset).cloned() else {
    //                 // This means we couldn't find a matching open segment. This
    //                 // must be because we missed a segment that was moved between
    //                 // opened and sealed group.
    //                 break;
    //             };

    //             // Since segments are closed in order, from lowest offset upward,
    //             // if we were able to find an open segment starting just before the
    //             // requested offset, the whole data must be in this segment. The question
    //             // is only how much of it can we serve, before switching
    //             // to next segment.

    //             let segment_end_log_offset = if let Some(segment) =
    //                 read_open_segments.get_after_containing_offset(*log_offset)
    //             {
    //                 segment.start_log_offset
    //             } else {
    //                 // No open segments after current one (at least yet).
    //                 // Just use request data as the end pointer
    //                 *log_offset + u64::from(*num_bytes_to_send)
    //             };

    //             (segment, segment_end_log_offset)
    //         };

    //         debug_assert!(segment.start_log_offset <= *log_offset);
    //         let bytes_available_in_segment = segment_end_log_offset - *log_offset;
    //         let file_offset =
    //             *log_offset - segment.start_log_offset + SegmentFileHeader::BYTE_SIZE_U64;

    //         debug_assert!(0 < bytes_available_in_segment);

    //         let bytes_to_send = cmp::min(bytes_available_in_segment, u64::from(*num_bytes_to_send));

    //         let stream_fd = stream.as_raw_fd();
    //         let file_fd = segment.fd.as_raw_fd();

    //         let bytes_written = tokio::task::spawn_blocking(move || -> io::Result<u64> {
    //             send_file_to_stream(file_fd, file_offset, stream_fd, bytes_to_send)
    //         })
    //         .await??;

    //         *num_bytes_to_send -= u32::expect_from(bytes_written);
    //         *log_offset += bytes_written;
    //     }
    //     Ok(())
    // }

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

            let commited_data_available = last_fsynced_log_offset.saturating_sub(log_offset);

            let num_bytes_to_send = cmp::min(u64::from(limit.0), commited_data_available);

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

        self.write_read_response_size(stream, u32::expect_from(num_bytes_to_send))
            .await?;

        // Because we have two sources of data (sealed and opened segments), and
        // no good (desireable) way to lock both of the them at the same time,
        // we possibly can miss some data between when the segment is closed
        // and added to sealed segments. But that's not a big issue, we can
        // loop and try again.
        // NOTE: The above is actually incorrect, as segment sealer appends
        // to sealed segments list first, only then removes from open segments list.
        while 0 < num_bytes_to_send {
            let log_data = self.shared.find_log_data(log_offset, num_bytes_to_send);

            trace!(
                log_data = ?log_data,
                "sending log data"
            );
            let stream_fd = stream.as_raw_fd();
            let bytes_written = tokio::task::spawn_blocking(move || -> io::Result<u64> {
                log_data.write_to_fd(stream_fd)
            })
            .await??;
            debug_assert!(bytes_written <= num_bytes_to_send);
            log_offset += bytes_written;
            num_bytes_to_send -= bytes_written;

            trace!(
                %log_offset,
                limit = limit.0,
                num_bytes_to_send,
                "read request progress"
            );
        }

        Ok(())
    }

    async fn handle_get_end_request(
        &self,
        stream: &mut TcpStream,
        #[allow(clippy::ptr_arg)] buf: &mut Vec<u8>,
    ) -> ConnectionResult<()> {
        let resp = GetEndResponse {
            offset: self.shared.fsynced_log_offset(),
        };

        resp.write(&mut binrw::io::NoSeek::new(
            &mut buf[..GetEndResponse::BYTE_SIZE],
        ))?;
        stream.write_all(&buf[..GetEndResponse::BYTE_SIZE]).await?;

        Ok(())
    }
}
