use binrw::{BinRead, BinWrite, ReadOptions, WriteOptions};
use derive_more::Sub;
use ioutil::{tcpstream_write_all, vec_extend_to_at_least};
use node::{Parameters, TermId};
use num_enum::FromPrimitive;
use opts::Opts;
use std::error::Error;
use std::io;
use std::io::{Cursor, Read, Seek};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::channel;
use tokio_uring::buf::IoBuf;
use tokio_uring::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::ioutil::tcpstream_read_fill;
use crate::node::Node;
use crate::segment::{EntryHeader, EntryTrailer, EntryWrite, LogStore};

mod ioutil;
mod node;
mod opts;
mod segment;

/// A `?`-like macro to call functions that return values in `(res, buf)` uring-convention
macro_rules! uring_try_rec {
    ($buf:ident, $e:expr) => {{
        let (res, res_buf) = $e;
        $buf = res_buf;

        match res {
            Err(e) => {
                return (Err(e.into()), $buf);
            }
            Ok(o) => o,
        }
    }};
}

/// A `?`-like macro to call functions that return a normal `Result`, when called from `(res, buf)`-returning uring-convention
// macro_rules! uring_try {
//     ($buf:ident, $e:expr) => {{
//         let res = $e;

//         match res {
//             Err(e) => {
//                 return (Err(e.into()), $buf);
//             }
//             Ok(o) => o,
//         }
//     }};
// }

/// Logical offset in an the binary log stream
///
/// Clients use this offset directly to traverse the log and
/// request new parts from the server.
///
/// Notably LogLog for performance reasons includes each entry's
/// header and trailer the log, but segment file header is not included.
#[derive(Copy, Clone, Debug, BinRead, BinWrite, PartialEq, Eq, PartialOrd, Ord, Sub)]
#[br(big)]
#[bw(big)]
pub struct LogOffset(u64);

/// External ID of the allocated event buffer
#[derive(Copy, Clone, BinRead, Debug, PartialEq, Eq)]
pub struct AllocationId {
    pub term: TermId,
    pub offset: LogOffset,
}

impl AllocationId {
    const BYTE_SIZE: usize = 10;

    /// Convert to bytes representation
    pub fn to_bytes(&self) -> [u8; Self::BYTE_SIZE] {
        let mut buf = [0; Self::BYTE_SIZE];

        buf[0..2].copy_from_slice(&self.term.0.to_be_bytes());
        buf[2..].copy_from_slice(&self.offset.0.to_be_bytes());

        buf
    }
}

#[test]
fn allocation_id_serde() {
    let v = AllocationId {
        term: TermId(0x0123),
        offset: LogOffset(0x456789abcdef0011),
    };

    assert_eq!(
        v,
        AllocationId::read(&mut Cursor::new(v.to_bytes())).unwrap()
    );
}

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
        .init();

    let opts = Opts::from_args();

    let params = Parameters::builder().db_path(opts.db_path.clone()).build();

    info!(
        listen = opts.listen.to_string(),
        db = opts.db_path.display().to_string(),
        "Starting loglogd"
    );
    std::fs::create_dir_all(&params.db_path)?;

    let segments = LogStore::load_db(&params.db_path)?;

    let next_segment_id = segments
        .last()
        .map(|segment| segment.file_meta.id + 1)
        .unwrap_or(0);
    let (entry_write_tx, _entry_write_rx) = channel(16);
    let (_future_segments_tx, future_segments_rx) = channel(4);
    let node = Arc::new(Node::new(
        params,
        segments,
        future_segments_rx,
        entry_write_tx,
    )?);

    tokio_uring::start(async {
        tokio_uring::spawn(node.clone().run_entry_write_loop(_entry_write_rx));
        tokio_uring::spawn(
            node.clone()
                .run_segment_preloading_loop(next_segment_id, _future_segments_tx),
        );

        let listener = TcpListener::bind(opts.listen)?;
        info!("Listening on: {}", opts.listen);

        loop {
            let (mut stream, _peer_addr) = listener.accept().await?;

            tokio_uring::spawn({
                let node = node.clone();
                async move {
                    if let Err(e) = handle_connection(&node, &mut stream).await {
                        info!("Connection error: {}", e);
                    }
                }
            });
        }
    })
}

#[derive(FromPrimitive, Debug)]
#[repr(u8)]
#[derive(BinRead)]
#[br(repr = u8)]
pub enum HeaderCmd {
    Peer = 0,
    Append = 1,
    Fill = 2,
    Read = 3,
    #[default]
    Other,
}

#[derive(BinRead, BinWrite, Debug, Copy, Clone)]
pub struct EntrySize(
    #[br(big, parse_with(EntrySize::parse))]
    #[bw(big, write_with(EntrySize::write))]
    u32,
);

impl EntrySize {
    fn parse<R: Read + Seek>(reader: &mut R, _ro: &ReadOptions, _: ()) -> binrw::BinResult<u32> {
        let mut bytes = [0u8; 3];
        reader.read_exact(&mut bytes)?;
        Ok(u32::from(bytes[0]) << 16 | u32::from(bytes[1]) << 8 | u32::from(bytes[2]))
    }

    fn write<W: binrw::io::Write + binrw::io::Seek>(
        &amount: &u32,
        writer: &mut W,
        _opts: &WriteOptions,
        _: (),
    ) -> binrw::BinResult<()> {
        let bytes = amount.to_be_bytes();
        writer.write_all(&bytes[1..])?;

        Ok(())
    }
}

#[derive(BinRead, Debug)]
pub struct StreamHeaderAppend {
    size: EntrySize,
}

#[derive(BinRead, Debug)]
pub struct StreamHeaderFill {
    size: EntrySize,
    allocation_id: AllocationId,
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("disconnected")]
    Disconected,
    #[error("invalid data")]
    Invalid,
    #[error("invalid data")]
    ParseError(#[from] binrw::Error),
    #[error("io")]
    IO(#[from] io::Error),
}

pub type RingConnectionResult<T> = (ConnectionResult<T>, Vec<u8>);

pub type ConnectionResult<T> = std::result::Result<T, ConnectionError>;

/// Read a 3-byte size from buffer
///
/// ```
/// assert_eq!(read_entry_size(&[0x11, 0xbb, 0xcc]), Ok(0x11_bb_cc));
/// aseert_eq!(read_entry_size(&[0xaa, 0xbb, 0xcc]), Err(ConnectionError::Invalid));
/// ```
pub fn read_entry_size(buf: &[u8]) -> ConnectionResult<usize> {
    assert!(buf.len() == 3);

    let res: u32 = u32::from(buf[0]) << 16 | u32::from(buf[1]) << 8 | u32::from(buf[2]);

    if 0x7f_ff_ff < res {
        Err(ConnectionError::Invalid)?;
    }

    Ok(usize::try_from(res).expect("will not work on 16bit machines"))
}

/// Handle connection
async fn handle_connection(node: &Arc<Node>, stream: &mut TcpStream) -> ConnectionResult<()> {
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
        let mut buf = node.pop_entry_buffer().await;
        vec_extend_to_at_least(&mut buf, 14);

        let (res, res_buf) = tcpstream_read_fill(stream, buf.slice(0..14)).await;

        if res.is_err() {
            node.put_entry_buffer(res_buf).await;
            return res;
        } else {
            buf = res_buf;
        }

        let cursor = &mut Cursor::new(&buf[..14]);
        let cmd = match HeaderCmd::read(cursor) {
            Ok(cmd) => cmd,
            Err(e) => {
                node.put_entry_buffer(buf).await;
                return Err(e.into());
            }
        };

        match cmd {
            HeaderCmd::Peer => {
                todo!();
            }
            HeaderCmd::Append => {
                let args = match StreamHeaderAppend::read(cursor) {
                    Ok(args) => args,
                    Err(e) => {
                        node.put_entry_buffer(buf).await;
                        return Err(e.into());
                    }
                };
                debug!(cmd = ?cmd, args = ?args);

                handle_append(node, stream, buf, args.size).await?;
            }
            HeaderCmd::Fill => {
                let args = match StreamHeaderFill::read(cursor) {
                    Ok(args) => args,
                    Err(e) => {
                        node.put_entry_buffer(buf).await;
                        return Err(e.into());
                    }
                };
                debug!(cmd = ?cmd, args = ?args);

                handle_fill(node, stream, buf, args.allocation_id, args.size).await?;
            }
            HeaderCmd::Read => {
                todo!();
            }
            HeaderCmd::Other => Err(ConnectionError::Invalid)?,
        }
    }
}

async fn handle_append(
    node: &Arc<Node>,
    stream: &mut TcpStream,
    buf: Vec<u8>,
    entry_size: EntrySize,
) -> ConnectionResult<()> {
    let allocation_id = node.allocate_new_entry(entry_size).await;

    // TODO: send the allocation id right away, in parallel, flush it, so the client can
    // start uploading to other nodes right away

    handle_fill(node, stream, buf, allocation_id, entry_size).await?;

    debug!("Sending response to append request");
    let mut resp_buf = node.pop_entry_buffer().await;
    // 0-byte == 0 -> success
    let res_size = 1 + AllocationId::BYTE_SIZE;
    vec_extend_to_at_least(&mut resp_buf, res_size);

    resp_buf[1..res_size].copy_from_slice(&allocation_id.to_bytes());

    let (res, res_buf) = tcpstream_write_all(stream, resp_buf.slice(..res_size)).await;

    node.put_entry_buffer(res_buf).await;

    res
}

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
                entry_header_size
                    ..(entry_header_size + usize::try_from(payload_size.0).expect("can't fail"))
            )
        )
        .await
    );

    (Ok(()), entry_buf)
}

async fn handle_fill(
    node: &Arc<Node>,
    stream: &mut TcpStream,
    mut buf: Vec<u8>,
    allocation_id: AllocationId,
    payload_size: EntrySize,
) -> ConnectionResult<()> {
    let entry_header_size = EntryHeader::BYTE_SIZE;
    let entry_trailer_size = EntryTrailer::BYTE_SIZE;
    // TODO: allocation_id.term vs node.term
    let header = segment::EntryHeader {
        term: allocation_id.term,
        payload_size,
    };

    let total_entry_size =
        entry_header_size + usize::try_from(payload_size.0).expect("not fail") + entry_trailer_size;
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
    let _ = node
        .entry_writer_tx
        .send(EntryWrite {
            offset: allocation_id.offset,
            entry: buf,
        })
        .await;

    read_res
}
