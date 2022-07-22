use binrw::{BinRead, BinWrite, ReadOptions, WriteOptions};
use ioutil::{tcpstream_write_all, vec_extend_to_at_least};
use nix::fcntl::FallocateFlags;
use node::{Parameters, TermId};
use num_enum::FromPrimitive;
use opts::Opts;
use std::error::Error;
use std::io;
use std::io::{Cursor, Read, Seek};
use std::os::unix::prelude::AsRawFd;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tokio_uring::buf::IoBuf;
use tokio_uring::fs::{File, OpenOptions};
use tokio_uring::net::{TcpListener, TcpStream};
use tracing::{debug, info, trace};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::ioutil::{file_write_all, tcpstream_read_fill};
use crate::node::Node;
use crate::segment::{EntryHeader, EntryTrailer, LogStore};

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
macro_rules! uring_try {
    ($buf:ident, $e:expr) => {{
        let res = $e;

        match res {
            Err(e) => {
                return (Err(e.into()), $buf);
            }
            Ok(o) => o,
        }
    }};
}

/// Logical offset in an the binary log stream
///
/// Clients use this offset directly to traverse the log and
/// request new parts from the server.
///
/// Notably LogLog for performance reasons includes each entry's
/// header and trailer the log, but segment file header is not included.
#[derive(Copy, Clone, Debug, BinRead, BinWrite, PartialEq, Eq)]
#[br(big)]
#[bw(big)]
pub struct LogOffset(u64);

/// External ID of the allocated event buffer
#[derive(BinRead, Debug, PartialEq, Eq)]
pub struct AllocationId {
    term: TermId,
    pos: LogOffset,
}

impl AllocationId {
    const BYTE_SIZE: usize = 10;

    /// Convert to bytes representation
    pub fn to_bytes(&self) -> [u8; Self::BYTE_SIZE] {
        let mut buf = [0; Self::BYTE_SIZE];

        buf[0..2].copy_from_slice(&self.term.0.to_be_bytes());
        buf[2..].copy_from_slice(&self.pos.0.to_be_bytes());

        buf
    }
}

#[test]
fn allocation_id_serde() {
    let v = AllocationId {
        term: TermId(0x0123),
        pos: LogOffset(0x456789abcdef0011),
    };

    assert_eq!(
        v,
        AllocationId::read(&mut Cursor::new(v.to_bytes())).unwrap()
    );
}

pub struct Segment {
    file: File,
    allocated_size: nix::libc::off_t,
    // The position in the stream of the first entry stored in this file
    file_stream_start_pos: u64,
}

impl Segment {
    async fn new(path: &Path, allocated_size: u64) -> io::Result<Self> {
        let allocated_size = i64::try_from(allocated_size).expect("not fail");
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .await?;

        let fd = file.as_raw_fd();

        tokio::task::spawn_blocking(move || {
            nix::fcntl::fallocate(fd, FallocateFlags::FALLOC_FL_ZERO_RANGE, 0, allocated_size)
        })
        .await??;

        Ok(Self {
            file,
            allocated_size,
            file_stream_start_pos: 0,
        })
    }
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

    let _segments = LogStore::load_db(&params.db_path)?;

    tokio_uring::start(async {
        let node = Arc::new(Node::new(params).await?);

        let listener = TcpListener::bind(opts.listen)?;
        info!("Listening on: {}", opts.listen);

        loop {
            let (mut stream, _peer_addr) = listener.accept().await?;

            tokio_uring::spawn({
                let node = node.clone();
                async move {
                    let mut buf = node.pop_entry_buffer().await;
                    let (res, res_buf) = handle_connection(&node, &mut stream, buf).await;
                    buf = res_buf;

                    if let Err(e) = res {
                        info!("Connection error: {}", e);
                    }
                    node.put_entry_buffer(buf).await;
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
async fn handle_connection(
    node: &Arc<Node>,
    stream: &mut TcpStream,
    mut buf: Vec<u8>,
) -> RingConnectionResult<()> {
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

    vec_extend_to_at_least(&mut buf, 14);
    // TODO: add timeouts?
    loop {
        uring_try_rec!(buf, tcpstream_read_fill(stream, buf.slice(0..14)).await);

        let cursor = &mut Cursor::new(&buf[..14]);
        let cmd = uring_try!(buf, HeaderCmd::read(cursor));

        match cmd {
            HeaderCmd::Peer => {
                todo!();
            }
            HeaderCmd::Append => {
                let args = uring_try!(buf, StreamHeaderAppend::read(cursor));
                debug!(cmd = ?cmd, args = ?args);
                uring_try_rec!(buf, handle_append(node, stream, buf, args.size).await);
            }
            HeaderCmd::Fill => {
                let args = uring_try!(buf, StreamHeaderFill::read(cursor));

                debug!(cmd = ?cmd, args = ?args);
                uring_try_rec!(
                    buf,
                    handle_fill(node, stream, buf, &args.allocation_id, args.size).await
                );
            }
            HeaderCmd::Read => {
                todo!();
            }
            HeaderCmd::Other => return (Err(ConnectionError::Invalid), buf),
        }
    }
}

async fn handle_append(
    node: &Arc<Node>,
    stream: &mut TcpStream,
    mut buf: Vec<u8>,
    entry_size: EntrySize,
) -> RingConnectionResult<()> {
    let allocation_id = node.advance_log_pos(entry_size);

    uring_try_rec!(
        buf,
        handle_fill(node, stream, buf, &allocation_id, entry_size).await
    );

    // 0-byte == 0 -> success
    let res_size = 1 + AllocationId::BYTE_SIZE;
    vec_extend_to_at_least(&mut buf, res_size);

    buf[1..res_size].copy_from_slice(&allocation_id.to_bytes());

    uring_try_rec!(
        buf,
        tcpstream_write_all(stream, buf.slice(..res_size)).await
    );

    (Ok(()), buf)
}

async fn handle_fill(
    node: &Arc<Node>,
    stream: &mut TcpStream,
    mut buf: Vec<u8>,
    allocation_id: &AllocationId,
    payload_size: EntrySize,
) -> RingConnectionResult<()> {
    let entry_header_size = EntryHeader::BYTE_SIZE;
    let entry_trailer_size = EntryTrailer::BYTE_SIZE;

    // TODO: allocation_id.term vs node.term
    let header = segment::EntryHeader {
        term: allocation_id.term,
        payload_size,
    };

    let trailer = segment::EntryTrailer { ff: () };

    debug!(header = ?header, trailer = ?trailer);

    let total_entry_size =
        entry_header_size + usize::try_from(payload_size.0).expect("not fail") + entry_trailer_size;
    vec_extend_to_at_least(&mut buf, total_entry_size);

    uring_try!(
        buf,
        header.write_to(&mut Cursor::new(&mut buf[0..entry_header_size]))
    );

    uring_try_rec!(
        buf,
        tcpstream_read_fill(
            stream,
            buf.slice(
                entry_header_size
                    ..(entry_header_size + usize::try_from(payload_size.0).expect("can't fail"))
            )
        )
        .await
    );
    uring_try!(
        buf,
        trailer.write_to(&mut Cursor::new(
            &mut buf[(total_entry_size - entry_trailer_size)..total_entry_size],
        ))
    );

    // TODO: check if we didn't already have this chunk, and if the offset seems valid (keep track in memory)
    let file_offset = allocation_id.pos.0 - node.segment.file_stream_start_pos;

    trace!(file_offset, total_entry_size, "writting to file");

    uring_try_rec!(
        buf,
        file_write_all(
            &node.segment.file,
            buf.slice(0..total_entry_size),
            file_offset,
        )
        .await
    );

    (Ok(()), buf)
}

#[test]
fn fail_me() {
    assert_eq!(1, 2);
}
