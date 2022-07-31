use binrw::{BinRead, BinWrite};
use convi::{CastFrom, ExpectFrom};
use loglogd_api::{
    EntryHeader, EntryTrailer, LogOffset, ReadDataSize, ReadRequestHeader, RequestHeaderCmd,
    REQUEST_HEADER_SIZE,
};
use std::io::Cursor;
use std::{io, net::SocketAddr};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("data decoding error: {0}")]
    Decoding(#[from] binrw::Error),
    #[error("data corrupted")]
    Corrupted,
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Client {
    log_offset: LogOffset,
    buf: Vec<u8>,
    next_event_i: usize,
    conn_read: ReadHalf<tokio::net::TcpStream>,
    conn_write: WriteHalf<tokio::net::TcpStream>,
}

impl Client {
    pub async fn connect(server_addr: SocketAddr, log_offset: Option<LogOffset>) -> Result<Self> {
        let stream = tokio::net::TcpStream::connect(server_addr).await?;

        let (conn_read, conn_write) = tokio::io::split(stream);

        let log_offset = if let Some(off) = log_offset {
            off
        } else {
            unimplemented!("send a command a find a first offset");
        };

        Ok(Self {
            buf: vec![],
            log_offset,
            next_event_i: 0,
            conn_read,
            conn_write,
        })
    }

    pub async fn next_inner(&mut self) -> Result<Option<&[u8]>> {
        debug_assert!(self.next_event_i <= self.buf.len());

        self.maybe_compact_buf();
        self.fetch_header().await?;

        let header = EntryHeader::read(&mut Cursor::new(&self.buf[self.next_event_i..]))?;

        while self.bytes_available()
            < usize::expect_from(header.payload_size.0)
                + EntryHeader::BYTE_SIZE
                + EntryTrailer::BYTE_SIZE
        {
            self.fetch_more_data().await?;
        }

        let trailer = EntryTrailer::read(&mut Cursor::new(&self.buf[self.next_event_i..]))?;

        if !trailer.is_valid().ok_or(Error::Corrupted)? {
            return Ok(None);
        }

        let payload_i = self.next_event_i + EntryHeader::BYTE_SIZE;

        self.next_event_i += EntryHeader::BYTE_SIZE
            + EntryTrailer::BYTE_SIZE
            + usize::expect_from(header.payload_size.0);

        Ok(Some(
            &self.buf[payload_i..payload_i + self.next_event_i - EntryTrailer::BYTE_SIZE],
        ))
    }

    pub async fn fetch_header(&mut self) -> Result<()> {
        while !self.has_header() {
            self.fetch_more_data().await?;
        }
        Ok(())
    }

    async fn fetch_more_data(&mut self) -> Result<()> {
        let mut cmd_buf = [0u8; REQUEST_HEADER_SIZE];

        let mut cursor = Cursor::new(&mut cmd_buf[0..]);

        std::io::Write::write_all(&mut cursor, &[RequestHeaderCmd::Read.into()])
            .expect("can't fail");

        let args = ReadRequestHeader {
            offset: self.log_offset,
            limit: ReadDataSize(1024 * 64),
        };

        args.write_to(&mut cursor).expect("can't fail");
        self.conn_write.write_all(&cmd_buf).await?;

        let mut data_size_buf = [0u8; ReadDataSize::BYTE_SIZE];

        self.conn_read.read_exact(&mut data_size_buf).await?;

        let data_size = ReadDataSize::read(&mut Cursor::new(data_size_buf.as_slice()))?.0;
        // TODO(perf): read into uninitialized buffer
        // https://github.com/rust-lang/rust/issues/78485
        let prev_len = self.buf.len();
        let new_len = prev_len + usize::cast_from(data_size);
        self.buf.resize(new_len, 0);
        match self
            .conn_read
            .read_exact(&mut self.buf[prev_len..new_len])
            .await
        {
            Ok(read) => {
                debug_assert_eq!(read, usize::cast_from(data_size));
                self.log_offset.0 += u64::from(data_size);
            }
            Err(e) => {
                // on failure, just discard any partially read data and move on
                // in case the client ever retries
                self.buf.truncate(prev_len);
                Err(e)?;
            }
        };

        Ok(())
    }

    fn maybe_compact_buf(&mut self) {
        // if we have less bytes available than free space before them, just move it to the beggining
        let bytes_available = self.bytes_available();
        if bytes_available * 2 < self.buf.len() {
            self.buf.copy_within(self.next_event_i.., 0);
            self.next_event_i = 0;
            self.buf.truncate(bytes_available);
        }
        debug_assert_eq!(bytes_available, self.bytes_available());
    }

    /// Does buffer have enough bytes to have a header?
    fn has_header(&mut self) -> bool {
        self.bytes_available() <= EntryHeader::BYTE_SIZE
    }

    fn bytes_available(&mut self) -> usize {
        self.buf.len() - self.next_event_i
    }
}
