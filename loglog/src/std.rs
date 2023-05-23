use binrw::io::NoSeek;
use binrw::{BinRead, BinWrite};
use convi::{CastFrom, ExpectFrom};
use loglogd_api::{
    AllocationId, AppendRequestHeader, ConnectionHello, EntryHeader, EntrySize, EntryTrailer,
    GetEndResponse, ReadDataSize, ReadRequestHeader, RequestHeaderCmd, LOGLOGD_VERSION_0,
    REQUEST_HEADER_SIZE,
};
use std::io::{Cursor, Read, Write};
use std::net::SocketAddr;
use std::net::TcpStream;
use std::{mem, result};
use tracing::{debug, trace};

use super::{Error, ReadData, Result};
pub use loglogd_api::LogOffset;

pub trait Client {
    type InEntry<'a>;
    type OutEntry<'a>
    where
        Self: 'a;
    type Error;
    /// Read the next entry in the log
    fn read(&mut self) -> result::Result<Self::OutEntry<'_>, Self::Error>;
    /// Read the next entry in the log, or `None` if none available
    fn read_nowait(&mut self) -> result::Result<Option<Self::OutEntry<'_>>, Self::Error>;
    /// Append an entry and wait for it to get commited in the log
    fn append(&mut self, raw_entry: Self::InEntry<'_>) -> result::Result<LogOffset, Self::Error>;
    /// Append an entry, without waiting for it to get commited in the log
    fn append_nocommit(
        &mut self,
        raw_entry: Self::InEntry<'_>,
    ) -> result::Result<LogOffset, Self::Error>;
    /// The [`LogOffset`] of end of the stream (right after last commited entry)
    fn end_offset(&mut self) -> Result<LogOffset>;
}

/// `loglog` client
pub struct RawClient {
    log_offset: LogOffset,
    buf: Vec<u8>,
    next_event_i: usize,
    conn: std::net::TcpStream,
}

impl Client for RawClient {
    type InEntry<'a> = &'a [u8];
    type OutEntry<'a> = &'a [u8];
    type Error = Error;

    fn read(&mut self) -> Result<&[u8]> {
        loop {
            match self.next_raw_inner(true)? {
                ReadData::None => {
                    return Err(Error::Empty);
                }
                ReadData::Invalid => {
                    continue;
                }
                // 🤷 https://github.com/rust-lang/rust/issues/68117#issuecomment-573309675
                ReadData::Some(d) => return unsafe { Ok(mem::transmute(d)) },
            };
        }
    }

    fn read_nowait(&mut self) -> Result<Option<&[u8]>> {
        loop {
            match self.next_raw_inner(true)? {
                ReadData::None => {
                    return Ok(None);
                }
                ReadData::Invalid => {
                    continue;
                }
                // 🤷 https://github.com/rust-lang/rust/issues/68117#issuecomment-573309675
                ReadData::Some(d) => return unsafe { Ok(mem::transmute(d)) },
            };
        }
    }

    fn append(&mut self, raw_entry: &[u8]) -> Result<LogOffset> {
        self.append_inner(raw_entry, true)
    }

    fn append_nocommit(&mut self, raw_entry: &[u8]) -> Result<LogOffset> {
        self.append_inner(raw_entry, false)
    }

    fn end_offset(&mut self) -> Result<LogOffset> {
        Self::inner_get_end(&mut self.conn)
    }
}

impl RawClient {
    pub fn connect(server_addr: SocketAddr, log_offset: Option<LogOffset>) -> Result<Self> {
        debug!(?server_addr, "Connecting to loglogd");
        let mut conn = std::net::TcpStream::connect(server_addr)?;
        trace!(?server_addr, "Connected");

        // We always prepare exact buffers to be sent immediately
        conn.set_nodelay(true)?;

        let mut buf = [0u8; ConnectionHello::BYTE_SIZE];
        conn.read_exact(&mut buf)?;
        let hello = ConnectionHello::read(&mut Cursor::new(&mut buf))?;

        if hello.version != LOGLOGD_VERSION_0 {
            Err(Error::ProtocolVersion(hello.version))?;
        }

        let log_offset = if let Some(off) = log_offset {
            off
        } else {
            Self::inner_get_end(&mut conn)?
        };

        Ok(Self {
            buf: vec![],
            log_offset,
            next_event_i: 0,
            conn,
        })
    }

    /// Return next raw entry
    fn next_raw_inner(&mut self, wait: bool) -> Result<ReadData<&'_ [u8]>> {
        debug_assert!(self.next_event_i <= self.buf.len());

        self.maybe_compact_buf();

        if self.fetch_header(wait)?.is_none() {
            return Ok(ReadData::None);
        }

        let header = EntryHeader::read(&mut Cursor::new(&self.buf[self.next_event_i..]))?;

        let payload_start_i = self.next_event_i + EntryHeader::BYTE_SIZE;
        let payload_end_i =
            self.next_event_i + EntryHeader::BYTE_SIZE + usize::cast_from(header.payload_size.0);

        while self.bytes_available()
            < usize::expect_from(header.payload_size.0)
                + EntryHeader::BYTE_SIZE
                + EntryTrailer::BYTE_SIZE
        {
            if self.fetch_more_data(wait)?.is_none() {
                return Ok(ReadData::None);
            }
        }

        let trailer = EntryTrailer::read(&mut Cursor::new(&self.buf[payload_end_i..]))?;

        if !trailer.is_valid().ok_or(Error::Corrupted)? {
            return Ok(ReadData::Invalid);
        }

        self.next_event_i = payload_end_i + EntryTrailer::BYTE_SIZE;

        Ok(ReadData::Some(&self.buf[payload_start_i..payload_end_i]))
    }

    fn append_inner(&mut self, raw_entry: &[u8], wait: bool) -> Result<LogOffset> {
        debug!(size = raw_entry.len(), "Appending new entry");

        let mut buf = Vec::with_capacity(REQUEST_HEADER_SIZE + raw_entry.len());

        std::io::Write::write_all(
            &mut buf,
            &[if wait {
                RequestHeaderCmd::AppendWait.into()
            } else {
                RequestHeaderCmd::Append.into()
            }],
        )
        .expect("can't fail");

        let args = AppendRequestHeader {
            size: EntrySize(u32::expect_from(raw_entry.len())),
        };

        // TODO(perf): instead of copy, use `write_vectored_all` when it stabilizes
        // https://github.com/rust-lang/rust/issues/70436
        args.write(&mut NoSeek::new(&mut buf)).expect("can't fail");

        buf.resize(REQUEST_HEADER_SIZE, 0);
        buf.extend_from_slice(raw_entry);

        self.conn.write_all(&buf)?;

        let mut entry_log_offset_buf = [0u8; AllocationId::BYTE_SIZE + 1];
        let offset = {
            self.conn.read_exact(&mut entry_log_offset_buf)?;

            AllocationId::read(&mut Cursor::new(&entry_log_offset_buf))?
        };

        debug!(offset = %offset.offset, "New entry offset");
        Ok(offset.offset)
    }

    /// Wait for the server to commit to the written entry
    pub fn wait_committed(&mut self, offset: LogOffset) -> Result<ReadData<LogOffset>> {
        let data_size = Self::inner_read(&mut self.conn, offset, 1, true)?;

        if data_size == 0 {
            return Ok(ReadData::None);
        }
        debug_assert_eq!(data_size, 1);
        // read that one byte out

        let mut one_byte = [0u8];
        self.conn.read_exact(&mut one_byte)?;

        Ok(ReadData::Some(offset))
    }

    fn fetch_header(&mut self, wait: bool) -> Result<Option<()>> {
        while !self.has_header() {
            match self.fetch_more_data(wait)? {
                Some(_) => {}
                None => return Ok(None),
            }
        }
        Ok(Some(()))
    }

    fn inner_get_end(conn: &mut TcpStream) -> Result<LogOffset> {
        let mut cmd_buf = [0u8; REQUEST_HEADER_SIZE];

        let mut cursor = Cursor::new(&mut cmd_buf[0..]);

        std::io::Write::write_all(&mut cursor, &[RequestHeaderCmd::GetEnd.into()])
            .expect("can't fail");

        conn.write_all(&cmd_buf)?;

        let mut data_size_buf = [0u8; GetEndResponse::BYTE_SIZE];

        conn.read_exact(&mut data_size_buf)?;

        let response = GetEndResponse::read(&mut Cursor::new(data_size_buf.as_slice()))?;

        Ok(response.offset)
    }

    /// Send a read command to the server, read back the response size
    ///
    /// Returns number of bytes the server is going to send
    /// over through the `read`
    fn inner_read(
        conn: &mut TcpStream,
        log_offset: LogOffset,
        limit: u32,
        wait: bool,
    ) -> Result<u32> {
        let mut cmd_buf = [0u8; REQUEST_HEADER_SIZE];

        let mut cursor = Cursor::new(&mut cmd_buf[0..]);

        std::io::Write::write_all(
            &mut cursor,
            &[if wait {
                RequestHeaderCmd::ReadWait.into()
            } else {
                RequestHeaderCmd::Read.into()
            }],
        )
        .expect("can't fail");

        let args = ReadRequestHeader {
            offset: log_offset,
            limit: ReadDataSize(limit),
        };

        args.write(&mut cursor).expect("can't fail");
        conn.write_all(&cmd_buf)?;

        let mut data_size_buf = [0u8; ReadDataSize::BYTE_SIZE];

        conn.read_exact(&mut data_size_buf)?;

        let data_size = ReadDataSize::read(&mut Cursor::new(data_size_buf.as_slice()))?.0;

        Ok(data_size)
    }

    /// Fetch more data into the buffer
    ///
    /// Err - there was an error
    /// Ok(None) - no more data available
    /// OK(Some(size)) - new data loaded
    fn fetch_more_data(&mut self, wait: bool) -> Result<Option<usize>> {
        let data_size = Self::inner_read(&mut self.conn, self.log_offset, 1024 * 64, wait)?;

        if data_size == 0 {
            assert!(!wait);
            return Ok(None);
        }

        // TODO(perf): read into uninitialized buffer
        // https://github.com/rust-lang/rust/issues/78485
        let prev_len = self.buf.len();
        let new_len = prev_len + usize::cast_from(data_size);
        self.buf.resize(new_len, 0);
        match self.conn.read_exact(&mut self.buf[prev_len..new_len]) {
            Ok(_read) => {
                self.log_offset += u64::from(data_size);
            }
            Err(e) => {
                // on failure, just discard any partially read data and move on
                // in case the client ever retries
                self.buf.truncate(prev_len);
                Err(e)?;
            }
        };

        Ok(Some(usize::cast_from(data_size)))
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
        EntryHeader::BYTE_SIZE <= self.bytes_available()
    }

    fn bytes_available(&mut self) -> usize {
        self.buf.len() - self.next_event_i
    }
}
