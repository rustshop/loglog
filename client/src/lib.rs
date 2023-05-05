use binrw::{BinRead, BinWrite};
use convi::{CastFrom, ExpectFrom};
use loglogd_api::{
    AllocationId, AppendRequestHeader, EntryHeader, EntrySize, EntryTrailer, ReadDataSize,
    ReadRequestHeader, RequestHeaderCmd, REQUEST_HEADER_SIZE,
};
use std::io::Cursor;
use std::mem;
use std::time::{Duration, Instant};
use std::{io, net::SocketAddr};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::time::sleep;
use tokio::try_join;
use tracing::{debug, trace};

pub use loglogd_api::LogOffset;

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

pub enum ReadData<T> {
    None,
    Invalid,
    Some(T),
}

impl Client {
    pub async fn connect(server_addr: SocketAddr, log_offset: Option<LogOffset>) -> Result<Self> {
        debug!(?server_addr, "Connecting to loglogd");
        let stream = tokio::net::TcpStream::connect(server_addr).await?;
        trace!(?server_addr, "Connected");

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

    pub async fn next_raw(&mut self) -> Result<&[u8]> {
        loop {
            match self.next_raw_inner().await? {
                ReadData::None => {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                ReadData::Invalid => {
                    continue;
                }
                // 🤷 https://github.com/rust-lang/rust/issues/68117#issuecomment-573309675
                ReadData::Some(d) => return unsafe { Ok(mem::transmute(d)) },
            };
        }
    }

    /// Return next raw entry
    pub async fn next_raw_inner(&mut self) -> Result<ReadData<&'_ [u8]>> {
        debug_assert!(self.next_event_i <= self.buf.len());

        self.maybe_compact_buf();

        if self.fetch_header().await?.is_none() {
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
            if self.fetch_more_data().await?.is_none() {
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

    async fn fetch_header(&mut self) -> Result<Option<()>> {
        while !self.has_header() {
            match self.fetch_more_data().await? {
                Some(_) => {}
                None => return Ok(None),
            }
        }
        Ok(Some(()))
    }

    /// Send a read command to the server, read back the response size
    ///
    /// Returns number of bytes the server is going to send
    /// over through the `read`
    async fn send_read_cmd(
        mut read: impl AsyncReadExt + Unpin,
        mut write: impl AsyncWriteExt + Unpin,
        log_offset: LogOffset,
        limit: u32,
    ) -> Result<u32> {
        let mut cmd_buf = [0u8; REQUEST_HEADER_SIZE];

        let mut cursor = Cursor::new(&mut cmd_buf[0..]);

        std::io::Write::write_all(&mut cursor, &[RequestHeaderCmd::Read.into()])
            .expect("can't fail");

        let args = ReadRequestHeader {
            offset: log_offset,
            limit: ReadDataSize(limit),
        };

        args.write_to(&mut cursor).expect("can't fail");
        write.write_all(&cmd_buf).await?;

        let mut data_size_buf = [0u8; ReadDataSize::BYTE_SIZE];

        read.read_exact(&mut data_size_buf).await?;

        let data_size = ReadDataSize::read(&mut Cursor::new(data_size_buf.as_slice()))?.0;

        Ok(data_size)
    }

    /// Fetch more data into the buffer
    ///
    /// Err - there was an error
    /// Ok(None) - no more data available
    /// OK(Some(size)) - new data loaded
    async fn fetch_more_data(&mut self) -> Result<Option<usize>> {
        let data_size = Self::send_read_cmd(
            &mut self.conn_read,
            &mut self.conn_write,
            self.log_offset,
            1024 * 64,
        )
        .await?;

        if data_size == 0 {
            return Ok(None);
        }

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

    pub async fn append_nocommit(&mut self, raw_entry: &[u8]) -> Result<LogOffset> {
        debug!(size = raw_entry.len(), "Appending new entry");
        let mut cmd_buf = [0u8; REQUEST_HEADER_SIZE];

        let mut cursor = Cursor::new(&mut cmd_buf[0..]);

        std::io::Write::write_all(&mut cursor, &[RequestHeaderCmd::Append.into()])
            .expect("can't fail");

        let args = AppendRequestHeader {
            size: EntrySize(u32::expect_from(raw_entry.len())),
        };

        args.write_to(&mut cursor).expect("can't fail");
        self.conn_write.write_all(&cmd_buf).await?;

        let mut entry_log_offset_buf = [0u8; AllocationId::BYTE_SIZE];

        let (offset_res, _sent) = try_join!(
            async {
                self.conn_read.read_exact(&mut entry_log_offset_buf).await?;

                let allocation_id = AllocationId::read(&mut Cursor::new(&entry_log_offset_buf));
                // TODO: in the future here we will start sending the `raw_entry` to other peers
                // right away using `Fill` call

                // TODO: should we have any sort of "we're done here"?
                // self.conn_read.read_u8().await?;
                Ok(allocation_id)
            },
            self.conn_write.write_all(raw_entry)
        )?;

        let offset = offset_res?;

        debug!(offset = %offset.offset, "New entry offset");
        Ok(offset.offset)
    }

    pub async fn append(&mut self, raw_entry: &[u8]) -> Result<LogOffset> {
        let offset = self.append_nocommit(raw_entry).await?;
        loop {
            let read_start = Instant::now();
            match self.append_read_commit(offset).await? {
                ReadData::None => {
                    let read_duration = read_start.elapsed();
                    let min_read_duration = Duration::from_secs(1);
                    if read_duration < min_read_duration {
                        sleep(min_read_duration - read_duration).await;
                    }
                    continue;
                }
                ReadData::Invalid => {
                    // we are actually not reading the whole entry, so we
                    // are not checking if the written entry is valid. It
                    // should be - if we returned any IO errors, we wouldn't be
                    // here.
                    unreachable!();
                }
                // 🤷 https://github.com/rust-lang/rust/issues/68117#issuecomment-573309675
                ReadData::Some(offset) => return Ok(offset),
            };
        }
    }

    /// Wait for the server to commit to the written entry
    ///
    /// We use the offset returned for the allocated entry and
    /// just need to
    pub async fn append_read_commit(&mut self, offset: LogOffset) -> Result<ReadData<LogOffset>> {
        let data_size =
            Self::send_read_cmd(&mut self.conn_read, &mut self.conn_write, offset, 1).await?;

        if data_size == 0 {
            return Ok(ReadData::None);
        }
        debug_assert_eq!(data_size, 1);
        // read that one byte out
        self.conn_read.read_u8().await?;

        Ok(ReadData::Some(offset))
    }
}
