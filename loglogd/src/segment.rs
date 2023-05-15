use crate::ioutil::{pwrite_all, vec_extend_to_at_least};
use std::{
    fmt,
    fs::OpenOptions,
    io::{BufReader, Cursor, Seek},
    os::fd::{AsRawFd, OwnedFd},
    path::{Path, PathBuf},
};

use binrw::{io, BinRead, BinWrite};
use loglogd_api::{EntryHeader, EntryTrailer, LogOffset};
use nix::fcntl::FallocateFlags;
use thiserror::Error;
use tracing::{debug, trace};

// use crate::ioutil::{file_write_all, vec_extend_to_at_least};

/// Segment file header
///
/// Every segment file starts with some internal data
/// that is not considered a part of the actual log.
/// Because of this, this is not a part of the public
/// API.
#[derive(BinRead, BinWrite, Debug)]
#[br(big)]
#[bw(big)]
pub struct SegmentFileHeader {
    /// Segment version.
    ///
    /// Right now only used to have a non-zero byte at the beginning to mark
    /// the file as initialized
    #[br(assert(version != 0))]
    pub version: u8,

    /// The starting log offset this segment file contains
    pub log_offset: LogOffset,

    /// Magic byte, just to be able to detect it's different than 0
    #[bw(magic = 0xffu8)]
    #[br(magic = 0xffu8)]
    pub ff_end: (),
}

impl SegmentFileHeader {
    #[allow(unused)]
    pub const BYTE_SIZE: usize = 1 + 8 + 1;
    pub const BYTE_SIZE_U64: u64 = 1 + 8 + 1;
    pub const VERSION_INVALID: u8 = 0;
}

/// Information about a segement
#[derive(Clone, Debug)]
pub struct SegmentMeta {
    pub file_meta: SegmentFileMeta,
    pub content_meta: SegmentContentMeta,
}

/// A sequential number (id) of a segment file
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentId(u64);

impl SegmentId {
    #[must_use]
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for SegmentId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
/// Segment metadata
///
/// Basically everything we can figure out about segment file
/// without opening it.
#[derive(Clone, Debug)]
pub struct SegmentFileMeta {
    // Segments are sequentially numbered and `id` used to create their name
    pub id: SegmentId,

    /// Path to a segment file, to avoid allocating to calculate it during normal operations
    pub path: PathBuf,

    /// File length
    pub file_len: u64,
}

impl SegmentFileMeta {
    pub fn new(id: impl Into<SegmentId>, file_len: u64, path: PathBuf) -> Self {
        Self {
            id: id.into(),
            file_len,
            path,
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Calculate path for a segment file of a given id
    pub(crate) fn get_path(db_path: &Path, id: SegmentId) -> PathBuf {
        let mut path = db_path.join(format!("{:016x}", id.0));
        path.set_extension(SegmentFileMeta::FILE_EXTENSION);
        path
    }
}

/// Metadata about segment file content, from the segment file content itself (mostly header)
#[derive(Debug, Copy, Clone)]
pub struct SegmentContentMeta {
    /// The starting byte of the stream this segment holds.
    ///
    /// This data is stored in the file header.
    pub start_log_offset: LogOffset,
    /// End of valid log content stored in this segment (start + size)
    pub end_log_offset: LogOffset,
}

pub struct EntryWrite {
    pub offset: LogOffset,
    pub entry: Vec<u8>,
}

#[derive(Debug)]
pub struct PreallocatedSegment {
    pub id: SegmentId,
    pub fd: OwnedFd,
    pub allocated_size: u64,
}

impl PreallocatedSegment {
    pub fn create_and_fallocate(
        path: &Path,
        id: SegmentId,
        allocated_size: u64,
    ) -> io::Result<Self> {
        trace!(path = %path.display(), "Creating new segment file");
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path)?;

        let fd: OwnedFd = file.into();

        nix::fcntl::fallocate(
            fd.as_raw_fd(),
            FallocateFlags::FALLOC_FL_ZERO_RANGE,
            0,
            i64::try_from(allocated_size).expect("not fail"),
        )?;

        Ok(Self {
            id,
            fd,
            allocated_size,
        })
    }

    pub fn finalize(self, start_log_offset: LogOffset) -> OpenSegment {
        OpenSegment {
            id: self.id,
            fd: self.fd,
            allocated_size: self.allocated_size,
            start_log_offset,
        }
    }
}

#[derive(Debug)]
pub struct OpenSegment {
    pub id: SegmentId,
    pub fd: OwnedFd,
    pub allocated_size: u64,
    // The position in the stream of the first entry stored in this file
    pub start_log_offset: LogOffset,
}

impl OpenSegment {
    pub fn write_file_header(&self, log_offset: LogOffset, mut buf: Vec<u8>) -> Vec<u8> {
        vec_extend_to_at_least(&mut buf, SegmentFileHeader::BYTE_SIZE);

        let header = SegmentFileHeader {
            version: 1,
            log_offset,
            ff_end: (),
        };

        header
            .write(&mut Cursor::new(&mut buf))
            .expect("can't fail");

        let res = pwrite_all(self.fd.as_raw_fd(), 0, &buf[..SegmentFileHeader::BYTE_SIZE]);

        if let Err(e) = res {
            panic!("IO Error when writting log: {}, crashing immediately", e);
        }

        buf
    }
}

// pub type SegmentOpenResult = std::result::Result<SegmentContentMeta, SegmentParseError>;

#[derive(Error, Debug)]
#[error("Segment parse error")]
pub struct SegmentParseError<R> {
    // content_meta: SegmentContentMeta,
    // /// Last valid file position
    // file_pos: u64,
    #[source]
    pub r#type: SegmentParseErrorType,
    pub res: R,
}

pub type SegmentParseResult<O, R> = std::result::Result<O, SegmentParseError<R>>;

#[derive(Error, Debug)]
pub enum SegmentParseErrorType {
    #[error("io")]
    Io(#[from] io::Error),
    #[error("file empty")]
    FileEmpty,
    #[error("file truncated")]
    FileTruncated,
    #[error("invalid file header")]
    InvalidFileHeader(#[source] binrw::Error),
    #[error("invalid entry header")]
    InvalidEntryHeader(#[source] binrw::Error),
    #[error("invalid payload")]
    FileSeekFailed(#[source] io::Error),
    #[error("invalid trailer")]
    InvalidEntryTrailer(#[source] binrw::Error),
}

impl SegmentParseErrorType {
    fn into_error<R>(self, res: R) -> SegmentParseError<R> {
        SegmentParseError { res, r#type: self }
    }
}

impl SegmentFileMeta {
    pub const FILE_EXTENSION: &'static str = "seg.loglog";
    pub const FILE_SUFFIX: &'static str = ".seg.loglog";

    pub fn file_name(&self) -> PathBuf {
        PathBuf::from(format!("{:016x}{}", self.id.as_u64(), Self::FILE_SUFFIX))
    }
}

pub struct SegmentContentRecoveredInfo {
    pub content_meta: Option<SegmentContentMeta>,
    pub truncate_size: u64,
    pub error_offset: u64,
}
impl SegmentContentMeta {
    /// Read the segment file trying to recover as much data as possible
    ///
    /// TODO: error handling here is super weird, as we:
    ///
    /// * want to bubble up IO erros separately
    /// * want to bubble up other errors
    /// * maybe have or maybe not some valid data
    pub fn read_from_file(
        file_meta: &SegmentFileMeta,
        file: &std::fs::File,
    ) -> io::Result<SegmentParseResult<SegmentContentMeta, SegmentContentRecoveredInfo>> {
        if file_meta.file_len < SegmentFileHeader::BYTE_SIZE_U64 {
            return Ok(Err(SegmentParseErrorType::FileTruncated.into_error(
                SegmentContentRecoveredInfo {
                    content_meta: None,
                    truncate_size: 0,
                    error_offset: file_meta.file_len,
                },
            )));
        }
        let mut reader = BufReader::new(file);
        let header = match SegmentFileHeader::read(&mut reader) {
            Ok(o) => {
                if o.version == SegmentFileHeader::VERSION_INVALID {
                    return Ok(Err(SegmentParseErrorType::FileTruncated.into_error(
                        SegmentContentRecoveredInfo {
                            content_meta: None,
                            truncate_size: 0,
                            error_offset: 0,
                        },
                    )));
                } else {
                    o
                }
            }
            Err(binrw::Error::AssertFail { pos, message }) => {
                if pos == 0 {
                    return Ok(Err(SegmentParseErrorType::FileEmpty.into_error(
                        SegmentContentRecoveredInfo {
                            content_meta: None,
                            truncate_size: 0,
                            error_offset: 0,
                        },
                    )));
                }
                return Ok(Err(SegmentParseErrorType::InvalidFileHeader(
                    binrw::Error::AssertFail { pos, message },
                )
                .into_error(SegmentContentRecoveredInfo {
                    content_meta: None,
                    truncate_size: 0,
                    error_offset: file_meta.file_len,
                })));
            }
            Err(binrw::Error::Io(e)) => return Err(e),
            Err(e) => {
                return Ok(Err(SegmentParseErrorType::InvalidFileHeader(e).into_error(
                    SegmentContentRecoveredInfo {
                        content_meta: None,
                        truncate_size: 0,
                        error_offset: file_meta.file_len,
                    },
                )))
            }
        };

        let file_pos_log_data_start = SegmentFileHeader::BYTE_SIZE_U64;
        let mut file_pos: u64 = file_pos_log_data_start;
        let mut file_size_left = file_meta.file_len - file_pos_log_data_start;

        // TODO: we're going to need that `term` to valide against leader log
        while file_meta.file_len != file_pos {
            match Self::check_entry(&mut reader, file_size_left) {
                Ok((_header, entry_size)) => {
                    let entry_size = u64::try_from(entry_size).expect("can't fail");
                    file_pos += entry_size;
                    file_size_left -= entry_size;
                }
                Err(SegmentParseError {
                    r#type,
                    res: error_file_offset,
                }) => {
                    let res = SegmentContentRecoveredInfo {
                        content_meta: if file_pos != file_pos_log_data_start {
                            Some(SegmentContentMeta {
                                start_log_offset: header.log_offset,
                                end_log_offset: LogOffset(
                                    header.log_offset.0 + file_pos - file_pos_log_data_start,
                                ),
                            })
                        } else {
                            None
                        },
                        truncate_size: file_pos,
                        error_offset: u64::try_from(error_file_offset).expect("must not fail"),
                    };

                    match r#type {
                        // We bubble up legitimate IO errors, instead of trying to truncate/delete possibly
                        // correct files due to underlying IO issues.
                        SegmentParseErrorType::Io(io)
                        | SegmentParseErrorType::InvalidEntryHeader(binrw::Error::Io(io))
                        | SegmentParseErrorType::FileSeekFailed(io)
                        | SegmentParseErrorType::InvalidEntryTrailer(binrw::Error::Io(io)) => {
                            return Err(io);
                        }
                        e @ SegmentParseErrorType::InvalidEntryHeader(_) => {
                            // warn!(
                            //     "Invalid header at file offset {}: {} ",
                            //     file_pos + u64::try_from(error_offset).expect("can't fail"),
                            //     e
                            // );
                            return Ok(Err(e.into_error(res)));
                        }
                        e @ SegmentParseErrorType::InvalidEntryTrailer(_) => {
                            // warn!(
                            //     "Failed payload seek at file offset {}: {} ",
                            //     file_pos + u64::try_from(error_offset).expect("can't fail"),
                            //     e
                            // );
                            return Ok(Err(e.into_error(res)));
                        }
                        e @ SegmentParseErrorType::FileTruncated => {
                            // warn!(
                            //     "File truncated at file offset: {}",
                            //     file_pos + u64::try_from(error_offset).expect("can't fail"),
                            // );
                            return Ok(Err(e.into_error(res)));
                        }
                        e @ SegmentParseErrorType::FileEmpty => {
                            return Ok(Err(e.into_error(res)));
                        }
                        SegmentParseErrorType::InvalidFileHeader(_) => {
                            panic!("parsing entry should not return this error")
                        }
                    }
                }
            }
        }

        Ok(Ok(SegmentContentMeta {
            start_log_offset: header.log_offset,
            end_log_offset: LogOffset(header.log_offset.0 + file_pos - file_pos_log_data_start),
        }))
    }

    fn check_entry<R>(
        reader: &mut BufReader<R>,
        file_size_left: u64,
    ) -> SegmentParseResult<(EntryHeader, usize), usize>
    where
        R: Seek + io::Read,
    {
        if file_size_left < EntryHeader::BYTE_SIZE_U64 {
            return Err(SegmentParseErrorType::FileTruncated
                .into_error(file_size_left.try_into().expect("can't fail")));
        }
        let header = EntryHeader::read(reader)
            .map_err(SegmentParseErrorType::InvalidEntryHeader)
            .map_err(|e| e.into_error(0))?;

        debug!(header = ?header, file_size_left, "read entry header");

        let entry_size = EntryHeader::BYTE_SIZE
            + usize::try_from(header.payload_size.0).expect("can't fail")
            + EntryTrailer::BYTE_SIZE;

        if file_size_left < entry_size.try_into().expect("can't fail") {
            return Err(SegmentParseErrorType::FileTruncated
                .into_error(file_size_left.try_into().expect("can't fail")));
        }

        reader
            .seek_relative(i64::from(header.payload_size.0))
            .map_err(SegmentParseErrorType::FileSeekFailed)
            .map_err(|e| e.into_error(EntryHeader::BYTE_SIZE))?;

        let _trailer = EntryTrailer::read(reader)
            .map_err(SegmentParseErrorType::InvalidEntryTrailer)
            .map_err(|e| {
                e.into_error(
                    EntryHeader::BYTE_SIZE
                        + usize::try_from(header.payload_size.0).expect("can't fail"),
                )
            })?;

        Ok((header, entry_size))
    }
}
