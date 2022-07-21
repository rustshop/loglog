use std::{
    io::{BufReader, Seek},
    os::unix::prelude::AsRawFd,
    path::{Path, PathBuf},
};

use binrw::{
    io::{self},
    BinRead, BinWrite,
};
use nix::{fcntl::FallocateFlags, unistd::ftruncate};
use thiserror::Error;
use tokio_uring::fs::{File, OpenOptions};
use tracing::{debug, info, warn};

use crate::{node::TermId, EntrySize, LogOffset};

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

    /// The starting  log offset this segment file contains
    pub log_offset: LogOffset,

    /// Magic byte, just to be able to detect it's different than 0
    #[bw(magic = 0xffu8)]
    #[br(magic = 0xffu8)]
    pub ff_end: (),
}

impl SegmentFileHeader {
    pub const BYTE_SIZE: usize = 1 + 8 + 1;
    pub const BYTE_SIZE_U64: u64 = 1 + 8 + 1;
}

#[derive(BinRead, BinWrite, Debug)]
#[br(big)]
#[bw(big)]
pub struct EntryHeader {
    pub term: TermId,
    pub payload_size: EntrySize,
}

impl EntryHeader {
    pub const BYTE_SIZE: usize = 5;
    pub const BYTE_SIZE_U64: u64 = 5;
}

#[derive(BinRead, BinWrite, Debug)]
#[br(big)]
#[bw(big)]
// Just something that we can detect at the end and make sure 0s turned into 1s
pub struct EntryTrailer {
    #[bw(magic = 0xffu8)]
    #[br(magic = 0xffu8)]
    pub ff: (),
}

impl EntryTrailer {
    pub const BYTE_SIZE: usize = 1;
}

/// Information about a segement
pub struct Segment {
    file_meta: SegmentFileMeta,
    content_meta: SegmentContentMeta,
}

/// Segment metadata
///
/// Basically everything we can figure out about segment file
/// without opening it.
pub struct SegmentFileMeta {
    // Segments are sequentially numbered and `id` used to create their name
    id: u64,
    /// Id in a `String` version, to avoid redoing it
    id_str: String,

    file_len: u64,
}

/// Data about segment content from the file content itself (mostly header)
#[derive(Debug, Copy, Clone)]
pub struct SegmentContentMeta {
    /// The starting byte of the stream this segment holds.
    ///
    /// This data is stored in the file header.
    start_stream_pos: LogOffset,
    /// End of valid stream content stored in this segment
    end_stream_pos: LogOffset,
}

#[derive(Error, Debug)]
pub enum ScanError {
    #[error("not a dir")]
    NotADir,
    #[error("can not list the db dir")]
    CanNotList(#[source] io::Error),
    #[error("io error")]
    Io(#[from] io::Error),
    #[error("invalid file path: {}", path.display())]
    InvalidFilePath { path: PathBuf },
}

pub type ScanResult<T> = std::result::Result<T, ScanError>;

pub struct LogStore {
    /// Known segments, sorted by `stream_offset`
    segments: Vec<Segment>,
}

impl LogStore {
    /// Scan `db_path` and find all the files that look like segment files.
    pub fn scan_db_path(db_path: &Path) -> ScanResult<Vec<SegmentFileMeta>> {
        if !db_path.is_dir() {
            Err(ScanError::NotADir)?
        }

        let mut segments = vec![];

        for entry in std::fs::read_dir(db_path).map_err(ScanError::CanNotList)? {
            let entry = entry?;

            let path = entry.path();
            let metadata = entry.metadata()?;

            if !path
                .to_str()
                .map(|s| s.ends_with(SegmentFileMeta::FILE_SUFFIX))
                .unwrap_or(false)
            {
                debug!(
                    path = ?path.display(), "Ignoring db dir path: not ending with {}", SegmentFileMeta::FILE_SUFFIX);
                continue;
            }

            let id_str = path
                .file_name()
                .expect("must have a name")
                .to_str()
                .ok_or(ScanError::InvalidFilePath {
                    path: path.to_owned(),
                })?
                .strip_suffix(SegmentFileMeta::FILE_SUFFIX)
                .expect("Can't fail")
                .to_owned();

            if id_str.len() != 8 * 2 {
                Err(ScanError::InvalidFilePath {
                    path: path.to_owned(),
                })?;
            }

            let id = u64::from_str_radix(&id_str, 16).map_err(|_| ScanError::InvalidFilePath {
                path: path.to_owned(),
            })?;

            segments.push(SegmentFileMeta {
                id,
                id_str,
                file_len: metadata.len(),
            });
        }

        Ok(segments)
    }

    pub fn load_db(db_path: &Path) -> ScanResult<LogStore> {
        let mut files_meta = Self::scan_db_path(db_path)?;

        files_meta.sort_by_key(|meta| meta.id);

        let mut segments = vec![];

        // TODO: parallelize with `pariter`
        for file_meta in files_meta {
            if let Some(content_meta) =
                Self::open_and_recover(&file_meta, db_path.join(file_meta.file_name()))?
            {
                segments.push(Segment {
                    file_meta,
                    content_meta,
                });
            } else {
                break;
            }
        }

        if segments.is_empty() {
            info!("No existing log segments found");
        } else {
            info!("Recovered {} log segment files", segments.len());
            let first = segments.first().expect("no empty");
            let last = segments.last().expect("not empty");
            info!(
                start_pos = first.content_meta.start_stream_pos.0,
                start_segment = first.file_meta.id_str,
                end_post = last.content_meta.end_stream_pos.0,
                end_segment = last.file_meta.id_str,
                num_segments = segments.len(),
                "Segments loaded"
            );
        }

        Ok(Self { segments })
    }

    fn open_and_recover(
        file_meta: &SegmentFileMeta,
        path: std::path::PathBuf,
    ) -> io::Result<Option<SegmentContentMeta>> {
        let file = std::fs::File::open(&path)?;
        match SegmentContentMeta::read_from_file(file_meta, &file, &path)? {
            Ok(o) => Ok(Some(o)),
            Err(SegmentParseError { r#type: type_, res }) => {
                if let Some(content_meta) = res.0 {
                    warn!(
                        path = ?path.display(),
                        error = ?type_,
                        from_size = file_meta.file_len,
                        to_size = res.1,
                        "removing segment file with no existing entries"
                    );
                    ftruncate(file.as_raw_fd(), i64::try_from(res.1).expect("can't fail"))?;
                    Ok(Some(content_meta))
                } else {
                    warn!(
                        path = ?path.display(),
                        error = ?type_,
                        "removing segment file with no existing entries"
                    );
                    std::fs::remove_file(path)?;
                    Ok(None)
                }
            }
        }
    }
}

pub struct OpenSegment {
    pub file: File,
    allocated_size: nix::libc::off_t,
    // The position in the stream of the first entry stored in this file
    pub file_stream_start_pos: u64,
}

impl OpenSegment {
    pub async fn new(path: &Path, allocated_size: u64) -> io::Result<Self> {
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

// pub type SegmentOpenResult = std::result::Result<SegmentContentMeta, SegmentParseError>;

#[derive(Error, Debug)]
#[error("Segment parse error")]
struct SegmentParseError<R> {
    // content_meta: SegmentContentMeta,
    // /// Last valid file position
    // file_pos: u64,
    #[source]
    r#type: SegmentParseErrorType,
    res: R,
}

pub type SegmentParseResult<O, R> = std::result::Result<O, SegmentParseError<R>>;

#[derive(Error, Debug)]
enum SegmentParseErrorType {
    #[error("io")]
    Io(#[from] io::Error),
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
    const FILE_SUFFIX: &'static str = ".seg.loglog";

    fn file_name(&self) -> PathBuf {
        PathBuf::from(format!("{}{}", self.id_str, Self::FILE_SUFFIX))
    }
}

impl SegmentContentMeta {
    /// Read the segment file trying to recover as much data as possible
    ///
    /// TODO: error handling here is super weird, as we:
    ///
    /// * want to bubble up IO erros separately
    /// * want to bubble up other errors
    /// * maybe have or maybe not some valid data
    fn read_from_file(
        file_meta: &SegmentFileMeta,
        file: &std::fs::File,
        path: &Path,
    ) -> io::Result<SegmentParseResult<SegmentContentMeta, (Option<SegmentContentMeta>, u64)>> {
        if file_meta.file_len < SegmentFileHeader::BYTE_SIZE_U64 {
            return Ok(Err(
                SegmentParseErrorType::FileTruncated.into_error((None, file_meta.file_len))
            ));
        }
        let mut reader = BufReader::new(file);
        let header = match SegmentFileHeader::read(&mut reader).map_err(|e| {
            warn!("could not read segment header: {}", path.display());
            e
        }) {
            Ok(o) => o,
            Err(binrw::Error::Io(e)) => return Err(e),
            Err(e) => {
                return Ok(Err(
                    SegmentParseErrorType::InvalidFileHeader(e).into_error((None, 0))
                ))
            }
        };

        let file_pos_start = SegmentFileHeader::BYTE_SIZE_U64;
        let mut file_pos: u64 = file_pos_start;
        let mut file_size_left = file_meta.file_len - file_pos_start;

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
                    res: error_offset,
                }) => match r#type {
                    // We bubble up legitimate IO errors, instead of trying to truncate/delete possibly
                    // correct files due to underlying IO issues.
                    SegmentParseErrorType::Io(io)
                    | SegmentParseErrorType::InvalidEntryHeader(binrw::Error::Io(io))
                    | SegmentParseErrorType::FileSeekFailed(io)
                    | SegmentParseErrorType::InvalidEntryTrailer(binrw::Error::Io(io)) => {
                        return Err(io);
                    }
                    SegmentParseErrorType::InvalidEntryHeader(e) => {
                        warn!(
                            "Invalid header at file offset {}: {} ",
                            file_pos + u64::try_from(error_offset).expect("can't fail"),
                            e
                        );
                        break;
                    }
                    SegmentParseErrorType::InvalidEntryTrailer(e) => {
                        warn!(
                            "Failed payload seek at file offset {}: {} ",
                            file_pos + u64::try_from(error_offset).expect("can't fail"),
                            e
                        );
                        break;
                    }
                    SegmentParseErrorType::FileTruncated => {
                        warn!(
                            "File truncated at file offset: {}",
                            file_pos + u64::try_from(error_offset).expect("can't fail"),
                        );
                        break;
                    }
                    SegmentParseErrorType::InvalidFileHeader(_) => {
                        panic!("parsing entry should not return this error")
                    }
                },
            }
        }

        Ok(Ok(SegmentContentMeta {
            start_stream_pos: header.log_offset,
            end_stream_pos: LogOffset(header.log_offset.0 + file_pos - file_pos_start),
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
