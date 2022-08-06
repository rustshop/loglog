use std::{
    io::{BufReader, Cursor, Seek},
    os::unix::prelude::AsRawFd,
    path::{Path, PathBuf},
};

use binrw::{
    io::{self},
    BinRead, BinWrite,
};
use loglogd_api::{EntryHeader, EntryTrailer, LogOffset};
use nix::{fcntl::FallocateFlags, unistd::ftruncate};
use thiserror::Error;
use tokio_uring::{
    buf::IoBuf,
    fs::{File, OpenOptions},
};
use tracing::{debug, info, trace, warn};

use crate::ioutil::{file_write_all, vec_extend_to_at_least};

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
}

/// Information about a segement
#[derive(Clone, Debug)]
pub struct SegmentMeta {
    pub file_meta: SegmentFileMeta,
    pub content_meta: SegmentContentMeta,
}

/// Segment metadata
///
/// Basically everything we can figure out about segment file
/// without opening it.
#[derive(Clone, Debug)]
pub struct SegmentFileMeta {
    // Segments are sequentially numbered and `id` used to create their name
    pub id: u64,

    /// Path to a file, to avoid redoing (allocating)
    pub path: PathBuf,

    pub file_len: u64,
}

impl SegmentFileMeta {
    pub fn new(id: u64, file_len: u64, path: PathBuf) -> Self {
        Self { id, file_len, path }
    }

    pub(crate) fn get_path(db_path: &Path, id: u64) -> PathBuf {
        let mut path = db_path.join(format!("{:016x}", id));
        path.set_extension(SegmentFileMeta::FILE_EXTENSION);
        path
    }
}

/// Data about segment content from the file content itself (mostly header)
#[derive(Debug, Copy, Clone)]
pub struct SegmentContentMeta {
    /// The starting byte of the stream this segment holds.
    ///
    /// This data is stored in the file header.
    pub start_log_offset: LogOffset,
    /// End of valid log content stored in this segment (start + size)
    pub end_log_offset: LogOffset,
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

pub struct EntryWrite {
    pub offset: LogOffset,
    pub entry: Vec<u8>,
}
pub type ScanResult<T> = std::result::Result<T, ScanError>;

#[derive(Clone, Debug)]
pub struct LogStore {
    db_path: PathBuf,
}

impl LogStore {
    pub fn open_or_create(db_path: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(&db_path)?;

        Ok(Self { db_path })
    }

    /// Scan `db_path` and find all the files that look like segment files.
    fn scan(&self) -> ScanResult<Vec<SegmentFileMeta>> {
        if !self.db_path.is_dir() {
            Err(ScanError::NotADir)?
        }
        let mut segments = vec![];

        for entry in std::fs::read_dir(&self.db_path).map_err(ScanError::CanNotList)? {
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
                file_len: metadata.len(),
                path: SegmentFileMeta::get_path(&self.db_path, id),
            });
        }

        Ok(segments)
    }

    pub fn load_db(&self) -> ScanResult<Vec<SegmentMeta>> {
        let mut files_meta = self.scan()?;

        files_meta.sort_by_key(|meta| meta.id);

        let mut segments = vec![];

        // TODO: parallelize with `pariter`
        for file_meta in files_meta {
            if let Some(content_meta) =
                Self::open_and_recover(&file_meta, self.db_path.join(file_meta.file_name()))?
            {
                segments.push(SegmentMeta {
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
                start_pos = first.content_meta.start_log_offset.0,
                start_segment = first.file_meta.id,
                end_post = last.content_meta.end_log_offset.0,
                end_segment = last.file_meta.id,
                num_segments = segments.len(),
                "Segments loaded"
            );
        }

        Ok(segments)
    }

    pub fn open_and_recover(
        file_meta: &SegmentFileMeta,
        path: std::path::PathBuf,
    ) -> io::Result<Option<SegmentContentMeta>> {
        trace!(path = %path.display(), "Opening sealed segment file");
        let file = std::fs::OpenOptions::new()
            .read(true)
            // just in case we need to truncate
            .write(true)
            .open(&path)?;
        match SegmentContentMeta::read_from_file(file_meta, &file, &path)? {
            Ok(o) => {
                debug!(start_log_offset = o.start_log_offset.0, end_log_offset = o.end_log_offset.0, path = ?path.display(), "segment file cleanly opened");
                Ok(Some(o))
            }
            Err(SegmentParseError { r#type: type_, res }) => {
                if let Some(content_meta) = res.content_meta {
                    warn!(
                        path = ?path.display(),
                        error = ?type_,
                        error_file_offset = res.error_offset,
                        from_size = file_meta.file_len,
                        to_size = res.truncate_size,
                        "truncating segment file"
                    );
                    ftruncate(
                        file.as_raw_fd(),
                        i64::try_from(res.truncate_size).expect("can't fail"),
                    )?;
                    Ok(Some(content_meta))
                } else {
                    warn!(
                        path = ?path.display(),
                        error = ?type_,
                        error_file_offset = res.error_offset,
                        "removing segment file with no existing entries"
                    );
                    std::fs::remove_file(path)?;
                    Ok(None)
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct OpenSegment {
    pub id: u64,
    pub file: File,
    pub allocated_size: u64,
    // The position in the stream of the first entry stored in this file
    // pub start_log_offset: LogOffset,
}

impl OpenSegment {
    pub async fn create_and_fallocate(
        path: &Path,
        id: u64,
        allocated_size: u64,
    ) -> io::Result<Self> {
        trace!(path = %path.display(), "Creating new segment file");
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path)
            .await?;

        let fd = file.as_raw_fd();

        tokio::task::spawn_blocking(move || -> nix::Result<()> {
            nix::fcntl::fallocate(
                fd,
                FallocateFlags::FALLOC_FL_ZERO_RANGE,
                0,
                i64::try_from(allocated_size).expect("not fail"),
            )?;
            Ok(())
        })
        .await??;

        Ok(Self {
            id,
            file,
            allocated_size,
        })
    }

    pub async fn write_header(&self, log_offset: LogOffset, mut buf: Vec<u8>) -> Vec<u8> {
        vec_extend_to_at_least(&mut buf, SegmentFileHeader::BYTE_SIZE);

        let header = SegmentFileHeader {
            version: 1,
            log_offset,
            ff_end: (),
        };

        header
            .write_to(&mut Cursor::new(&mut buf))
            .expect("can't fail");

        let (res, res_buf) =
            file_write_all(&self.file, buf.slice(..SegmentFileHeader::BYTE_SIZE), 0).await;

        if let Err(e) = res {
            panic!("IO Error when writting log: {}, crashing immediately", e);
        }

        res_buf
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
    r#type: SegmentParseErrorType,
    res: R,
}

pub type SegmentParseResult<O, R> = std::result::Result<O, SegmentParseError<R>>;

#[derive(Error, Debug)]
pub enum SegmentParseErrorType {
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
    pub const FILE_EXTENSION: &'static str = "seg.loglog";
    pub const FILE_SUFFIX: &'static str = ".seg.loglog";

    fn file_name(&self) -> PathBuf {
        PathBuf::from(format!("{:016x}{}", self.id, Self::FILE_SUFFIX))
    }
}

struct SegmentContentRecoveredInfo {
    content_meta: Option<SegmentContentMeta>,
    truncate_size: u64,
    error_offset: u64,
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
        let header = match SegmentFileHeader::read(&mut reader).map_err(|e| {
            warn!("could not read segment header: {}", path.display());
            e
        }) {
            Ok(o) => o,
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
