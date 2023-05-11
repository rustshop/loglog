use std::{
    io,
    os::fd::AsRawFd,
    path::{Path, PathBuf},
};

use nix::unistd::ftruncate;
use thiserror::Error;
use tracing::{debug, info, trace, warn};

use crate::segment::{SegmentContentMeta, SegmentFileMeta, SegmentMeta, SegmentParseError};

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

/// Scan `db_path` and find all the files that look like segment files.
fn scan_segments(data_dir: &Path) -> ScanResult<Vec<SegmentFileMeta>> {
    if !data_dir.is_dir() {
        Err(ScanError::NotADir)?
    }
    let mut segments = vec![];

    for entry in std::fs::read_dir(&data_dir).map_err(ScanError::CanNotList)? {
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

        let id = u64::from_str_radix(&id_str, 16)
            .map_err(|_| ScanError::InvalidFilePath {
                path: path.to_owned(),
            })?
            .into();

        segments.push(SegmentFileMeta {
            id,
            file_len: metadata.len(),
            path: SegmentFileMeta::get_path(&data_dir, id),
        });
    }

    Ok(segments)
}

pub fn load_db(data_dir: &Path) -> ScanResult<Vec<SegmentMeta>> {
    std::fs::create_dir_all(&data_dir)?;

    let mut files_meta = scan_segments(data_dir)?;

    files_meta.sort_by_key(|meta| meta.id);

    let mut segments = vec![];

    // TODO: parallelize with `pariter`
    for file_meta in files_meta {
        if let Some(content_meta) =
            self::open_and_recover(&file_meta, data_dir.join(file_meta.file_name()))?
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
            start_segment = %first.file_meta.id,
            end_post = last.content_meta.end_log_offset.0,
            end_segment = %last.file_meta.id,
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
