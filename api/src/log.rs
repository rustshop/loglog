//! Log datastructres
//!
//! LogLog's logical log is almost 1:1 mapping of the
//! low level storage. Because of this the clients
//! are responsbile for parsing log data.
//!
//! This module contains types describing the logical
//! log (list of entries).
use binrw::binrw;

use crate::{EntrySize, TermId};

/// Log Entry header
///
/// Every log entry is prefixed with a small
/// header.
#[derive(Debug, Copy, Clone)]
#[binrw]
#[brw(big)]
pub struct EntryHeader {
    /// This is used only for distributed consistency (Raft),
    /// but for performance reasons it's stored in the log
    /// (Raft consensus is log-based as well).
    pub term: TermId,

    /// Number of bytes of the actual payload entry.
    pub payload_size: EntrySize,
}

impl EntryHeader {
    pub const BYTE_SIZE: usize = TermId::BYTE_SIZE + EntrySize::BYTE_SIZE;
    pub const BYTE_SIZE_U64: u64 = Self::BYTE_SIZE as u64;
}

/// Entry suffix.
///
/// Every entry ends with a fixed suffix. This is primarily useful
/// for detecting if the entry was fully and correctly written to
/// storage.
#[derive(Debug)]
#[binrw]
#[brw(big)]
pub struct EntryTrailer {
    // 0xff = valid
    // 0x55 = entry invalid (e.g. client disconnected before fully uploading)
    // 00 = probably write never completed
    // other = data corruption?
    #[br(assert(marker == Self::ENTRY_INVALID || marker == Self::ENTRY_VALID))]
    pub marker: u8,
}

impl EntryTrailer {
    pub const BYTE_SIZE: usize = 1;
    pub const BYTE_SIZE_U64: u64 = 1;

    pub const ENTRY_VALID: u8 = 0xff;
    pub const ENTRY_INVALID: u8 = 0x55;

    pub fn valid() -> Self {
        Self {
            marker: Self::ENTRY_VALID,
        }
    }

    pub fn invalid() -> Self {
        Self {
            marker: Self::ENTRY_INVALID,
        }
    }

    pub fn is_valid(self) -> Option<bool> {
        match self.marker {
            Self::ENTRY_VALID => Some(true),
            Self::ENTRY_INVALID => Some(false),
            _ => None,
        }
    }
}
