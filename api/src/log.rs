use binrw::{BinRead, BinWrite};

use crate::{EntrySize, TermId};

#[derive(BinRead, BinWrite, Debug, Copy, Clone)]
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
    // 0xff = valid
    // 0x55 = entry invalid (e.g. client disconnected before fully uploading)
    // 00 = probably write never completed
    // other = data corruption?
    #[br(assert(marker == Self::ENTRY_INVALID || marker == Self::ENTRY_VALID))]
    marker: u8,
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

    #[allow(unused)]
    pub fn is_valid(self) -> Option<bool> {
        match self.marker {
            Self::ENTRY_VALID => Some(true),
            Self::ENTRY_INVALID => Some(false),
            _ => None,
        }
    }
}
