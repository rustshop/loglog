use binrw::{BinRead, BinWrite};
use num_enum::{FromPrimitive, IntoPrimitive};

use crate::{AllocationId, EntrySize, LogOffset};

pub const REQUEST_HEADER_SIZE: usize = 14;
/// Request header- command
///
/// Every request starts with a one byte command
#[derive(FromPrimitive, IntoPrimitive, Debug)]
#[repr(u8)]
#[derive(BinRead)]
#[br(repr = u8)]
pub enum RequestHeaderCmd {
    Peer = 0,
    Append = 1,
    Fill = 2,
    Read = 3,
    #[default]
    Other,
}

/// Arguments for [`RequestHeaderCmd::Append`]
#[derive(BinRead, Debug)]
pub struct AppendRequestHeader {
    pub size: EntrySize,
}

/// Arguments for [`RequestHeaderCmd::Fill`]
#[derive(BinRead, Debug)]
pub struct FillRequestHeader {
    pub size: EntrySize,
    pub allocation_id: AllocationId,
}

/// Arguments for [`RequestHeaderCmd::Read`]
#[derive(BinRead, BinWrite, Debug)]
#[br(big)]
#[bw(big)]
pub struct ReadRequestHeader {
    pub offset: LogOffset,
    pub limit: ReadDataSize,
}

#[derive(BinRead, BinWrite, Debug)]
#[br(big)]
#[bw(big)]
pub struct ReadDataSize(pub u32);

impl ReadDataSize {
    pub const BYTE_SIZE: usize = 4;
}
