use binrw::BinRead;
use num_enum::FromPrimitive;

use crate::{AllocationId, EntrySize, LogOffset};

/// Request header- command
///
/// Every request starts with a one byte command
#[derive(FromPrimitive, Debug)]
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
#[derive(BinRead, Debug)]
#[br(big)]
pub struct ReadRequestHeader {
    pub offset: LogOffset,
    pub limit: u32,
}
