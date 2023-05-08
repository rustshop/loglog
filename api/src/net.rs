use binrw::{binrw, BinRead, BinWrite};
use num_enum::{FromPrimitive, IntoPrimitive};

use crate::{AllocationId, EntrySize, LogOffset};

pub const LOGLOGD_VERSION_0: u8 = 0;

/// Connectin headerr
///
/// On every connect server sends some initial data.
#[derive(BinRead, BinWrite, Debug, Copy, Clone)]
#[br(big)]
#[bw(big)]
pub struct ConnectionHello {
    /// Protocol version
    pub version: u8,
}

impl ConnectionHello {
    pub const BYTE_SIZE: usize = 1;
}

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
    /// Append an entry to the log
    Append = 1,
    /// Fill a previously allocated place in the log with data
    Fill = 2,
    /// Read the log
    Read = 3,
    /// Get the current log position
    GetEnd = 4,
    #[default]
    Other,
}

/// Arguments for [`RequestHeaderCmd::Append`]
#[derive(Debug)]
#[binrw]
#[brw(big)]
pub struct AppendRequestHeader {
    pub size: EntrySize,
}

/// Arguments for [`RequestHeaderCmd::Fill`]
#[derive(Debug)]
#[binrw]
#[brw(big)]
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
#[derive(BinRead, BinWrite, Debug, Copy, Clone)]
#[br(big)]
#[bw(big)]
pub struct GetEndResponse {
    pub offset: LogOffset,
}

impl GetEndResponse {
    pub const BYTE_SIZE: usize = 8;
}
