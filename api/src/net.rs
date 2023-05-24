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

/// Request header- command
///
/// Every request starts with a one byte command
#[derive(FromPrimitive, IntoPrimitive, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
#[repr(u8)]
#[binrw]
#[brw(repr = u8)]
pub enum RequestHeaderCmd {
    Peer = 0,
    /// Append an entry to the log
    Append = 8,
    /// Append an entry to the log
    AppendWait = 9,
    // /// Fill a previously allocated place in the log with data
    // Fill = 13,
    /// Read the log
    Read = 16,
    /// Read the log and wait if more not available
    ReadWait = 17,
    /// Get the current log position
    GetEnd = 32,
    #[default]
    Other,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[binrw]
#[brw(big)]
pub enum Request {
    #[brw(magic(8u8))]
    Append(AppendRequestHeader),
    #[brw(magic(9u8))]
    AppendWait(AppendRequestHeader),
    #[brw(magic(16u8))]
    Read(ReadRequestHeader),
    #[brw(magic(17u8))]
    ReadWait(ReadRequestHeader),
    #[brw(magic(32u8))]
    GetEnd,
}

impl Request {
    pub const BYTE_SIZE: usize = 14;
}

/// Arguments for [`RequestHeaderCmd::Append`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[binrw]
#[brw(big)]
pub struct AppendRequestHeader {
    pub size: EntrySize,
}

/// Arguments for [`RequestHeaderCmd::Fill`]
#[derive(Debug, Clone, Copy)]
#[binrw]
#[brw(big)]
pub struct FillRequestHeader {
    pub size: EntrySize,
    pub allocation_id: AllocationId,
}

/// Arguments for [`RequestHeaderCmd::Read`]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[binrw]
#[brw(big)]
pub struct ReadRequestHeader {
    pub offset: LogOffset,
    pub limit: ReadDataSize,
}

#[derive(BinRead, BinWrite, Debug, Copy, Clone, PartialEq, Eq)]
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
