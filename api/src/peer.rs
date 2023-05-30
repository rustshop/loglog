use binrw::{BinRead, BinWrite};

use crate::{AllocationId, LogOffset, TermId};

/// Update send from leader to follower (corresponds to AppendEntries RPC)
#[derive(BinRead, BinWrite, Debug, Copy, Clone)]
#[br(big)]
#[bw(big)]
pub struct Update {
    /// Current term leader thinks it is
    pub current_term: TermId,
    // AllocationId of the first log leader things the follower is missing
    pub start: AllocationId,
    /// Last fsynced offset of the leader, or a lower value (if too much data to transfer at once)
    pub end: LogOffset,
    // Followed by `end - start` bytes of log content
}

impl Update {
    pub const BYTE_SIZE: usize = TermId::BYTE_SIZE + AllocationId::BYTE_SIZE + LogOffset::BYTE_SIZE;
}

/// Response from the follower to `Update`
#[derive(BinRead, BinWrite, Debug, Copy, Clone)]
#[br(big)]
#[bw(big)]
pub enum UpdateResponse {
    /// Client is at `Update::end` (fsynced)
    #[brw(magic(0u8))]
    Success,
    /// Client is out of sync, and it is it's responsibility to synchronize the logs with the leader
    #[brw(magic(1u8))]
    Failure,
}
