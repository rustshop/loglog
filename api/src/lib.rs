use binrw::{binrw, Endian};
use binrw::{BinRead, BinWrite};
use derive_more::Display;
use std::io::Read;
use std::io::Seek;
use std::ops;

mod net;
pub use self::net::*;

mod log;
pub use self::log::*;
/// Logical offset in an the binary log stream
///
/// Clients use this offset directly to traverse the log and
/// request new parts from the server.
///
/// Notably LogLog for performance reasons includes each entry's
/// header and trailer the log, but segment file header is not included.
#[derive(Copy, Clone, Debug, BinRead, BinWrite, PartialEq, Eq, PartialOrd, Ord, Display)]
#[br(big)]
#[bw(big)]
pub struct LogOffset(pub u64);

impl ops::Add<u64> for LogOffset {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl ops::Sub<Self> for LogOffset {
    type Output = u64;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0 - rhs.0
    }
}

/// External ID of the allocated event buffer
///
/// It coins `TermId`, so that clients "Filling"
/// data at offsets in non-leader node can be rejected
/// if the current leader of the node doesn't match.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[binrw]
#[brw(big)]
pub struct AllocationId {
    pub term: TermId,
    pub offset: LogOffset,
}

impl AllocationId {
    pub const BYTE_SIZE: usize = 10;

    /// Convert to bytes representation
    pub fn to_bytes(&self) -> [u8; Self::BYTE_SIZE] {
        let mut buf = [0; Self::BYTE_SIZE];

        buf[0..2].copy_from_slice(&self.term.0.to_be_bytes());
        buf[2..].copy_from_slice(&self.offset.0.to_be_bytes());

        buf
    }
}

#[test]
fn allocation_id_serde() {
    use std::io::Cursor;
    let v = AllocationId {
        term: TermId(0x0123),
        offset: LogOffset(0x456789abcdef0011),
    };

    assert_eq!(
        v,
        AllocationId::read(&mut Cursor::new(v.to_bytes())).unwrap()
    );
}

/// Node Id
#[derive(Copy, Clone, Debug, BinRead, PartialEq, Eq)]
#[br(big)]
pub struct NodeId(pub u8);

/// Raft term (election id)
#[derive(Copy, Clone, Debug, BinRead, BinWrite, PartialEq, Eq)]
#[br(big)]
#[bw(big)]
pub struct TermId(pub u16);

/// A size of an entry
///
/// Even though the type here is `u32`, we store (read&write)
/// only 3Bs - it's just there's no better type to put it in
/// (like `u24`).
#[derive(Debug, Copy, Clone)]
#[binrw]
#[brw(big)]
pub struct EntrySize(
    #[br(big, parse_with(EntrySize::parse))]
    #[bw(big, write_with(EntrySize::write))]
    pub u32,
);

impl EntrySize {
    fn parse<R: Read + Seek>(reader: &mut R, _endian: Endian, _: ()) -> binrw::BinResult<u32> {
        let mut bytes = [0u8; 3];
        reader.read_exact(&mut bytes)?;
        Ok(u32::from(bytes[0]) << 16 | u32::from(bytes[1]) << 8 | u32::from(bytes[2]))
    }

    fn write<W: binrw::io::Write + binrw::io::Seek>(
        &amount: &u32,
        writer: &mut W,
        _endian: Endian,
        _: (),
    ) -> binrw::BinResult<()> {
        let bytes = amount.to_be_bytes();
        writer.write_all(&bytes[1..])?;

        Ok(())
    }
}
