#[cfg(feature = "rmp")]
pub mod rmp;
pub mod std;
#[cfg(feature = "tokio")]
pub mod tokio;

mod error {
    use ::std::io;
    use thiserror::Error;
    #[derive(Error, Debug)]
    pub enum Error {
        #[error("empty response")]
        Empty,
        #[error("io error: {0}")]
        Io(#[from] io::Error),
        #[error("data decoding error: {0}")]
        Decoding(#[from] binrw::Error),
        #[error("invalid protocol version: {0}")]
        ProtocolVersion(u8),
        #[error("data corrupted")]
        Corrupted,
    }
}

pub use self::error::Error;
pub type Result<T> = ::std::result::Result<T, Error>;

pub enum ReadData<T> {
    None,
    Invalid,
    Some(T),
}
