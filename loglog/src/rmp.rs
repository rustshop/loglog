use std::result;

use crate::std::RawClient;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("raw error: {0}")]
    Raw(#[from] crate::Error),
    #[error("decode error: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("encode error: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
}

pub type Result<T> = result::Result<T, Error>;

pub struct MsgPackClient<T> {
    inner: RawClient,
    _type: ::std::marker::PhantomData<T>,
}

impl<T> crate::std::Client for MsgPackClient<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    type InEntry<'a> = &'a T;

    type OutEntry<'a> = T;
    type Error = Error;

    fn read(&mut self) -> Result<Self::OutEntry<'_>> {
        Ok(rmp_serde::decode::from_slice(self.inner.read()?)?)
    }

    fn read_nowait(&mut self) -> Result<Option<Self::OutEntry<'_>>> {
        Ok(self
            .inner
            .read_nowait()?
            .map(rmp_serde::decode::from_slice)
            .transpose()?)
    }

    fn append(&mut self, raw_entry: Self::InEntry<'_>) -> Result<loglogd_api::LogOffset> {
        Ok(self.inner.append(&rmp_serde::encode::to_vec(raw_entry)?)?)
    }

    fn append_nocommit(&mut self, raw_entry: Self::InEntry<'_>) -> Result<loglogd_api::LogOffset> {
        Ok(self
            .inner
            .append_nocommit(&rmp_serde::encode::to_vec(raw_entry)?)?)
    }

    fn end_offset(&mut self) -> crate::Result<loglogd_api::LogOffset> {
        self.inner.end_offset()
    }
}
