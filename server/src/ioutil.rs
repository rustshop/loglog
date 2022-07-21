use tokio_uring::{
    buf::{IoBuf, Slice},
    fs::File,
    net::TcpStream,
};

use crate::{ConnectionError, RingConnectionResult};

pub fn vec_extend_to_at_least(v: &mut Vec<u8>, len: usize) {
    if v.len() < len {
        v.resize(len, 0);
    }
}

pub async fn tcpstream_read_fill(
    stream: &mut tokio_uring::net::TcpStream,
    mut buf: Slice<Vec<u8>>,
) -> RingConnectionResult<()> {
    while !buf.is_empty() {
        let (res, ret_buf) = stream.read(buf).await;
        buf = ret_buf;

        match res {
            Err(e) => return (Err(e.into()), buf.into_inner()),
            Ok(bytes) => {
                if bytes == 0 {
                    return (Err(ConnectionError::Disconected), buf.into_inner());
                }
                buf = {
                    let start = buf.begin();
                    let new_start = start + bytes;
                    let end = buf.end();
                    if new_start == end {
                        break;
                    }
                    buf.into_inner().slice(new_start..end)
                };
            }
        }
    }

    (Ok(()), buf.into_inner())
}

pub async fn tcpstream_write_all(
    stream: &TcpStream,
    mut buf: Slice<Vec<u8>>,
) -> RingConnectionResult<()> {
    while !buf.is_empty() {
        let (res, ret_buf) = stream.write(buf).await;
        buf = ret_buf;

        match res {
            Err(e) => return (Err(e.into()), buf.into_inner()),
            Ok(bytes) => {
                if bytes == 0 {
                    return (Err(ConnectionError::Disconected), buf.into_inner());
                }
                buf = {
                    let start = buf.begin();
                    let new_start = start + bytes;
                    let end = buf.end();
                    if new_start == end {
                        break;
                    }
                    buf.into_inner().slice(start + bytes..end)
                };
            }
        }
    }

    (Ok(()), buf.into_inner())
}

pub async fn file_write_all(
    stream: &File,
    mut buf: Slice<Vec<u8>>,
    mut file_pos: u64,
) -> RingConnectionResult<()> {
    while !buf.is_empty() {
        let (res, ret_buf) = stream.write_at(buf, file_pos).await;
        buf = ret_buf;

        match res {
            Err(e) => return (Err(e.into()), buf.into_inner()),
            Ok(bytes) => {
                if bytes == 0 {
                    return (Err(ConnectionError::Disconected), buf.into_inner());
                }
                buf = {
                    file_pos += u64::try_from(bytes).expect("must not fail");
                    let start = buf.begin();
                    let new_start = start + bytes;
                    let end = buf.end();
                    if new_start == end {
                        break;
                    }
                    buf.into_inner().slice(start + bytes..end)
                };
            }
        }
    }

    (Ok(()), buf.into_inner())
}
