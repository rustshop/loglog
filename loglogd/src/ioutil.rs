use convi::ExpectFrom;
use nix::sys::uio::pwrite;
use std::{
    io::{self, ErrorKind},
    os::fd::RawFd,
};

pub fn vec_extend_to_at_least(v: &mut Vec<u8>, len: usize) {
    if v.len() < len {
        v.resize(len, 0);
    }
}

pub fn pwrite_all(fd: RawFd, mut file_offset: u64, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        match pwrite(fd, buf, i64::expect_from(file_offset)) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            Ok(written) => {
                buf = &buf[written..];
                file_offset += u64::expect_from(written);
            }
            Err(_) => todo!(),
        }
    }
    Ok(())
}

pub fn send_file_to_fd(
    file_fd: RawFd,
    mut file_offset: u64,
    stream_fd: RawFd,
    mut bytes_to_send: u64,
) -> io::Result<u64> {
    let mut bytes_written_total = 0u64;
    while 0 < bytes_to_send {
        let mut file_offset_mut: i64 = i64::expect_from(file_offset);
        // TODO: fallback to `send`?
        let bytes_sent = nix::sys::sendfile::sendfile64(
            stream_fd,
            file_fd,
            Some(&mut file_offset_mut),
            usize::expect_from(bytes_to_send),
        )
        .map_err(io::Error::from)?;

        let bytes_sent = u64::expect_from(bytes_sent);

        bytes_to_send -= bytes_sent;
        file_offset += u64::expect_from(bytes_sent);
        bytes_written_total += bytes_sent;
    }

    Ok(u64::expect_from(bytes_written_total))
}
