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
