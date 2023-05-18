use clap::Parser;
use std::num::ParseIntError;
use std::str::FromStr;
use std::{net::SocketAddr, path::PathBuf};

#[derive(Parser, Debug, Clone)]
pub struct Opts {
    #[clap(long = "data-dir", default_value = "/var/lib/loglogd")]
    pub data_dir: PathBuf,

    #[clap(long = "listen", default_value = "127.0.0.1:8080")]
    pub listen: SocketAddr,

    #[clap(long = "segment-size", value_parser = parse_byte_unit)]
    pub base_segment_file_size: Option<u64>,
}

impl Opts {
    pub fn from_args() -> Self {
        Opts::parse()
    }
}

fn parse_byte_unit(s: &str) -> std::result::Result<u64, ParseIntError> {
    let s = s.trim().to_lowercase();

    Ok(if let Some(num) = s.strip_suffix('k') {
        u64::from_str(num)? * 1024
    } else if let Some(num) = s.strip_suffix('m') {
        u64::from_str(num)? * 1024 * 1024
    } else if let Some(num) = s.strip_suffix('g') {
        u64::from_str(num)? * 1024 * 1024 * 1024
    } else {
        u64::from_str(&s)?
    })
}

#[test]
fn parse_byte_unit_test() {
    assert_eq!(parse_byte_unit("0"), Ok(0));
    assert_eq!(parse_byte_unit("0000"), Ok(0));
    assert_eq!(parse_byte_unit("1"), Ok(1));
    assert_eq!(parse_byte_unit("0001"), Ok(1));
    assert_eq!(parse_byte_unit("2000"), Ok(2000));
    assert_eq!(parse_byte_unit("1k"), Ok(1024));
    assert_eq!(parse_byte_unit("12k"), Ok(12 * 1024));
    assert_eq!(parse_byte_unit("12m"), Ok(12 * 1024 * 1024));
    assert_eq!(parse_byte_unit("12g"), Ok(12 * 1024 * 1024 * 1024));
    assert_eq!(parse_byte_unit("0g"), Ok(0));
    assert!(parse_byte_unit("m").is_err());
    assert!(parse_byte_unit("1w").is_err());
}
