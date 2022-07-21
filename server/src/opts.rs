use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;

#[derive(Parser, Debug, Clone)]
pub struct Opts {
    #[clap(long = "db-path", default_value = "/var/loglog")]
    pub db_path: PathBuf,
    #[clap(long = "listen", default_value = "127.0.0.1:8080")]
    pub listen: SocketAddr,
}

impl Opts {
    pub fn from_args() -> Self {
        Opts::parse()
    }
}
