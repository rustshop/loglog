use std::net::SocketAddr;

use clap::Parser;

#[derive(Parser, Debug, Clone)]
pub struct Opts {
    #[clap(long = "server", default_value = "127.0.0.1:8080")]
    pub server_addr: SocketAddr,
}

impl Opts {
    pub fn from_args() -> Self {
        Opts::parse()
    }
}
