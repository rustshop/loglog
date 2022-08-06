#![feature(map_first_last)]
#![deny(clippy::as_conversions)]
use node::Parameters;
use opts::Opts;
use std::error::Error;
use std::io::{self};
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::node::Node;

mod ioutil;
mod node;
mod opts;
mod segment;

/// A `?`-like macro to call functions that return values in `(res, buf)` uring-convention
#[macro_export]
macro_rules! uring_try_rec {
    ($buf:ident, $e:expr) => {{
        let (res, res_buf) = $e;
        $buf = res_buf;

        match res {
            Err(e) => {
                return (Err(e.into()), $buf);
            }
            Ok(o) => o,
        }
    }};
}

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(atty::is(atty::Stream::Stderr))
                .with_writer(io::stderr),
        )
        .init();

    let opts = Opts::from_args();

    let params = Parameters::builder().db_path(opts.db_path.clone());

    let params = if let Some(segment_size) = opts.base_segment_file_size {
        params.base_segment_file_size(segment_size)
    } else {
        params.base_segment_file_size(Parameters::DEFAULT_BASE_SEGMENT_SIZE)
    };

    let params = params.build();

    tokio_uring::start(async {
        let _node = Node::new(opts.listen, params).await?;

        loop {
            sleep(Duration::from_secs(60)).await;
        }
    })
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("disconnected")]
    Disconected,
    #[error("invalid data")]
    Invalid,
    #[error("invalid data: {0}")]
    ParseError(#[from] binrw::Error),
    #[error("io: {0}")]
    IO(#[from] io::Error),
    #[error("join: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

pub type RingConnectionResult<T> = (ConnectionResult<T>, Vec<u8>);

pub type ConnectionResult<T> = std::result::Result<T, ConnectionError>;
