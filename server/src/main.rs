#![feature(map_first_last)]
#![deny(clippy::as_conversions)]
use node::Parameters;
use opts::Opts;
use std::error::Error;
use std::io::{self};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::channel;
use tokio_uring::net::TcpListener;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::node::Node;
use crate::segment::LogStore;

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
        .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
        .init();

    let opts = Opts::from_args();

    let params = Parameters::builder().db_path(opts.db_path.clone());

    let params = if let Some(segment_size) = opts.base_segment_file_size {
        params.base_segment_file_size(segment_size)
    } else {
        params.base_segment_file_size(Parameters::DEFAULT_BASE_SEGMENT_SIZE)
    };

    let params = params.build();

    info!(
        listen = opts.listen.to_string(),
        db = opts.db_path.display().to_string(),
        "Starting loglogd"
    );
    std::fs::create_dir_all(&params.db_path)?;

    let segments = LogStore::load_db(&params.db_path)?;

    let next_segment_id = segments
        .last()
        .map(|segment| segment.file_meta.id + 1)
        .unwrap_or(0);
    let (entry_write_tx, _entry_write_rx) = channel(16);
    let (_future_segments_tx, future_segments_rx) = channel(4);
    let node = Arc::new(Node::new(
        params,
        segments,
        future_segments_rx,
        entry_write_tx,
    )?);

    tokio_uring::start(async {
        // TODO: move these into `impl Node` somewhere?
        tokio_uring::spawn(node.clone().run_entry_write_loop(_entry_write_rx));
        tokio_uring::spawn(
            node.clone()
                .run_segment_preloading_loop(next_segment_id, _future_segments_tx),
        );

        tokio_uring::spawn(node.clone().run_fsync_loop());
        let listener = TcpListener::bind(opts.listen)?;
        info!("Listening on: {}", opts.listen);

        loop {
            let (mut stream, _peer_addr) = listener.accept().await?;

            tokio_uring::spawn({
                let node = node.clone();
                async move {
                    if let Err(e) = node.handle_connection(&mut stream).await {
                        info!("Connection error: {}", e);
                    }
                }
            });
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
