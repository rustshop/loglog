#![deny(clippy::as_conversions)]
use node::Parameters;
use opts::Opts;
use std::error::Error;
use std::io;
use thiserror::Error;
use tokio::signal;
use tokio_uring::net::TcpListener;
use tracing::info;
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

    let params = Parameters::builder().data_dir(opts.data_dir.clone());

    let params = if let Some(segment_size) = opts.base_segment_file_size {
        params.base_segment_file_size(segment_size)
    } else {
        params.base_segment_file_size(Parameters::DEFAULT_BASE_SEGMENT_SIZE)
    };

    let params = params.build();
    let listen = opts.listen;

    tokio_uring::start(async {
        info!(
            listen = %listen,
            "data-dir" = %params.data_dir.display(),
            "Starting loglogd"
        );
        info!("Listening on: {:?}", listen);
        let listener = TcpListener::bind(listen)?;

        let node = Node::new(listener, params.clone()).await?;
        let node_ctrl = node.get_ctrl();

        tokio_uring::spawn(async move {
            wait_for_shutdown_signal().await;
            info!("signal received, starting graceful shutdown");
            node_ctrl.stop();
        });

        let _listener = node.wait().await?;

        // Workaround: for some reason finishing this `tokio-uring` scope
        // hangs, most probably on the outstanding `accept` request on
        // the` listener` that was `timeout`ed in the `request_handling_loop`.
        // We don't really anything to do, so might as well terminate with success.
        std::process::exit(0);
    })
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
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
