#![deny(clippy::as_conversions)]
use node::{NodeCtrl, Parameters};
use std::{error::Error, net::SocketAddr};
use std::{io, thread};
use thiserror::Error;
use tokio_uring::net::TcpListener;
use tracing::info;

use crate::node::Node;

mod ioutil;
pub mod node;
mod segment;

pub async fn start_in_tokio_uring(
    params: Parameters,
    listen: SocketAddr,
) -> Result<Node, Box<dyn Error>> {
    info!(
        listen = %listen,
        "data-dir" = %params.data_dir.display(),
        "Starting loglogd"
    );
    let listener = TcpListener::bind(listen)?;
    info!("Listening on: {:?}", listener.local_addr());

    Ok(Node::new(listener, params.clone()).await?)
}

async fn start_in_tokio_uring_and_send(
    params: Parameters,
    listen: SocketAddr,
    tx: std::sync::mpsc::Sender<NodeCtrl>,
) -> Result<Node, Box<dyn Error>> {
    let node = start_in_tokio_uring(params, listen).await?;

    let node_ctrl = node.get_ctrl();

    let _ = tx.send(node_ctrl);

    let _listener = node.wait().await?;

    // Workaround: for some reason finishing this `tokio-uring` scope
    // hangs, most probably on the outstanding `accept` request on
    // the` listener` that was `timeout`ed in the `request_handling_loop`.
    // We don't really anything to do, so might as well terminate with success.
    std::process::exit(0);
}

pub fn start(
    params: Parameters,
    listen: SocketAddr,
) -> Result<node::NodeCtrl, Box<dyn Error + Send + Sync + 'static>> {
    let (tx, rx) = std::sync::mpsc::channel();

    thread::spawn(move || {
        tokio_uring::start(async move {
            let _ = start_in_tokio_uring_and_send(params, listen, tx).await;
        })
    });

    Ok(rx.recv()?)
}

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
