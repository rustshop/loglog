#![deny(clippy::as_conversions)]
use opts::Opts;
use std::error::Error;
use std::io;
use tokio::signal;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod opts;

use loglogd_tokio_uring::node::Parameters;

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

    tokio_uring::start(async {
        let node = loglogd_tokio_uring::start_in_tokio_uring(params.build(), opts.listen).await?;

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

    #[allow(inactive-code)]
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
