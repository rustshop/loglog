#![deny(clippy::as_conversions)]

mod opts;

use loglogd::Parameters;
use opts::Opts;
use std::io;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn main() -> anyhow::Result<()> {
    init_logging();

    let opts = Opts::from_args();

    let params = Parameters::builder().data_dir(opts.data_dir.clone());

    let params = if let Some(segment_size) = opts.base_segment_file_size {
        params.base_segment_file_size(segment_size)
    } else {
        params.base_segment_file_size(Parameters::DEFAULT_BASE_SEGMENT_SIZE)
    };

    let node = loglogd::Node::new(opts.listen, params.build())?;

    let node_ctrl = node.get_ctrl();
    node_ctrl.install_signal_handler()?;

    node.wait();

    Ok(())
}

fn init_logging() {
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
}
