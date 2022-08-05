use derive_more::Display;
use error_stack::{Context, IntoReport, ResultExt};
use loglog::{Client, LogOffset};
use rand::Rng;
use std::{io, time::Duration};
use tokio::time::sleep;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Display)]
#[display(fmt = "application error")]
pub struct AppError;

impl Context for AppError {}

pub type AppResult<T> = error_stack::Result<T, AppError>;

#[tokio::main]
async fn main() -> AppResult<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
        .init();

    let opts = loglog_e2e::Opts::from_args();

    let mut client = Client::connect(opts.server_addr, Some(LogOffset(0)))
        .await
        .report()
        .change_context(AppError)?;

    let mut rng = rand::thread_rng();

    loop {
        let len = rng.gen_range(0..10);
        client
            .append_nocommit(&(0..).take(len).collect::<Vec<u8>>())
            .await
            .report()
            .change_context(AppError)?;

        sleep(Duration::from_millis(rng.gen_range(0..1000))).await;
    }
}