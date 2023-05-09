use std::io;

use derive_more::Display;
use error_stack::{Context, IntoReport, ResultExt};
use loglog_tokio::{Client, LogOffset, RawClient};
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

    let mut client = RawClient::connect(opts.server_addr, Some(LogOffset(0)))
        .await
        .report()
        .change_context(AppError)?;

    loop {
        let entry = client.read().await.report().change_context(AppError)?;
        println!("{:?}", entry);
    }
}
