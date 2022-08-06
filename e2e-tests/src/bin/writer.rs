use clap::Parser;
use derive_more::Display;
use error_stack::{Context, IntoReport, Report, ResultExt};
use loglog::{Client, LogOffset};
use rand::{prelude::StdRng, Rng, SeedableRng};
use std::{io, time::Duration};
use tokio::time::sleep;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Display)]
#[display(fmt = "application error")]
pub struct AppError;

impl Context for AppError {}

pub type AppResult<T> = error_stack::Result<T, AppError>;

#[derive(Parser, Debug, Clone)]
pub struct Opts {
    #[clap(flatten)]
    pub common: loglog_e2e::Opts,

    #[clap(long, default_value = "1000")]
    pub delay: u64,

    #[clap(long, default_value = "1")]
    pub threads: u64,

    #[clap(long = "entry-size", default_value = "10")]
    pub entry_size: usize,
}

impl Opts {
    pub fn from_args() -> Self {
        Opts::parse()
    }
}
#[tokio::main]
async fn main() -> AppResult<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
        .init();

    let opts = Opts::from_args();

    let mut join: Vec<tokio::task::JoinHandle<Result<(), Report<AppError>>>> = vec![];

    for _ in 0..opts.threads {
        join.push(tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let entry_buf = (0..)
                .map(|v| v as u8)
                .take(opts.entry_size)
                .collect::<Vec<u8>>();

            let mut client = Client::connect(opts.common.server_addr, Some(LogOffset(0)))
                .await
                .report()
                .change_context(AppError)?;

            loop {
                let len = rng.gen_range(0..opts.entry_size);
                client
                    .append_nocommit(&entry_buf[0..len])
                    .await
                    .report()
                    .change_context(AppError)?;

                if opts.delay != 0 {
                    sleep(Duration::from_millis(rng.gen_range(0..opts.delay))).await;
                }
            }
        }));
    }

    for join in join {
        join.await.report().change_context(AppError)??;
    }

    Ok(())
}
