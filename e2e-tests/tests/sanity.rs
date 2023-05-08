use anyhow::anyhow;
use anyhow::Result;
use loglog::LogOffset;
use loglogd::node::Parameters;
use std::io;
use std::path::Path;
use std::{net::SocketAddr, str::FromStr};
use tokio::test;
use tracing_subscriber::prelude::*;

pub struct TestLoglogd {
    data_dir: tempfile::TempDir,
    node_ctrl: loglogd::node::NodeCtrl,
}

impl TestLoglogd {
    pub fn new() -> anyhow::Result<Self> {
        let dir = tempfile::tempdir()?;
        let params = Parameters::builder().data_dir(dir.path().to_owned());
        let node_ctrl = loglogd::start(params.build(), SocketAddr::from_str("[::]:0")?)
            .map_err(|e| anyhow!(e))?;
        Ok(Self {
            data_dir: dir,
            node_ctrl,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.node_ctrl.local_addr()
    }

    pub fn data_dir(&self) -> &Path {
        self.data_dir.path()
    }

    pub async fn new_client(&self) -> Result<loglog::Client> {
        Ok(loglog::Client::connect(self.local_addr(), Some(LogOffset(0))).await?)
    }
}

impl Drop for TestLoglogd {
    fn drop(&mut self) {
        self.node_ctrl.stop()
    }
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

#[test]
async fn basic_sanity() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut client = server.new_client().await?;

    client.append(&[1, 2, 3]).await?;
    client.append(&[4, 3, 2]).await?;
    assert_eq!(client.next_raw().await?, [1, 2, 3]);
    assert_eq!(client.next_raw().await?, [4, 3, 2]);

    Ok(())
}

#[test]
async fn basic_concurrent() -> anyhow::Result<()> {
    // init_logging();

    let server = TestLoglogd::new()?;

    let mut writer_client = server.new_client().await?;
    let mut reader_client = server.new_client().await?;

    let writer_task = tokio::spawn(async move {
        for b in 0u8..100 {
            let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

            writer_client.append(&msg).await.unwrap();
        }
    });

    let reader_task = tokio::spawn(async move {
        for b in 0u8..100 {
            let msg: Vec<u8> = std::iter::repeat(b).take(b as usize).collect();

            assert_eq!(reader_client.next_raw().await.unwrap(), &msg);
        }
    });

    reader_task.await?;
    writer_task.await?;

    Ok(())
}
