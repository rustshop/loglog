use anyhow::anyhow;
use anyhow::Result;
use loglog::LogOffset;
use loglogd::node::Parameters;
use std::path::Path;
use std::{net::SocketAddr, str::FromStr};
use tokio::test;

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

#[test]
async fn sanity_x() -> anyhow::Result<()> {
    let server = TestLoglogd::new()?;

    println!("{}", server.node_ctrl.local_addr());

    let mut client = server.new_client().await?;

    client.append(&[1, 2, 3]).await?;
    assert_eq!(client.next_raw().await?, [1, 2]);

    Ok(())
}
