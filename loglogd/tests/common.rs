#![allow(unused)]

use anyhow::Result;
use loglogd::Parameters;
use std::path::Path;
use std::{net::SocketAddr, str::FromStr};

pub struct TestLoglogd {
    #[allow(unused)]
    data_dir: tempfile::TempDir,
    node: loglogd::Node,
}

impl TestLoglogd {
    pub fn new() -> anyhow::Result<Self> {
        let dir = tempfile::tempdir()?;
        let params = Parameters::builder().data_dir(dir.path().to_owned());
        let node = loglogd::Node::new(SocketAddr::from_str("[::]:0")?, params.build())?;
        Ok(Self {
            data_dir: dir,
            node,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.node.get_ctrl().local_addr()
    }

    pub fn data_dir(&self) -> &Path {
        self.data_dir.path()
    }

    pub async fn new_client_async(&self) -> Result<loglog_tokio::RawClient> {
        Ok(loglog_tokio::RawClient::connect(self.local_addr(), None).await?)
    }

    pub fn new_client(&self) -> Result<loglog::RawClient> {
        Ok(loglog::RawClient::connect(self.local_addr(), None)?)
    }
}

impl Drop for TestLoglogd {
    fn drop(&mut self) {
        self.node.get_ctrl().stop()
    }
}
