#![allow(unused)]

use anyhow::Result;
use loglogd::Parameters;
use std::path::Path;
use std::{net::SocketAddr, str::FromStr};

pub struct TestLoglogd {
    #[allow(unused)]
    data_dir: tempfile::TempDir,
    node: Option<loglogd::Node>,
}

impl TestLoglogd {
    pub fn new() -> anyhow::Result<Self> {
        let dir = tempfile::tempdir()?;
        let params = Parameters::builder().data_dir(dir.path().to_owned());
        let node = loglogd::Node::new(params.build())?;
        Ok(Self {
            data_dir: dir,
            node: Some(node),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.node().get_ctrl().rpc_addr()
    }

    pub fn node(&self) -> &loglogd::Node {
        self.node.as_ref().expect("Node was already dropped")
    }

    pub fn data_dir(&self) -> &Path {
        self.data_dir.path()
    }

    pub async fn new_client_async(&self) -> Result<loglog::tokio::RawClient> {
        Ok(loglog::tokio::RawClient::connect(self.local_addr(), None).await?)
    }

    pub fn new_client(&self) -> Result<loglog::std::RawClient> {
        Ok(loglog::std::RawClient::connect(self.local_addr(), None)?)
    }
}

impl Drop for TestLoglogd {
    fn drop(&mut self) {
        self.node().get_ctrl().stop();
        self.node.take(); // drop before tmp directory is cleaned
    }
}
