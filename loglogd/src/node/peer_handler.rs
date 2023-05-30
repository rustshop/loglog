use std::{
    io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use binrw::{io::NoSeek, BinWrite};
use loglogd_api::{AllocationId, LogOffset};
use tokio::{
    net::{TcpListener, TcpStream},
    time::{self, sleep, timeout},
};
use tracing::{info, warn};

use crate::{raft, task::AutoJoinHandle, NodeShared};

pub struct PeerHandler {
    local_addr: SocketAddr,
    #[allow(unused)]
    join_handle: AutoJoinHandle,
}

impl PeerHandler {
    pub fn new(
        shared: Arc<NodeShared>,
        listen_addr: SocketAddr,
        peers: Vec<SocketAddr>,
    ) -> anyhow::Result<Self> {
        let rt = tokio::runtime::Runtime::new()?;

        let (tx, rx) = flume::bounded(1);

        let join_handle = AutoJoinHandle::spawn_res(move || -> Result<(), io::Error> {
            let _guard = scopeguard::guard((), |_| {
                info!("RequestHandler is done");
            });
            let res: Result<(), io::Error> = rt.block_on(async {
                let listener = tokio::net::TcpListener::bind(listen_addr).await?;

                let peer_outgoing_handles: Vec<_> = peers
                    .iter()
                    .copied()
                    .map({
                        |peer| {
                            let egress = PeerHandlerEgress {
                                last_log_offset: None,
                                shared: shared.clone(),
                                peer,
                            };

                            tokio::spawn(async move {
                                let guard = egress.shared.panic_guard("peer-outgoing");
                                egress.run().await;
                                guard.done();
                            })
                        }
                    })
                    .collect();

                tx.send(listener.local_addr()?)
                    .expect("local_addr rx not there?");

                (PeerHandlerIngress { shared }).run(listener).await;

                for join_handle in peer_outgoing_handles {
                    join_handle.await?;
                }

                Ok(())
            });

            res?;

            info!("Waiting for PeerHandler to complete all connections...");
            rt.shutdown_timeout(Duration::from_secs(60));

            Ok(())
        });

        let local_addr = rx.recv()?;

        Ok(Self {
            join_handle,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

pub struct PeerHandlerIngress {
    shared: Arc<NodeShared>,
}

impl PeerHandlerIngress {
    async fn run(&mut self, listener: TcpListener) {
        // TODO
        while !self.shared.is_node_shutting_down() {
            sleep(Duration::from_secs(1)).await;
        }
    }
}

pub struct PeerHandlerEgress {
    shared: Arc<NodeShared>,
    /// Last log offset we think the peer hash
    last_log_offset: Option<AllocationId>,
    peer: SocketAddr,
}

impl PeerHandlerEgress {
    /// Handle outgoing peer connection
    ///
    /// Over the outgoing connection we:
    ///
    /// * send updates & heartbeats (if leader), orignally called "AppendEntries RPC"
    /// * send the "RequestVote RPC" requests
    async fn run(&self) {
        const HEARTBEAT_PERIOD: Duration = Duration::from_millis(100);
        let mut raft_state_change_rx = self.shared.raft_state_change_rx.clone();
        let mut fsynced_log_offset_rx = self.shared.fsynced_log_offset_rx.clone();
        let mut current_state = *raft_state_change_rx.borrow_and_update();

        // We can't do anything if we don't have a network connection, so reconnecting is the outter loop
        'reconnect: loop {
            let Some(mut conn) = self.connect_peer_retry().await else { break; };
            // anytime we connect, we assume we need to send the heartbeat immediately
            let mut next_heartbeat = Instant::now();

            // while having a connection open, handle all the events
            loop {
                let now = Instant::now();
                let heartbeat_timeout = next_heartbeat.saturating_duration_since(now);

                tokio::select! {
                    _ = time::sleep(heartbeat_timeout), if current_state == raft::PeerState::Leader => {
                        let Ok(()) = self.send_update(&mut conn).await else {
                            break 'reconnect;
                        };
                        next_heartbeat = now + HEARTBEAT_PERIOD;
                    }

                    _ = fsynced_log_offset_rx.changed(), if current_state == raft::PeerState::Leader => {
                        let Ok(()) = self.send_update(&mut conn).await else {
                            break 'reconnect;
                        };
                        next_heartbeat = now + HEARTBEAT_PERIOD;
                    }

                    new_state = raft_state_change_rx.changed() => match new_state {
                        Ok(()) => {
                            let new_state = *raft_state_change_rx.borrow();
                            let Ok(()) = self.handle_entering_state(&mut conn, new_state).await else {
                                break 'reconnect;
                            };
                            current_state = new_state;
                        },
                        Err(_) => {
                            info!("raft_state_change disconnected");
                            return;
                        },
                    }
                }
            }
        }
    }

    /// Send the current "AppendEntries RPC"
    async fn send_update(&self, conn: &mut TcpStream) -> anyhow::Result<()> {
        let last_fsynced_offset = self.shared.fsynced_log_offset();

        let mut buf = [0; 1234];
        (loglogd_api::peer::Update {
            current_term: self.shared.persistent_state.current_term,
            // TODO
            start: self.last_log_offset.expect("TODO: Not implemented"),
            end: last_fsynced_offset,
        })
        .write(&mut NoSeek::new(&mut buf[..]))
        .expect("Can't fail");

        Ok(())
    }

    /// React to peer state change
    async fn handle_entering_state(
        &self,
        _conn: &mut TcpStream,
        _state: raft::PeerState,
    ) -> anyhow::Result<()> {
        todo!();
    }

    async fn connect_peer_retry(&self) -> Option<TcpStream> {
        let conn = loop {
            match timeout(Duration::from_secs(1), TcpStream::connect(self.peer)).await {
                Ok(Ok(conn)) => break conn,
                Ok(Err(e)) => {
                    warn!("Failed to connect to peer {}: {e}", self.peer);
                }
                Err(_e) => {
                    warn!("Failed to connect to peer {}: timeout", self.peer);
                }
            }
        };

        Some(conn)
    }
}
