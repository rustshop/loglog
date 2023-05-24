use loglogd_api::{NodeId, TermId};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct PeerId(u8);

#[allow(unused)] // TODO
#[derive(Default)]
pub enum PeerState {
    #[default]
    // Follows the leader
    // Responds to read requests
    Follower,
    Candidate,
    // Leads
    // Handles writes
    Leader,
}

pub struct PersistentState {
    /// Our own `NodeId`
    pub id: NodeId,
    /// Current term we're aware of
    pub current_term: TermId,
    /// The peer we voted for in the `current_terrm`
    pub voted_for: Option<PeerId>,
}

#[allow(unused)] // TODO
pub struct VolatileState {}
