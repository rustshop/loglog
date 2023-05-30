use loglogd_api::{NodeId, TermId};

#[allow(unused)] // TODO
#[derive(Default, Clone, Copy, PartialEq, Eq)]
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

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PersistentState {
    /// Our own `NodeId`
    pub id: NodeId,
    /// Current term we're aware of
    pub current_term: TermId,
    /// The peer we voted for in the `current_terrm`
    pub voted_for: Option<NodeId>,
}

#[allow(unused)] // TODO
pub struct VolatileState {}
