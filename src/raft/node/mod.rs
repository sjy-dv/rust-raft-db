mod candidate;
mod follower;
mod leader;

use super::{Address, Driver, Event, Index, Instruction, Log, Message, State};
use crate::error::Result;
use candidate::Candidate;
use follower::Follower;
use leader::Leader;

use ::log::{debug, info};
use rand::Rng as _;
use serde_derive::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

/// A node ID.
pub type NodeID = u8;

/// A leader term.
pub type Term = u64;

/// A logical clock interval as number of ticks.
pub type Ticks = u8;

/// The interval between leader heartbeats, in ticks.
const HEARTBEAT_INTERVAL: Ticks = 3;

/// The randomized election timeout range (min-max), in ticks. This is
/// randomized per node to avoid ties.
const ELECTION_TIMEOUT_RANGE: std::ops::Range<u8> = 10..20;

/// Generates a randomized election timeout.
fn rand_election_timeout() -> Ticks {
    rand::thread_rng().gen_range(ELECTION_TIMEOUT_RANGE)
}

/// Node status
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub server: NodeID,
    pub leader: NodeID,
    pub term: Term,
    pub node_last_index: HashMap<NodeID, Index>,
    pub commit_index: Index,
    pub apply_index: Index,
    pub storage: String,
    pub storage_size: u64,
}

/// The local Raft node state machine.
pub enum Node {
    Candidate(RoleNode<Candidate>),
    Follower(RoleNode<Follower>),
    Leader(RoleNode<Leader>),
}

impl Node {
    /// Creates a new Raft node, starting as a follower, or leader if no peers.
    pub async fn new(
        id: NodeID,
        peers: HashSet<NodeID>,
        mut log: Log,
        mut state: Box<dyn State>,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<Self> {
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let mut driver = Driver::new(id, state_rx, node_tx.clone());
        driver.apply_log(&mut *state, &mut log)?;
        tokio::spawn(driver.drive(state));

        let (term, voted_for) = log.get_term()?;
        let mut node = RoleNode {
            id,
            peers,
            term,
            log,
            node_tx,
            state_tx,
            role: Follower::new(None, voted_for),
        };
        if node.peers.is_empty() {
            info!("No peers specified, starting as leader");
            // If we didn't vote for ourself in the persisted term, bump the
            // term and vote for ourself to ensure we have a valid leader term.
            if voted_for != Some(id) {
                node.term += 1;
                node.log.set_term(node.term, Some(id))?;
            }
            let (last_index, _) = node.log.get_last_index();
            Ok(node.become_role(Leader::new(HashSet::new(), last_index)).into())
        } else {
            Ok(node.into())
        }
    }

    /// Returns the node ID.
    pub fn id(&self) -> NodeID {
        match self {
            Node::Candidate(n) => n.id,
            Node::Follower(n) => n.id,
            Node::Leader(n) => n.id,
        }
    }

    /// Processes a message.
    pub fn step(self, msg: Message) -> Result<Self> {
        debug!("Stepping {:?}", msg);
        match self {
            Node::Candidate(n) => n.step(msg),
            Node::Follower(n) => n.step(msg),
            Node::Leader(n) => n.step(msg),
        }
    }

    /// Moves time forward by a tick.
    pub fn tick(self) -> Result<Self> {
        match self {
            Node::Candidate(n) => n.tick(),
            Node::Follower(n) => n.tick(),
            Node::Leader(n) => n.tick(),
        }
    }
}

impl From<RoleNode<Candidate>> for Node {
    fn from(rn: RoleNode<Candidate>) -> Self {
        Node::Candidate(rn)
    }
}

impl From<RoleNode<Follower>> for Node {
    fn from(rn: RoleNode<Follower>) -> Self {
        Node::Follower(rn)
    }
}

impl From<RoleNode<Leader>> for Node {
    fn from(rn: RoleNode<Leader>) -> Self {
        Node::Leader(rn)
    }
}

// A Raft node with role R
pub struct RoleNode<R> {
    id: NodeID,
    peers: HashSet<NodeID>,
    term: Term,
    log: Log,
    node_tx: mpsc::UnboundedSender<Message>,
    state_tx: mpsc::UnboundedSender<Instruction>,
    role: R,
}

impl<R> RoleNode<R> {
    /// Transforms the node into another role.
    fn become_role<T>(self, role: T) -> RoleNode<T> {
        RoleNode {
            id: self.id,
            peers: self.peers,
            term: self.term,
            log: self.log,
            node_tx: self.node_tx,
            state_tx: self.state_tx,
            role,
        }
    }

    /// Returns the quorum size of the cluster.
    fn quorum(&self) -> u64 {
        (self.peers.len() as u64 + 1) / 2 + 1
    }

    /// Sends an event
    fn send(&self, to: Address, event: Event) -> Result<()> {
        let msg = Message { term: self.term, from: Address::Node(self.id), to, event };
        debug!("Sending {:?}", msg);
        Ok(self.node_tx.send(msg)?)
    }

    /// Asserts common node invariants.
    fn assert_node(&mut self) -> Result<()> {
        debug_assert_eq!(self.term, self.log.get_term()?.0, "Term does not match log");
        Ok(())
    }

    /// Asserts message invariants when stepping.
    ///
    /// In a real production database, these should be errors instead, since
    /// external input from the network can't be trusted to uphold invariants.
    fn assert_step(&self, msg: &Message) {
        // Messages must be addressed to the local node, or a broadcast.
        match msg.to {
            Address::Broadcast => {}
            Address::Client => panic!("Message to client"),
            Address::Node(id) => assert_eq!(id, self.id, "Message to other node"),
        }

        match msg.from {
            // The broadcast address can't send anything.
            Address::Broadcast => panic!("Message from broadcast address"),
            // Clients can only send ClientRequest without a term.
            Address::Client => {
                assert_eq!(msg.term, 0, "Client message with term");
                assert!(
                    matches!(msg.event, Event::ClientRequest { .. }),
                    "Non-request message from client"
                );
            }
            // Nodes must be known, and must include their term.
            Address::Node(id) => {
                assert!(id == self.id || self.peers.contains(&id), "Unknown sender {}", id);
                // TODO: For now, accept ClientResponse without term, since the
                // state driver does not have access to it.
                assert!(
                    msg.term > 0 || matches!(msg.event, Event::ClientResponse { .. }),
                    "Message without term"
                );
            }
        }
    }
}
