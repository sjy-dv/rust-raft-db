use super::super::{Address, Event, Message};
use super::{rand_election_timeout, Follower, Leader, Node, NodeID, RoleNode, Term, Ticks};
use crate::error::{Error, Result};

use ::log::{debug, info, warn};
use std::collections::HashSet;

/// A candidate is campaigning to become a leader.
#[derive(Debug)]
pub struct Candidate {
    /// Votes received (including ourself).
    votes: HashSet<NodeID>,
    /// Ticks elapsed since election start.
    election_duration: Ticks,
    /// Election timeout, in ticks.
    election_timeout: Ticks,
}

impl Candidate {
    /// Creates a new candidate role.
    pub fn new() -> Self {
        Self {
            votes: HashSet::new(),
            election_duration: 0,
            election_timeout: rand_election_timeout(),
        }
    }
}

impl RoleNode<Candidate> {
    /// Asserts internal invariants.
    fn assert(&mut self) -> Result<()> {
        self.assert_node()?;

        assert_ne!(self.term, 0, "Candidates can't have term 0");
        assert!(self.role.votes.contains(&self.id), "Candidate did not vote for self");
        debug_assert_eq!(Some(self.id), self.log.get_term()?.1, "Log vote does not match self");

        assert!(
            self.role.election_duration < self.role.election_timeout,
            "Election timeout passed"
        );

        Ok(())
    }

    /// Transforms the node into a follower. We either lost the election
    /// and follow the winner, or we discovered a new term in which case
    /// we step into it as a leaderless follower.
    fn become_follower(mut self, term: Term, leader: Option<NodeID>) -> Result<RoleNode<Follower>> {
        assert!(term >= self.term, "Term regression {} -> {}", self.term, term);

        if let Some(leader) = leader {
            // We lost the election, follow the winner.
            assert_eq!(term, self.term, "Can't follow leader in different term");
            info!("Lost election, following leader {} in term {}", leader, term);
            let voted_for = Some(self.id); // by definition
            Ok(self.become_role(Follower::new(Some(leader), voted_for)))
        } else {
            // We found a new term, but we don't necessarily know who the leader
            // is yet. We'll find out when we step a message from it.
            assert_ne!(term, self.term, "Can't become leaderless follower in current term");
            info!("Discovered new term {}", term);
            self.term = term;
            self.log.set_term(term, None)?;
            Ok(self.become_role(Follower::new(None, None)))
        }
    }

    /// Transition to leader role.
    fn become_leader(self) -> Result<RoleNode<Leader>> {
        info!("Won election for term {}, becoming leader", self.term);
        let peers = self.peers.clone();
        let (last_index, _) = self.log.get_last_index();
        let mut node = self.become_role(Leader::new(peers, last_index));
        node.heartbeat()?;

        // Propose an empty command when assuming leadership, to disambiguate
        // previous entries in the log. See section 8 in the Raft paper.
        node.propose(None)?;
        Ok(node)
    }

    /// Processes a message.
    pub fn step(mut self, msg: Message) -> Result<Node> {
        self.assert()?;
        self.assert_step(&msg);

        // Drop messages from past terms.
        if msg.term < self.term && msg.term > 0 {
            debug!("Dropping message from past term ({:?})", msg);
            return Ok(self.into());
        }

        // If we receive a message for a future term, become a leaderless
        // follower in it and step the message. If the message is a Heartbeat or
        // AppendEntries from the leader, stepping it will follow the leader.
        if msg.term > self.term {
            return self.become_follower(msg.term, None)?.step(msg);
        }

        match msg.event {
            // Ignore other candidates when we're also campaigning.
            Event::SolicitVote { .. } => {}

            // We received a vote. Record it, and if we have quorum, assume
            // leadership.
            Event::GrantVote => {
                self.role.votes.insert(msg.from.unwrap());
                if self.role.votes.len() as u64 >= self.quorum() {
                    return Ok(self.become_leader()?.into());
                }
            }

            // If we receive a heartbeat or entries in this term, we lost the
            // election and have a new leader. Follow it and step the message.
            Event::Heartbeat { .. } | Event::AppendEntries { .. } => {
                return self.become_follower(msg.term, Some(msg.from.unwrap()))?.step(msg);
            }

            // Abort any inbound client requests while candidate.
            Event::ClientRequest { id, .. } => {
                self.send(msg.from, Event::ClientResponse { id, response: Err(Error::Abort) })?;
            }

            // We're not a leader in this term, nor are we forwarding requests,
            // so we shouldn't see these.
            Event::ConfirmLeader { .. }
            | Event::AcceptEntries { .. }
            | Event::RejectEntries { .. }
            | Event::ClientResponse { .. } => warn!("Received unexpected message {:?}", msg),
        }
        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node> {
        self.assert()?;

        self.role.election_duration += 1;
        if self.role.election_duration >= self.role.election_timeout {
            self.campaign()?;
        }
        Ok(self.into())
    }

    /// Campaign for leadership by increasing the term, voting for ourself, and
    /// soliciting votes from all peers.
    pub(super) fn campaign(&mut self) -> Result<()> {
        let term = self.term + 1;
        info!("Starting new election for term {}", term);
        self.role = Candidate::new();
        self.role.votes.insert(self.id); // vote for ourself
        self.term = term;
        self.log.set_term(term, Some(self.id))?;

        let (last_index, last_term) = self.log.get_last_index();
        self.send(Address::Broadcast, Event::SolicitVote { last_index, last_term })?;
        Ok(())
    }
}