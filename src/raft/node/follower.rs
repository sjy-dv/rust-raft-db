use super::super::{Address, Event, Instruction, Message, RequestID, Response};
use super::{rand_election_timeout, Candidate, Node, NodeID, RoleNode, Term, Ticks};
use crate::error::{Error, Result};

use ::log::{debug, error, info, warn};
use std::collections::HashSet;

// A follower replicates state from a leader.
#[derive(Debug)]
pub struct Follower {
    /// The leader, or None if just initialized.
    leader: Option<NodeID>,
    /// The number of ticks since the last message from the leader.
    leader_seen: Ticks,
    /// The leader_seen timeout before triggering an election.
    election_timeout: Ticks,
    /// The node we voted for in the current term, if any.
    voted_for: Option<NodeID>,
    // Local client requests that have been forwarded to the leader. These are
    // aborted on leader/term changes.
    pub(super) forwarded: HashSet<RequestID>,
}

impl Follower {
    /// Creates a new follower role.
    pub fn new(leader: Option<NodeID>, voted_for: Option<NodeID>) -> Self {
        Self {
            leader,
            voted_for,
            leader_seen: 0,
            election_timeout: rand_election_timeout(),
            forwarded: HashSet::new(),
        }
    }
}

impl RoleNode<Follower> {
    /// Asserts internal invariants.
    fn assert(&mut self) -> Result<()> {
        self.assert_node()?;

        if let Some(leader) = self.role.leader {
            assert_ne!(leader, self.id, "Can't follow self");
            assert!(self.peers.contains(&leader), "Leader not in peers");
            assert_ne!(self.term, 0, "Followers with leaders can't have term 0");
        } else {
            assert!(self.role.forwarded.is_empty(), "Leaderless follower has forwarded requests");
        }

        // NB: We allow voted_for not in peers, since this can happen when
        // removing nodes from the cluster via a cold restart. We also allow
        // voted_for self, which can happen if we lose an election.

        debug_assert_eq!(self.role.voted_for, self.log.get_term()?.1, "Vote does not match log");
        assert!(self.role.leader_seen < self.role.election_timeout, "Election timeout passed");

        Ok(())
    }

    /// Transforms the node into a candidate, by campaigning for leadership in a
    /// new term.
    fn become_candidate(mut self) -> Result<RoleNode<Candidate>> {
        // Abort any forwarded requests. These must be retried with new leader.
        self.abort_forwarded()?;

        let mut node = self.become_role(Candidate::new());
        node.campaign()?;
        Ok(node)
    }

    /// Transforms the node into a follower, either a leaderless follower in a
    /// new term or following a leader in the current term.
    fn become_follower(mut self, leader: Option<NodeID>, term: Term) -> Result<RoleNode<Follower>> {
        assert!(term >= self.term, "Term regression {} -> {}", self.term, term);

        // Abort any forwarded requests. These must be retried with new leader.
        self.abort_forwarded()?;

        if let Some(leader) = leader {
            // We found a leader in the current term.
            assert_eq!(self.role.leader, None, "Already have leader in term");
            assert_eq!(term, self.term, "Can't follow leader in different term");
            info!("Following leader {} in term {}", leader, term);
            self.role = Follower::new(Some(leader), self.role.voted_for);
        } else {
            // We found a new term, but we don't necessarily know who the leader
            // is yet. We'll find out when we step a message from it.
            assert_ne!(term, self.term, "Can't become leaderless follower in current term");
            info!("Discovered new term {}", term);
            self.term = term;
            self.log.set_term(term, None)?;
            self.role = Follower::new(None, None);
        }
        Ok(self)
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
            return self.become_follower(None, msg.term)?.step(msg);
        }

        // Record when we last saw a message from the leader (if any).
        if self.is_leader(&msg.from) {
            self.role.leader_seen = 0
        }

        match msg.event {
            // The leader will send periodic heartbeats. If we don't have a
            // leader in this term yet, follow it. If the commit_index advances,
            // apply state transitions.
            Event::Heartbeat { commit_index, commit_term } => {
                // Check that the heartbeat is from our leader.
                let from = msg.from.unwrap();
                match self.role.leader {
                    Some(leader) => assert_eq!(from, leader, "Multiple leaders in term"),
                    None => self = self.become_follower(Some(from), msg.term)?,
                }

                // Advance commit index and apply entries if possible.
                let has_committed = self.log.has(commit_index, commit_term)?;
                let (old_commit_index, _) = self.log.get_commit_index();
                if has_committed && commit_index > old_commit_index {
                    self.log.commit(commit_index)?;
                    let mut scan = self.log.scan((old_commit_index + 1)..=commit_index)?;
                    while let Some(entry) = scan.next().transpose()? {
                        self.state_tx.send(Instruction::Apply { entry })?;
                    }
                }
                self.send(msg.from, Event::ConfirmLeader { commit_index, has_committed })?;
            }

            // Replicate entries from the leader. If we don't have a leader in
            // this term yet, follow it.
            Event::AppendEntries { base_index, base_term, entries } => {
                // Check that the entries are from our leader.
                let from = msg.from.unwrap();
                match self.role.leader {
                    Some(leader) => assert_eq!(from, leader, "Multiple leaders in term"),
                    None => self = self.become_follower(Some(from), msg.term)?,
                }

                // Append the entries, if possible.
                if base_index > 0 && !self.log.has(base_index, base_term)? {
                    debug!("Rejecting log entries at base {}", base_index);
                    self.send(msg.from, Event::RejectEntries)?
                } else {
                    let last_index = self.log.splice(entries)?;
                    self.send(msg.from, Event::AcceptEntries { last_index })?
                }
            }

            // A candidate in this term is requesting our vote.
            Event::SolicitVote { last_index, last_term } => {
                let from = msg.from.unwrap();

                // If we already voted for someone else in this term, ignore it.
                if let Some(voted_for) = self.role.voted_for {
                    if from != voted_for {
                        return Ok(self.into());
                    }
                }

                // Only vote if the candidate's log is at least as up-to-date as
                // our log.
                let (log_index, log_term) = self.log.get_last_index();
                if last_term > log_term || last_term == log_term && last_index >= log_index {
                    info!("Voting for {} in term {} election", from, self.term);
                    self.send(Address::Node(from), Event::GrantVote)?;
                    self.log.set_term(self.term, Some(from))?;
                    self.role.voted_for = Some(from);
                }
            }

            // Forward client requests to the leader, or abort them if there is
            // none (the client must retry).
            Event::ClientRequest { ref id, .. } => {
                if msg.from != Address::Client {
                    error!("Received client request from non-client {:?}", msg.from);
                    return Ok(self.into());
                }

                let id = id.clone();
                if let Some(leader) = self.role.leader {
                    debug!("Forwarding request to leader {}: {:?}", leader, msg);
                    self.role.forwarded.insert(id);
                    self.send(Address::Node(leader), msg.event)?
                } else {
                    self.send(msg.from, Event::ClientResponse { id, response: Err(Error::Abort) })?
                }
            }

            // Returns client responses for forwarded requests.
            Event::ClientResponse { id, mut response } => {
                if !self.is_leader(&msg.from) {
                    error!("Received client response from non-leader {:?}", msg.from);
                    return Ok(self.into());
                }

                // TODO: Get rid of this field, it should be returned at the RPC
                // server level instead.
                if let Ok(Response::Status(ref mut status)) = response {
                    status.server = self.id;
                }
                if self.role.forwarded.remove(&id) {
                    self.send(Address::Client, Event::ClientResponse { id, response })?;
                }
            }

            // We're not a leader nor candidate in this term, so we shoudn't see these.
            Event::ConfirmLeader { .. }
            | Event::AcceptEntries { .. }
            | Event::RejectEntries { .. }
            | Event::GrantVote { .. } => warn!("Received unexpected message {:?}", msg),
        };
        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node> {
        self.assert()?;

        self.role.leader_seen += 1;
        if self.role.leader_seen >= self.role.election_timeout {
            return Ok(self.become_candidate()?.into());
        }
        Ok(self.into())
    }

    /// Aborts all forwarded requests.
    fn abort_forwarded(&mut self) -> Result<()> {
        for id in std::mem::take(&mut self.role.forwarded) {
            debug!("Aborting forwarded request {:x?}", id);
            self.send(Address::Client, Event::ClientResponse { id, response: Err(Error::Abort) })?;
        }
        Ok(())
    }

    /// Checks if an address is the current leader.
    fn is_leader(&self, from: &Address) -> bool {
        if let Some(leader) = &self.role.leader {
            if let Address::Node(from) = from {
                return leader == from;
            }
        }
        false
    }
}