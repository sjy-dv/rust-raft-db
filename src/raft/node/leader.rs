use super::super::{Address, Event, Index, Instruction, Message, Request, Response, Status};
use super::{Follower, Node, NodeID, RoleNode, Term, Ticks, HEARTBEAT_INTERVAL};
use crate::error::Result;

use ::log::{debug, info};
use std::collections::{HashMap, HashSet};

/// Peer replication progress.
#[derive(Debug)]
struct Progress {
    /// The next index to replicate to the peer.
    next: Index,
    /// The last index known to be replicated to the peer.
    last: Index,
}

// A leader serves requests and replicates the log to followers.
#[derive(Debug)]
pub struct Leader {
    /// Peer replication progress.
    progress: HashMap<NodeID, Progress>,
    /// Number of ticks since last periodic heartbeat.
    since_heartbeat: Ticks,
}

impl Leader {
    /// Creates a new leader role.
    pub fn new(peers: HashSet<NodeID>, last_index: Index) -> Self {
        let next = last_index + 1;
        let progress = peers.into_iter().map(|p| (p, Progress { next, last: 0 })).collect();
        Self { progress, since_heartbeat: 0 }
    }
}

impl RoleNode<Leader> {
    /// Asserts internal invariants.
    fn assert(&mut self) -> Result<()> {
        self.assert_node()?;

        assert_ne!(self.term, 0, "Leaders can't have term 0");
        debug_assert_eq!(Some(self.id), self.log.get_term()?.1, "Log vote does not match self");

        Ok(())
    }

    /// Transforms the leader into a follower. This can only happen if we find a
    /// new term, so we become a leaderless follower.
    fn become_follower(mut self, term: Term) -> Result<RoleNode<Follower>> {
        assert!(term >= self.term, "Term regression {} -> {}", self.term, term);
        assert!(term > self.term, "Can only become follower in later term");

        info!("Discovered new term {}", term);
        self.term = term;
        self.log.set_term(term, None)?;
        self.state_tx.send(Instruction::Abort)?;
        Ok(self.become_role(Follower::new(None, None)))
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
            return self.become_follower(msg.term)?.step(msg);
        }

        match msg.event {
            // There can't be two leaders in the same term.
            Event::Heartbeat { .. } | Event::AppendEntries { .. } => {
                panic!("Saw other leader {} in term {}", msg.from.unwrap(), msg.term);
            }

            // A follower received one of our heartbeats and confirms that we
            // are its leader. If it doesn't have the commit index in its local
            // log, replicate the log to it.
            Event::ConfirmLeader { commit_index, has_committed } => {
                let from = msg.from.unwrap();
                self.state_tx.send(Instruction::Vote {
                    term: msg.term,
                    index: commit_index,
                    address: msg.from,
                })?;
                if !has_committed {
                    self.send_log(from)?;
                }
            }

            // A follower appended log entries we sent it. Record its progress
            // and attempt to commit new entries.
            Event::AcceptEntries { last_index } => {
                assert!(
                    last_index <= self.log.get_last_index().0,
                    "Follower accepted entries after last index"
                );

                let from = msg.from.unwrap();
                self.role.progress.entry(from).and_modify(|p| {
                    p.last = last_index;
                    p.next = last_index + 1;
                });
                self.maybe_commit()?;
            }

            // A follower rejected log entries we sent it, typically because it
            // does not have the base index in its log. Try to replicate from
            // the previous entry.
            //
            // This linear probing, as described in the Raft paper, can be very
            // slow with long divergent logs, but we keep it simple.
            Event::RejectEntries => {
                let from = msg.from.unwrap();
                self.role.progress.entry(from).and_modify(|p| {
                    if p.next > 1 {
                        p.next -= 1
                    }
                });
                self.send_log(from)?;
            }

            Event::ClientRequest { id, request: Request::Query(command) } => {
                let (commit_index, _) = self.log.get_commit_index();
                self.state_tx.send(Instruction::Query {
                    id,
                    address: msg.from,
                    command,
                    term: self.term,
                    index: commit_index,
                    quorum: self.quorum(),
                })?;
                self.state_tx.send(Instruction::Vote {
                    term: self.term,
                    index: commit_index,
                    address: Address::Node(self.id),
                })?;
                self.heartbeat()?;
            }

            Event::ClientRequest { id, request: Request::Mutate(command) } => {
                let index = self.propose(Some(command))?;
                self.state_tx.send(Instruction::Notify { id, address: msg.from, index })?;
                if self.peers.is_empty() {
                    self.maybe_commit()?;
                }
            }

            Event::ClientRequest { id, request: Request::Status } => {
                let engine_status = self.log.status()?;
                let status = Box::new(Status {
                    server: self.id,
                    leader: self.id,
                    term: self.term,
                    node_last_index: self
                        .role
                        .progress
                        .iter()
                        .map(|(id, p)| (*id, p.last))
                        .chain(std::iter::once((self.id, self.log.get_last_index().0)))
                        .collect(),
                    commit_index: self.log.get_commit_index().0,
                    apply_index: 0,
                    storage: engine_status.name.clone(),
                    storage_size: engine_status.size,
                });
                self.state_tx.send(Instruction::Status { id, address: msg.from, status })?
            }

            Event::ClientResponse { id, mut response } => {
                if let Ok(Response::Status(ref mut status)) = response {
                    status.server = self.id;
                }
                self.send(Address::Client, Event::ClientResponse { id, response })?;
            }

            // Votes can come in after we won the election, ignore them.
            Event::SolicitVote { .. } | Event::GrantVote => {}
        }

        Ok(self.into())
    }

    /// Processes a logical clock tick.
    pub fn tick(mut self) -> Result<Node> {
        self.assert()?;

        self.role.since_heartbeat += 1;
        if self.role.since_heartbeat >= HEARTBEAT_INTERVAL {
            self.heartbeat()?;
            self.role.since_heartbeat = 0;
        }
        Ok(self.into())
    }

    /// Broadcasts a heartbeat to all peers.
    pub(super) fn heartbeat(&mut self) -> Result<()> {
        let (commit_index, commit_term) = self.log.get_commit_index();
        self.send(Address::Broadcast, Event::Heartbeat { commit_index, commit_term })?;
        // NB: We don't reset self.since_heartbeat here, because we want to send
        // periodic heartbeats regardless of any on-demand heartbeats.
        Ok(())
    }

    /// Proposes a command for consensus by appending it to our log and
    /// replicating it to peers. If successful, it will eventually be committed
    /// and applied to the state machine.
    pub(super) fn propose(&mut self, command: Option<Vec<u8>>) -> Result<Index> {
        let index = self.log.append(self.term, command)?;
        for peer in self.peers.clone() {
            self.send_log(peer)?;
        }
        Ok(index)
    }

    /// Commits any new log entries that have been replicated to a quorum,
    /// and schedules them for state machine application.
    fn maybe_commit(&mut self) -> Result<Index> {
        // Determine the new commit index, i.e. the last index replicated to a
        // quorum of peers.
        let mut last_indexes = self
            .role
            .progress
            .values()
            .map(|p| p.last)
            .chain(std::iter::once(self.log.get_last_index().0))
            .collect::<Vec<_>>();
        last_indexes.sort_unstable();
        last_indexes.reverse();
        let commit_index = last_indexes[self.quorum() as usize - 1];

        // A 0 commit index means we haven't committed anything yet.
        if commit_index == 0 {
            return Ok(commit_index);
        }

        // Make sure the commit index does not regress.
        let (prev_commit_index, _) = self.log.get_commit_index();
        assert!(
            commit_index >= prev_commit_index,
            "Commit index regression {} -> {}",
            prev_commit_index,
            commit_index
        );

        // We can only safely commit up to an entry from our own term, see
        // figure 8 in Raft paper.
        match self.log.get(commit_index)? {
            Some(entry) if entry.term == self.term => {}
            Some(_) => return Ok(prev_commit_index),
            None => panic!("Commit index {} missing", commit_index),
        };

        // Commit and apply the new entries.
        if commit_index > prev_commit_index {
            self.log.commit(commit_index)?;
            // TODO: Move application elsewhere, but needs access to applied index.
            let mut scan = self.log.scan((prev_commit_index + 1)..=commit_index)?;
            while let Some(entry) = scan.next().transpose()? {
                self.state_tx.send(Instruction::Apply { entry })?;
            }
        }
        Ok(commit_index)
    }

    /// Sends pending log entries to a peer.
    fn send_log(&mut self, peer: NodeID) -> Result<()> {
        let (base_index, base_term) = match self.role.progress.get(&peer) {
            Some(Progress { next, .. }) if *next > 1 => match self.log.get(next - 1)? {
                Some(entry) => (entry.index, entry.term),
                None => panic!("Missing base entry {}", next - 1),
            },
            Some(_) => (0, 0),
            None => panic!("Unknown peer {}", peer),
        };

        let entries = self.log.scan((base_index + 1)..)?.collect::<Result<Vec<_>>>()?;
        debug!("Replicating {} entries at base {} to {}", entries.len(), base_index, peer);
        self.send(Address::Node(peer), Event::AppendEntries { base_index, base_term, entries })?;
        Ok(())
    }
}