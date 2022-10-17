use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot;
use futures::future::Fuse;
use futures::task::SpawnExt;
use futures::{select, FutureExt, StreamExt};
use futures_timer::Delay;
use rand::Rng;
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

const HEARTBEAT_INTERVAL: u64 = 100;
const TIMEOUT_MIN: u64 = 200;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
#[derive(Debug)]
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

#[derive(PartialEq, Clone, Copy, Default)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

impl std::fmt::Debug for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ident = match self {
            Role::Follower => "Follower",
            Role::Candidate => "Candidate",
            Role::Leader => "Leader",
        };
        write!(f, "{}", ident)
    }
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub role: Role,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    pub fn is_candidate(&self) -> bool {
        self.role == Role::Candidate
    }

    #[allow(dead_code)]
    pub fn is_follower(&self) -> bool {
        self.role == Role::Follower
    }
}

// regular actions
enum Actions {
    SendHeartbeat,
    StartElection,
}

enum RepliesFrom {
    RequestVoteReplyFrom(u64, RequestVoteReply),
    AppendEntriesReplyFrom(u64, u64, AppendEntriesReply),
}

struct ResetTimer;

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: State,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    // vote infos
    voted_for: i64,
    voters: Vec<u64>,

    // the index this state machine should commit up to
    commit_index: u64,
    // index of the highest log entry, that this state machine has applied
    last_applied: u64,
    // index of the next log entry to send to i-th server
    next_index: Vec<u64>,
    // index of the highest log entry known to be replicated on i-th server
    match_index: Vec<u64>,
    // XXX: log
    log: Vec<LogEntry>,

    last_included_index: u64,
    last_included_term: u64,

    //todo: send applyMsg to upper layer application, rx is in kvstore
    #[allow(dead_code)]
    apply_tx: UnboundedSender<ApplyMsg>,
    // send regular actions, rx is in loop, tx in send_actions
    action_tx: Option<UnboundedSender<Actions>>,
    // send reply handler messages to invoke reply handlers
    reply_tx: Option<UnboundedSender<RepliesFrom>>,
    // reset timer channel, reset upon recv
    timer_tx: Option<UnboundedSender<ResetTimer>>,

    // thread pool, simulate go runtime
    tp: futures::executor::ThreadPool,
}

macro_rules! rfinfo {
    ($raft:expr, $($args:tt)+) => {
        info!("rf [me: {}] [state: {:?}], {}", $raft.me, $raft.state, format_args!($($args)+));
    };
}

macro_rules! rfdebug {
    ($raft:expr, $($args:tt)+) => {
        debug!("rf [me: {}] [state: {:?}], {}", $raft.me, $raft.state, format_args!($($args)+));
    };
}

macro_rules! rfpanic {
    ($raft:expr, $($args:tt)+) => {
        error!("rf [me: {}] [state: {:?}], {}", $raft.me, $raft.state, format_args!($($args)+));
        panic!();
    };
}

// macro_rules! rfpanic_on {
//     ($cond: expr, $raft: expr, $($args:tt)+) => {
//         if $cond {
//             rfpanic!($raft, $($args)+);
//         }
//     }
// }

macro_rules! rfwarn {
    ($raft:expr, $($args:tt)+) => {
        warn!("rf [me: {}] [state: {:?}], {}", $raft.me, $raft.state, format_args!($($args)+));
    };
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let npeers = peers.len();
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: State::default(),
            voted_for: -1,
            voters: vec![],
            // XXX: log entry index start with 1
            commit_index: 0,
            last_applied: 0,
            match_index: vec![0; npeers],
            next_index: vec![1; npeers],
            // XXX: log entry index start with 1
            log: vec![],
            last_included_index: 0,
            last_included_term: 0,
            action_tx: None,
            reply_tx: None,
            timer_tx: None,
            apply_tx: apply_ch,
            tp: futures::executor::ThreadPool::new().unwrap(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).
        if !self.is_leader() {
            return Err(Error::NotLeader);
        }
        rfinfo!(self, "Start replicating command {:?}", command);
        let entry = LogEntry {
            term: self.term(),
            rb: buf,
        };
        rfdebug!(
            self,
            "pushing log {:?} into the log at index {}",
            &entry,
            self.last_log_index_logical() + 1, // fuck checker
        );
        self.log.push(entry);
        self.reset_timer();
        self.persist();
        Ok((self.last_log_index_logical(), self.last_log_term()))
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }
}

// RPCs
impl Raft {
    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    #[allow(dead_code)]
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            let reply = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            tx.send(reply).unwrap();
        });
        rx
    }

    // Node::request_vote directs to here, handler
    fn request_vote_handler(&mut self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        let mut reply = RequestVoteReply {
            term: self.state.term(),
            granted: false,
        };

        if args.term < self.state.term() {
            // do not vote if sender is stale
            return Ok(reply);
        }

        if args.term > self.state.term() {
            // vote for no one, vote when next HB arrives
            self.turn_follower(args.term, Some(-1));
        }

        // if I have not vote, or I have vote for the sender, I will try vote for candidate
        // but only if the candidate is up-to-date, will I vote him
        if (self.vote_for_nobody() || self.voted_for == args.cid as i64)
            && self.candidate_up_to_date(&args)
        {
            // we can vote only to up-to-date candidates
            self.voted_for = args.cid as i64;
            reply.granted = true;
            self.reset_timer();
            self.persist();
        }

        reply.term = self.term();

        Ok(reply)
    }

    /// send AppendEntries RPC to a peer
    #[allow(dead_code)]
    fn send_append_entries(
        &self,
        server: usize,
        args: AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        let (tx, rx) = sync_channel::<Result<AppendEntriesReply>>(1);
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            let reply = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            tx.send(reply).unwrap();
        });
        rx
    }

    fn append_entries_handler(
        &mut self,
        args: AppendEntriesArgs,
    ) -> labrpc::Result<AppendEntriesReply> {
        let mut reply = AppendEntriesReply {
            term: self.state.term(),
            success: false,
            conflict_index: 0,
        };
        if args.term < self.state.term() {
            return Ok(reply);
        }

        if args.term > self.state.term()
            || (self.role() == Role::Candidate && args.term == self.term())
        {
            self.turn_follower(args.term, Some(-1));
        }
        self.reset_timer();

        // log replication when recv append_entries RPC
        if !self.is_follower() {
            rfpanic!(
                self,
                "candidate or leader should never recv append_entries by logic, args: {:?}",
                args,
            );
        }

        // matches the prefix of logs?
        let matches =
            self.term_at_logical(args.prev_log_index as usize) == Some(args.prev_log_term);

        if !matches {
            // the prefix is not match, the log has some conflicts...
            reply.success = false;

            // provide conflict index for leader
            reply.conflict_index = if self.last_log_index_logical() < args.prev_log_index {
                self.last_log_index_logical() + 1
            } else if args.prev_log_index > 0 {
                // ATTENTION: find the first log has the term of [the term of conflicted log]
                // since the logs before prev_log_index are thought to be sync
                let conflict_term = self.term_at_logical(args.prev_log_index as usize).unwrap();
                // todo(last_included_index)
                let mut conflict_index = 0;
                for index in self.last_included_index + 1..=args.prev_log_index {
                    if self.term_at_logical(index as usize).unwrap() == conflict_term {
                        conflict_index = index;
                        break;
                    }
                }
                conflict_index
            } else {
                0
            };
        } else {
            // the prefix matches, start replicating logs
            let mut consistent_with_leader = true;
            for (i, log) in args.log_entries.iter().enumerate() {
                // prev = 10, new logs: [11, 12, 13, ....]
                let logical_index = args.prev_log_index + (i as u64) + 1;
                if self.term_at_logical(logical_index as usize) != Some(log.term) {
                    consistent_with_leader = false;
                }
            }

            // if consistent with leader, we don't need to do anything on our logs
            // if inconsistence with leader, we force it
            if !consistent_with_leader {
                while self.last_log_index_logical() > args.prev_log_index {
                    self.log.pop().unwrap();
                }
                args.log_entries
                    .into_iter()
                    .for_each(|log| self.log.push(log));
            }

            // if !match, state is untouched
            self.persist();

            rfdebug!(self, "Replicated! log: {:?}", self.log);

            reply.success = true;
            self.update_commit_index(args.leader_commit);
            self.apply();
        }

        Ok(reply)
    }
}

// utils
impl Raft {
    fn reset_timer(&mut self) {
        self.timer_tx
            .as_ref()
            .unwrap()
            .unbounded_send(ResetTimer)
            .unwrap();
    }

    fn turn_follower(&mut self, new_term: u64, voted_for: Option<i64>) {
        self.state.role = Role::Follower;
        self.state.term = new_term;
        self.voters = vec![];
        if let Some(v) = voted_for {
            self.voted_for = v;
        }
        self.persist();
    }

    fn turn_candidate(&mut self) {
        self.state.role = Role::Candidate;
        self.state.term += 1;
        self.voters = vec![self.me as u64];
        self.voted_for = self.me as i64;
        self.persist();
    }

    fn turn_leader(&mut self) {
        self.state.role = Role::Leader;
    }

    fn is_leader(&self) -> bool {
        self.state.is_leader()
    }

    fn is_candidate(&self) -> bool {
        self.state.is_candidate()
    }

    fn is_follower(&self) -> bool {
        self.state.is_follower()
    }

    fn term(&self) -> u64 {
        self.state.term()
    }

    fn role(&self) -> Role {
        self.state.role
    }

    fn vote_for_nobody(&self) -> bool {
        self.voted_for == -1
    }

    /// returns whether the candidate's log is up to date
    fn candidate_up_to_date(&self, args: &RequestVoteArgs) -> bool {
        let my_last_log_term = match self.log.last() {
            None => 0,
            Some(e) => e.term,
        };

        // (term, log.len()) decides the precedence
        let cond1 = args.last_log_term > my_last_log_term;
        let cond2 = args.last_log_term == my_last_log_term
            && args.last_log_index >= (self.log.len() as u64);

        if cond1 || cond2 {
            return true;
        }

        false
    }

    fn index_logical_to_physical(&self, logical_index: usize) -> Option<usize> {
        logical_index
            .checked_sub(self.last_included_index as usize)
            .and_then(|a| a.checked_sub(1))
    }

    fn log_at_logical(&self, logical_index: usize) -> Option<LogEntry> {
        self.index_logical_to_physical(logical_index)
            .and_then(|p| self.log.get(p).cloned())
    }

    fn term_at_logical(&self, logical_index: usize) -> Option<u64> {
        if logical_index == self.last_included_index as usize {
            return Some(self.last_included_term);
        }
        self.index_logical_to_physical(logical_index)
            .and_then(|p| self.log.get(p).map(|l| l.term))
    }

    fn data_at_logical(&self, logical_index: usize) -> Option<Vec<u8>> {
        self.index_logical_to_physical(logical_index)
            .and_then(|p| self.log.get(p).map(|l| l.rb.clone()))
    }

    fn last_log_index_logical(&self) -> u64 {
        self.log.len() as u64 + self.last_included_index
    }

    fn last_log_term(&self) -> u64 {
        match self.log.last() {
            Some(l) => l.term,
            None => self.last_included_term,
        }
    }

    /// called after the log is replicated from leader, so last_log_index_logical() represends
    /// the index of last new entry
    fn update_commit_index(&mut self, leader_commit: u64) {
        if leader_commit > self.commit_index {
            self.commit_index = std::cmp::min(leader_commit, self.last_log_index_logical())
        }
        // rfinfo!(
        //     self,
        //     "new commit_index: {}, leader commit: {}, my last log index: {}",
        //     self.commit_index,
        //     leader_commit,
        //     self.last_log_index_logical()
        // );
    }

    fn valid_commit_index_from_majority(&self) -> u64 {
        let mut n = self.commit_index;
        for i in self.commit_index + 1..=self.last_log_index_logical() {
            // how many peers have >= i?
            let mut nmatches = 1; // me
            for j in 0..self.peers.len() {
                if j != self.me && self.match_index[j] >= i {
                    nmatches += 1;
                }
            }

            // there exists an N, > commit index, majority of matchIndex >== N
            // and log[N].term == currentTerm, set commitIndex = N
            if (nmatches > (self.peers.len() / 2))
                && (self.term_at_logical(i as usize) == Some(self.term()))
            {
                n = i;
                break;
            }
        }
        n
    }

    fn apply(&mut self) {
        if self.last_applied > self.commit_index {
            rfwarn!(
                self,
                "last_applied {} >= self.commit_index {}???",
                self.last_applied,
                self.commit_index
            );
        }
        // (last_applied, commit_index]
        let interval = self.last_applied + 1..=self.commit_index;
        for index in interval {
            let msg = ApplyMsg::Command {
                data: self.data_at_logical(index as usize).unwrap(),
                index,
            };
            rfinfo!(self, "applying {:?}", msg);
            self.apply_tx.unbounded_send(msg).unwrap();
            self.last_applied += 1;
        }
    }

    /// advance the commit index to majority's commit index
    /// and if updated, we try to apply that to state machine
    fn advance_commit_index_and_apply(&mut self) {
        self.commit_index = self.valid_commit_index_from_majority();
        rfinfo!(
            self,
            "successfully advance commit index to {}",
            self.commit_index
        );
        self.apply();
    }

    fn append_entries_args_for(&self, peer: u64) -> AppendEntriesArgs {
        let mut log_entries = vec![];
        let start_logical = self.next_index[peer as usize];
        let end_logical = self.last_log_index_logical();
        for i in start_logical..=end_logical {
            log_entries.push(self.log_at_logical(i as usize).unwrap_or_else(|| {
                rfpanic!(
                    self,
                    "next_index: {:?}, i: {}, start: {}, end: {}",
                    self.next_index,
                    i,
                    start_logical,
                    end_logical
                );
            }));
        }
        rfdebug!(self, "AE log entries for {}: {:?}", peer, log_entries);
        let prev_log_index = self.next_index[peer as usize] - 1;
        let prev_log_term = if prev_log_index == self.last_included_index {
            self.last_included_term
        } else {
            self.term_at_logical(prev_log_index as usize).unwrap()
        };
        AppendEntriesArgs {
            term: self.term(),
            leader_id: self.me as u64,
            leader_commit: self.commit_index,
            prev_log_term,
            prev_log_index,
            log_entries,
        }
    }
}

// actions
impl Raft {
    // poll from main loop, call this as handler when action_chan has a hb request
    fn send_heartbeat(&mut self) {
        if !self.is_leader() {
            return;
        }

        rfdebug!(self, "Sending heartbeat");

        // prev: 10, log:[11, 12], next = 13
        for i in 0..self.peers.len() {
            if i == self.me {
                continue;
            }

            let args = self.append_entries_args_for(i as u64);
            let next_index_on_success = args.prev_log_index + (args.log_entries.len()) as u64 + 1;

            let fut = self.peers[i].append_entries(&args);
            let reply_tx = self.reply_tx.as_ref().unwrap().clone();

            self.tp
                .spawn(async move {
                    if let Ok(reply) = fut.await {
                        reply_tx
                            .unbounded_send(RepliesFrom::AppendEntriesReplyFrom(
                                i as u64,
                                next_index_on_success,
                                reply,
                            ))
                            .unwrap()
                    }
                    // we also need to send the next_index for leader to update
                })
                .unwrap();
        }
    }

    fn start_election(&mut self) {
        if self.is_leader() {
            return;
        }
        rfdebug!(self, "starting election");
        self.turn_candidate();

        let args = RequestVoteArgs {
            term: self.term(),
            cid: self.me as u64,
            last_log_index: self.last_log_index_logical(),
            last_log_term: self.last_log_term(),
        };

        for i in 0..self.peers.len() {
            if i == self.me {
                continue;
            }

            // damn, how to pass the check w/o this shitty way...
            let fut = self.peers[i].request_vote(&args);
            let reply_tx = self.reply_tx.as_ref().unwrap().clone();

            self.tp
                .spawn(async move {
                    if let Ok(reply) = fut.await {
                        reply_tx
                            .unbounded_send(RepliesFrom::RequestVoteReplyFrom(i as u64, reply))
                            .unwrap();
                    }
                })
                .unwrap();
        }
    }
}

// handlers
impl Raft {
    fn mux_actions(&mut self, action: Actions) {
        match action {
            Actions::StartElection => self.start_election(),
            Actions::SendHeartbeat => self.send_heartbeat(),
        }
    }

    fn mux_replies(&mut self, reply_from: RepliesFrom) {
        match reply_from {
            RepliesFrom::RequestVoteReplyFrom(peer, reply) => {
                self.handle_request_vote_reply(peer, reply)
            }
            RepliesFrom::AppendEntriesReplyFrom(peer, next, reply) => {
                self.handle_append_entries_reply(peer, next, reply)
            }
        }
    }

    /// this is for election, after we send
    fn handle_request_vote_reply(&mut self, from: u64, reply: RequestVoteReply) {
        rfdebug!(self, "handling RV reply, reply: {:?}", reply);
        if reply.term > self.term() {
            self.turn_follower(reply.term, Some(-1));
        }

        if !self.is_candidate() {
            return;
        }

        if reply.term == self.term() && reply.granted {
            if !self.voters.contains(&from) {
                self.voters.push(from);
            }

            if self.voters.len() > self.peers.len() / 2 {
                self.turn_leader();
                self.send_heartbeat();
            }
        }
    }
    fn handle_append_entries_reply(
        &mut self,
        from: u64,
        next_index: u64,
        reply: AppendEntriesReply,
    ) {
        if reply.term > self.term() {
            self.turn_follower(reply.term, Some(-1));
        }
        //todo: handle log
        if !self.is_leader() {
            rfwarn!(
                self,
                "recv append entries reply from {} when I am not a leader",
                from
            );
            return;
        }
        // if success, means that the log sent is replicated on `from`
        if reply.success {
            // change(next_index): place 1
            self.next_index[from as usize] = next_index;
            self.match_index[from as usize] = next_index - 1;
            rfdebug!(
                self,
                "AE reply[from: {}] handler: success, next_index: {:?}, match_index: {:?}",
                from,
                self.next_index,
                self.match_index
            );
            self.advance_commit_index_and_apply(); // todo
        } else {
            // update the next index based on conflict index
            if reply.conflict_index == 0 {
                // invalid index, quit fast backup
                self.next_index[from as usize] =
                    self.next_index[from as usize].saturating_sub(1).max(1);
            } else {
                self.next_index[from as usize] = reply.conflict_index;
            }
            rfdebug!(
                self,
                "AE reply[from: {}] handler: failed, next_index: {:?}, match_index: {:?}",
                from,
                self.next_index,
                self.match_index
            );
        }
    }
}

// persist apis
impl Raft {
    fn pack_nvstate(&self) -> RaftNonVolatileState {
        // rfpanic_on!(
        //     self.voted_for == -1,
        //     self,
        //     "voted_for should never be -1 when persist"
        // );
        RaftNonVolatileState {
            current_term: self.term(),
            voted_for: self.voted_for,
            log: self.log.clone(),
            last_included_index: self.last_included_index,
            last_included_term: self.last_included_term,
        }
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        let nv_state = self.pack_nvstate();
        let mut state = vec![];
        labcodec::encode(&nv_state, &mut state).unwrap();
        self.persister.save_raft_state(state);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        match labcodec::decode(data) {
            Ok(nv_state) => {
                let nv_state: RaftNonVolatileState = nv_state;
                self.state.term = nv_state.current_term;
                self.voted_for = nv_state.voted_for as i64;
                self.log = nv_state.log;
                self.last_included_index = nv_state.last_included_index;
                self.last_included_term = nv_state.last_included_term;
            }

            Err(e) => {
                rfpanic!(self, "error decoding non-volatile data, err: {:?}", e);
            }
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        // let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    rf: Arc<Mutex<Raft>>,
    tp: futures::executor::ThreadPool,
    kill_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>, // fuck checker.....
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let (kill_tx, kill_rx) = oneshot::channel();
        let mut node = Node {
            rf: Arc::new(Mutex::new(raft)),
            tp: futures::executor::ThreadPool::new().unwrap(),
            kill_tx: Arc::new(Mutex::new(Some(kill_tx))),
        };

        node.timer();
        node.run(kill_rx.fuse());

        node
    }

    fn run(&mut self, mut kill_rx: Fuse<oneshot::Receiver<()>>) {
        let (action_tx, mut action_rx) = futures::channel::mpsc::unbounded();
        let (reply_tx, mut reply_rx) = futures::channel::mpsc::unbounded();

        // when raft needs to handle replies or actions, it sends to these channel, then,
        // the loop polls from these channels and call corresponding handlers
        let mut rf = self.rf.lock().unwrap();
        rf.action_tx = Some(action_tx);
        rf.reply_tx = Some(reply_tx);
        rf.reset_timer();
        drop(rf);

        let rf = self.rf.clone();
        // poll from channels' rx
        self.tp
            .spawn(async move {
                loop {
                    select! {
                        action = action_rx.select_next_some() => {
                            rf.lock().unwrap().mux_actions(action);
                        }

                        reply = reply_rx.select_next_some() => {
                            rf.lock().unwrap().mux_replies(reply);
                        }

                        _ = kill_rx => {
                            break;
                        }
                    }
                }
            })
            .unwrap();
    }

    fn timer(&mut self) {
        let (timer_tx, mut timer_rx) = futures::channel::mpsc::unbounded();

        let mut rf = self.rf.lock().unwrap();
        rf.timer_tx = Some(timer_tx);
        drop(rf);

        let rf = self.rf.clone();

        // two timers, heartbeat timer and timeout timer
        let mut heartbeat_timer = Node::rebuild_heartbeat_timer();
        let mut timeout_timer = Node::rebuild_timeout_timer();

        self.tp
            .spawn(async move {
                loop {
                    select! {
                        _ = timer_rx.select_next_some() => {
                            timeout_timer = Node::rebuild_timeout_timer();
                        }

                        _ = heartbeat_timer => {
                            match rf.lock().unwrap().action_tx.as_ref().unwrap().unbounded_send(Actions::SendHeartbeat) {
                                Ok(_) => heartbeat_timer = Node::rebuild_heartbeat_timer(),
                                _ => break,
                            }
                        }

                        _ = timeout_timer => {
                            match rf.lock().unwrap().action_tx.as_ref().unwrap().unbounded_send(Actions::StartElection) {
                                Ok(_) => timeout_timer = Node::rebuild_timeout_timer(),
                                _ => break,
                            }
                        }
                    }
                }
            })
            .unwrap();
    }

    fn rebuild_heartbeat_timer() -> Fuse<Delay> {
        Delay::new(Duration::from_millis(HEARTBEAT_INTERVAL)).fuse()
    }

    fn rebuild_timeout_timer() -> Fuse<Delay> {
        let timeout = rand::thread_rng().gen_range(TIMEOUT_MIN, TIMEOUT_MIN * 3);
        Delay::new(Duration::from_millis(timeout)).fuse()
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        let mut rf = self.rf.lock().unwrap();
        rf.start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.rf.lock().unwrap().term()
    }

    pub fn role(&self) -> Role {
        self.rf.lock().unwrap().role()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.rf.lock().unwrap().is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            role: self.role(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        let kill_tx = self.kill_tx.lock().unwrap().take().unwrap();
        kill_tx.send(()).unwrap();
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let mut rf = self.rf.lock().unwrap();
        rf.request_vote_handler(args)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let mut rf = self.rf.lock().unwrap();
        rf.append_entries_handler(args)
    }
}
