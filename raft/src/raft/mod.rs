use futures::channel::mpsc::UnboundedSender;
// use futures::select;
use futures::task::SpawnExt;
use rand::Rng;
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;
use std::sync::Mutex;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
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

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
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

    pub fn is_follower(&self) -> bool {
        self.role == Role::Follower
    }
}

// regular events
enum Events {
    Heartbeat,
    ElecTimeout,
    ResetTimer,
}

enum RepliesFrom {
    RequestVoteReplyFrom(u64, RequestVoteReply),
    AppendEntriesReplyFrom(u64, AppendEntriesReply),
}

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
    voted_for: i64,

    // timeout limit, used to build timer
    timeout_limit: u128,

    // channel
    // send applyMsg to upper layer application, rx is in kvstore
    apply_tx: UnboundedSender<ApplyMsg>,
    // send regular events, rx is in loop, tx in send_events
    event_tx: Option<UnboundedSender<Events>>,
    // send reply handler messages to invoke reply handlers
    reply_tx: Option<UnboundedSender<RepliesFrom>>,

    // thread pool, simulate go runtime
    tp: futures::executor::ThreadPool,
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
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: State::default(),
            voted_for: -1,
            timeout_limit: 0,
            event_tx: None,
            reply_tx: None,
            apply_tx: apply_ch,
            tp: futures::executor::ThreadPool::new().unwrap(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf.reset_timer_info();

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// loop of a raft instance
    fn run() {}

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
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
    fn request_vote(&mut self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
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

        // if I have not vote, or I have vote for the sender, I will vote for sender
        if self.vote_for_nobody() || self.voted_for == args.cid as i64 {
            self.voted_for = args.cid as i64;
            reply.granted = true;
            self.reset_timer();
        }

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

    fn append_entries(&mut self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let reply = AppendEntriesReply {
            term: self.state.term(),
            success: false,
        };

        if args.term < self.state.term() {
            return Ok(reply);
        }

        if args.term > self.state.term() {
            self.turn_follower(args.term, Some(-1));
        }

        self.reset_timer();

        Ok(reply)
    }
}

// utils
impl Raft {
    fn reset_timer_info(&mut self) {
        let mut rng = rand::thread_rng();
        // timeout: [150, 300)
        self.timeout_limit = rng.gen_range(0, 150) + 150;
    }

    fn reset_timer(&mut self) {
        self.reset_timer_info();
        self.event_tx
            .as_ref()
            .unwrap()
            .unbounded_send(Events::ResetTimer)
            .unwrap();
    }

    fn turn_follower(&mut self, new_term: u64, voted_for: Option<i64>) {
        self.state.role = Role::Follower;
        self.state.term = new_term;
        if let Some(v) = voted_for {
            self.voted_for = v;
        }
    }

    fn turn_candidate(&mut self) {
        self.state.role = Role::Candidate;
        self.state.term += 1;
        self.voted_for = self.me as i64;
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
}

// actions
impl Raft {
    // poll from main loop, call this as handler when event_chan has a hb request
    fn send_heartbeat(&mut self) {
        if !self.is_leader() {
            return;
        }

        let args = AppendEntriesArgs {
            term: self.state.term(),
            leader_id: self.me as u64,
            //todos: log entries should be piggybacked in HB
            leader_commit: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            log_entries: vec![],
        };

        for i in 0..self.peers.len() {
            if i == self.me {
                continue;
            }

            let fut = self.peers[i].append_entries(&args);

            self.tp
                .spawn(async move {
                    fut.await.unwrap();
                })
                .unwrap();
        }
    }

    fn start_election(&mut self) {
        if self.is_leader() {
            return;
        }
        self.turn_candidate();

        let args = RequestVoteArgs {
            term: self.term(),
            cid: self.me as u64,
            ..Default::default()
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

// reply handlers
impl Raft {
    fn handle_request_vote_reply(&mut self, reply: RequestVoteReply) {}
    fn handle_append_entries_reply(&mut self, reply: AppendEntriesReply) {}
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        let _ = self.send_request_vote(0, Default::default());
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
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        Self {
            rf: Arc::new(Mutex::new(raft)),
            tp: futures::executor::ThreadPool::new().unwrap(),
        }
    }

    fn run(&self) {
        loop {
            // poll from channels' rx
            self.tp.spawn(async move {}).unwrap();
        }
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
        crate::your_code_here(command)
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
        rf.request_vote(args)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let mut rf = self.rf.lock().unwrap();
        rf.append_entries(args)
    }
}
