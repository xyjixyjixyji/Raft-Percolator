use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::channel::mpsc::{unbounded, UnboundedReceiver};
use futures::channel::oneshot;
use futures::executor::{block_on, ThreadPool};
use futures::{select, FutureExt};
use futures_timer::Delay;

use crate::proto::kvraftpb::*;
use crate::raft::{self, ApplyMsg};

const COMMAND_TIMEOUT: u64 = 2000;
const OP_UNKNOWN: i32 = 0;
const OP_PUT: i32 = 1;
const OP_APPEND: i32 = 2;

impl TryFrom<GetRequest> for Op {
    type Error = ();
    fn try_from(value: GetRequest) -> Result<Self, Self::Error> {
        Ok(Op {
            key: value.key,
            value: String::from(""),
            op_type: "Get".to_string(),
            name: value.name,
            reqno: value.reqno,
        })
    }
}

impl TryFrom<PutAppendRequest> for Op {
    type Error = ();
    fn try_from(value: PutAppendRequest) -> Result<Self, Self::Error> {
        let op_type = if value.op == OP_PUT { "Put" } else { "Append" };

        Ok(Op {
            key: value.key,
            value: value.value,
            op_type: op_type.to_string(),
            name: value.name,
            reqno: value.reqno,
        })
    }
}

impl Into<GetReply> for OpReply {
    fn into(self) -> GetReply {
        GetReply {
            wrong_leader: self.wrong_leader,
            err: self.err,
            value: self.value,
        }
    }
}

impl Into<PutAppendReply> for OpReply {
    fn into(self) -> PutAppendReply {
        PutAppendReply {
            wrong_leader: self.wrong_leader,
            err: self.err,
        }
    }
}

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    apply_rx: UnboundedReceiver<ApplyMsg>,
    max_reqno_map: HashMap<String, u64>, // <name -> max_reqno>
    last_applied_index: u64,             // snapshot usage

    // event notifying when command done by raft layer
    // logic: Node start polling from apply_rx, and apply() anything from it
    //        the KvServer apply() the command and generate corresponding results to a channel
    //        the channel was polled by RPC handler by node, and returns to client
    event_signal_map: HashMap<u64, Option<oneshot::Sender<Op>>>, // <index -> receiver>
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (apply_tx, apply_rx) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, apply_tx);

        KvServer {
            rf: raft::Node::new(rf),
            me,
            maxraftstate,
            apply_rx,
            max_reqno_map: HashMap::new(),
            last_applied_index: 0,
            event_signal_map: HashMap::new(),
        }
    }

    // request handlers start replication at raft peer
    // raft peer replicates and commit through apply_ch
    // kvserver's Node run a loop, the loop polls cmd from
    // apply_ch and handle that
    fn generic_op_handler(&mut self, op: Op) -> labrpc::Result<OpReply> {
        let mut reply = OpReply {
            wrong_leader: false,
            err: String::from(""),
            value: String::from(""),
        };

        // let timer = Delay::new(Duration::from_millis(COMMAND_TIMEOUT)).fuse();
        let (get_tx, mut get_rx) = oneshot::channel();

        if let Ok((index, _term)) = self.rf.start(&op) {
            self.event_signal_map.insert(index, Some(get_tx));
            // poll the channel here
            let op_to_apply = block_on(get_rx).unwrap();
            self.event_signal_map.remove(&index);
            self.apply(op_to_apply);
        } else {
            // not leader
            reply.wrong_leader = true;
        }

        Ok(reply)
    }

    // apply this to state machine
    async fn apply(&mut self, op: Op) {}
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    kv: Arc<RwLock<KvServer>>,
    tp: ThreadPool,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        let mut me = Node {
            kv: Arc::new(RwLock::new(kv)),
            tp: ThreadPool::new().unwrap(),
        };

        me.poll();

        me
    }

    pub fn poll(&mut self) {}

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        self.kv.read().unwrap().rf.get_state()
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    // this should start a get in the raft peer, poll from the apply_ch
    // and reply UNTIL we have an result
    async fn get(&self, arg: GetRequest) -> labrpc::Result<GetReply> {
        // Your code here.
        let op = Op::try_from(arg).unwrap();
        let mut kv = self.kv.write().unwrap();
        Ok(kv.generic_op_handler(op).unwrap().into())
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        // Your code here.
        let op = Op::try_from(arg).unwrap();
        let mut kv = self.kv.write().unwrap();
        Ok(kv.generic_op_handler(op).unwrap().into())
    }
}
