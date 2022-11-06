use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use futures::{executor::block_on, select, FutureExt};
use futures_timer::Delay;

use crate::proto::kvraftpb::*;

const REQ_TIMEOUT: u64 = 500;

const OP_PUT: i32 = 1;
const OP_APPEND: i32 = 2;
enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
    last_leader: AtomicU64,
    next_reqno: AtomicU64,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        // Clerk { name, servers }
        Clerk {
            name,
            servers,
            last_leader: AtomicU64::new(0),
            next_reqno: AtomicU64::new(1), // index starts from one
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        block_on(self.real_get(key))
    }

    pub async fn real_get(&self, key: String) -> String {
        // You will have to modify this function.
        let mut index = self.last_leader.load(Ordering::SeqCst);
        let reqno = self.next_reqno.fetch_add(1, Ordering::SeqCst);
        let args = GetRequest {
            key,
            name: self.name.clone(),
            reqno,
        };
        'loop1: loop {
            let mut fut = self.servers[index as usize].get(&args).fuse();
            let mut timeout_timer = Delay::new(Duration::from_millis(REQ_TIMEOUT)).fuse();
            //todo: make this async
            //todo: timeout!!
            'loop2: loop {
                select! {
                    result = fut => {
                        match result {
                            Ok(reply) => {
                                if !reply.wrong_leader && reply.err.is_empty() {
                                    self.last_leader.store(index, Ordering::SeqCst);
                                    break 'loop1 reply.value;
                                }
                            }
                            Err(_) => break 'loop2,
                        }
                    }

                    _ = timeout_timer => break 'loop2,
                }
            }

            index = (index + 1) % (self.servers.len() as u64);
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    async fn put_append(&self, op: Op) {
        // You will have to modify this function.
        let mut index = self.last_leader.load(Ordering::SeqCst);
        let reqno = self.next_reqno.fetch_add(1, Ordering::SeqCst);
        let args = match op {
            Op::Append(key, value) => PutAppendRequest {
                key,
                value,
                op: OP_APPEND,
                name: self.name.clone(),
                reqno,
            },

            Op::Put(key, value) => PutAppendRequest {
                key,
                value,
                op: OP_PUT,
                name: self.name.clone(),
                reqno,
            },
        };

        'loop1: loop {
            let mut fut = self.servers[index as usize].put_append(&args).fuse();
            let mut timeout_timer = Delay::new(Duration::from_millis(REQ_TIMEOUT)).fuse();

            'loop2: loop {
                select! {
                    result = fut => {
                        match result {
                            Ok(reply) => {
                                if !reply.wrong_leader && reply.err.is_empty() {
                                    self.last_leader.store(index, Ordering::SeqCst);
                                    break 'loop1;
                                }
                            }
                            Err(_) => break 'loop2,
                        }
                    }

                    _ = timeout_timer => break 'loop2,
                }
            }
            index = (index + 1) % (self.servers.len() as u64);
        }
    }

    pub fn put(&self, key: String, value: String) {
        block_on(self.put_append(Op::Put(key, value)));
    }

    pub async fn real_put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value)).await
    }

    pub fn append(&self, key: String, value: String) {
        block_on(self.real_append(key, value));
    }

    pub async fn real_append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value)).await
    }
}
