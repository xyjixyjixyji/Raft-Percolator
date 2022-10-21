use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

use futures::executor::block_on;

use crate::proto::kvraftpb::*;

const OP_UNKNOWN: i32 = 0;
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
        // You will have to modify this function.
        let mut index = self.last_leader.load(Ordering::SeqCst);
        let reqno = self.next_reqno.fetch_add(1, Ordering::SeqCst);
        let args = GetRequest {
            key,
            name: self.name.clone(),
            reqno,
        };
        loop {
            let fut = self.servers[index as usize].get(&args);
            //todo: make this async
            if let Ok(reply) = block_on(fut) {
                if !reply.wrong_leader {
                    if reply.err.is_empty() {
                        self.last_leader.store(index, Ordering::SeqCst);
                        return reply.value;
                    }
                }
            }
            index = (index + 1) % (self.servers.len() as u64);
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.
        let mut index = self.last_leader.load(Ordering::SeqCst);
        let reqno = self.next_reqno.fetch_add(1, Ordering::SeqCst);
        let args = match op {
            Op::Append(key, value) => PutAppendRequest {
                key,
                value,
                op: OP_PUT,
                name: self.name.clone(),
                reqno,
            },

            Op::Put(key, value) => PutAppendRequest {
                key,
                value,
                op: OP_APPEND,
                name: self.name.clone(),
                reqno,
            },
        };

        loop {
            let fut = self.servers[index as usize].put_append(&args);
            //todo: make this async
            if let Ok(reply) = block_on(fut) {
                if !reply.wrong_leader {
                    if reply.err.is_empty() {
                        self.last_leader.store(index, Ordering::SeqCst);
                        return;
                    }
                }
            }
            index = (index + 1) % (self.servers.len() as u64);
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
