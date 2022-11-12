use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::msg::*;
use crate::service::*;
use crate::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    next: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        Ok(TimestampResponse {
            ts: self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

impl Value {
    fn to_vec(&self) -> Vec<u8> {
        match self {
            Value::Timestamp(ts) => panic!("to_vec: is ts"),
            Value::Vector(v) => v.to_vec(),
        }
    }

    fn to_timestamp(&self) -> u64 {
        match self {
            Value::Timestamp(ts) => *ts,
            Value::Vector(v) => panic!("to_timestamp: is vec"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

// Operations here is txn-irrelavant, they are just dummy "as is" operations
impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: &[u8],
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        let col = match column {
            Column::Data => &self.data,
            Column::Lock => &self.lock,
            Column::Write => &self.write,
        };
        // construct bounds
        let key_rb_start = (key.to_vec(), ts_start_inclusive.unwrap_or(0));
        let key_rb_end = (key.to_vec(), ts_end_inclusive.unwrap_or(u64::MAX));
        col.range((Included(key_rb_start), Included(key_rb_end)))
            .last()
            .to_owned()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: &[u8], column: Column, ts: u64, value: Value) {
        // Your code here.
        let mut col = match column {
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
            Column::Write => &mut self.write,
        };
        col.insert((key.to_vec(), ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: &[u8], column: Column, commit_ts: u64) {
        // Your code here.
        let mut col = match column {
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
            Column::Write => &mut self.write,
        };
        col.remove(&(key.to_vec(), commit_ts));
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        let bigtable = self.data.lock().unwrap();
        loop {
            // there are still pending locks in [0, start_ts]
            if let Some(((key, ts), value)) =
                bigtable.read(&req.key, Column::Lock, None, Some(req.start_ts))
            {
                self.back_off_maybe_clean_up_lock(req.start_ts, &req.key);
                continue;
            }

            // now there is no pending locks, find the latest write and return
            let resp = if let Some(((key, ts), value)) =
                bigtable.read(&req.key, Column::Write, None, Some(req.start_ts))
            {
                let ts = value.to_timestamp();
                let v = bigtable
                    .read(key, Column::Data, Some(ts), Some(ts))
                    .unwrap()
                    .1
                    .to_vec();
                GetResponse { value: v }
            } else {
                // no write on this row
                GetResponse { value: vec![] }
            };

            return Ok(resp);
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.
        let mut bigtable = self.data.lock().unwrap();

        let PrewriteRequest { ts, w, primary } = req;
        let w = w.unwrap();
        let primary = primary.unwrap();

        let ok = if bigtable
            .read(&w.key, Column::Write, Some(ts), None)
            .is_some()
            || bigtable.read(&w.key, Column::Lock, None, None).is_some()
        {
            // case1: abort on writes after this ts
            // case2: abort on locks at any ts
            false
        } else {
            bigtable.write(&w.key, Column::Data, ts, Value::Vector(w.value));
            bigtable.write(&w.key, Column::Lock, ts, Value::Vector(primary.key));
            true
        };

        Ok(PrewriteResponse { ok })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        let CommitRequest {
            writes,
            start_ts,
            commit_ts,
        } = req;
        let primary = writes.first().unwrap().to_owned();
        let secondaries = writes[1..].to_owned();

        // prewrite the primary
        let primary_pw_req = PrewriteRequest {
            ts: start_ts,
            w: Some(primary.clone()),
            primary: Some(primary.clone()),
        };
        if !self.prewrite(primary_pw_req).await.unwrap().ok {
            return Ok(CommitResponse { ok: false });
        }

        // prewrite the secondaries
        for secondary in secondaries.iter() {
            let pw_req = PrewriteRequest {
                ts: start_ts,
                w: Some(secondary.clone()),
                primary: Some(primary.clone()),
            };
            if !self.prewrite(pw_req).await.unwrap().ok {
                return Ok(CommitResponse { ok: false });
            }
        }

        // commit primary, the record can logically be seen after we write the "Write" column
        // start the bigtable txn
        let mut bigtable = self.data.lock().unwrap();
        // abort while working, cause the cell is not locked
        if bigtable
            .read(&primary.key, Column::Lock, Some(start_ts), Some(start_ts))
            .is_none()
        {
            return Ok(CommitResponse { ok: false });
        }
        // logically commit the primary
        bigtable.write(
            &primary.key,
            Column::Write,
            commit_ts,
            Value::Timestamp(start_ts),
        );
        bigtable.erase(&primary.key, Column::Lock, start_ts);

        // commit the secondaries, and we are safe to say that the secondaries are locked
        for secondary in secondaries.into_iter() {
            bigtable.write(
                &secondary.key,
                Column::Write,
                commit_ts,
                Value::Timestamp(start_ts),
            );
            bigtable.erase(&secondary.key, Column::Lock, start_ts);
        }

        Ok(CommitResponse { ok: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: &[u8]) {
        // Your code here.
        unimplemented!()
    }
}
