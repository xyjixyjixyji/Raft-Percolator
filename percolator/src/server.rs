use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

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
    Timestamp(u64, Instant),
    Vector(Vec<u8>, Instant),
}

impl Value {
    fn to_vec(&self) -> Vec<u8> {
        match self {
            Value::Timestamp(ts, _) => panic!("to_vec: is ts"),
            Value::Vector(v, _) => v.to_vec(),
        }
    }

    fn to_timestamp(&self) -> u64 {
        match self {
            Value::Timestamp(ts, _) => *ts,
            Value::Vector(v, _) => panic!("to_timestamp: is vec"),
        }
    }

    /// the elapsed time
    pub fn expired(&self, ttl: u64) -> bool {
        let d = match self {
            Value::Timestamp(_, i) => i.elapsed(),
            Value::Vector(_, i) => i.elapsed(),
        };
        info!("d: {:?}, ttl: {:?}", d, ttl);
        d > Duration::from_nanos(ttl)
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
        let mut bigtable = self.data.lock().unwrap();
        loop {
            // there are still pending locks in [0, start_ts]
            let lock = bigtable
                .read(&req.key, Column::Lock, None, Some(req.start_ts))
                .map(|(k, v)| (k.to_owned(), v.to_owned()));
            if let Some(((_, ts), _)) = lock {
                bigtable = MemoryStorage::back_off_maybe_clean_up_lock(bigtable, ts, &req.key);
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
            bigtable.write(
                &w.key,
                Column::Data,
                ts,
                Value::Vector(w.value, Instant::now()),
            );

            info!("locking on key: {:?}, ts: {}", format_key(&w.key), ts);
            bigtable.write(
                &w.key,
                Column::Lock,
                ts,
                Value::Vector(primary.key, Instant::now()),
            );
            true
        };

        Ok(PrewriteResponse { ok })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.

        // commit primary, the record can logically be seen after we write the "Write" column
        // start the bigtable txn
        let mut bigtable = self.data.lock().unwrap();

        let CommitRequest {
            is_primary,
            commit_key,
            start_ts,
            commit_ts,
        } = req;

        info!(
            "server: commiting primary: {}, key: {:?}, start_ts: {}, commit_ts: {}",
            is_primary,
            commit_key.iter().map(|k| *k as char).collect::<Vec<_>>(),
            start_ts,
            commit_ts
        );

        // if is primary, we have to check if the primary is locked
        let primary_locked = if is_primary {
            info!("server: commiting primary");
            bigtable
                .read(&commit_key, Column::Lock, Some(start_ts), Some(start_ts))
                .is_some()
        } else {
            true
        };

        info!("server: primary locked {}", primary_locked);
        if !primary_locked {
            return Ok(CommitResponse { ok: false });
        }

        // commit the record and erase the lock
        bigtable.write(
            &commit_key,
            Column::Write,
            commit_ts,
            Value::Timestamp(start_ts, Instant::now()),
        );
        // todo: paper said it should be commit_ts, I doubt that
        info!(
            "unlocking on key: {:?}, ts: {}",
            format_key(&commit_key),
            start_ts
        );
        bigtable.erase(&commit_key, Column::Lock, start_ts);

        Ok(CommitResponse { ok: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock<'a>(
        mut bigtable: MutexGuard<'a, KvTable>,
        ts: u64,
        key: &[u8],
    ) -> MutexGuard<'a, KvTable> {
        // Your code here.
        info!(
            "backing off and maybe clean up lock, req.key: {:?}",
            key.iter().map(|b| *b as char).collect::<Vec<_>>(),
        );
        // look up the lock conflicting with current request
        // the request starts at start_ts, so we look up lock before that
        let lock = bigtable
            .read(key, Column::Lock, None, Some(ts))
            .map(|(k, v)| (k.to_owned(), v.to_owned()));

        if let Value::Vector(primary_key, time) = lock.unwrap().1 {
            let is_primary = primary_key == key;
            if is_primary {
                info!("Backing off a primary");
                // primary lock is conflict, try to remove this
                if bigtable.try_remove_expired_lock(ts, &primary_key) {
                    // the lock has been removed, and we need to backoff the transaction
                    bigtable.erase(key, Column::Data, ts);
                } else {
                    // todo: exponentially back off to wait
                }
            } else {
                info!(
                    "Backing off a secondary, ts: {}, primary: {:?}",
                    ts,
                    &primary_key.iter().map(|c| *c as char).collect::<Vec<_>>()
                );
                // secondary lock is conflict, find its primary
                info!(
                    "Checking lock of key {:?} on range [{}, {}]",
                    format_key(key),
                    ts,
                    ts
                );
                let primary_lock = bigtable.read(&primary_key, Column::Lock, Some(ts), Some(ts));
                if primary_lock.is_some() {
                    // rollback primary
                    info!("Primary is locked");
                    if bigtable.try_remove_expired_lock(ts, &primary_key) {
                        info!("Stale lock removed");
                        bigtable.erase(&primary_key, Column::Data, ts);
                    }
                } else {
                    info!("Primary is not locked");
                    // primary is not locked
                    //  1. the previous transaction has not commited(and will not be able to commit right now)
                    //  2. the previous transaction has already commited
                    if let Some(((_, commit_ts), _)) = bigtable
                        .read(&primary_key, Column::Write, Some(ts), None)
                        .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    {
                        info!(
                            "Commiting the key: {:?} on ts: {} by writing to Column::write",
                            key.iter().map(|c| *c as char).collect::<Vec<_>>(),
                            ts
                        );

                        // the previous transaction has already committed, therefore, this secondary is supposed to be committed
                        bigtable.write(
                            key,
                            Column::Write,
                            commit_ts,
                            Value::Timestamp(ts, Instant::now()),
                        );
                    }
                    // in both cases, we remove the lock on the key
                    //  1. if already commited, we help it commit and erase the lock
                    //  2. if will not committed, the lock should appear as if never set
                    info!(
                        "Erasing the lock on key: {:?}, ts: {}",
                        key.iter().map(|c| *c as char).collect::<Vec<_>>(),
                        ts
                    );

                    bigtable.erase(key, Column::Lock, ts);
                }
            }
            bigtable
        } else {
            panic!("value in lock column should always be a key")
        }
    }
}

impl KvTable {
    /// return @removed: whether the lock is expired and REMOVED
    fn try_remove_expired_lock(&mut self, start_ts: u64, key: &[u8]) -> bool {
        if let Some(lock) = self.read(key, Column::Lock, Some(start_ts), Some(start_ts)) {
            if lock.1.expired(TTL) {
                // if expired, we remove the lock
                self.erase(key, Column::Lock, start_ts);
                return true;
            }
        }

        // no lock removed by me
        false
    }
}

fn format_key(key: &[u8]) -> Vec<char> {
    key.iter().map(|c| *c as char).collect()
}
