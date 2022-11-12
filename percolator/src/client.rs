use futures::executor::block_on;
use futures::Future;
use labrpc::*;
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};

use crate::msg::*;
use crate::service::{TSOClient, TransactionClient};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

lazy_static::lazy_static! {
    static ref RT: Runtime = Runtime::new().unwrap();
}

#[derive(Clone)]
struct Txn {
    ts: u64,
    writes: Vec<Write>,
}

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    txn: Option<Txn>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client,
            txn: None,
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        RT.block_on(auto_retry(|| self.real_get_timestamp()))
    }

    async fn real_get_timestamp(&self) -> Result<u64> {
        self.tso_client
            .get_timestamp(&TimestampRequest {})
            .await
            .map(|resp| resp.ts)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        assert!(
            self.txn.is_none(),
            "this client is in the middle of another txn"
        );
        self.txn = Some(Txn {
            ts: self.get_timestamp().unwrap(),
            writes: vec![],
        });
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        RT.block_on(self.real_get(key))
    }

    async fn real_get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        assert!(self.txn.is_some(), "must begin a txn when get");
        self.txn_client
            .get(&GetRequest {
                key,
                start_ts: self.txn.as_ref().unwrap().ts,
            })
            .await
            .map(|resp| resp.value)
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        assert!(self.txn.is_some(), "must begin a txn when set");
        self.txn.as_mut().unwrap().writes.push(Write { key, value });
    }

    /// Commits a transaction.
    pub fn commit(&mut self) -> Result<bool> {
        // Your code here.
        RT.block_on(self.real_commit())
    }

    async fn real_commit(&mut self) -> Result<bool> {
        assert!(self.txn.is_some(), "must begin a txn when commit");
        let commit_ts = self.get_timestamp().unwrap();
        let Txn { ts, writes } = self.txn.as_ref().unwrap().to_owned();

        if writes.is_empty() {
            return Ok(true);
        }

        self.txn_client
            .commit(&CommitRequest {
                writes,
                start_ts: ts,
                commit_ts,
            })
            .await
            .map(|resp| resp.ok)
    }
}

async fn auto_retry<T, F>(f: impl Fn() -> F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let mut r = f().await;
    for i in 0..RETRY_TIMES {
        match r {
            Ok(_) => return r,
            Err(_) => sleep(Duration::from_millis(BACKOFF_TIME_MS)).await,
        }
        r = f().await;
    }
    r
}
