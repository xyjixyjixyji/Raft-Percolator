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
        let Txn { ts, writes } = self.txn.as_ref().unwrap().to_owned();
        let start_ts = ts;

        if writes.is_empty() {
            return Ok(true);
        }

        // designate the primary and secondaries
        let primary = writes.first().unwrap().to_owned();
        let secondaries = writes[1..].to_owned();

        // prewrite the primary
        let primary_pw_req = PrewriteRequest {
            ts: start_ts,
            w: Some(primary.clone()),
            primary: Some(primary.clone()),
        };
        if !self.txn_client.prewrite(&primary_pw_req).await.unwrap().ok {
            return Ok(false);
        }

        // prewrite the secondaries
        for secondary in secondaries.iter() {
            let pw_req = PrewriteRequest {
                ts: start_ts,
                w: Some(secondary.clone()),
                primary: Some(primary.clone()),
            };
            if !self.txn_client.prewrite(&pw_req).await.unwrap().ok {
                return Ok(false);
            }
        }

        let commit_ts = self.real_get_timestamp().await.unwrap();

        // commit the primary
        info!(
            "commit primary, start_ts: {:?}, commit_ts: {:?}",
            start_ts, commit_ts
        );

        let primary_commit_req = CommitRequest {
            is_primary: true,
            commit_key: primary.key,
            start_ts,
            commit_ts,
        };
        // only if success and resp.ok we proceed
        let r = auto_retry(|| self.txn_client.commit(&primary_commit_req)).await;
        info!("txn_client.commit response {:?}", &r);
        match r {
            Ok(CommitResponse { ok: false }) => return Ok(false),
            Ok(_) => {}
            Err(Error::Other(reason)) if reason == "reqhook" => return Ok(false),
            Err(e) => return Err(e),
        }

        info!("primary committed");

        // commit the secondaries
        for secondary in secondaries.into_iter() {
            info!(
                "commit secondary, start_ts: {:?}, commit_rs: {:?}",
                start_ts, commit_ts
            );
            let secondary_commit_req = CommitRequest {
                is_primary: false,
                commit_key: secondary.key,
                start_ts,
                commit_ts,
            };
            // secondaries commit, even if dropped we see it as success
            let _ = auto_retry(|| self.txn_client.commit(&secondary_commit_req)).await;
            info!("secondary committed");
        }
        info!("all secondary committed");

        Ok(true)
    }
}

async fn auto_retry<T, F>(f: impl Fn() -> F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    for i in 0..RETRY_TIMES {
        let r = f().await;
        if r.is_ok() {
            return r;
        }
        sleep(Duration::from_millis(BACKOFF_TIME_MS << i)).await;
    }
    f().await
}
