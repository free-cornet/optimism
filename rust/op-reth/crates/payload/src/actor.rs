
//! HTTP JSON-RPC client for the ACTOR transaction mempool.

use alloy_consensus::{SignableTransaction, TxEip1559};
use alloy_eips::eip2718::{Decodable2718, Encodable2718};
use alloy_primitives::{B256, Bytes, TxKind, U256, keccak256};
use alloy_rpc_types_engine::PayloadId;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{
    sync::{
        Arc, Mutex,
        mpsc::{self, Sender},
    },
    thread,
    time::Duration,
};
use tracing::info;

#[derive(Debug)]
#[derive(Clone)]
/// Client for fetching ordered transaction batches from ACTOR.
pub struct ActorClient {
    requests: Sender<RpcCommand>,
    fetch_state: Arc<Mutex<FetchState>>,
    metadata_signer: PrivateKeySigner,
}

#[derive(Debug, Default)]
struct FetchState {
    key: Option<(u64, B256, PayloadId)>,
    batches: Vec<(u64, u64, ActorTxBatch)>,
    complete: bool,
    failed: bool,
    pending_consumed_txs: u64,
}

/// A replayable ACTOR fetch sequence for one payload build.
#[derive(Debug)]
pub struct ActorFetchSession<'a> {
    client: &'a ActorClient,
    key: (u64, B256, PayloadId),
    index: usize,
    pending_consumed_txs: Option<u64>,
}

/// Maximum signed ACTx size accepted by the metadata transaction.
pub const ACTX_MAX_BYTES: u64 = 65_536;
/// Reserved transaction gas, including the worst-case calldata floor cost.
pub const ACTX_GAS_RESERVE: u64 = 21_000 + 40 * ACTX_MAX_BYTES;

fn metadata_transaction<Tx: Decodable2718>(
    signer: &PrivateKeySigner,
    chain_id: u64,
    nonce: u64,
    base_fee: u64,
    actx: &RawValue,
) -> anyhow::Result<Tx> {
    let input = actx.get().as_bytes();
    anyhow::ensure!(input.len() as u64 <= ACTX_MAX_BYTES, "ACTx exceeds metadata size limit");
    let transaction = TxEip1559 {
        chain_id,
        nonce,
        gas_limit: 21_000 + 40 * input.len() as u64,
        max_fee_per_gas: u128::from(base_fee),
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(signer.address()),
        value: U256::ZERO,
        input: Bytes::copy_from_slice(input),
        ..Default::default()
    };
    let signature = signer.sign_hash_sync(&transaction.signature_hash())?;
    let signed = transaction.into_signed(signature);
    Ok(Tx::decode_2718_exact(&signed.encoded_2718())?)
}

enum RpcCommand {
    FetchTxs {
        max_gas: u64,
        num_consumed_txs: u64,
        response: Sender<anyhow::Result<ActorTxBatch>>,
    },
}

#[derive(Serialize)]
struct RpcRequest {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: RpcParams,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RpcParams {
    max_gas: u64,
    num_consumed_txs: u64,
}

#[derive(Deserialize)]
struct RpcResponse {
    result: Option<RpcResult>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcResult {
    txs: Vec<Bytes>,
    #[serde(default)]
    actx: Option<Box<RawValue>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcActx {
    #[serde(default)]
    new_pending_txs: Vec<String>,
    #[serde(default)]
    ordered_txs: Vec<String>,
}

/// A batch of transactions returned by ACTOR and whether it completes the current block.
#[derive(Clone, Debug)]
pub struct ActorTxBatch {
    /// Ordered transactions to execute.
    pub txs: Vec<Bytes>,
    /// Whether ACTOR has no more transactions for the current block.
    pub block_complete: bool,
    /// Complete signed ACTOR metadata, as returned by the mempool.
    pub actx: Option<Box<RawValue>>,
}

impl From<RpcResult> for ActorTxBatch {
    fn from(result: RpcResult) -> Self {
        Self { txs: result.txs, block_complete: result.actx.is_some(), actx: result.actx }
    }
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

impl ActorClient {
    /// Creates a client for the ACTOR JSON-RPC endpoint.
    ///
    /// The endpoint should be an HTTP URL, such as `http://127.0.0.1:12345`.
    pub fn new(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let metadata_signer: PrivateKeySigner = std::env::var("ACTOR_METADATA_PRIVATE_KEY")
            .map_err(|_| anyhow::anyhow!("ACTOR_METADATA_PRIVATE_KEY must be configured"))?
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid ACTOR_METADATA_PRIVATE_KEY"))?;
        Self::with_signer(endpoint, metadata_signer)
    }

    pub(crate) fn with_signer(
        endpoint: impl Into<String>,
        metadata_signer: PrivateKeySigner,
    ) -> anyhow::Result<Self> {
        let endpoint = endpoint.into();
        let (requests, receiver) = mpsc::channel();
        let (ready_sender, ready_receiver) = mpsc::channel::<Result<(), String>>();

        thread::Builder::new()
            .name("actor-http-client".into())
            .spawn(move || {
                let http = match Client::builder().timeout(Duration::from_millis(2000)).build() {
                    Ok(http) => http,
                    Err(err) => {
                        let _ = ready_sender.send(Err(err.to_string()));
                        return;
                    }
                };

                if ready_sender.send(Ok(())).is_err() {
                    return;
                }

                while let Ok(RpcCommand::FetchTxs {
                    max_gas,
                    num_consumed_txs,
                    response,
                }) = receiver.recv()
                {
                    let _ = response.send(fetch_txs(
                        &http,
                        &endpoint,
                        max_gas,
                        num_consumed_txs,
                    ));
                }
            })?;

        ready_receiver
            .recv()
            .map_err(|err| anyhow::anyhow!("ACTOR HTTP client thread stopped: {err}"))?
            .map_err(anyhow::Error::msg)?;

        Ok(Self {
            requests,
            fetch_state: Arc::new(Mutex::new(FetchState::default())),
            metadata_signer,
        })
    }

    /// Returns the address used to sign ACTx metadata transactions.
    pub fn metadata_signer_address(&self) -> alloy_primitives::Address {
        self.metadata_signer.address()
    }

    /// Signs an ACTx-carrying transaction for the current chain and account nonce.
    pub fn metadata_transaction<Tx: Decodable2718>(
        &self,
        chain_id: u64,
        nonce: u64,
        base_fee: u64,
        actx: &RawValue,
    ) -> anyhow::Result<Tx> {
        metadata_transaction(&self.metadata_signer, chain_id, nonce, base_fee, actx)
    }

    /// Opens a replayable fetch sequence for a payload build.
    pub fn start_payload(
        &self,
        block_number: u64,
        parent_hash: B256,
        payload_id: PayloadId,
    ) -> ActorFetchSession<'_> {
        ActorFetchSession {
            client: self,
            key: (block_number, parent_hash, payload_id),
            index: 0,
            pending_consumed_txs: None,
        }
    }
}

impl ActorFetchSession<'_> {
    /// Fetches the next ordered batch, replaying batches from an earlier build of this payload.
    pub fn fetch_txs(&mut self, max_gas: u64) -> anyhow::Result<ActorTxBatch> {
        let mut state = self.client
            .fetch_state
            .lock()
            .map_err(|_| anyhow::anyhow!("ACTOR fetch state mutex poisoned"))?;
        if state.key != Some(self.key) {
            if let Some(previous) = state.key {
                anyhow::ensure!(
                    state.complete && self.key.0 > previous.0,
                    "ACTOR cannot switch payloads before advancing a completed block"
                );
            }
            let pending_consumed_txs = state.pending_consumed_txs;
            *state = FetchState { key: Some(self.key), pending_consumed_txs, ..Default::default() };
        }
        if let Some((cached_gas, cached_consumed, batch)) = state.batches.get(self.index) {
            anyhow::ensure!(*cached_gas == max_gas, "ACTOR replay gas budget changed");
            if let Some(consumed) = self.pending_consumed_txs.take() {
                anyhow::ensure!(consumed == *cached_consumed, "ACTOR replay consumed count changed");
            }
            self.index += 1;
            return Ok(batch.clone());
        }
        anyhow::ensure!(!state.complete, "ACTOR payload is already complete");
        anyhow::ensure!(!state.failed, "ACTOR fetch previously failed with an ambiguous result");
        let num_consumed_txs = self.pending_consumed_txs.take().unwrap_or(state.pending_consumed_txs);
        state.pending_consumed_txs = 0;
        let (response, result) = mpsc::channel();
        let response = self.client.requests
            .send(RpcCommand::FetchTxs { max_gas, num_consumed_txs, response })
            .map_err(|err| anyhow::anyhow!("ACTOR HTTP client thread stopped: {err}"))
            .and_then(|()| result.recv()
                .map_err(|err| anyhow::anyhow!("ACTOR HTTP response channel closed: {err}"))?);
        let batch = match response {
            Ok(batch) => batch,
            Err(err) => {
                state.failed = true;
                return Err(err);
            }
        };
        state.complete = batch.block_complete;
        state.batches.push((max_gas, num_consumed_txs, batch.clone()));
        self.index += 1;
        Ok(batch)
    }

    /// Records the consumed count for the next ACTOR request.
    pub fn record_consumed_txs(&mut self, count: u64) -> anyhow::Result<()> {
        let mut state = self.client.fetch_state.lock()
            .map_err(|_| anyhow::anyhow!("ACTOR fetch state mutex poisoned"))?;
        anyhow::ensure!(state.key == Some(self.key), "ACTOR payload has been replaced");
        state.pending_consumed_txs = count;
        self.pending_consumed_txs = Some(count);
        Ok(())
    }
}

fn fetch_txs(
    http: &Client,
    endpoint: &str,
    max_gas: u64,
    num_consumed_txs: u64,
) -> anyhow::Result<ActorTxBatch> {
    let request = RpcRequest {
        jsonrpc: "2.0",
        id: 1,
        method: "fetchTxs",
        params: RpcParams { max_gas, num_consumed_txs },
    };

    let response: RpcResponse = http
        .post(endpoint)
        .json(&request)
        .send()?
        .error_for_status()?
        .json()?;

    if let Some(error) = response.error {
        anyhow::bail!("fetchTxs failed ({}): {}", error.code, error.message);
    }

    let result = response
        .result
        .ok_or_else(|| anyhow::anyhow!("fetchTxs response is missing result"))?;
    if let Some(actx) = &result.actx {
        let actx: RpcActx = serde_json::from_str(actx.get())?;
        for hash in &actx.new_pending_txs {
            info!("fetchTxs|NEW_PENDING_TX|{hash}");
        }
        for hash in &actx.ordered_txs {
            info!("fetchTxs|ORDERED_TX|{hash}");
        }
    }
    for tx in &result.txs {
        let hash: String = keccak256(tx).iter().map(|b| format!("{b:02x}")).collect();
        info!("fetchTxs|FETCHED_TX_HASH|{hash}");
    }
    info!("Received {} transactions", result.txs.len());
    Ok(result.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::Transaction;
    use op_alloy_consensus::OpTxEnvelope;
    use reth_primitives_traits::SignerRecoverable;

    #[test]
    fn request_params_include_gas_and_processed_transaction_count() {
        let params = RpcParams { max_gas: 100_000, num_consumed_txs: 3 };

        assert_eq!(
            serde_json::to_value(params).unwrap(),
            serde_json::json!({
                "maxGas": 100_000,
                "numConsumedTxs": 3,
            })
        );
    }

    #[test]
    fn response_with_actx_marks_batch_complete() {
        let response: RpcResponse = serde_json::from_str(
            r#"{"result":{"txs":["0x02"],"actx":{"blockNumber":3119,"currentPoolHash":"c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470","nextPoolHash":"fe81ecfd6080a6d3f84b8edc376e4179e08af8805b4133dbecee3592371ccb62","nextTxGas":0,"orderedTxs":[],"signature":"29753f06062cda61ba1c7379251feeb1226a4051f0ca74953877ac3a289b0a7e870aa2e37930ad38a5e89bcdac7c3c93de3590c2e63203a16d5770f60372f486"}}}"#,
        )
        .unwrap();

        let batch: ActorTxBatch = response.result.unwrap().into();

        assert_eq!(batch.txs, vec![Bytes::from_static(&[0x02])]);
        assert!(batch.block_complete);
        assert!(batch.actx.unwrap().get().contains("\"signature\":"));
    }

    #[test]
    fn response_without_actx_does_not_mark_batch_complete() {
        let response: RpcResponse =
            serde_json::from_str(r#"{"result":{"txs":["0x02"]}}"#).unwrap();

        let batch: ActorTxBatch = response.result.unwrap().into();

        assert_eq!(batch.txs, vec![Bytes::from_static(&[0x02])]);
        assert!(!batch.block_complete);
    }

    #[test]
    fn metadata_transaction_preserves_actx_and_signer() {
        let signer: PrivateKeySigner =
            "0000000000000000000000000000000000000000000000000000000000000001"
                .parse()
                .unwrap();
        let actx = RawValue::from_string(
            r#"{"orderedTxs":[],"signature":"abcdef","blockNumber":23}"#.to_owned(),
        )
        .unwrap();
        let transaction: OpTxEnvelope = metadata_transaction(&signer, 10, 4, 100, &actx).unwrap();

        assert_eq!(transaction.input().as_ref(), actx.get().as_bytes());
        assert_eq!(transaction.nonce(), 4);
        assert_eq!(transaction.chain_id(), Some(10));
        assert_eq!(transaction.recover_signer().unwrap(), signer.address());
    }

}
