
//! HTTP JSON-RPC client for the ACTOR transaction mempool.

use alloy_primitives::Bytes;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::{
    sync::mpsc::{self, Sender},
    thread,
    time::Duration,
};

#[derive(Debug)]
#[derive(Clone)]
/// Client for fetching ordered transaction batches from ACTOR.
pub struct ActorClient {
    requests: Sender<RpcCommand>,
}

enum RpcCommand {
    FetchTxs {
        max_gas: u64,
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
    actx: Option<serde::de::IgnoredAny>,
}

/// A batch of transactions returned by ACTOR and whether it completes the current block.
#[derive(Debug)]
pub struct ActorTxBatch {
    /// Ordered transactions to execute.
    pub txs: Vec<Bytes>,
    /// Whether ACTOR has no more transactions for the current block.
    pub block_complete: bool,
}

impl From<RpcResult> for ActorTxBatch {
    fn from(result: RpcResult) -> Self {
        Self { txs: result.txs, block_complete: result.actx.is_some() }
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

                while let Ok(RpcCommand::FetchTxs { max_gas, response }) = receiver.recv() {
                    let _ = response.send(fetch_txs(&http, &endpoint, max_gas));
                }
            })?;

        ready_receiver
            .recv()
            .map_err(|err| anyhow::anyhow!("ACTOR HTTP client thread stopped: {err}"))?
            .map_err(anyhow::Error::msg)?;

        Ok(Self { requests })
    }

    /// Fetches the next ordered batch.
    ///
    /// The remote mempool consumes the returned transactions.
    /// Do not automatically retry a request after an ambiguous
    /// transport failure unless the RPC protocol guarantees
    /// that doing so cannot skip transactions.
    pub fn fetch_txs(&self, max_gas: u64) -> anyhow::Result<ActorTxBatch> {
        let (response, result) = mpsc::channel();
        println!("Sending request to fetch txs with max_gas: {}", max_gas);
        self.requests
            .send(RpcCommand::FetchTxs { max_gas, response })
            .map_err(|err| anyhow::anyhow!("ACTOR HTTP client thread stopped: {err}"))?;
        result
            .recv()
            .map_err(|err| anyhow::anyhow!("ACTOR HTTP response channel closed: {err}"))?
    }
}

fn fetch_txs(http: &Client, endpoint: &str, max_gas: u64) -> anyhow::Result<ActorTxBatch> {
    let request = RpcRequest {
        jsonrpc: "2.0",
        id: 1,
        method: "fetchTxs",
        params: RpcParams { max_gas },
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
    println!("Received {} transactions", result.txs.len());
    Ok(result.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_with_actx_marks_batch_complete() {
        let response: RpcResponse = serde_json::from_str(
            r#"{"result":{"txs":["0x02"],"actx":{"blockNumber":3119,"currentPoolHash":"c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470","nextPoolHash":"fe81ecfd6080a6d3f84b8edc376e4179e08af8805b4133dbecee3592371ccb62","nextTxGas":0,"orderedTxs":[],"signature":"29753f06062cda61ba1c7379251feeb1226a4051f0ca74953877ac3a289b0a7e870aa2e37930ad38a5e89bcdac7c3c93de3590c2e63203a16d5770f60372f486"}}}"#,
        )
        .unwrap();

        let batch: ActorTxBatch = response.result.unwrap().into();

        assert_eq!(batch.txs, vec![Bytes::from_static(&[0x02])]);
        assert!(batch.block_complete);
    }

    #[test]
    fn response_without_actx_does_not_mark_batch_complete() {
        let response: RpcResponse =
            serde_json::from_str(r#"{"result":{"txs":["0x02"]}}"#).unwrap();

        let batch: ActorTxBatch = response.result.unwrap().into();

        assert_eq!(batch.txs, vec![Bytes::from_static(&[0x02])]);
        assert!(!batch.block_complete);
    }
}
