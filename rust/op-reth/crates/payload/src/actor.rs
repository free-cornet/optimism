
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
        response: Sender<anyhow::Result<Vec<Bytes>>>,
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

#[derive(Debug, Deserialize)]
struct RpcResponse {
    result: Option<RpcResult>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcResult {
    txs: Vec<Bytes>,
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
    pub fn fetch_txs(&self, max_gas: u64) -> anyhow::Result<Vec<Bytes>> {
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

fn fetch_txs(http: &Client, endpoint: &str, max_gas: u64) -> anyhow::Result<Vec<Bytes>> {
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

    println!("Received response: {:?}", response);

    response
        .result
        .map(|result| result.txs)
        .ok_or_else(|| anyhow::anyhow!("fetchTxs response is missing result"))
}
