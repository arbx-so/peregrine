use std::{
    hash::{Hash, Hasher},
    pin::Pin,
    sync::Arc,
};

use ahash::AHasher;
use async_stream::try_stream;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::post,
};
use futures_util::{Stream, TryStreamExt};
use http_body_util::StreamBody;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use reqwest::Client;
use serde_json::from_value;
use solana_sdk::pubkey::Pubkey;

use crate::{
    GPA_MAP, PROGRAM_CONFIG,
    structs::{
        AppState, GetProgramAccountsParams, GetProgramAccountsResult, HashableAccount, RpcRequest,
        RpcRequestType,
    },
};

const INITIAL_BUFFER_CAPACITY: usize = 256; // Reserve 256 bytes per account

type ApiResult<T> = Result<T, (StatusCode, String)>;
type StreamedBytes = Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

/// Start the API server
pub async fn listen() {
    let config = PROGRAM_CONFIG.get().expect("Config not initialized");
    let app = create_app(config);
    let listener = create_listener(config).await;

    log::info!("API listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

fn create_app(config: &crate::config::Config) -> Router {
    Router::new()
        .route("/", post(handler))
        .with_state(Arc::new(AppState {
            client: build_http_client(config),
            rpc_url: config.rpc.endpoint.to_string(),
        }))
}

fn build_http_client(config: &crate::config::Config) -> Client {
    Client::builder()
        .http2_prior_knowledge()
        .pool_idle_timeout(config.rpc.keep_alive_timeout)
        .timeout(config.rpc.request_timeout)
        .build()
        .expect("Failed to build HTTP client")
}

async fn create_listener(config: &crate::config::Config) -> tokio::net::TcpListener {
    let bind_addr = format!("{}:{}", config.api.bind_address, config.api.port);
    tokio::net::TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|_| panic!("Port {} is already in use", config.api.port))
}

/// Handle incoming RPC requests
async fn handler(
    State(state): State<Arc<AppState>>,
    raw_body: Bytes,
) -> ApiResult<impl IntoResponse> {
    log::debug!("Received {} bytes", raw_body.len());

    let request = parse_rpc_request(&raw_body)?;
    log::info!("Processing {} (id: {})", request.method, request.id);

    match request.method {
        RpcRequestType::GetProgramAccounts => {
            handle_get_program_accounts(state, request, raw_body).await
        }
        _ => {
            log::debug!("Forwarding {:?} to upstream", request.method);
            forward_to_upstream(state, raw_body).await
        }
    }
}

fn parse_rpc_request(raw_body: &Bytes) -> ApiResult<RpcRequest> {
    serde_json::from_slice::<RpcRequest>(raw_body).map_err(|e| {
        log::error!("Failed to parse RPC request: {e}");
        (StatusCode::BAD_REQUEST, format!("Invalid JSON-RPC: {e}"))
    })
}

async fn handle_get_program_accounts(
    state: Arc<AppState>,
    request: RpcRequest,
    raw_body: Bytes,
) -> ApiResult<Response<Body>> {
    let params = parse_gpa_params(&request)?;
    let cache_key = compute_cache_key(&params);

    if let Some(cached) = get_cached_response(&cache_key, &request).await {
        log::info!("Cache hit for {} (hash: {})", params.0, cache_key);
        Ok(cached)
    } else {
        log::info!("Cache miss for {} (hash: {})", params.0, cache_key);
        forward_to_upstream(state, raw_body).await
    }
}

fn parse_gpa_params(request: &RpcRequest) -> ApiResult<GetProgramAccountsParams> {
    from_value(request.params.clone()).map_err(|e| {
        log::error!("Invalid GetProgramAccounts params: {e}");
        (StatusCode::BAD_REQUEST, format!("Invalid params: {e}"))
    })
}

fn compute_cache_key(params: &GetProgramAccountsParams) -> u64 {
    let mut hasher = AHasher::default();
    params.0.hash(&mut hasher);

    if let Some(filters) = &params.1.filters {
        let combined_hash = filters
            .iter()
            .map(|filter| {
                let mut filter_hasher = AHasher::default();
                filter.normalized_hash(&mut filter_hasher);
                filter_hasher.finish()
            })
            .fold(0u64, |acc, hash| acc ^ hash);

        combined_hash.hash(&mut hasher);
    }

    hasher.finish()
}

async fn get_cached_response(cache_key: &u64, request: &RpcRequest) -> Option<Response<Body>> {
    let record = GPA_MAP.get(&cache_key.to_string())?;

    let entries: Vec<(Pubkey, Arc<HashableAccount>)> = record
        .value()
        .iter()
        .map(|x| {
            let pair = x.pair();
            (*pair.0, pair.1.clone())
        })
        .collect();
    drop(record);

    Some(build_streamed_response(entries, request).await)
}

async fn build_streamed_response(
    entries: Vec<(Pubkey, Arc<HashableAccount>)>,
    request: &RpcRequest,
) -> Response<Body> {
    let prefix = format!(r#"{{"jsonrpc":"{}","result":["#, request.json_rpc);
    let suffix = format!(r#"],"id":{}}}"#, request.id);

    let chunks = serialize_entries_parallel(entries).await;
    let body_stream = create_response_stream(prefix, chunks, suffix);

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from_stream(StreamBody::new(body_stream)))
        .unwrap()
}

async fn serialize_entries_parallel(entries: Vec<(Pubkey, Arc<HashableAccount>)>) -> Vec<Bytes> {
    tokio::task::spawn_blocking(move || {
        entries
            .into_par_iter()
            .enumerate()
            .map(|(i, (pubkey, account))| {
                let mut buf = Vec::with_capacity(INITIAL_BUFFER_CAPACITY);

                simd_json::to_writer(&mut buf, &GetProgramAccountsResult { account, pubkey })
                    .unwrap();

                if i != 0 {
                    let mut result = Vec::with_capacity(buf.len() + 1);
                    result.push(b',');
                    result.extend_from_slice(&buf);
                    Bytes::from(result)
                } else {
                    Bytes::from(buf)
                }
            })
            .collect()
    })
    .await
    .expect("Serialization task failed")
}

fn create_response_stream(prefix: String, chunks: Vec<Bytes>, suffix: String) -> StreamedBytes {
    Box::pin(try_stream! {
        yield Bytes::from(prefix);
        for chunk in chunks {
            yield chunk;
        }
        yield Bytes::from(suffix);
    })
}

async fn forward_to_upstream(state: Arc<AppState>, raw_body: Bytes) -> ApiResult<Response<Body>> {
    log::debug!("Forwarding to {}", state.rpc_url);

    let upstream_response = send_upstream_request(&state, raw_body).await?;

    let status = upstream_response.status();
    if !status.is_success() {
        log::warn!(
            "Upstream returned {}: {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("Unknown")
        );
    }

    Ok(build_proxy_response(upstream_response))
}

async fn send_upstream_request(state: &AppState, body: Bytes) -> ApiResult<reqwest::Response> {
    state
        .client
        .post(&state.rpc_url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| {
            log::error!("Upstream request failed: {e}");
            (
                e.status().unwrap_or(StatusCode::BAD_GATEWAY),
                format!("Upstream error: {e}"),
            )
        })
}

fn build_proxy_response(upstream: reqwest::Response) -> Response<Body> {
    let status = upstream.status();
    let headers = upstream.headers().clone();

    let byte_stream = upstream.bytes_stream().map_err(std::io::Error::other);
    let body = Body::from_stream(StreamBody::new(byte_stream));

    let mut builder = Response::builder().status(status.as_u16());
    for (name, value) in headers {
        if let Some(name) = name {
            builder = builder.header(name.as_str(), value.as_bytes());
        }
    }

    builder.body(body).expect("Response building failed")
}
