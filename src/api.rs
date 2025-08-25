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

use solana_rpc_client_api::filter::MemcmpEncodedBytes;
use solana_sdk::bs58;

use crate::{
    GPA_MAP, PROGRAM_CONFIG,
    config::{DynamicFilter, ProgramAccountFilter},
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

    // Check if we need to apply dynamic filtering
    if let Some(cached) = get_cached_response_with_filtering(&params, &request).await {
        log::info!("Cache hit for {} with dynamic filtering", params.0);
        Ok(cached)
    } else {
        log::debug!("Dynamic filtering not applicable or no match found");
        let cache_key = compute_cache_key(&params);
        if let Some(cached) = get_cached_response(&cache_key, &request).await {
            log::info!("Cache hit for {} (hash: {})", params.0, cache_key);
            Ok(cached)
        } else {
            log::info!("Cache miss for {} (hash: {})", params.0, cache_key);
            forward_to_upstream(state, raw_body).await
        }
    }
}

fn parse_gpa_params(request: &RpcRequest) -> ApiResult<GetProgramAccountsParams> {
    from_value(request.params.clone()).map_err(|e| {
        log::error!("Invalid GetProgramAccounts params: {e}");
        (StatusCode::BAD_REQUEST, format!("Invalid params: {e}"))
    })
}

fn compute_cache_key_with_filters(program_id: &str, filters: &[ProgramAccountFilter]) -> u64 {
    let mut hasher = AHasher::default();
    program_id.hash(&mut hasher);

    if !filters.is_empty() {
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

fn compute_cache_key(params: &GetProgramAccountsParams) -> u64 {
    compute_cache_key_with_filters(&params.0, params.1.filters.as_deref().unwrap_or(&[]))
}

async fn get_cached_response_with_filtering(
    params: &GetProgramAccountsParams,
    request: &RpcRequest,
) -> Option<Response<Body>> {
    let config = PROGRAM_CONFIG.get()?;

    // Find the program configuration that matches this request
    let program_config = config.programs.iter().find(|p| p.program_id == params.0)?;

    // Check if this program has dynamic filters configured
    let dynamic_filters_config: Vec<&DynamicFilter> = program_config
        .filters
        .as_ref()?
        .iter()
        .filter_map(|f| match f {
            ProgramAccountFilter::Dynamic(d) => Some(d),
            _ => None,
        })
        .collect();

    if dynamic_filters_config.is_empty() {
        return None;
    }

    // Check if the request contains memcmp filters that match dynamic filter positions
    let request_filters = params.1.filters.as_ref()?;
    let mut dynamic_filters = Vec::new();

    for filter in request_filters {
        if let ProgramAccountFilter::Memcmp(memcmp) = filter {
            // Check if this memcmp matches any dynamic filter position
            for dynamic_cfg in &dynamic_filters_config {
                if memcmp.offset == dynamic_cfg.offset {
                    let bytes = match &memcmp.bytes {
                        MemcmpEncodedBytes::Base58(s) => {
                            bs58::decode(s).into_vec().unwrap_or_default()
                        }
                        MemcmpEncodedBytes::Base64(s) => {
                            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                                .unwrap_or_default()
                        }
                        MemcmpEncodedBytes::Bytes(b) => b.clone(),
                    };
                    if bytes.len() == dynamic_cfg.length {
                        dynamic_filters.push((dynamic_cfg.offset, bytes));
                    }
                }
            }
        }
    }

    if dynamic_filters.is_empty() {
        return None;
    }

    // Compute the cache key without the dynamic filters
    let mut base_filters = Vec::new();
    for filter in request_filters {
        match filter {
            ProgramAccountFilter::Memcmp(memcmp) => {
                // Skip memcmp filters that match dynamic positions
                if !dynamic_filters_config
                    .iter()
                    .any(|d| d.offset == memcmp.offset)
                {
                    base_filters.push(filter.clone());
                }
            }
            _ => base_filters.push(filter.clone()),
        }
    }

    let base_key = compute_cache_key_with_filters(&params.0, &base_filters);
    log::debug!(
        "Looking for cache with base key: {} ({:x}) for program {}",
        base_key,
        base_key,
        params.0
    );
    log::debug!(
        "Available cache keys: {:?}",
        GPA_MAP.iter().map(|e| e.key().clone()).collect::<Vec<_>>()
    );
    let record = GPA_MAP.get(&base_key.to_string())?;

    // Apply dynamic filtering to the cached entries
    let filtered_entries: Vec<(Pubkey, Arc<HashableAccount>)> = record
        .value()
        .iter()
        .filter_map(|x| {
            let pair = x.pair();
            let account = pair.1.clone();

            // Decode the account data
            let account_data = base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                &account.data[0],
            )
            .ok()?;

            // Check all dynamic filters
            for (offset, expected_bytes) in &dynamic_filters {
                if account_data.len() < offset + expected_bytes.len() {
                    return None;
                }

                let actual_bytes = &account_data[*offset..*offset + expected_bytes.len()];
                if actual_bytes != expected_bytes.as_slice() {
                    return None;
                }
            }

            Some((*pair.0, account))
        })
        .collect();

    drop(record);

    // Always return a response, even if empty (to avoid fallback to upstream)
    Some(build_streamed_response(filtered_entries, request).await)
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
            let status = e.status().unwrap_or(StatusCode::BAD_GATEWAY);
            log::error!("Upstream request failed: {e}");
            (
                status,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": status.as_u16(),
                        "message": e.to_string()
                    },
                    "id": 1
                })
                .to_string(),
            )
        })
}

fn build_proxy_response(upstream: reqwest::Response) -> Response<Body> {
    let status = upstream.status();
    let headers = upstream.headers().clone();

    let byte_stream = upstream.bytes_stream().map_err(std::io::Error::other);
    let body = Body::from_stream(StreamBody::new(byte_stream));

    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        if let Some(name) = name {
            builder = builder.header(name.as_str(), value.as_bytes());
        }
    }

    builder.body(body).expect("Response building failed")
}
