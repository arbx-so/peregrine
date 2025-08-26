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
use serde_json::{Value, from_value, json};
use solana_sdk::pubkey::Pubkey;
use std::{
    hash::{Hash, Hasher},
    pin::Pin,
    sync::Arc,
};

use crate::{
    BLOOM_CACHE, GPA_MAP, PROGRAM_CONFIG,
    config::{DynamicFilter, ProgramAccountFilter},
    structs::{
        AppState, GetProgramAccountsParams, GetProgramAccountsResult, HashableAccount, RpcRequest,
        RpcRequestType,
    },
};

type ApiResult<T> = Result<T, Box<Response<Body>>>;
type StreamedBytes = Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

/// Build a JSON-RPC 2.0 error response
fn build_error_response(
    id: Option<&Value>,
    code: u16,
    message: &str,
    data: Option<String>,
) -> Box<Response<Body>> {
    let error_body = json!({
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message,
            "data": data,
        },
        "id": id.cloned().unwrap_or(Value::Null),
    });

    Box::new(
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(error_body.to_string()))
            .expect("Failed to build error response"),
    )
}

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
async fn handler(State(state): State<Arc<AppState>>, raw_body: Bytes) -> impl IntoResponse {
    log::debug!("Received {} bytes", raw_body.len());

    let request = match parse_rpc_request(&raw_body) {
        Ok(req) => req,
        Err(err_response) => return *err_response,
    };

    log::info!("Processing {} (id: {})", request.method, request.id);

    match request.method {
        RpcRequestType::GetProgramAccounts => handle_get_program_accounts(state, request, raw_body)
            .await
            .unwrap_or_else(|err| *err),
        _ => {
            log::debug!("Forwarding {:?} to upstream", request.method);
            forward_to_upstream(state, raw_body, Some(&request.id))
                .await
                .unwrap_or_else(|err| *err)
        }
    }
}

fn parse_rpc_request(raw_body: &Bytes) -> ApiResult<RpcRequest> {
    serde_json::from_slice::<RpcRequest>(raw_body).map_err(|e| {
        log::error!("Failed to parse RPC request: {e}");
        build_error_response(
            None,
            StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            "Parse error",
            Some(e.to_string()),
        )
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
        let cache_key = compute_cache_key(&params);
        if let Some(cached) = get_cached_response(&cache_key, &request).await {
            log::info!("Cache hit for {} (hash: {})", params.0, cache_key);
            Ok(cached)
        } else {
            log::info!("Cache miss for {} (hash: {})", params.0, cache_key);
            forward_to_upstream(state, raw_body, Some(&request.id)).await
        }
    }
}

fn parse_gpa_params(request: &RpcRequest) -> ApiResult<GetProgramAccountsParams> {
    from_value(request.params.clone()).map_err(|e| {
        log::error!("Invalid GetProgramAccounts params: {e}");
        build_error_response(
            Some(&request.id),
            StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            "Invalid params",
            Some(e.to_string()),
        )
    })
}

fn compute_cache_key_with_filters(program_id: &str, filters: &[ProgramAccountFilter]) -> u64 {
    let mut hasher = ahash::AHasher::default();
    program_id.hash(&mut hasher);
    
    let mut combined_hash: u64 = 0;
    for filter in filters {
        if !matches!(filter, ProgramAccountFilter::Dynamic(_)) {
            let mut filter_hasher = ahash::AHasher::default();
            filter.normalized_hash(&mut filter_hasher);
            combined_hash ^= filter_hasher.finish();
        }
    }
    combined_hash.hash(&mut hasher);

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
    let program_config = config.programs.iter().find(|p| p.program_id == params.0)?;

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

    let request_filters = params.1.filters.as_ref()?;
    let mut dynamic_filters = Vec::with_capacity(dynamic_filters_config.len());

    for filter in request_filters {
        if let ProgramAccountFilter::Memcmp(memcmp) = filter {
            for dynamic_cfg in &dynamic_filters_config {
                if memcmp.offset == dynamic_cfg.offset {
                    let bytes = memcmp.bytes_as_vec();
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

    let mut base_filters = Vec::with_capacity(request_filters.len());
    for filter in request_filters {
        match filter {
            ProgramAccountFilter::Memcmp(memcmp) => {
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
    
    log::debug!("Base filters count: {}, filters: {:?}", base_filters.len(), base_filters);
    log::debug!("Looking for base cache with key: {} for program {}", base_key, params.0);
    log::debug!("Available cache keys: {:?}", GPA_MAP.iter().map(|e| e.key().clone()).collect::<Vec<_>>());
    
    let record = GPA_MAP.get(&base_key.to_string())?;
    
    log::info!("Dynamic filter cache hit for {} (base_key: {})", params.0, base_key);
    let bloom_key = {
        let mut hasher = ahash::AHasher::default();
        base_key.hash(&mut hasher);
        for (offset, bytes) in &dynamic_filters {
            offset.hash(&mut hasher);
            bytes.hash(&mut hasher);
        }
        hasher.finish().to_string()
    };

    let bloom_filter = BLOOM_CACHE
        .get(&bloom_key)
        .map(|entry| entry.value().clone());
    let filtered_entries = tokio::task::spawn_blocking({
        let dynamic_filters = dynamic_filters.clone();
        let entries: Vec<_> = record
            .value()
            .iter()
            .map(|x| {
                let pair = x.pair();
                (*pair.0, pair.1.clone())
            })
            .collect();

        move || {
            use bloomfilter::Bloom;

            let has_cached_bloom = bloom_filter.is_some();
            let bloom = bloom_filter.unwrap_or_else(|| {
                let expected_items = entries.len().max(1000);
                let false_positive_rate = 0.01;
                Arc::new(Bloom::new_for_fp_rate(expected_items, false_positive_rate))
            });

            let mut results = Vec::with_capacity(entries.len() / 10);
            let mut new_bloom = if !has_cached_bloom {
                Some(Bloom::new_for_fp_rate(entries.len().max(1000), 0.01))
            } else {
                None
            };

            for (pubkey, account) in entries {
                let mut filter_key = Vec::with_capacity(32 + dynamic_filters.len() * 16);
                filter_key.extend_from_slice(pubkey.as_ref());
                for (offset, bytes) in &dynamic_filters {
                    filter_key.extend_from_slice(&offset.to_le_bytes());
                    filter_key.extend_from_slice(bytes);
                }

                if has_cached_bloom && !bloom.check(&filter_key) {
                    continue;
                }

                let account_data = match base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    &account.data[0],
                ) {
                    Ok(data) => data,
                    Err(_) => continue,
                };

                let max_end = dynamic_filters
                    .iter()
                    .map(|(offset, bytes)| offset + bytes.len())
                    .max()
                    .unwrap_or(0);

                if account_data.len() < max_end {
                    continue;
                }

                let mut matches = true;
                for (offset, expected_bytes) in &dynamic_filters {
                    let actual_bytes = &account_data[*offset..*offset + expected_bytes.len()];
                    if actual_bytes != expected_bytes.as_slice() {
                        matches = false;
                        break;
                    }
                }

                if matches {
                    results.push((pubkey, account));
                    if let Some(ref mut new_bloom) = new_bloom {
                        new_bloom.set(&filter_key);
                    }
                }
            }

            if let Some(new_bloom) = new_bloom {
                BLOOM_CACHE.insert(bloom_key.clone(), Arc::new(new_bloom));
            }

            results
        }
    })
    .await
    .ok()?;

    drop(record);

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
        let chunk_size = entries
            .len()
            .saturating_div(rayon::current_num_threads())
            .max(64);

        entries
            .into_par_iter()
            .enumerate()
            .with_min_len(chunk_size)
            .map(|(i, (pubkey, account))| {
                let estimated_size = account.space.min(4096) + 256;
                let mut buf = Vec::with_capacity(estimated_size);

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

async fn forward_to_upstream(
    state: Arc<AppState>,
    raw_body: Bytes,
    request_id: Option<&Value>,
) -> ApiResult<Response<Body>> {
    log::debug!("Forwarding to {}", state.rpc_url);

    let upstream_response = send_upstream_request(&state, raw_body, request_id).await?;

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

async fn send_upstream_request(
    state: &AppState,
    body: Bytes,
    request_id: Option<&Value>,
) -> ApiResult<reqwest::Response> {
    state
        .client
        .post(&state.rpc_url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| {
            log::error!("Upstream request failed: {e}");
            build_error_response(
                request_id,
                e.status().unwrap_or_default().as_u16(),
                "Server error",
                Some(format!("Upstream request failed: {e}")),
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
