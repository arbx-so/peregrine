use ahash::AHasher;
use axum::http::HeaderValue;
use dashmap::DashMap;
use env_logger::{Builder, Env};
use futures_util::{StreamExt, stream::FuturesUnordered};
use log::{error, info};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::MemcmpEncodedBytes,
};
use solana_rpc_client::http_sender::HttpSender;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    str::FromStr,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};
use structs::{BloomFilterCache, ProgramAccountMap};
use tokio::sync::{OnceCell, RwLock, Semaphore};
use tonic_health::pb::health_client::HealthClient;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient, InterceptorXToken};
use yellowstone_grpc_proto::{
    geyser::{
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
        SubscribeRequestFilterAccountsFilterMemcmp, SubscribeUpdateAccount,
        geyser_client::GeyserClient, subscribe_request_filter_accounts_filter::Filter,
        subscribe_request_filter_accounts_filter_memcmp::Data, subscribe_update::UpdateOneof,
    },
    tonic::{self, codec::CompressionEncoding},
};

mod api;
mod cli;
mod config;
mod error;
mod structs;

use clap::Parser;
use cli::{Cli, Commands};
use config::{Config, ProgramAccountFilter, ProgramConfig};
use error::{PeregrineError, PeregrineResult};

const DEFAULT_MAP_CAPACITY: usize = 1024;
const DEFAULT_LOG_FILTER: &str = "api=info,peregrine=info";

/// Global cache for program accounts, indexed by program hash
pub static GPA_MAP: LazyLock<Arc<ProgramAccountMap>> = LazyLock::new(|| {
    Arc::new(DashMap::with_capacity_and_hasher(
        DEFAULT_MAP_CAPACITY,
        ahash::RandomState::new(),
    ))
});

/// Global bloom filter cache for dynamic filters
pub static BLOOM_CACHE: LazyLock<Arc<BloomFilterCache>> = LazyLock::new(|| {
    Arc::new(DashMap::with_capacity_and_hasher(
        DEFAULT_MAP_CAPACITY,
        ahash::RandomState::new(),
    ))
});

/// Global program configuration
pub static PROGRAM_CONFIG: OnceCell<Arc<Config>> = OnceCell::const_new();

/// Global connection pool for RPC and gRPC clients
pub static CONNECTION_POOL: OnceCell<Arc<ConnectionPool>> = OnceCell::const_new();

/// Thread-safe gRPC client type
type GrpcClientType = Arc<tokio::sync::Mutex<GeyserGrpcClient<InterceptorXToken>>>;

/// Connection pool managing gRPC and RPC clients
pub struct ConnectionPool {
    grpc_client: Arc<RwLock<Option<GrpcClientType>>>,
    rpc_client: Arc<RpcClient>,
    semaphore: Arc<Semaphore>,
    config: Arc<Config>,
}

impl ConnectionPool {
    pub fn new(config: Arc<Config>) -> PeregrineResult<Self> {
        let http_client = reqwest::Client::builder()
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                if config.performance.enable_compression {
                    headers.insert(
                        reqwest::header::ACCEPT_ENCODING,
                        HeaderValue::from_static("gzip, deflate, br, zstd"),
                    );
                }
                headers
            })
            .timeout(config.rpc.request_timeout)
            .pool_idle_timeout(config.rpc.keep_alive_timeout)
            .pool_max_idle_per_host(config.rpc.max_idle_per_host)
            .build()
            .map_err(|e| PeregrineError::Config(format!("Failed to build HTTP client: {e}")))?;

        let sender = HttpSender::new_with_client(&config.rpc.endpoint, http_client);
        let rpc_client = Arc::new(RpcClient::new_sender(
            sender,
            solana_client::rpc_client::RpcClientConfig {
                commitment_config: CommitmentConfig::processed(),
                confirm_transaction_initial_timeout: None,
            },
        ));

        let semaphore = Arc::new(Semaphore::new(config.rpc.max_connections));

        Ok(Self {
            grpc_client: Arc::new(RwLock::new(None)),
            rpc_client,
            semaphore,
            config,
        })
    }

    /// Get or create a gRPC client instance
    pub async fn get_grpc_client(&self) -> PeregrineResult<GrpcClientType> {
        let mut client_guard = self.grpc_client.write().await;

        if client_guard.is_none() {
            let client = Arc::new(tokio::sync::Mutex::new(self.create_grpc_client().await?));
            *client_guard = Some(client.clone());
            Ok(client)
        } else {
            Ok(client_guard
                .as_ref()
                .expect("Should always be able to access client")
                .clone())
        }
    }

    async fn create_grpc_client(&self) -> PeregrineResult<GeyserGrpcClient<InterceptorXToken>> {
        let endpoint =
            tonic::transport::Endpoint::from_shared(self.config.grpc.endpoint.to_string())
                .map_err(|e| PeregrineError::GrpcConnection(format!("Invalid gRPC endpoint: {e}")))?
                .tcp_nodelay(true)
                .http2_adaptive_window(true)
                .timeout(self.config.grpc.request_timeout)
                .connect_timeout(self.config.grpc.connection_timeout);

        let endpoint = if self.config.grpc.use_tls {
            endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?
        } else {
            endpoint
        };

        let channel = endpoint.connect().await?;

        let interceptor = InterceptorXToken {
            x_token: self
                .config
                .grpc
                .api_token
                .as_ref()
                .map(|t| t.clone().try_into().unwrap()),
            x_request_snapshot: false,
        };

        let health = HealthClient::with_interceptor(channel.clone(), interceptor.clone());
        let geyser = GeyserClient::with_interceptor(channel, interceptor)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);
        let grpc_client = GeyserGrpcClient::new(health, geyser);

        Ok(grpc_client)
    }

    /// Execute an RPC operation with exponential backoff retry
    pub async fn execute_rpc<F, T>(&self, f: F) -> PeregrineResult<T>
    where
        F: Fn(Arc<RpcClient>) -> futures_util::future::BoxFuture<'static, PeregrineResult<T>>,
    {
        let _permit = self.semaphore.acquire().await?;

        let backoff_config = backoff::ExponentialBackoff {
            initial_interval: std::time::Duration::from_millis(100),
            randomization_factor: 0.1,
            multiplier: 2.0,
            max_interval: std::time::Duration::from_secs(10),
            max_elapsed_time: Some(std::time::Duration::from_secs(60)),
            ..Default::default()
        };

        backoff::future::retry(backoff_config, || async {
            match f(self.rpc_client.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => {
                    log::warn!("RPC operation failed, retrying: {e}");
                    Err(backoff::Error::transient(e))
                }
            }
        })
        .await
    }

    /// Execute a gRPC operation with exponential backoff retry
    pub async fn execute_grpc<F, T>(&self, f: F) -> PeregrineResult<T>
    where
        F: Fn(GrpcClientType) -> futures_util::future::BoxFuture<'static, PeregrineResult<T>>
            + Send
            + 'static,
        T: Send + 'static,
    {
        let initial_interval =
            Duration::from_millis(self.config.grpc.retry_delay.as_millis().min(60000) as u64);

        let backoff_config = backoff::ExponentialBackoff {
            initial_interval,
            randomization_factor: 0.1,
            multiplier: 2.0,
            max_interval: Duration::from_secs(30),
            max_elapsed_time: None,
            ..Default::default()
        };

        let retry_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let retry_count_clone = retry_count.clone();
        let max_retries = self.config.grpc.retry_attempts;

        backoff::future::retry(backoff_config, || {
            let retry_count = retry_count_clone.clone();
            let f = &f;
            async move {
                let attempt = retry_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                if attempt >= max_retries {
                    let e = PeregrineError::GrpcConnection(
                        format!("Max retry attempts ({max_retries}) exceeded")
                    );
                    return Err(backoff::Error::permanent(e));
                }

                let attempt_display = attempt + 1;

                let client = match self.get_grpc_client().await {
                    Ok(c) => c,
                    Err(e) => {
                        log::warn!("Failed to get gRPC client (attempt {attempt_display}/{max_retries}): {e}");
                        self.invalidate_grpc_client().await;
                        return Err(backoff::Error::transient(e));
                    }
                };

                match f(client).await {
                    Ok(result) => Ok(result),
                    Err(e) => {
                        log::warn!("gRPC operation failed (attempt {attempt_display}/{max_retries}): {e}");

                        if e.to_string().contains("transport")
                            || e.to_string().contains("connection")
                            || e.to_string().contains("tonic")
                        {
                            self.invalidate_grpc_client().await;
                        }

                        Err(backoff::Error::transient(e))
                    }
                }
            }
        })
        .await
    }

    /// Invalidate the current gRPC client to force reconnection
    async fn invalidate_grpc_client(&self) {
        let mut client_guard = self.grpc_client.write().await;
        *client_guard = None;
    }
}

fn main() -> PeregrineResult<()> {
    Builder::from_env(Env::default().default_filter_or(DEFAULT_LOG_FILTER)).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            config: config_path,
        } => {
            let config = Arc::new(
                tokio::runtime::Runtime::new()
                    .map_err(|e| PeregrineError::Config(format!("Failed to create runtime: {e}")))?
                    .block_on(Config::from_file(config_path.to_str().ok_or_else(
                        || PeregrineError::Config("Invalid UTF-8 in config path".to_string()),
                    )?))
                    .map_err(|e| PeregrineError::Config(format!("Failed to load config: {e}")))?,
            );
            config.validate()?;

            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(config.performance.worker_threads)
                .max_blocking_threads(config.performance.blocking_threads)
                .enable_all()
                .build()
                .map_err(|e| PeregrineError::Config(format!("Failed to build runtime: {e}")))?;

            runtime.block_on(async move { run_application(config).await })
        }
        Commands::Validate {
            config: config_path,
        } => {
            validate_config(config_path.to_str().ok_or_else(|| {
                PeregrineError::Config("Invalid UTF-8 in config path".to_string())
            })?)
        }
        Commands::Generate { output } => {
            generate_config(output.to_str().ok_or_else(|| {
                PeregrineError::Config("Invalid UTF-8 in output path".to_string())
            })?)
        }
    }
}

fn validate_config(path: &str) -> PeregrineResult<()> {
    let config = tokio::runtime::Runtime::new()
        .map_err(|e| PeregrineError::Config(format!("Failed to create runtime: {e}")))?
        .block_on(Config::from_file(path))?;

    config.validate()?;
    println!("✓ Configuration is valid");
    println!("  - Programs: {}", config.programs.len());
    println!("  - gRPC endpoint: {}", config.grpc.endpoint);
    println!("  - RPC endpoint: {}", config.rpc.endpoint);
    println!("  - API port: {}", config.api.port);
    Ok(())
}

fn generate_config(path: &str) -> PeregrineResult<()> {
    let example_config = include_str!("../config.example.json");

    let json_string = serde_json::to_string_pretty(
        &serde_json::from_str::<serde_json::Value>(example_config)
            .expect("Should load a valid example configuration"),
    )
    .map_err(|e| PeregrineError::Serialization(format!("Failed to serialize config: {e}")))?;

    std::fs::write(path, json_string).map_err(PeregrineError::Io)?;

    println!("✓ Generated configuration at {path}");
    Ok(())
}

async fn run_application(config: Arc<Config>) -> PeregrineResult<()> {
    PROGRAM_CONFIG.set(config.clone()).unwrap();

    let pool = Arc::new(ConnectionPool::new(config.clone())?);
    CONNECTION_POOL.set(pool.clone()).ok();

    info!(
        "Starting Peregrine with {} worker threads",
        config.performance.worker_threads
    );
    info!("Loaded config with {} programs", config.programs.len());

    let mut tasks = FuturesUnordered::new();
    let batch_size = config.performance.batch_size;

    for (idx, program) in config.programs.iter().enumerate() {
        if idx > 0 && idx.is_multiple_of(batch_size) {
            while let Some(result) = tasks.next().await {
                if let Err(e) = result {
                    error!("Task failed: {e}");
                }
            }
        }
        let hash = compute_program_hash(program);
        let key = hash.to_string();

        GPA_MAP.entry(key.clone()).or_insert_with(|| {
            DashMap::with_capacity_and_hasher(
                config.performance.buffer_size / 64,
                ahash::RandomState::new(),
            )
        });

        info!(
            "Spawning task for program {} (hash {hash:x})",
            program.program_id
        );

        let pool_clone = pool.clone();
        let program_clone = program.clone();
        tasks.push(tokio::spawn(async move {
            let start = Instant::now();
            match populate_in_memory_map(pool_clone, hash, &program_clone).await {
                Ok(count) => {
                    info!(
                        "Program {} initialized with {} entries in {:?}",
                        program_clone.program_id,
                        count,
                        start.elapsed()
                    );
                }
                Err(err) => {
                    error!("populate_in_memory_map({hash:x}) failed: {err}");
                }
            }
        }));
    }

    while let Some(result) = tasks.next().await {
        if let Err(e) = result {
            error!("Task failed: {e}");
        }
    }

    let yellowstone_handle = if config.grpc.enabled {
        Some(tokio::spawn(yellowstone_listener(pool.clone())))
    } else {
        info!("Yellowstone listener disabled (grpc.enabled = false)");
        None
    };

    let api_handle = tokio::spawn(api::listen());

    if let Some(yellowstone_handle) = yellowstone_handle {
        tokio::select! {
            result = yellowstone_handle => {
                match result {
                    Ok(Ok(())) => info!("Yellowstone listener exited normally"),
                    Ok(Err(e)) => {
                        error!("Yellowstone listener failed: {e}");
                        return Err(e);
                    }
                    Err(e) => {
                        error!("Yellowstone listener task panicked: {e}");
                        return Err(PeregrineError::Task(format!("Yellowstone listener panicked: {e}")));
                    }
                }
            }
            result = api_handle => {
                match result {
                    Ok(()) => info!("API server exited normally"),
                    Err(e) => {
                        error!("API server task panicked: {e}");
                        return Err(PeregrineError::Task(format!("API server panicked: {e}")));
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal");
            }
        }
    } else {
        tokio::select! {
            result = api_handle => {
                match result {
                    Ok(()) => info!("API server exited normally"),
                    Err(e) => {
                        error!("API server task panicked: {e}");
                        return Err(PeregrineError::Task(format!("API server panicked: {e}")));
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal");
            }
        }
    }

    info!("Shutting down...");
    Ok(())
}

/// Compute a unique hash for a program based on its ID and filters
/// Excludes Dynamic filters since they're used for runtime filtering
fn compute_program_hash(program: &ProgramConfig) -> u64 {
    let mut hasher = AHasher::default();
    program.program_id.hash(&mut hasher);

    if let Some(filters) = &program.filters {
        let mut combined_hash: u64 = 0;
        for filter in filters {
            // Skip Dynamic filters in hash computation
            if !matches!(filter, ProgramAccountFilter::Dynamic(_)) {
                let mut filter_hasher = AHasher::default();
                filter.normalized_hash(&mut filter_hasher);
                combined_hash ^= filter_hasher.finish();
            }
        }
        combined_hash.hash(&mut hasher);
    }

    hasher.finish()
}

/// Populate in-memory cache with program accounts
pub async fn populate_in_memory_map(
    pool: Arc<ConnectionPool>,
    hash: u64,
    program: &ProgramConfig,
) -> PeregrineResult<usize> {
    let program_pubkey = Pubkey::from_str(&program.program_id)
        .map_err(|e| PeregrineError::Config(format!("Invalid program ID: {e}")))?;

    let accounts = pool
        .execute_rpc(|client| {
            let filters = program.filters.clone();
            Box::pin(async move {
                let result = client
                    .get_program_accounts_with_config(
                        &program_pubkey,
                        RpcProgramAccountsConfig {
                            filters: filters.as_ref().map(|f| {
                                f.iter()
                                    .filter_map(|filter| match filter {
                                        ProgramAccountFilter::DataSize(n) => Some(
                                            solana_client::rpc_filter::RpcFilterType::DataSize(*n),
                                        ),
                                        ProgramAccountFilter::Memcmp(m) => {
                                            Some(solana_client::rpc_filter::RpcFilterType::Memcmp(
                                                solana_client::rpc_filter::Memcmp::new(
                                                    m.offset,
                                                    m.bytes.clone(),
                                                ),
                                            ))
                                        }
                                        ProgramAccountFilter::Dynamic(_) => None,
                                    })
                                    .collect()
                            }),
                            account_config: RpcAccountInfoConfig {
                                encoding: Some(UiAccountEncoding::Base64),
                                commitment: Some(CommitmentConfig::processed()),
                                ..Default::default()
                            },
                            with_context: Some(false),
                            sort_results: Some(false),
                        },
                    )
                    .await;
                result.map_err(|e| PeregrineError::RpcConnection(format!("RPC error: {e}")))
            })
        })
        .await?;

    let count = accounts.len();
    if let Some(set) = GPA_MAP.get(&hash.to_string()) {
        for (pubkey, account) in accounts {
            set.entry(pubkey)
                .or_insert_with(|| Arc::new(account.into()));
        }
    }
    Ok(count)
}

async fn yellowstone_listener(pool: Arc<ConnectionPool>) -> PeregrineResult<()> {
    let config = PROGRAM_CONFIG.get().unwrap();
    let mut account_filters: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();

    for program in &config.programs {
        let hash = compute_program_hash(program);
        let key = hash.to_string();

        account_filters.insert(
            key.clone(),
            SubscribeRequestFilterAccounts {
                owner: vec![program.program_id.to_string()],
                filters: program.filters.as_ref().map_or(Vec::new(), |filters| {
                    filters
                        .iter()
                        .filter_map(|f| match f {
                            ProgramAccountFilter::DataSize(n) => {
                                Some(SubscribeRequestFilterAccountsFilter {
                                    filter: Some(Filter::Datasize(*n)),
                                })
                            }
                            ProgramAccountFilter::Memcmp(m) => {
                                Some(SubscribeRequestFilterAccountsFilter {
                                    filter: Some(Filter::Memcmp(
                                        SubscribeRequestFilterAccountsFilterMemcmp {
                                            offset: m.offset as u64,
                                            data: Some(match &m.bytes {
                                                MemcmpEncodedBytes::Base58(s) => {
                                                    Data::Base58(s.clone())
                                                }
                                                MemcmpEncodedBytes::Base64(s) => {
                                                    Data::Base64(s.clone())
                                                }
                                                MemcmpEncodedBytes::Bytes(b) => {
                                                    Data::Bytes(b.clone())
                                                }
                                            }),
                                        },
                                    )),
                                })
                            }
                            ProgramAccountFilter::Dynamic(_) => None,
                        })
                        .collect()
                }),
                ..Default::default()
            },
        );
    }

    let mut stream = pool
        .execute_grpc(move |client| {
            let filters = account_filters.clone();
            Box::pin(async move {
                let mut client_guard = client.lock().await;
                let (_, stream) = client_guard
                    .subscribe_with_request(Some(SubscribeRequest {
                        accounts: filters,
                        ..Default::default()
                    }))
                    .await
                    .map_err(|e| {
                        PeregrineError::GrpcConnection(format!(
                            "Failed to subscribe to account updates: {e}"
                        ))
                    })?;
                Ok(stream)
            })
        })
        .await?;

    while let Some(event) = stream.next().await {
        let event = event?;
        if let Some(UpdateOneof::Account(SubscribeUpdateAccount {
            account: Some(acc), ..
        })) = event.update_oneof
            && let Some(id) = event.filters.first()
            && let Some(set) = GPA_MAP.get(id)
        {
            let pubkey: Pubkey = acc.pubkey.clone().try_into().unwrap();

            // If account data is empty (account closed), remove it from cache
            if acc.data.is_empty() || acc.lamports == 0 {
                set.remove(&pubkey);
                log::debug!("Removed closed account {pubkey} from cache");
            } else {
                set.insert(pubkey, Arc::new(acc.into()));
            }
        }
    }
    Ok(())
}
