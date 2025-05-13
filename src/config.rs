use crate::error::{PeregrineError, PeregrineResult};
use serde::{Deserialize, Serialize};
use solana_rpc_client_api::filter::MemcmpEncodedBytes;
use solana_sdk::bs58;
use std::time::Duration;

/// Main configuration for the Peregrine application
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub grpc: GrpcConfig,
    pub rpc: RpcConfig,
    pub api: ApiConfig,
    pub performance: PerformanceConfig,
    pub programs: Vec<ProgramConfig>,
}

/// gRPC configuration settings
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrpcConfig {
    #[serde(default = "defaults::grpc_endpoint")]
    pub endpoint: url::Url,
    pub api_token: Option<String>,
    #[serde(default = "defaults::true_value")]
    pub use_tls: bool,
    #[serde(with = "humantime_serde", default = "defaults::connection_timeout")]
    pub connection_timeout: Duration,
    #[serde(with = "humantime_serde", default = "defaults::request_timeout")]
    pub request_timeout: Duration,
    #[serde(default = "defaults::retry_attempts")]
    pub retry_attempts: u32,
    #[serde(with = "humantime_serde", default = "defaults::retry_delay")]
    pub retry_delay: Duration,
}

/// RPC configuration settings
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcConfig {
    #[serde(default = "defaults::rpc_endpoint")]
    pub endpoint: url::Url,
    #[serde(with = "humantime_serde", default = "defaults::connection_timeout")]
    pub connection_timeout: Duration,
    #[serde(with = "humantime_serde", default = "defaults::request_timeout")]
    pub request_timeout: Duration,
    #[serde(with = "humantime_serde", default = "defaults::keep_alive")]
    pub keep_alive_timeout: Duration,
    #[serde(default = "defaults::max_connections")]
    pub max_connections: usize,
    #[serde(default = "defaults::max_idle")]
    pub max_idle_per_host: usize,
}

/// API server configuration settings
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiConfig {
    #[serde(default = "defaults::bind_address")]
    pub bind_address: String,
    #[serde(default = "defaults::port")]
    pub port: u16,
    #[serde(default = "defaults::api_max_connections")]
    pub max_connections: usize,
}

/// Performance tuning configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PerformanceConfig {
    #[serde(default = "defaults::worker_threads")]
    pub worker_threads: usize,
    #[serde(default = "defaults::blocking_threads")]
    pub blocking_threads: usize,
    #[serde(default = "defaults::buffer_size")]
    pub buffer_size: usize,
    #[serde(default = "defaults::batch_size")]
    pub batch_size: usize,
    #[serde(default = "defaults::true_value")]
    pub enable_compression: bool,
}

/// Configuration for a specific program to cache
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProgramConfig {
    pub program_id: String,
    pub filters: Option<Vec<ProgramAccountFilter>>,
}

/// Memory comparison filter configuration
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct Memcmp {
    pub offset: usize,
    #[serde(flatten)]
    pub bytes: MemcmpEncodedBytes,
}

impl Memcmp {
    pub fn bytes_as_vec(&self) -> Vec<u8> {
        match &self.bytes {
            MemcmpEncodedBytes::Base58(s) => bs58::decode(s).into_vec().unwrap_or_default(),
            MemcmpEncodedBytes::Base64(s) => {
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                    .unwrap_or_default()
            }
            MemcmpEncodedBytes::Bytes(b) => b.clone(),
        }
    }
}

/// Filter types for program accounts
#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ProgramAccountFilter {
    DataSize(u64),
    Memcmp(Memcmp),
}

impl ProgramAccountFilter {
    pub fn normalized_hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use std::hash::Hash;
        match self {
            ProgramAccountFilter::DataSize(size) => {
                0u8.hash(state);
                size.hash(state);
            }
            ProgramAccountFilter::Memcmp(memcmp) => {
                1u8.hash(state);
                memcmp.offset.hash(state);
                memcmp.bytes_as_vec().hash(state);
            }
        }
    }
}

impl Config {
    pub async fn from_file(path: &str) -> PeregrineResult<Self> {
        let data = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| PeregrineError::Config(format!("Failed to read config file: {e}")))?;
        let config: Config = serde_json::from_str(&data)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> PeregrineResult<()> {
        if self.programs.is_empty() {
            return Err(PeregrineError::Config("No programs configured".to_string()));
        }
        if self.performance.worker_threads == 0 {
            return Err(PeregrineError::Config(
                "Worker threads must be > 0".to_string(),
            ));
        }
        if self.api.port == 0 {
            return Err(PeregrineError::Config("API port must be > 0".to_string()));
        }
        Ok(())
    }
}

mod defaults {
    use std::time::Duration;

    pub fn grpc_endpoint() -> url::Url {
        url::Url::parse("https://solana-yellowstone-grpc.publicnode.com").expect("Valid URL")
    }

    pub fn rpc_endpoint() -> url::Url {
        url::Url::parse("https://api.mainnet-beta.solana.com").expect("Valid URL")
    }

    pub fn bind_address() -> String {
        "127.0.0.1".to_string()
    }

    pub fn port() -> u16 {
        1945
    }

    pub fn connection_timeout() -> Duration {
        Duration::from_secs(10)
    }

    pub fn request_timeout() -> Duration {
        Duration::from_secs(30)
    }

    pub fn keep_alive() -> Duration {
        Duration::from_secs(60)
    }

    pub fn retry_delay() -> Duration {
        Duration::from_millis(1000)
    }

    pub fn retry_attempts() -> u32 {
        3
    }

    pub fn max_connections() -> usize {
        100
    }

    pub fn max_idle() -> usize {
        10
    }

    pub fn api_max_connections() -> usize {
        1000
    }

    pub fn worker_threads() -> usize {
        16
    }

    pub fn blocking_threads() -> usize {
        64
    }

    pub fn buffer_size() -> usize {
        65536
    }

    pub fn batch_size() -> usize {
        100
    }

    pub fn true_value() -> bool {
        true
    }
}
