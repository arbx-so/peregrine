use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use base64::Engine;
use dashmap::DashMap;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{DisplayFromStr, serde_as};
use solana_client::rpc_filter::RpcFilterType;
use solana_rpc_client_api::filter::MemcmpEncodedBytes;
use solana_sdk::{account::Account, bs58, pubkey::Pubkey};
use yellowstone_grpc_proto::geyser::{
    SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp,
    SubscribeUpdateAccountInfo, subscribe_request_filter_accounts_filter::Filter,
    subscribe_request_filter_accounts_filter_memcmp::Data,
};

/// Represents a Solana account in a hashable format for caching
#[serde_as]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HashableAccount {
    /// Account data in base64 format with encoding type
    pub data: [String; 2],
    /// Whether the account is executable
    pub executable: bool,
    /// Account balance in lamports
    pub lamports: u64,
    /// Account owner public key
    pub owner: String,
    /// Rent epoch for the account
    pub rent_epoch: u64,
    /// Size of the account data
    pub space: usize,
}

impl Hash for HashableAccount {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.lamports.hash(state);
        self.owner.hash(state);
        self.executable.hash(state);
        self.rent_epoch.hash(state);
        self.data.hash(state);
        self.space.hash(state);
    }
}

impl From<SubscribeUpdateAccountInfo> for HashableAccount {
    fn from(value: SubscribeUpdateAccountInfo) -> Self {
        let engine = base64::engine::general_purpose::STANDARD;

        HashableAccount {
            space: value.data.len(),
            lamports: value.lamports,
            data: [engine.encode(value.data), "base64".to_string()],
            owner: Pubkey::try_from(value.owner).unwrap().to_string(),
            executable: value.executable,
            rent_epoch: value.rent_epoch,
        }
    }
}

impl From<Account> for HashableAccount {
    fn from(value: Account) -> Self {
        let engine = base64::engine::general_purpose::STANDARD;

        Self {
            space: value.data.len(),
            data: [engine.encode(value.data), "base64".to_string()],
            executable: value.executable,
            lamports: value.lamports,
            owner: value.owner.to_string(),
            rent_epoch: value.rent_epoch,
        }
    }
}

/// Map for caching program accounts
/// Key: String - Hash of Program ID + Filters
/// Value: `DashMap` of account public keys to account data
pub type ProgramAccountMap =
    DashMap<String, DashMap<Pubkey, Arc<HashableAccount>, ahash::RandomState>, ahash::RandomState>;

/// Memory comparison filter for account data
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct Memcmp {
    /// Offset in the account data to start comparison
    pub offset: usize,
    /// Bytes to compare against
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

/// Filter for program accounts
#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProgramAccountFilter {
    /// Filter by account data size
    DataSize(u64),
    /// Filter by memory comparison
    Memcmp(Memcmp),
}

impl ProgramAccountFilter {
    pub fn normalized_hash<H: Hasher>(&self, state: &mut H) {
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

impl From<ProgramAccountFilter> for RpcFilterType {
    fn from(value: ProgramAccountFilter) -> Self {
        match value {
            ProgramAccountFilter::DataSize(n) => Self::DataSize(n),
            ProgramAccountFilter::Memcmp(n) => {
                Self::Memcmp(solana_client::rpc_filter::Memcmp::new(n.offset, n.bytes))
            }
        }
    }
}

impl From<ProgramAccountFilter> for SubscribeRequestFilterAccountsFilter {
    fn from(value: ProgramAccountFilter) -> Self {
        let filter = match value {
            ProgramAccountFilter::DataSize(n) => Filter::Datasize(n),
            ProgramAccountFilter::Memcmp(n) => {
                Filter::Memcmp(SubscribeRequestFilterAccountsFilterMemcmp {
                    data: Some({
                        match n.bytes {
                            MemcmpEncodedBytes::Base58(n) => Data::Base58(n),
                            MemcmpEncodedBytes::Base64(n) => Data::Base64(n),
                            MemcmpEncodedBytes::Bytes(n) => Data::Bytes(n),
                        }
                    }),
                    offset: n.offset as u64,
                })
            }
        };

        Self {
            filter: Some(filter),
        }
    }
}

/// Parameters for `GetProgramAccounts` RPC method
#[derive(Deserialize, Debug)]
pub struct GetProgramAccountsParams(pub String, pub GetProgramAccountsConfig);

/// Configuration for `GetProgramAccounts` request
#[derive(Deserialize, Debug)]
pub struct GetProgramAccountsConfig {
    /// Optional filters to apply to the accounts
    pub filters: Option<Vec<ProgramAccountFilter>>,
}

/// JSON-RPC request structure
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RpcRequest {
    /// JSON-RPC version
    #[serde(rename = "jsonrpc")]
    pub json_rpc: String,
    /// Request ID
    pub id: Value,
    /// RPC method name
    pub method: RpcRequestType,
    /// Method parameters
    pub params: Value,
}

/// Application state for the API server
#[derive(Clone)]
pub struct AppState {
    /// HTTP client for forwarding requests
    pub client: Client,
    /// RPC endpoint URL
    pub rpc_url: String,
}

/// Result structure for `GetProgramAccounts` response
#[serde_as]
#[derive(Debug, Serialize, Clone)]
pub struct GetProgramAccountsResult {
    /// Account data
    #[serde(serialize_with = "serialize_arc")]
    pub account: Arc<HashableAccount>,
    /// Account public key
    #[serde_as(as = "DisplayFromStr")]
    pub pubkey: Pubkey,
}

fn serialize_arc<S>(arc: &Arc<HashableAccount>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    (**arc).serialize(serializer)
}

/// Supported RPC request types
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum RpcRequestType {
    /// `GetProgramAccounts` method (cached)
    GetProgramAccounts,
    /// Any other method (proxied to the upstream)
    ProxiedMethod,
}

impl fmt::Display for RpcRequestType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let method = match self {
            RpcRequestType::GetProgramAccounts => "getProgramAccounts",
            RpcRequestType::ProxiedMethod => "proxiedMethod",
        };

        write!(f, "{method}")
    }
}

impl<'de> Deserialize<'de> for RpcRequestType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let out = match s.as_str() {
            "getProgramAccounts" => RpcRequestType::GetProgramAccounts,
            _ => RpcRequestType::ProxiedMethod,
        };
        Ok(out)
    }
}

impl Serialize for RpcRequestType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
