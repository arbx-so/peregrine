use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use base64::Engine;
use bloomfilter::Bloom;
use dashmap::DashMap;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{DisplayFromStr, serde_as};
use solana_sdk::{account::Account, pubkey::Pubkey};
use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

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

/// Bloom filter cache for dynamic filter lookups
pub type BloomFilterCache = DashMap<String, Arc<Bloom<Vec<u8>>>, ahash::RandomState>;

/// Parameters for `GetProgramAccounts` RPC method
#[derive(Deserialize, Debug)]
pub struct GetProgramAccountsParams(pub String, pub GetProgramAccountsConfig);

/// Configuration for `GetProgramAccounts` request
#[derive(Deserialize, Debug)]
pub struct GetProgramAccountsConfig {
    /// Optional filters to apply to the accounts
    pub filters: Option<Vec<crate::config::ProgramAccountFilter>>,
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
