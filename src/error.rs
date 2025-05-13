use thiserror::Error;
use yellowstone_grpc_proto::tonic;
#[derive(Debug, Error)]
pub enum PeregrineError {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("RPC connection error: {0}")]
    RpcConnection(String),
    #[error("gRPC connection error: {0}")]
    GrpcConnection(String),
    #[error("Data processing error: {0}")]
    DataProcessing(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Timeout error: {0}")]
    Timeout(String),
    #[error("Solana client error: {0}")]
    SolanaClient(String),
    #[error("Yellowstone error: {0}")]
    Yellowstone(String),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Task join error: {0}")]
    TokioJoin(#[from] tokio::task::JoinError),
    #[error("Semaphore acquire error: {0}")]
    SemaphoreAcquire(String),
    #[error("Task error: {0}")]
    Task(String),
}

impl From<solana_client::client_error::ClientError> for PeregrineError {
    fn from(err: solana_client::client_error::ClientError) -> Self {
        Self::SolanaClient(err.to_string())
    }
}

impl From<yellowstone_grpc_client::GeyserGrpcClientError> for PeregrineError {
    fn from(err: yellowstone_grpc_client::GeyserGrpcClientError) -> Self {
        Self::Yellowstone(err.to_string())
    }
}

impl From<tonic::Status> for PeregrineError {
    fn from(err: tonic::Status) -> Self {
        Self::GrpcConnection(err.to_string())
    }
}

impl From<tonic::transport::Error> for PeregrineError {
    fn from(err: tonic::transport::Error) -> Self {
        Self::GrpcConnection(err.to_string())
    }
}

impl From<tokio::sync::AcquireError> for PeregrineError {
    fn from(err: tokio::sync::AcquireError) -> Self {
        Self::SemaphoreAcquire(err.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for PeregrineError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout("Operation timed out".to_string())
    }
}

pub type PeregrineResult<T> = Result<T, PeregrineError>;
