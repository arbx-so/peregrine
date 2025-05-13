# Peregrine

High-performance Solana program account caching service with Yellowstone gRPC integration for up-to-date data.

## Overview

Peregrine is a blazing-fast caching layer for Solana program accounts that sits between your application and Solana RPC nodes. It leverages Yellowstone gRPC for real-time account updates and provides an efficient in-memory cache for frequently accessed program accounts.

## Features

- **Real-time Account Updates**: Integrates with Yellowstone gRPC for instant account change notifications
- **High-Performance Caching**: In-memory cache using DashMap with AHash for optimal performance
- **Parallel Processing**: Utilizes Rayon for parallel serialization and processing
- **Configurable Filters**: Support for all Solana RPC filters on program accounts
- **Streaming Responses**: Efficient streaming JSON responses for large datasets
- **Automatic Retry Logic**: Built-in exponential backoff for network resilience

## Installation

### Prerequisites

- Rust 1.70+ (2024 edition)
- Cargo

### Building from Source

```bash
git clone https://github.com/arbx-so/peregrine
cd peregrine
cargo build --release
```

## Configuration

Peregrine uses a JSON configuration file. Generate a sample configuration:

```bash
./target/release/peregrine generate config.json
```

### Configuration Options

#### gRPC Settings
- `endpoint`: Yellowstone gRPC endpoint URL
- `api_token`: Optional authentication token
- `use_tls`: Enable TLS for gRPC connections
- `connection_timeout`: Connection timeout duration
- `request_timeout`: Request timeout duration
- `retry_attempts`: Number of retry attempts
- `retry_delay`: Delay between retries

#### RPC Settings
- `endpoint`: Solana RPC endpoint URL
- `connection_timeout`: Connection timeout
- `request_timeout`: Request timeout
- `max_connections`: Maximum concurrent connections
- `max_idle_per_host`: Maximum idle connections per host
- `keep_alive_timeout`: Keep-alive timeout

#### API Settings
- `bind_address`: API server bind address
- `port`: API server port (default: 1945)
- `max_connections`: Maximum API connections

#### Performance Tuning
- `worker_threads`: Number of worker threads
- `blocking_threads`: Number of blocking threads
- `buffer_size`: Internal buffer size
- `batch_size`: Batch processing size
- `enable_compression`: Enable HTTP compression

#### Programs
Configure which programs to cache with optional filters:

```json
"programs": [
    {
        "program_id": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        "filters": [
            {
                "memcmp": {
                    "offset": 0,
                    "bytes": [6]
                }
            },
            {
                "memcmp": {
                    "offset": 400,
                    "bytes": "So11111111111111111111111111111111111111112"
                }
            }
        ]
    }
]
```

## Usage

### Running the Service

```bash
# Run with default config.json
./target/release/peregrine run

# Run with custom config
./target/release/peregrine run custom-config.json
```

### Validating Configuration

```bash
./target/release/peregrine validate config.json
```

### Commands

- `run`: Start the Peregrine service
- `validate`: Validate configuration file
- `generate`: Generate sample configuration

## API

Peregrine acts as a transparent proxy for Solana RPC calls. Point your application to:

```
http://localhost:1945
```

The service will:
1. Cache and serve `getProgramAccounts` calls from memory when available
2. Forward all other RPC calls to the configured upstream endpoint
3. Keep the cache synchronized via Yellowstone gRPC subscriptions

## Architecture

### Core Components

- **Connection Pool**: Manages gRPC and RPC client connections with automatic retry logic
- **Cache Layer**: High-performance concurrent HashMap (DashMap) for account storage
- **API Server**: Axum-based HTTP server for RPC proxying
- **Yellowstone Listener**: Subscribes to account updates via gRPC streaming
- **Parallel Processor**: Rayon-based parallel processing for serialization

### Performance Optimizations

- SIMD-accelerated JSON serialization
- Zero-copy streaming responses
- Connection pooling and reuse
- Efficient memory management with Arc-wrapped accounts
- Configurable thread pools for different workloads

## Development

### Project Structure

```
src/
├── main.rs          # Application entry point and core logic
├── api.rs           # API server and request handling
├── config.rs        # Configuration structures and validation
├── cli.rs           # Command-line interface
├── error.rs         # Error types and handling
└── structs.rs       # Data structures and types
```

### Building for Development

```bash
cargo build
cargo run -- run
```

### Running Tests

```bash
cargo test
```