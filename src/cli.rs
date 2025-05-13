use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "peregrine")]
#[command(author = "Peregrine Team")]
#[command(version = "0.1.0")]
#[command(about = "High-performance Solana program account caching with Yellowstone gRPC")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Run {
        #[arg(default_value = "config.json")]
        config: PathBuf,
    },
    Validate {
        #[arg(default_value = "config.json")]
        config: PathBuf,
    },
    Generate {
        #[arg(default_value = "config.json")]
        output: PathBuf,
    },
}
