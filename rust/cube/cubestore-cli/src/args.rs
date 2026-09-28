use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "cubestore-cli", version, about, long_about = None)]
pub struct Cli {
    /// Full WebSocket URL (ws://host:port or wss://host:port). Takes precedence over --host/--port.
    #[arg(long, env = "CUBESTORE_URL")]
    pub url: Option<String>,

    #[arg(long, env = "CUBESTORE_HOST", default_value = "127.0.0.1")]
    pub host: String,

    #[arg(long, env = "CUBESTORE_PORT", default_value_t = 3030)]
    pub port: u16,

    #[arg(long, env = "CUBESTORE_USER")]
    pub user: Option<String>,

    /// Pass `-` to read from a TTY prompt.
    #[arg(long, env = "CUBESTORE_PASSWORD")]
    pub password: Option<String>,

    /// Execute a single SQL statement and exit.
    #[arg(short = 'c', long = "command")]
    pub command: Option<String>,

    /// Execute SQL statements from a file and exit.
    #[arg(short = 'f', long = "file")]
    pub file: Option<PathBuf>,
}
