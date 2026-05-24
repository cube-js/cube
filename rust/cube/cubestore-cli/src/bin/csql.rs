use std::io::{IsTerminal, Read};

use anyhow::{Context, Result};
use clap::Parser;
use cubestore_cli::args::Cli;
use cubestore_cli::{exec, repl};
use cubestore_ws_transport::{Client, ClientConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    let cli = Cli::parse();

    let url = build_url(&cli)?;
    let mut cfg = ClientConfig::new(url);
    // CLI flags override credentials embedded in the URL, but only when they
    // are actually provided — otherwise we'd clobber the URL-side creds with
    // None.
    if cli.user.is_some() {
        cfg.username = cli.user.clone();
    }
    if let Some(pass) = resolve_password(&cli)? {
        cfg.password = Some(pass);
    }

    let client = Client::connect(cfg)
        .await
        .context("failed to connect to cubestore")?;

    // Dispatch:
    //   -c <SQL>          → run single statement, exit
    //   -f <path>         → run file as script, exit
    //   stdin not a TTY   → read all stdin as script, exit
    //   otherwise         → REPL
    let result = if let Some(sql) = cli.command.as_deref() {
        let ok = exec::run_script(&client, sql, true).await?;
        Ok::<bool, anyhow::Error>(ok)
    } else if let Some(path) = cli.file.as_deref() {
        let script = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let ok = exec::run_script(&client, &script, true).await?;
        Ok(ok)
    } else if !std::io::stdin().is_terminal() {
        let mut script = String::new();
        std::io::stdin().read_to_string(&mut script)?;
        let ok = exec::run_script(&client, &script, true).await?;
        Ok(ok)
    } else {
        print_banner(&client);
        repl::run(&client, true).await?;
        Ok(true)
    };

    client.close();
    if !result? {
        std::process::exit(1);
    }

    Ok(())
}

fn build_url(cli: &Cli) -> Result<url::Url> {
    if let Some(raw) = cli.url.as_deref() {
        return url::Url::parse(raw).with_context(|| format!("invalid --url: {raw}"));
    }

    let raw = format!("ws://{}:{}", cli.host, cli.port);
    url::Url::parse(&raw).with_context(|| format!("invalid host/port: {raw}"))
}

fn resolve_password(cli: &Cli) -> Result<Option<String>> {
    match cli.password.as_deref() {
        Some("-") => {
            let pw = rpassword::prompt_password("Password: ")?;
            Ok(Some(pw))
        }
        Some(p) => Ok(Some(p.to_string())),
        None => Ok(None),
    }
}

fn print_banner(client: &Client) {
    let ver = client.server_version().unwrap_or("unknown");
    println!("csql {}  (server: {})", env!("CARGO_PKG_VERSION"), ver);
    println!("Type \"\\h\" for help, \"\\q\" to quit.");
}
