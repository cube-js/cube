use std::io::IsTerminal as _;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use owo_colors::OwoColorize;
use serde::Deserialize;

/// GitHub repository that hosts CLI release assets. Overridable for testing.
pub fn release_repo() -> String {
    std::env::var("CUBE_UPDATE_REPO").unwrap_or_else(|_| "cube-js/cube".to_string())
}

/// GitHub API base URL. Overridable for tests and GitHub Enterprise mirrors.
fn release_api_base() -> String {
    std::env::var("CUBE_UPDATE_API").unwrap_or_else(|_| "https://api.github.com".to_string())
}

pub const CURRENT_VERSION: &str = env!("CUBE_CLI_VERSION");

/// The release target triple this binary maps to. Linux always maps to the
/// musl asset — that's the only Linux artifact we ship, and it runs anywhere.
pub fn release_target() -> Option<&'static str> {
    Some(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "x86_64-unknown-linux-musl",
        ("linux", "aarch64") => "aarch64-unknown-linux-musl",
        ("macos", "x86_64") => "x86_64-apple-darwin",
        ("macos", "aarch64") => "aarch64-apple-darwin",
        ("windows", "x86_64") => "x86_64-pc-windows-msvc",
        _ => return None,
    })
}

pub fn asset_name() -> Option<String> {
    release_target().map(|t| format!("cube-{t}.tar.gz"))
}

#[derive(Debug, Deserialize)]
pub struct Release {
    pub tag_name: String,
    #[serde(default)]
    pub assets: Vec<Asset>,
}

#[derive(Debug, Deserialize)]
pub struct Asset {
    pub name: String,
    pub browser_download_url: String,
}

impl Release {
    pub fn version(&self) -> &str {
        self.tag_name.trim_start_matches('v')
    }

    pub fn asset_for_this_platform(&self) -> Option<&Asset> {
        let name = asset_name()?;
        self.assets.iter().find(|a| a.name == name)
    }
}

/// Fetch the latest release metadata from the GitHub API.
pub async fn latest_release(http: &reqwest::Client) -> Result<Release> {
    let url = format!(
        "{}/repos/{}/releases/latest",
        release_api_base(),
        release_repo()
    );
    let res = http
        .get(&url)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    if !res.status().is_success() {
        bail!("release lookup failed ({}) at {url}", res.status());
    }
    res.json::<Release>()
        .await
        .map_err(|e| anyhow!("could not parse release metadata: {e}"))
}

/// Order-compare two dotted versions numerically, segment by segment.
fn newer_than(candidate: &str, current: &str) -> bool {
    let parse = |v: &str| -> Vec<u64> {
        v.split(['.', '-'])
            .map_while(|s| s.parse::<u64>().ok())
            .collect()
    };
    let (a, b) = (parse(candidate), parse(current));
    if a.is_empty() || b.is_empty() {
        return candidate != current;
    }
    a > b
}

/// Spawn a background check for a newer release. Await the returned handle
/// after the command finishes; it resolves to a printable notice, or `None`.
/// Failures (offline, rate limit) resolve silently to `None`.
pub fn spawn_check() -> tokio::task::JoinHandle<Option<String>> {
    tokio::spawn(async {
        if std::env::var_os("CUBE_NO_UPDATE_CHECK").is_some() {
            return None;
        }
        let http = reqwest::Client::builder()
            .user_agent(concat!("cube-cli/", env!("CUBE_CLI_VERSION")))
            .build()
            .ok()?;
        let release = latest_release(&http).await.ok()?;
        let latest = release.version().to_string();
        if newer_than(&latest, CURRENT_VERSION) {
            Some(format!(
                "\n{} {} → {}\nRun {} to install it.",
                "A new release of Cube CLI is available:".yellow(),
                CURRENT_VERSION.dimmed(),
                latest.bold().green(),
                "cube update".bold().cyan(),
            ))
        } else {
            None
        }
    })
}

/// Print a pending update notice (best effort, never blocks long).
pub async fn print_notice(handle: tokio::task::JoinHandle<Option<String>>) {
    if !std::io::stderr().is_terminal() {
        return;
    }
    // The check runs concurrently with the command; give a short grace
    // period in case the command finished faster than the API call.
    if let Ok(Ok(Some(notice))) = tokio::time::timeout(Duration::from_millis(1500), handle).await {
        eprintln!("{notice}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_than_compares_numerically() {
        assert!(newer_than("1.7.10", "1.7.2"));
        assert!(newer_than("1.8.0", "1.7.9"));
        assert!(!newer_than("1.7.2", "1.7.2"));
        assert!(!newer_than("1.7.1", "1.7.2"));
        assert!(newer_than("2.0.0", "1.99.99"));
    }
}
