use std::io::IsTerminal as _;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use owo_colors::{OwoColorize, Style};
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

/// Outcome of the background release check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateCheck {
    /// A release newer than this binary is available.
    Newer(String),
    /// This binary is already on the latest release.
    UpToDate,
    /// Checks are opted out of via `CUBE_NO_UPDATE_CHECK`.
    Disabled,
    /// The check could not be completed — offline, rate limited, or slower
    /// than the command it ran alongside.
    Unknown,
}

/// Spawn a background check for a newer release. Resolve the returned handle
/// with [`resolve`] once the command finishes. Failures (offline, rate limit)
/// resolve to [`UpdateCheck::Unknown`] rather than surfacing an error.
pub fn spawn_check() -> tokio::task::JoinHandle<UpdateCheck> {
    tokio::spawn(async {
        if std::env::var_os("CUBE_NO_UPDATE_CHECK").is_some() {
            return UpdateCheck::Disabled;
        }
        let Ok(http) = reqwest::Client::builder()
            .user_agent(concat!("cube-cli/", env!("CUBE_CLI_VERSION")))
            .build()
        else {
            return UpdateCheck::Unknown;
        };
        let Ok(release) = latest_release(&http).await else {
            return UpdateCheck::Unknown;
        };
        let latest = release.version().to_string();
        if newer_than(&latest, CURRENT_VERSION) {
            UpdateCheck::Newer(latest)
        } else {
            UpdateCheck::UpToDate
        }
    })
}

/// Await the background check (best effort, never blocks long). The check runs
/// concurrently with the command; give it a short grace period in case the
/// command finished faster than the API call.
pub async fn resolve(handle: tokio::task::JoinHandle<UpdateCheck>) -> UpdateCheck {
    match tokio::time::timeout(Duration::from_millis(1500), handle).await {
        Ok(Ok(outcome)) => outcome,
        _ => UpdateCheck::Unknown,
    }
}

/// Print the "new release available" notice, if there is one to print.
/// Returns whether it was printed. Interactive terminals only — an unprompted
/// nag has no place in piped or logged output.
pub fn print_notice(outcome: &UpdateCheck) -> bool {
    if !std::io::stderr().is_terminal() {
        return false;
    }
    let UpdateCheck::Newer(latest) = outcome else {
        return false;
    };
    eprintln!(
        "\n{} {} → {}\nRun {} to install it.",
        "A new release of Cube CLI is available:".yellow(),
        CURRENT_VERSION.dimmed(),
        latest.bold().green(),
        "cube update".bold().cyan(),
    );
    true
}

/// Hint printed under an API error. A CLI that lags the API is a common cause
/// of otherwise puzzling API errors, so point at `cube update` — but only when
/// this binary really is behind: telling someone already on the latest release
/// to update is noise that teaches them to ignore the hint.
///
/// `announced` says whether [`print_notice`] just reported the same release, so
/// the hint can point back at it instead of repeating the version and command.
///
/// Unlike the notice, this is not limited to interactive terminals: it is
/// attached to a failure the user asked for rather than an unprompted nag, and
/// a stale pinned CLI in CI is exactly where the advice pays off.
pub fn api_error_hint(outcome: &UpdateCheck, announced: bool) -> Option<String> {
    hint_for(outcome, announced, std::io::stderr().is_terminal())
}

/// Split out from [`api_error_hint`] so the policy can be tested without
/// touching process-wide environment or terminal state.
fn hint_for(outcome: &UpdateCheck, announced: bool, color: bool) -> Option<String> {
    let (label, emphasis) = if color {
        (Style::new().yellow(), Style::new().bold().cyan())
    } else {
        (Style::new(), Style::new())
    };
    let hint = label.style("hint:");
    match outcome {
        // Already current, or the user opted out of update checks: nothing to
        // suggest that could plausibly resolve the error.
        UpdateCheck::UpToDate | UpdateCheck::Disabled => None,
        UpdateCheck::Newer(_) if announced => Some(format!(
            "{hint} this request failed on the API side — the newer release above may already fix it"
        )),
        UpdateCheck::Newer(latest) => Some(format!(
            "{hint} this request failed on the API side, and Cube CLI {} is available — run {} \
             to upgrade from {CURRENT_VERSION}, then try again",
            emphasis.style(latest),
            emphasis.style("cube update"),
        )),
        // Could not find out whether a newer release exists, so the advice is
        // worth giving but not worth asserting.
        UpdateCheck::Unknown => Some(format!(
            "{hint} this request failed on the API side; a newer release may already fix it — run \
             {} to check, then try again",
            emphasis.style("cube update"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hint(outcome: &UpdateCheck, announced: bool) -> Option<String> {
        hint_for(outcome, announced, false)
    }

    #[test]
    fn a_current_cli_is_never_told_to_update() {
        assert_eq!(hint(&UpdateCheck::UpToDate, false), None);
        assert_eq!(hint(&UpdateCheck::Disabled, false), None);
    }

    #[test]
    fn a_stale_cli_is_pointed_at_the_new_release() {
        let hint = hint(&UpdateCheck::Newer("9.9.9".into()), false).unwrap();
        assert!(hint.contains("cube update"), "{hint}");
        assert!(hint.contains("9.9.9"), "{hint}");
        assert!(hint.contains(CURRENT_VERSION), "{hint}");
    }

    #[test]
    fn an_announced_release_is_referenced_rather_than_repeated() {
        let hint = hint(&UpdateCheck::Newer("9.9.9".into()), true).unwrap();
        assert!(!hint.contains("cube update"), "{hint}");
        assert!(hint.contains("above"), "{hint}");
    }

    #[test]
    fn an_undetermined_check_suggests_looking_rather_than_asserting() {
        let hint = hint(&UpdateCheck::Unknown, false).unwrap();
        assert!(hint.contains("cube update"), "{hint}");
        assert!(hint.contains("may"), "{hint}");
    }

    #[test]
    fn color_is_applied_only_when_asked_for() {
        let plain = hint_for(&UpdateCheck::Unknown, false, false).unwrap();
        let colored = hint_for(&UpdateCheck::Unknown, false, true).unwrap();
        assert!(!plain.contains('\u{1b}'), "{plain}");
        assert!(colored.contains('\u{1b}'), "{colored}");
    }

    #[test]
    fn newer_than_compares_numerically() {
        assert!(newer_than("1.7.10", "1.7.2"));
        assert!(newer_than("1.8.0", "1.7.9"));
        assert!(!newer_than("1.7.2", "1.7.2"));
        assert!(!newer_than("1.7.1", "1.7.2"));
        assert!(newer_than("2.0.0", "1.99.99"));
    }
}
