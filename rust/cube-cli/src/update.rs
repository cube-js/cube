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

/// The `Cargo.toml` placeholder build.rs falls back to when it can't read
/// `lerna.json` — read from the same place, so the two can't drift. Such a
/// binary has no real version to compare with a release.
const UNVERSIONED: &str = env!("CARGO_PKG_VERSION");

pub(crate) fn is_unversioned(version: &str) -> bool {
    version == UNVERSIONED
}

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
    pub draft: bool,
    #[serde(default)]
    pub prerelease: bool,
    #[serde(default)]
    pub assets: Vec<Asset>,
}

#[derive(Debug, Deserialize)]
pub struct Asset {
    pub name: String,
    pub browser_download_url: String,
    /// `uploaded` once the upload finished; an asset still being uploaded is
    /// listed with another state and can't be downloaded yet.
    #[serde(default)]
    pub state: Option<String>,
}

impl Release {
    pub fn version(&self) -> &str {
        self.tag_name.trim_start_matches('v')
    }

    pub fn asset_for_this_platform(&self) -> Option<&Asset> {
        let name = asset_name()?;
        self.asset(&name)
    }

    fn asset(&self, name: &str) -> Option<&Asset> {
        self.assets
            .iter()
            .find(|a| a.name == name && a.state.as_deref().is_none_or(|s| s == "uploaded"))
    }
}

/// How many of the most recently published releases `cube update` picks from.
pub const RELEASES_TO_SCAN: u32 = 30;

/// The background check runs alongside every command, so it reads a smaller
/// page: the current line's last few releases are enough to spot a newer one.
const BACKGROUND_RELEASES_TO_SCAN: u32 = 10;

/// Fetch the newest release that ships a binary for this platform.
///
/// Not `/releases/latest`: its binaries upload after publishing and can fail
/// (v1.7.14 lacked `aarch64-apple-darwin`). The installers pick the same way.
pub async fn latest_release(http: &reqwest::Client, scan: u32) -> Result<Release> {
    let url = format!(
        "{}/repos/{}/releases?per_page={scan}",
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
    let releases = res
        .json::<Vec<Release>>()
        .await
        .map_err(|e| anyhow!("could not parse release metadata: {e}"))?;
    let asset = asset_name();
    newest_installable(releases, asset.as_deref()).ok_or_else(|| {
        anyhow!(
            "none of the last {scan} releases has a binary for this platform ({}-{})",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })
}

/// The highest stable release that lists `asset`. Maintenance releases of
/// older lines are interleaved by publish date, so pick by version, not position.
fn newest_installable(releases: Vec<Release>, asset: Option<&str>) -> Option<Release> {
    releases
        .into_iter()
        .filter(|r| !r.draft && !r.prerelease)
        .filter(|r| asset.is_none_or(|name| r.asset(name).is_some()))
        .max_by(|a, b| version_key(a.version()).cmp(&version_key(b.version())))
}

/// The numeric segments of a dotted version, for ordering.
fn version_key(version: &str) -> Vec<u64> {
    version
        .split(['.', '-'])
        .map_while(|s| s.parse::<u64>().ok())
        .collect()
}

/// Order-compare two dotted versions numerically, segment by segment.
pub(crate) fn newer_than(candidate: &str, current: &str) -> bool {
    let (a, b) = (version_key(candidate), version_key(current));
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
        let Ok(release) = latest_release(&http, BACKGROUND_RELEASES_TO_SCAN).await else {
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

/// Await the background check, but only when its answer will be used: the
/// notice is interactive-only and the hint only runs on an API failure, so a
/// piped command that succeeded must not pay for a GitHub round trip it will
/// throw away.
pub async fn resolve(
    handle: Option<tokio::task::JoinHandle<UpdateCheck>>,
    api_error: bool,
) -> UpdateCheck {
    let Some(handle) = handle else {
        return UpdateCheck::Unknown;
    };
    if !wanted(api_error, std::io::stderr().is_terminal()) {
        return UpdateCheck::Unknown;
    }
    // The check runs concurrently with the command; give it a short grace
    // period in case the command finished faster than the API call.
    match tokio::time::timeout(Duration::from_millis(1500), handle).await {
        Ok(Ok(outcome)) => outcome,
        _ => UpdateCheck::Unknown,
    }
}

/// Whether anything will read the check's answer — [`print_notice`] on a
/// terminal, or [`api_error_hint`] under an API failure.
fn wanted(api_error: bool, interactive: bool) -> bool {
    api_error || interactive
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
        "\n{} {}\nRun {} to install it.",
        "A new release of Cube CLI is available:".yellow(),
        upgrade_path(latest, CURRENT_VERSION, true),
        "cube update".bold().cyan(),
    );
    true
}

/// `current → latest`, or just `latest` for an unversioned build, whose
/// placeholder version would only confuse.
fn upgrade_path(latest: &str, current: &str, color: bool) -> String {
    let (dim, emphasis) = if color {
        (Style::new().dimmed(), Style::new().bold().green())
    } else {
        (Style::new(), Style::new())
    };
    if is_unversioned(current) {
        emphasis.style(latest).to_string()
    } else {
        format!("{} → {}", dim.style(current), emphasis.style(latest))
    }
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
    hint_for(
        outcome,
        announced,
        std::io::stderr().is_terminal(),
        CURRENT_VERSION,
    )
}

/// Split out from [`api_error_hint`] so the policy can be tested without
/// touching process-wide environment or terminal state.
fn hint_for(outcome: &UpdateCheck, announced: bool, color: bool, current: &str) -> Option<String> {
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
        UpdateCheck::Newer(latest) => {
            let from = if is_unversioned(current) {
                String::new()
            } else {
                format!(" from {current}")
            };
            Some(format!(
                "{hint} this request failed on the API side, and Cube CLI {} is available — run {} \
                 to upgrade{from}, then try again",
                emphasis.style(latest),
                emphasis.style("cube update"),
            ))
        }
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
        hint_for(outcome, announced, false, "1.8.3")
    }

    #[test]
    fn a_piped_command_that_succeeded_does_not_wait_on_the_check() {
        // Neither consumer would read the answer, so there is nothing to await.
        assert!(!wanted(false, false));
        // A terminal gets the notice; an API failure gets the hint.
        assert!(wanted(false, true));
        assert!(wanted(true, false));
    }

    #[test]
    fn an_unversioned_build_is_offered_the_release_without_its_placeholder() {
        assert_eq!(upgrade_path("1.8.5", UNVERSIONED, false), "1.8.5");
        assert_eq!(upgrade_path("1.8.5", "1.8.3", false), "1.8.3 → 1.8.5");
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
        assert!(hint.contains("from 1.8.3"), "{hint}");
    }

    #[test]
    fn an_unversioned_cli_is_pointed_at_the_new_release_without_its_placeholder() {
        let hint = hint_for(
            &UpdateCheck::Newer("9.9.9".into()),
            false,
            false,
            UNVERSIONED,
        )
        .unwrap();
        assert!(hint.contains("cube update"), "{hint}");
        assert!(!hint.contains(UNVERSIONED), "{hint}");
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
        let plain = hint_for(&UpdateCheck::Unknown, false, false, "1.8.3").unwrap();
        let colored = hint_for(&UpdateCheck::Unknown, false, true, "1.8.3").unwrap();
        assert!(!plain.contains('\u{1b}'), "{plain}");
        assert!(colored.contains('\u{1b}'), "{colored}");
    }

    fn releases(json: &str) -> Vec<Release> {
        serde_json::from_str(json).unwrap()
    }

    const ASSET: &str = "cube-aarch64-apple-darwin.tar.gz";

    fn asset(tag: &str, state: &str) -> String {
        format!(
            r#"{{"name":"{ASSET}","state":"{state}","browser_download_url":"https://example.com/{tag}/{ASSET}"}}"#
        )
    }

    fn release(tag: &str, prerelease: bool, assets: &[String]) -> String {
        format!(
            r#"{{"tag_name":"{tag}","draft":false,"prerelease":{prerelease},"assets":[{}]}}"#,
            assets.join(",")
        )
    }

    fn newest(json: &str) -> Option<String> {
        newest_installable(releases(json), Some(ASSET)).map(|r| r.tag_name)
    }

    #[test]
    fn a_latest_release_missing_this_platform_falls_back_to_the_previous_one() {
        // v1.7.14 shipped without its aarch64-apple-darwin binary.
        let json = format!(
            "[{},{}]",
            release("v1.7.14", false, &[]),
            release("v1.7.13", false, &[asset("v1.7.13", "uploaded")]),
        );
        assert_eq!(newest(&json).as_deref(), Some("v1.7.13"));
    }

    #[test]
    fn an_asset_still_uploading_is_not_installable() {
        let json = format!(
            "[{},{}]",
            release("v1.8.1", false, &[asset("v1.8.1", "starting")]),
            release("v1.8.0", false, &[asset("v1.8.0", "uploaded")]),
        );
        assert_eq!(newest(&json).as_deref(), Some("v1.8.0"));
    }

    #[test]
    fn a_maintenance_release_published_later_does_not_win_over_a_higher_version() {
        let json = format!(
            "[{},{}]",
            release("v1.6.75", false, &[asset("v1.6.75", "uploaded")]),
            release("v1.8.12", false, &[asset("v1.8.12", "uploaded")]),
        );
        assert_eq!(newest(&json).as_deref(), Some("v1.8.12"));
    }

    #[test]
    fn prereleases_and_drafts_are_skipped() {
        let json = format!(
            r#"[{},{{"tag_name":"v1.9.1","draft":true,"assets":[{}]}},{}]"#,
            release("v2.0.0-beta.1", true, &[asset("v2.0.0-beta.1", "uploaded")]),
            asset("v1.9.1", "uploaded"),
            release("v1.9.0", false, &[asset("v1.9.0", "uploaded")]),
        );
        assert_eq!(newest(&json).as_deref(), Some("v1.9.0"));
    }

    #[test]
    fn no_release_with_this_platform_is_none() {
        let json = format!("[{}]", release("v1.8.0", false, &[]));
        assert_eq!(newest(&json), None);
    }

    #[test]
    fn an_unsupported_platform_still_learns_the_newest_release() {
        let json = format!(
            "[{},{}]",
            release("v1.8.0", false, &[]),
            release("v1.8.1", false, &[]),
        );
        let newest = newest_installable(releases(&json), None).map(|r| r.tag_name);
        assert_eq!(newest.as_deref(), Some("v1.8.1"));
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
