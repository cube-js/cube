use std::sync::Mutex;
use std::time::Duration;

use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

/// Anonymous usage telemetry, wire-compatible with the legacy `cubejs` CLI
/// (`@cubejs-backend/shared` `track()`): events are POSTed as a JSON array to
/// track.cube.dev with the same field names, so they land in the existing
/// pipeline. No personal data is sent — the anonymous id is a SHA-256 hash
/// of the OS machine id.
///
/// Disabled when `CUBE_NO_TELEMETRY`/`CUBEJS_TELEMETRY=false` is set, or in
/// CI (the `CI` env var, matching the legacy CLI).
const TRACK_URL: &str = "https://track.cube.dev/track";

fn track_url() -> String {
    std::env::var("CUBE_TELEMETRY_URL").unwrap_or_else(|_| TRACK_URL.to_string())
}

static QUEUE: Mutex<Vec<Value>> = Mutex::new(Vec::new());

pub fn enabled() -> bool {
    if std::env::var_os("CI").is_some() || std::env::var_os("CUBE_NO_TELEMETRY").is_some() {
        return false;
    }
    !matches!(
        std::env::var("CUBEJS_TELEMETRY").as_deref(),
        Ok("false") | Ok("0")
    )
}

/// Queue an event. `props` are merged into the payload.
pub fn event(name: &str, props: Map<String, Value>) {
    if !enabled() {
        return;
    }
    let mut payload = json!({
        "event": name,
        "cliVersion": env!("CUBE_CLI_VERSION"),
        "clientTimestamp": timestamp(),
        "id": random_id(),
        "platform": platform(),
        "arch": arch(),
        "anonymousId": anonymous_id(),
        "sentFrom": "backend",
    });
    if let Some(obj) = payload.as_object_mut() {
        obj.extend(props);
    }
    QUEUE.lock().unwrap().push(payload);
}

/// Flush queued events (best effort, bounded, silent on failure).
pub async fn flush() {
    let events = std::mem::take(&mut *QUEUE.lock().unwrap());
    if events.is_empty() {
        return;
    }
    let Ok(http) = reqwest::Client::builder()
        .user_agent(concat!("cube-cli/", env!("CUBE_CLI_VERSION")))
        .timeout(Duration::from_secs(2))
        .build()
    else {
        return;
    };
    let sent_at = timestamp();
    let body: Vec<Value> = events
        .into_iter()
        .map(|mut e| {
            if let Some(obj) = e.as_object_mut() {
                obj.insert("sentAt".into(), json!(sent_at));
            }
            e
        })
        .collect();
    let _ = http.post(track_url()).json(&body).send().await;
}

/// ISO-8601 UTC timestamp without a chrono dependency.
fn timestamp() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let (secs, millis) = (now.as_secs() as i64, now.subsec_millis());
    // Civil-from-days algorithm (Howard Hinnant), valid across leap years.
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{millis:03}Z")
}

/// Random-enough event id (not security sensitive).
fn random_id() -> String {
    let mut hasher = Sha256::new();
    hasher.update(std::process::id().to_le_bytes());
    hasher.update(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes(),
    );
    hasher.update(QUEUE.lock().unwrap().len().to_le_bytes());
    format!("{:x}", hasher.finalize())[..32].to_string()
}

/// Node-compatible platform name, so dashboards see one vocabulary.
fn platform() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    }
}

/// Node-compatible arch name.
fn arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        other => other,
    }
}

/// SHA-256 of the OS machine id — same sources as the legacy CLI's
/// machine-id module (dbus/systemd id on Linux, IOPlatformUUID on macOS,
/// MachineGuid on Windows), falling back to the hostname.
fn anonymous_id() -> String {
    let raw = machine_id().unwrap_or_else(|| "unknown".to_string());
    let mut hasher = Sha256::new();
    hasher.update(raw.trim().as_bytes());
    format!("{:x}", hasher.finalize())
}

fn machine_id() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        for path in ["/var/lib/dbus/machine-id", "/etc/machine-id"] {
            if let Ok(id) = std::fs::read_to_string(path) {
                if !id.trim().is_empty() {
                    return Some(id);
                }
            }
        }
        return hostname();
    }
    #[cfg(target_os = "macos")]
    {
        let out = std::process::Command::new("ioreg")
            .args(["-rd1", "-c", "IOPlatformExpertDevice"])
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&out.stdout).to_string();
        return text
            .lines()
            .find(|l| l.contains("IOPlatformUUID"))
            .and_then(|l| l.split('"').nth(3))
            .map(|s| s.to_lowercase());
    }
    #[cfg(target_os = "windows")]
    {
        let out = std::process::Command::new("REG")
            .args([
                "QUERY",
                r"HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Cryptography",
                "/v",
                "MachineGuid",
            ])
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&out.stdout).to_string();
        return text.split_whitespace().last().map(|s| s.to_lowercase());
    }
    #[allow(unreachable_code)]
    hostname()
}

fn hostname() -> Option<String> {
    let out = std::process::Command::new("hostname").output().ok()?;
    let name = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!name.is_empty()).then_some(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_is_iso8601() {
        let ts = timestamp();
        // e.g. 2026-07-20T12:34:56.789Z
        assert_eq!(ts.len(), 24);
        assert!(ts.ends_with('Z'));
        assert_eq!(&ts[4..5], "-");
        assert_eq!(&ts[10..11], "T");
        assert!(ts.starts_with("20"));
    }

    #[test]
    fn ids_are_stable_length_and_distinct() {
        assert_eq!(anonymous_id().len(), 64);
        assert_eq!(anonymous_id(), anonymous_id());
        assert_eq!(random_id().len(), 32);
    }
}
