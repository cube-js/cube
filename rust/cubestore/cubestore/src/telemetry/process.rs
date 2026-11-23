//! Identity and liveness of a Cube Store process, reported the same way by every binary.

use crate::app_metrics;
use crate::util::metrics;
use std::panic::PanicHookInfo;
use std::sync::Once;
use std::time::Duration;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Identifies builds made between releases, when set at compile time; `unknown` otherwise.
pub const BUILD_ID: &str = match option_env!("CUBESTORE_BUILD_ID") {
    Some(id) => id,
    None => "unknown",
};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

/// The same tag names for every process: a statsd bridge can drop samples of a metric whose label
/// names differ from the ones it first saw. `slot` names a pool subprocess, `main` otherwise.
pub fn identity_tags(role: &str, slot: Option<&str>) -> Vec<String> {
    vec![
        metrics::format_tag("version", VERSION),
        metrics::format_tag("build", &metrics::sanitize_tag_value(BUILD_ID)),
        metrics::format_tag("role", &metrics::sanitize_tag_value(role)),
        metrics::format_tag("slot", &metrics::sanitize_tag_value(slot.unwrap_or("main"))),
    ]
}

/// Reports the start of this process, counts its panics and keeps reporting that it is alive.
/// Call once per process, as soon as it knows its role; later calls are ignored.
pub fn start_process_metrics(role: &str, slot: Option<&str>) {
    static STARTED: Once = Once::new();
    STARTED.call_once(|| {
        let tags = identity_tags(role, slot);

        log::info!(
            "Cube Store process started: role={} version={} build={} pid={}",
            role,
            VERSION,
            BUILD_ID,
            std::process::id()
        );
        app_metrics::PROCESS_STARTS.add_with_tags(1, Some(&tags));
        install_panic_hook(tags.clone());

        // A plain thread, so liveness is reported even while the async runtime is stalled.
        let spawned = std::thread::Builder::new()
            .name("cubestore-process-metrics".to_string())
            .spawn(move || loop {
                report_sample(&tags);
                std::thread::sleep(SAMPLE_INTERVAL);
            });
        if let Err(e) = spawned {
            log::error!("Can't start process metrics thread: {}", e);
        }
    });
}

fn report_sample(tags: &Vec<String>) {
    app_metrics::PROCESS_HEARTBEAT.add_with_tags(1, Some(tags));
    if let Some(memory) = read_memory_status() {
        app_metrics::PROCESS_RSS_BYTES.report_with_tags(memory.rss_bytes as i64, Some(tags));
        app_metrics::PROCESS_PEAK_RSS_BYTES
            .report_with_tags(memory.peak_rss_bytes as i64, Some(tags));
    }
}

fn install_panic_hook(tags: Vec<String>) {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        app_metrics::PANICS.add_with_tags(1, Some(&tags));
        // Not through the logger: the panic may have happened inside it.
        eprintln!("{}", panic_summary(info));
        previous(info);
    }));
}

// One line, so that a log search for the panic also returns where and why it happened.
fn panic_summary(info: &PanicHookInfo) -> String {
    let payload = info
        .payload()
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| info.payload().downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string payload".to_string());
    let location = info
        .location()
        .map(|l| format!("{}:{}", l.file(), l.line()))
        .unwrap_or_else(|| "unknown location".to_string());
    format!("Panic at {}: {}", location, payload.replace('\n', " "))
}

#[derive(Debug, PartialEq)]
struct MemoryStatus {
    rss_bytes: u64,
    peak_rss_bytes: u64,
}

#[cfg(target_os = "linux")]
fn read_memory_status() -> Option<MemoryStatus> {
    parse_memory_status(&std::fs::read_to_string("/proc/self/status").ok()?)
}

#[cfg(not(target_os = "linux"))]
fn read_memory_status() -> Option<MemoryStatus> {
    None
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_memory_status(status: &str) -> Option<MemoryStatus> {
    let kilobytes = |key: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(key))
            .and_then(|value| value.trim().strip_suffix("kB"))
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(|kb| kb * 1024)
    };
    Some(MemoryStatus {
        rss_bytes: kilobytes("VmRSS:")?,
        peak_rss_bytes: kilobytes("VmHWM:")?,
    })
}

/// OOM kills in this container's memory cgroup so far. Unlike the container restart counter,
/// they include kills of subprocesses.
#[cfg(target_os = "linux")]
pub fn container_oom_kills() -> Option<u64> {
    // Cgroup files don't come and go while the process runs, so the probe is done once.
    static SOURCE: std::sync::OnceLock<Option<&'static str>> = std::sync::OnceLock::new();
    let path = SOURCE.get_or_init(|| {
        [
            "/sys/fs/cgroup/memory.events",
            "/sys/fs/cgroup/memory/memory.oom_control",
        ]
        .into_iter()
        .find(|path| std::path::Path::new(path).exists())
    });
    parse_oom_kills(&std::fs::read_to_string((*path)?).ok()?)
}

#[cfg(not(target_os = "linux"))]
pub fn container_oom_kills() -> Option<u64> {
    None
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_oom_kills(content: &str) -> Option<u64> {
    content
        .lines()
        .find_map(|line| line.strip_prefix("oom_kill "))
        .and_then(|value| value.trim().parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::metrics::test_capture;

    #[test]
    fn reports_start_heartbeat_and_panics_with_identity() {
        test_capture::watch("cs.process.started");
        test_capture::watch("cs.process.heartbeat");
        test_capture::watch("cs.panics");

        start_process_metrics("test-role", Some("sel1"));
        let panicked = std::panic::catch_unwind(|| panic!("first line\nsecond line"));
        assert!(panicked.is_err());

        let expected_tags = vec![
            format!("version:{}", VERSION),
            format!("build:{}", metrics::sanitize_tag_value(BUILD_ID)),
            "role:test-role".to_string(),
            "slot:sel1".to_string(),
        ];
        assert_eq!(
            test_capture::samples("cs.process.started"),
            vec![(1, expected_tags.clone())]
        );
        // Other tests may panic on purpose while this one runs, so only look for this hook's tags.
        assert!(test_capture::samples("cs.panics")
            .iter()
            .any(|(value, tags)| *value == 1 && *tags == expected_tags));

        for _ in 0..1000 {
            if !test_capture::samples("cs.process.heartbeat").is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            test_capture::samples("cs.process.heartbeat").first(),
            Some(&(1, expected_tags))
        );
    }

    #[test]
    fn parses_memory_status() {
        let status =
            "Name:\tcubestored\nVmPeak:\t 9000 kB\nVmHWM:\t    2048 kB\nVmRSS:\t    1024 kB\n";
        assert_eq!(
            parse_memory_status(status),
            Some(MemoryStatus {
                rss_bytes: 1024 * 1024,
                peak_rss_bytes: 2048 * 1024,
            })
        );
        assert_eq!(parse_memory_status("Name:\tcubestored\n"), None);
    }

    #[test]
    fn parses_oom_kills_of_both_cgroup_versions() {
        let v2 = "low 0\nhigh 0\nmax 12\noom 3\noom_kill 2\noom_group_kill 0\n";
        assert_eq!(parse_oom_kills(v2), Some(2));
        let v1 = "oom_kill_disable 0\nunder_oom 0\noom_kill 7\n";
        assert_eq!(parse_oom_kills(v1), Some(7));
        assert_eq!(parse_oom_kills("oom_kill_disable 0\n"), None);
    }
}
