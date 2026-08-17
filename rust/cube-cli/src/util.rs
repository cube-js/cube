use std::io::Read;

use anyhow::{bail, Context as _, Result};
use serde_json::{Map, Value};

use crate::client::Query;

/// Parse a `--data` argument into a JSON object.
///
/// Accepts inline JSON (`'{"name": "x"}'`), `@path/to/file.json`, or `-`
/// to read from stdin — the same convention as `gh api` / `curl -d`.
pub fn parse_data(data: Option<&str>) -> Result<Map<String, Value>> {
    let Some(data) = data else {
        return Ok(Map::new());
    };
    let raw = if data == "-" {
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        buf
    } else if let Some(path) = data.strip_prefix('@') {
        std::fs::read_to_string(path).with_context(|| format!("failed to read {path}"))?
    } else {
        data.to_string()
    };
    let value: Value = serde_json::from_str(&raw).context("--data is not valid JSON")?;
    match value {
        Value::Object(map) => Ok(map),
        _ => bail!("--data must be a JSON object"),
    }
}

/// Insert a flag value into a JSON body if it was provided on the CLI.
pub fn set<T: serde::Serialize>(body: &mut Map<String, Value>, key: &str, value: &Option<T>) {
    if let Some(v) = value {
        body.insert(key.to_string(), serde_json::to_value(v).unwrap());
    }
}

/// Push a query parameter if the flag was provided.
pub fn push<T: ToString>(query: &mut Query, key: &str, value: &Option<T>) {
    if let Some(v) = value {
        query.push((key.to_string(), v.to_string()));
    }
}

/// How one endpoint implements the deprecated offset paging it still accepts,
/// which is what decides whether the CLI has to read the deprecated `data`
/// field to honor it. Verified per endpoint against the server, not assumed —
/// the two shapes want opposite handling, so there is no safe blanket answer.
#[derive(Clone, Copy)]
pub struct ListPaging {
    /// The command's own deprecated flag names, for the error message.
    flags: &'static str,
    /// Whether `data` alone is sliced. When false the slicing happens in the
    /// query, so `items` already holds exactly the requested page.
    data_only: bool,
}

/// Sliced by the database query: `items` and `data` hold the same rows, so
/// render `items` and stay correct after `data` is eventually removed.
pub const OFFSET_LIMIT_IN_QUERY: ListPaging = ListPaging {
    flags: "--offset/--limit",
    data_only: false,
};

/// As [`OFFSET_LIMIT_IN_QUERY`], for `reports list`, which deprecates a
/// different pair of flags.
pub const LIMIT_PAGE_IN_QUERY: ListPaging = ListPaging {
    flags: "--limit/--page",
    data_only: false,
};

/// Applied in memory to `data` alone, while `items` is the full cursor page
/// regardless of the flags. Only reading `data` honors them.
pub const OFFSET_LIMIT_DATA_ONLY: ListPaging = ListPaging {
    flags: "--offset/--limit",
    data_only: true,
};

/// Which response field a list command should render, given how its endpoint
/// pages and the flags it was invoked with — the two deprecated ones (in the
/// order `paging` names them), then `--first`/`--after`. `Some("data")`
/// selects the deprecated offset-sliced page; `None` the canonical `items`.
///
/// The API deprecated offset paging in favor of the `first`/`after` cursor
/// and the two address different pages of the same response, so an endpoint
/// given both either rejects the request (deployments, reports) or silently
/// honors just one (environments, user attributes). Reject it here instead,
/// so the CLI never prints a page the caller did not ask for.
///
/// Reading `data` is only ever right for a [`ListPaging::data_only`]
/// endpoint. Doing it everywhere "for uniformity" would be actively harmful:
/// on a query-sliced endpoint `items` is already the requested page and stays
/// so after `data` is dropped, whereas asking for `data` would then fail a
/// command the server still honors perfectly well.
pub fn paging_field(
    paging: ListPaging,
    deprecated_a: Option<u64>,
    deprecated_b: Option<u64>,
    first: Option<u64>,
    after: Option<&str>,
) -> Result<Option<&'static str>> {
    let uses_offset = deprecated_a.is_some() || deprecated_b.is_some();
    let uses_cursor = first.is_some() || after.is_some();
    if uses_offset && uses_cursor {
        bail!(
            "{} are deprecated and cannot be combined with --first/--after",
            paging.flags
        );
    }
    Ok((uses_offset && paging.data_only).then_some("data"))
}

/// Normalize a user-supplied Cube Cloud URL: trim whitespace and trailing
/// slashes, and default to `https://` when no scheme is given (reqwest
/// otherwise fails with "relative URL without a base").
pub fn normalize_url(url: &str) -> String {
    let url = url.trim().trim_end_matches('/');
    if url.is_empty() || url.contains("://") {
        url.to_string()
    } else {
        format!("https://{url}")
    }
}

/// Parse a `KEY=VALUE` pair (for `cube variables set`).
pub fn parse_kv(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(format!("`{s}` is not in KEY=VALUE format")),
    }
}

pub fn body(map: Map<String, Value>) -> Value {
    Value::Object(map)
}

/// Reject a branch name or git ref that was supplied but empty.
///
/// "Omitted" and "empty" must not collapse into the same request. Both travel as a
/// present-but-empty field, and the server reads that as absent — so the value does
/// not fail, it silently falls back to whatever the server would have picked anyway:
/// the deployment's active or deploy branch for `branchName`, the dbt integration's
/// tracked branch for `ref`. Both were measured against a live tenant; neither
/// errors. That is the one failure mode a CI gate must not have, because the run
/// still exits 0 — green for code it never looked at.
///
/// Empty values reach the CLI from scripts, not just typos: `$GITHUB_HEAD_REF` is
/// empty on every trigger except `pull_request`, and `jq -r` prints nothing at all
/// for empty input. (A missing *field* prints `null`, which travels as a literal
/// name and gets a 404 — loud, and the right answer.)
///
/// Every flag carrying a branch or a ref goes through here before it is sent.
pub fn require_nonempty(flag: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!(
            "{flag} cannot be empty — the server reads an empty value as \
             \"not specified\" and falls back to the branch it would have picked \
             anyway, so this would silently run against the wrong code"
        );
    }

    Ok(())
}

/// `require_nonempty` for an optional flag: only checks a value that was supplied.
pub fn require_nonempty_opt(flag: &str, value: &Option<String>) -> Result<()> {
    match value {
        Some(v) => require_nonempty(flag, v),
        None => Ok(()),
    }
}

/// Parse a wait duration: a bare number of seconds, or a number with a `s`/`m`/`h`
/// suffix (`90`, `30s`, `15m`, `1h`).
///
/// Written for the `--wait` flags, whose useful range spans seconds (a poll
/// interval) to tens of minutes (a dbt sync). A bare number would have to pick one
/// of those as its unit and silently surprise anyone who meant the other, so the
/// suffix carries it — with seconds as the bare-number default, since that is what
/// every other CLI does.
pub fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    let raw = s.trim();
    // Case-insensitive: `30S` and `15M` are obviously meant, and rejecting them
    // teaches nothing.
    let (digits, multiplier) = match raw.chars().last().map(|c| c.to_ascii_lowercase()) {
        Some('s') => (&raw[..raw.len() - 1], 1),
        Some('m') => (&raw[..raw.len() - 1], 60),
        Some('h') => (&raw[..raw.len() - 1], 3600),
        _ => (raw, 1),
    };
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("`{s}` is not a duration (try 30s, 15m, or 1h)"))?;
    if value == 0 {
        return Err(format!("`{s}` must be greater than zero"));
    }
    // Checked: the whole job of this function is to reject input it can't honour,
    // so it must not be the thing that panics (debug) or wraps to a nonsense
    // duration (release) on an absurd number of hours.
    let seconds = value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("`{s}` is longer than this can represent"))?;

    Ok(std::time::Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_kv_accepts_key_value_pairs() {
        assert_eq!(parse_kv("A=1").unwrap(), ("A".to_string(), "1".to_string()));
        assert_eq!(
            parse_kv("A=b=c").unwrap(),
            ("A".to_string(), "b=c".to_string())
        );
        assert!(parse_kv("no-equals").is_err());
        assert!(parse_kv("=value").is_err());
    }

    #[test]
    fn parse_data_accepts_inline_json_objects_only() {
        let map = parse_data(Some(r#"{"name": "x", "n": 1}"#)).unwrap();
        assert_eq!(map.get("name"), Some(&json!("x")));
        assert_eq!(map.get("n"), Some(&json!(1)));
        assert!(parse_data(Some("[1, 2]")).is_err());
        assert!(parse_data(Some("not json")).is_err());
        assert!(parse_data(None).unwrap().is_empty());
    }

    #[test]
    fn normalize_url_defaults_to_https() {
        assert_eq!(
            normalize_url("cloud.cubecloud.dev"),
            "https://cloud.cubecloud.dev"
        );
        assert_eq!(
            normalize_url(" cloud.cubecloud.dev/ "),
            "https://cloud.cubecloud.dev"
        );
        assert_eq!(
            normalize_url("https://cloud.cubecloud.dev/"),
            "https://cloud.cubecloud.dev"
        );
        assert_eq!(
            normalize_url("http://localhost:4000"),
            "http://localhost:4000"
        );
        assert_eq!(normalize_url(""), "");
    }

    #[test]
    fn require_nonempty_rejects_supplied_but_empty() {
        assert!(require_nonempty("--branch", "main").is_ok());
        assert!(require_nonempty("--ref", "dbt-sync/x").is_ok());
        assert!(require_nonempty("--branch", "").is_err());
        assert!(require_nonempty("--ref", "   ").is_err());
    }

    #[test]
    fn require_nonempty_opt_distinguishes_omitted_from_empty() {
        // The whole point: `None` is fine, `Some("")` is not.
        assert!(require_nonempty_opt("--ref", &None).is_ok());
        assert!(require_nonempty_opt("--ref", &Some("main".into())).is_ok());
        assert!(require_nonempty_opt("--ref", &Some(String::new())).is_err());
        assert!(require_nonempty_opt("--ref", &Some(" ".into())).is_err());
    }

    #[test]
    fn parse_duration_reads_the_unit_suffix() {
        use std::time::Duration;
        assert_eq!(parse_duration("90").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("15m").unwrap(), Duration::from_secs(900));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration(" 5m ").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("30S").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("15M").unwrap(), Duration::from_secs(900));
    }

    #[test]
    fn parse_duration_rejects_what_it_cannot_honour() {
        // A zero interval would spin, and a zero timeout would expire before the
        // first poll — both are mistakes, not configurations.
        assert!(parse_duration("0").is_err());
        assert!(parse_duration("0m").is_err());
        assert!(parse_duration("").is_err());
        assert!(parse_duration("soon").is_err());
        assert!(parse_duration("-5s").is_err());
        assert!(parse_duration("5d").is_err());
        // Rejected, not panicked on in debug or wrapped in release.
        assert!(parse_duration("9999999999999999999h").is_err());
        assert!(parse_duration(&format!("{}h", u64::MAX)).is_err());
    }

    #[test]
    fn set_skips_missing_flags() {
        let mut map = Map::new();
        set(&mut map, "present", &Some("v"));
        set(&mut map, "absent", &None::<String>);
        assert_eq!(map.get("present"), Some(&json!("v")));
        assert!(!map.contains_key("absent"));
    }

    #[test]
    fn only_a_data_only_endpoint_renders_the_deprecated_field() {
        // The in-memory list: `items` ignores offset paging, so `data` it is.
        for (a, b) in [(Some(10), None), (None, Some(5)), (Some(10), Some(5))] {
            let field = paging_field(OFFSET_LIMIT_DATA_ONLY, a, b, None, None).unwrap();
            assert_eq!(field, Some("data"));
        }

        // Query-sliced endpoints must NOT ask for `data`: `items` is already
        // the requested page, and stays right after `data` is removed —
        // asking would fail a command the server still honors.
        for paging in [OFFSET_LIMIT_IN_QUERY, LIMIT_PAGE_IN_QUERY] {
            let field = paging_field(paging, Some(10), Some(5), None, None).unwrap();
            assert_eq!(field, None);
        }
    }

    #[test]
    fn paging_field_renders_items_whenever_offset_paging_is_unused() {
        for paging in [
            OFFSET_LIMIT_DATA_ONLY,
            OFFSET_LIMIT_IN_QUERY,
            LIMIT_PAGE_IN_QUERY,
        ] {
            assert_eq!(paging_field(paging, None, None, None, None).unwrap(), None);
            for (first, after) in [(Some(5), None), (None, Some("cursor"))] {
                assert_eq!(
                    paging_field(paging, None, None, first, after).unwrap(),
                    None
                );
            }
        }
    }

    #[test]
    fn paging_field_rejects_mixing_and_names_the_command_s_own_flags() {
        assert!(paging_field(OFFSET_LIMIT_DATA_ONLY, Some(10), None, Some(5), None).is_err());
        assert!(paging_field(OFFSET_LIMIT_IN_QUERY, None, Some(5), None, Some("cursor")).is_err());

        // `reports list` deprecates --limit/--page, not --offset/--limit.
        let err = paging_field(LIMIT_PAGE_IN_QUERY, Some(50), None, Some(5), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--limit/--page"), "got: {err}");
        assert!(err.contains("--first/--after"), "got: {err}");
    }
}
