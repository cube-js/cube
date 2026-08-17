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

/// Resolve which paging style a list command was invoked with, returning
/// `true` for the deprecated `offset`/`limit` one.
///
/// The API deprecated `offset`/`limit` in favor of the `first`/`after`
/// cursor and the two address different pages of the same response, so an
/// endpoint given both either rejects the request (deployments) or silently
/// honors just one (environments, user attributes). Reject it here instead,
/// so the CLI never prints a page the caller did not ask for.
pub fn offset_paging(uses_offset: bool, uses_cursor: bool) -> Result<bool> {
    if uses_offset && uses_cursor {
        bail!("--offset/--limit are deprecated and cannot be combined with --first/--after");
    }
    Ok(uses_offset)
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
    fn set_skips_missing_flags() {
        let mut map = Map::new();
        set(&mut map, "present", &Some("v"));
        set(&mut map, "absent", &None::<String>);
        assert_eq!(map.get("present"), Some(&json!("v")));
        assert!(!map.contains_key("absent"));
    }

    #[test]
    fn offset_paging_rejects_mixing_the_two_styles() {
        assert!(!offset_paging(false, false).unwrap());
        assert!(offset_paging(true, false).unwrap());
        assert!(!offset_paging(false, true).unwrap());
        assert!(offset_paging(true, true).is_err());
    }
}
