use anyhow::{bail, Result};
use clap::{Subcommand, ValueEnum};
use serde_json::Value;

use crate::client::Query;
use crate::{output, util, Ctx};

#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Clone, Copy, ValueEnum)]
enum Status {
    Success,
    Error,
    /// No response yet
    Running,
    /// No response, and silent for five minutes
    NoResponse,
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Status::Success => "success",
            Status::Error => "error",
            Status::Running => "running",
            Status::NoResponse => "noResponse",
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum ApiType {
    Rest,
    Graphql,
    Sql,
    Ws,
}

impl ApiType {
    fn as_str(self) -> &'static str {
        match self {
            ApiType::Rest => "rest",
            ApiType::Graphql => "graphql",
            ApiType::Sql => "sql",
            ApiType::Ws => "ws",
        }
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// List a deployment's API requests, newest first (production, data queries only)
    #[command(alias = "ls")]
    List {
        /// Deployment id
        deployment: i64,
        /// Start of the window, ISO 8601 (default: 24 hours before --to)
        #[arg(long)]
        from: Option<String>,
        /// End of the window, ISO 8601 (default: now)
        #[arg(long)]
        to: Option<String>,
        /// Only requests with this status
        #[arg(long, value_enum)]
        status: Option<Status>,
        /// Only requests on this API; repeat for several
        #[arg(long = "api-type", value_enum)]
        api_type: Vec<ApiType>,
        /// Only requests that took at least this many milliseconds
        #[arg(long)]
        min_duration_ms: Option<u64>,
        /// Substring of the query, security context, request id or span id
        #[arg(long)]
        search: Option<String>,
        /// Page size (up to 200, default 100)
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor); it keeps the
        /// first page's filters, so it does not take the other filter flags
        #[arg(
            long,
            conflicts_with_all = ["from", "to", "status", "api_type", "min_duration_ms", "search"]
        )]
        after: Option<String>,
    },
    /// Show one request with the SQL it generated
    Get {
        /// Deployment id
        deployment: i64,
        /// Request id from `list` (`<epochMs>-<spanId>`, as in the Query History link)
        request: String,
    },
}

#[allow(clippy::too_many_arguments)]
fn list_query(
    from: Option<String>,
    to: Option<String>,
    status: Option<Status>,
    api_type: &[ApiType],
    min_duration_ms: Option<u64>,
    search: Option<String>,
    first: Option<u64>,
    after: Option<String>,
) -> Query {
    let mut query = Vec::new();
    util::push(&mut query, "from", &from);
    util::push(&mut query, "to", &to);
    util::push(&mut query, "status", &status.map(Status::as_str));
    for api_type in api_type {
        query.push(("apiType".to_string(), api_type.as_str().to_string()));
    }
    util::push(&mut query, "minDurationMs", &min_duration_ms);
    util::push(&mut query, "search", &search);
    util::push(&mut query, "first", &first);
    util::push(&mut query, "after", &after);
    query
}

const LIST_COLUMNS: [(&str, &str); 7] = [
    ("ID", "id"),
    ("TIME", "timestamp"),
    ("STATUS", "status"),
    ("API", "apiType"),
    ("MS", "durationMs"),
    ("CACHE", "cacheType"),
    ("QUERY", "query"),
];

/// A query is whatever an API caller sent: one bounded, printable line per cell keeps a multi-line
/// SQL query, a huge REST query or an escape sequence from breaking the table or the terminal.
fn list_table(res: &Value) -> (Vec<&'static str>, Vec<Vec<String>>) {
    let headers = LIST_COLUMNS.iter().map(|(header, _)| *header).collect();
    let rows = output::items(res)
        .iter()
        .map(|item| {
            LIST_COLUMNS
                .iter()
                .map(|(_, path)| util::one_cell(&output::field(item, path)))
                .collect()
        })
        .collect();
    (headers, rows)
}

/// The cursor of the page after this one, or None on the last page.
fn next_cursor(res: &Value) -> Option<&str> {
    if res.pointer("/pageInfo/hasNextPage") != Some(&Value::Bool(true)) {
        return None;
    }
    res.pointer("/pageInfo/endCursor")
        .and_then(Value::as_str)
        .filter(|cursor| !cursor.is_empty())
}

/// The id goes into the path unencoded, so anything but its own alphabet is refused here
/// rather than letting a `/` or `?` address a different route.
fn check_request_id(request: &str) -> Result<()> {
    if request.is_empty()
        || !request
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
    {
        bail!("`{request}` is not a Query History request id: expected <epochMs>-<spanId>, as `list` prints");
    }
    Ok(())
}

/// One generated query's heading; a value APM has not recorded yet (a query still running) is left
/// out rather than printed empty.
fn sql_heading(sql: &Value) -> String {
    let mut parts = Vec::new();
    let executor = output::field(sql, "executor");
    if !executor.is_empty() {
        parts.push(format!("[{executor}]"));
    }
    for (label, path) in [("", "durationMs"), ("queued ", "queueDurationMs")] {
        let ms = output::field(sql, path);
        if !ms.is_empty() {
            parts.push(format!("{label}{ms} ms"));
        }
    }
    let error = output::field(sql, "error");
    if !error.is_empty() {
        parts.push(format!("error: {}", util::one_cell(&error)));
    }
    parts.join(", ")
}

fn print_request(request: &Value) {
    let line = |label: &str, path: &str| {
        let value = output::field(request, path);
        if !value.is_empty() {
            println!("{label:<10} {}", util::printable(&value));
        }
    };
    line("ID", "id");
    line("Time", "timestamp");
    line("Status", "status");
    line("API", "apiType");
    line("Duration", "durationMs");
    line("Cache", "cacheType");
    line("Error", "error");
    line("Query", "query");
    line("Context", "securityContext");

    let sql_queries = request
        .get("sqlQueries")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    println!("\nSQL queries: {}", sql_queries.len());
    for sql in &sql_queries {
        println!("\n{}", sql_heading(sql));
        println!("{}", util::printable(&output::field(sql, "sql")));
        if let Some(params) = sql.get("params").and_then(Value::as_array) {
            if !params.is_empty() {
                println!(
                    "params: {}",
                    util::printable(&Value::Array(params.clone()).to_string())
                );
            }
        }
    }
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            deployment,
            from,
            to,
            status,
            api_type,
            min_duration_ms,
            search,
            first,
            after,
        } => {
            let query = list_query(
                from,
                to,
                status,
                &api_type,
                min_duration_ms,
                search,
                first,
                after,
            );
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/query-history/requests"),
                    &query,
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                let (headers, rows) = list_table(&res);
                output::table(&headers, rows);
                if let Some(cursor) = next_cursor(&res) {
                    eprintln!("Next page: --after {}", util::shell_quote(cursor));
                }
            }
        }
        Cmd::Get {
            deployment,
            request,
        } => {
            check_request_id(&request)?;
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/query-history/requests/{request}"),
                    &Vec::new(),
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                print_request(&res);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_cells_are_one_bounded_printable_line() {
        let res = serde_json::json!({
            "items": [{
                "id": "1-a",
                "query": format!("SELECT 1\nFROM t\u{1b}[2J{}", "x".repeat(500)),
            }]
        });
        let (headers, rows) = list_table(&res);
        assert_eq!(headers.len(), rows[0].len());
        let query = rows[0].last().unwrap();
        assert!(!query.contains('\n') && !query.contains('\u{1b}'));
        assert!(query.starts_with("SELECT 1 FROM t"));
        assert!(query.chars().count() <= util::CELL_LIMIT + 1);
    }

    #[test]
    fn next_page_hint_only_when_a_next_page_exists() {
        let page = |has_next: bool, cursor: &str| serde_json::json!({ "pageInfo": { "hasNextPage": has_next, "endCursor": cursor } });
        assert_eq!(next_cursor(&page(true, "abc")), Some("abc"));
        assert_eq!(next_cursor(&page(false, "abc")), None);
        assert_eq!(next_cursor(&page(true, "")), None);
    }

    #[test]
    fn after_refuses_other_filters_but_takes_first() {
        use clap::Parser;

        #[derive(Parser)]
        struct Cli {
            #[command(subcommand)]
            cmd: Cmd,
        }

        let parse = |args: &[&str]| Cli::try_parse_from([&["cube", "list", "7"], args].concat());
        assert!(parse(&["--after", "c", "--status", "error"]).is_err());
        assert!(parse(&["--after", "c", "--api-type", "sql"]).is_err());
        assert!(parse(&["--after", "c", "--first", "5"]).is_ok());
    }

    #[test]
    fn sql_heading_leaves_out_what_is_not_recorded_yet() {
        let full = serde_json::json!({
            "executor": "dataSource", "durationMs": 19, "queueDurationMs": 11, "error": null
        });
        assert_eq!(sql_heading(&full), "[dataSource], 19 ms, queued 11 ms");
        let running = serde_json::json!({ "executor": null, "durationMs": null, "error": "boom" });
        assert_eq!(sql_heading(&running), "error: boom");
    }

    #[test]
    fn request_id_keeps_to_its_alphabet() {
        assert!(check_request_id("1787184311517-5c5ca57b-1d2e").is_ok());
        assert!(check_request_id("1/../../tokens").is_err());
        assert!(check_request_id("1?x=1").is_err());
        assert!(check_request_id("").is_err());
    }

    #[test]
    fn list_query_repeats_api_type_and_skips_unset_filters() {
        let query = list_query(
            Some("2026-11-19T00:00:00Z".to_string()),
            None,
            Some(Status::Error),
            &[ApiType::Sql, ApiType::Rest],
            Some(500),
            None,
            Some(20),
            None,
        );
        let pairs: Vec<(&str, &str)> = query
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        assert_eq!(
            pairs,
            vec![
                ("from", "2026-11-19T00:00:00Z"),
                ("status", "error"),
                ("apiType", "sql"),
                ("apiType", "rest"),
                ("minDurationMs", "500"),
                ("first", "20"),
            ]
        );
    }
}
