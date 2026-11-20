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
        /// first page's filters, so the other filter flags are ignored with it
        #[arg(long)]
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
        println!(
            "\n[{}] {} ms, queued {} ms{}",
            output::field(sql, "executor"),
            output::field(sql, "durationMs"),
            output::field(sql, "queueDurationMs"),
            match output::field(sql, "error") {
                error if error.is_empty() => String::new(),
                error => format!(", error: {}", util::one_cell(&error)),
            }
        );
        println!("{}", util::printable(&output::field(sql, "sql")));
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
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("TIME", "timestamp"),
                    ("STATUS", "status"),
                    ("API", "apiType"),
                    ("MS", "durationMs"),
                    ("CACHE", "cacheType"),
                    ("QUERY", "query"),
                ],
            );
            if !ctx.json {
                if let Some(cursor) = res.pointer("/pageInfo/endCursor").and_then(Value::as_str) {
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
