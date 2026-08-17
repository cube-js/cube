use anyhow::Result;
use clap::Subcommand;

use crate::{output, util, Ctx};

#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List environments of a deployment
    #[command(alias = "ls")]
    List {
        /// Deployment id
        deployment: i64,
        /// Filter by type: production, staging, development
        #[arg(long = "type")]
        env_type: Option<String>,
        /// Deprecated: pagination offset (use --after)
        #[arg(long)]
        offset: Option<u64>,
        /// Deprecated: maximum number of items to return (use --first)
        #[arg(long)]
        limit: Option<u64>,
        /// Page size for cursor-based pagination
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// List tokens issued for an environment
    Tokens {
        /// Deployment id
        deployment: i64,
        /// Environment id
        environment: i64,
        /// Deprecated: pagination offset (use --after)
        #[arg(long)]
        offset: Option<u64>,
        /// Deprecated: maximum number of items to return (use --first)
        #[arg(long)]
        limit: Option<u64>,
        /// Page size for cursor-based pagination
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Create an environment token
    CreateToken {
        /// Deployment id
        deployment: i64,
        /// Environment id
        environment: i64,
        /// Security context as JSON (inline, @file, or - for stdin)
        #[arg(long)]
        security_context: String,
        /// Token TTL in seconds (1-3600)
        #[arg(long)]
        expires_in: Option<u64>,
        /// Token scopes (repeatable)
        #[arg(long)]
        scope: Vec<String>,
        /// Issue a token for metadata sync instead of a regular token
        #[arg(long)]
        meta_sync: bool,
    },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            deployment,
            env_type,
            offset,
            limit,
            first,
            after,
        } => {
            let page_field = util::paging_field(offset, limit, first, after.as_deref())?;
            let mut query = Vec::new();
            util::push(&mut query, "type", &env_type);
            util::push(&mut query, "offset", &offset);
            util::push(&mut query, "limit", &limit);
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/environments"),
                    &query,
                )
                .await?;
            // The environment list is assembled in memory, so `items` is the
            // cursor page (the whole list unless --first is given) while only
            // the deprecated `data` is sliced by offset/limit.
            output::print_list_from(
                ctx.json,
                &res,
                page_field,
                &[
                    ("ID", "id"),
                    ("TYPE", "type"),
                    ("BRANCH", "branch"),
                    ("USER", "user"),
                ],
            )?;
        }
        Cmd::Tokens {
            deployment,
            environment,
            offset,
            limit,
            first,
            after,
        } => {
            let page_field = util::paging_field(offset, limit, first, after.as_deref())?;
            let mut query = Vec::new();
            util::push(&mut query, "offset", &offset);
            util::push(&mut query, "limit", &limit);
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/environments/{environment}/tokens"),
                    &query,
                )
                .await?;
            // Tokens page in the database, so `items` and `data` hold the same
            // rows either way — read the same field as every other list so the
            // deprecated one can't quietly stop being honored.
            output::print_list_from(
                ctx.json,
                &res,
                page_field,
                &[
                    ("TOKEN", "token"),
                    ("CREATED", "created_at"),
                    ("EXPIRES", "expires_at"),
                ],
            )?;
        }
        Cmd::CreateToken {
            deployment,
            environment,
            security_context,
            expires_in,
            scope,
            meta_sync,
        } => {
            let mut body = serde_json::Map::new();
            body.insert(
                "security_context".to_string(),
                serde_json::Value::Object(util::parse_data(Some(&security_context))?),
            );
            util::set(&mut body, "expires_in", &expires_in);
            if !scope.is_empty() {
                util::set(&mut body, "scopes", &Some(scope));
            }
            let suffix = if meta_sync {
                "tokens-for-meta-sync"
            } else {
                "tokens"
            };
            let res = api
                .post(
                    &format!(
                        "/api/v1/deployments/{deployment}/environments/{environment}/{suffix}"
                    ),
                    Some(&util::body(body)),
                )
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                println!("{}", output::field(&res, "data.token"));
            }
        }
    }
    Ok(())
}
