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
        /// Pagination offset
        #[arg(long)]
        offset: Option<u64>,
        /// Maximum number of items to return
        #[arg(long)]
        limit: Option<u64>,
    },
    /// List tokens issued for an environment
    Tokens {
        /// Deployment id
        deployment: i64,
        /// Environment id
        environment: i64,
        /// Pagination offset
        #[arg(long)]
        offset: Option<u64>,
        /// Maximum number of items to return
        #[arg(long)]
        limit: Option<u64>,
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
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "type", &env_type);
            util::push(&mut query, "offset", &offset);
            util::push(&mut query, "limit", &limit);
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/environments"),
                    &query,
                )
                .await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("TYPE", "type"),
                    ("BRANCH", "branch"),
                    ("USER", "user"),
                ],
            );
        }
        Cmd::Tokens {
            deployment,
            environment,
            offset,
            limit,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "offset", &offset);
            util::push(&mut query, "limit", &limit);
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/environments/{environment}/tokens"),
                    &query,
                )
                .await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("TOKEN", "token"),
                    ("CREATED", "created_at"),
                    ("EXPIRES", "expires_at"),
                ],
            );
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
