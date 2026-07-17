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
    /// List deployments
    #[command(alias = "ls")]
    List {
        /// Filter by creation step (repeatable): project, upload, schema, github, ssh, databases, ready, demo
        #[arg(long = "creation-step")]
        creation_step: Vec<String>,
        #[arg(long)]
        offset: Option<u64>,
        #[arg(long)]
        limit: Option<u64>,
        /// Page size for cursor-based pagination
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for fetching the next page
        #[arg(long)]
        after: Option<String>,
    },
    /// Show a single deployment
    Get { deployment: i64 },
    /// Create a deployment
    Create {
        /// Deployment name
        #[arg(long)]
        name: Option<String>,
        /// Region name (see `cube regions`), e.g. aws-us-east-1-2
        #[arg(long)]
        region: Option<String>,
        /// Cloud provider: cubecloud, aws, gcp
        #[arg(long, default_value = "cubecloud")]
        cloud_provider: String,
        /// Target platform, e.g. aws, gcp
        #[arg(long, default_value = "aws")]
        target_platform: String,
        /// Provision a self-managed (BYOC/k8s-hybrid) deployment instead of managed
        #[arg(long)]
        unmanaged: bool,
        /// Creation step: project, upload, schema, github, ssh, databases, ready, demo
        #[arg(long, default_value = "project")]
        creation_step: String,
        /// Scaffold a project and run the first build so the deployment serves
        /// immediately (POST /build/api/v1/deployments). Without this, the
        /// deployment row is created but has no build until code is deployed.
        #[arg(long, short = 'b')]
        bootstrap: bool,
        /// Full CreateDeploymentInput as JSON (overrides the flags above)
        #[arg(long, short = 'd')]
        data: Option<String>,
    },
    /// Update a deployment (rename, or full UpdateDeploymentInput via --data)
    Update {
        deployment: i64,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, short = 'd')]
        data: Option<String>,
    },
    /// Delete a deployment
    #[command(alias = "rm")]
    Delete { deployment: i64 },
    /// Generate a Cube API token for a deployment
    Token { deployment: i64 },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            creation_step,
            offset,
            limit,
            first,
            after,
        } => {
            let mut query = Vec::new();
            for step in creation_step {
                query.push(("creationStep".to_string(), step));
            }
            util::push(&mut query, "offset", &offset);
            util::push(&mut query, "limit", &limit);
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api.get("/api/v1/deployments/", &query).await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("NAME", "name"),
                    ("URL", "deploymentUrl"),
                    ("STEP", "creationStep"),
                ],
            );
        }
        Cmd::Get { deployment } => {
            let res = api
                .get(&format!("/api/v1/deployments/{deployment}"), &Vec::new())
                .await?;
            output::print_json(&res);
        }
        Cmd::Create {
            name,
            region,
            cloud_provider,
            target_platform,
            unmanaged,
            creation_step,
            bootstrap,
            data,
        } => {
            // Flags populate the body; --data (if given) overrides them.
            let mut body = serde_json::Map::new();
            util::set(&mut body, "name", &name);
            util::set(&mut body, "region", &region);
            body.insert("cloudProvider".into(), serde_json::json!(cloud_provider));
            body.insert("targetPlatform".into(), serde_json::json!(target_platform));
            body.insert("isManaged".into(), serde_json::json!(!unmanaged));
            body.insert("creationStep".into(), serde_json::json!(creation_step));
            for (k, v) in util::parse_data(data.as_deref())? {
                body.insert(k, v);
            }
            for required in ["name", "region"] {
                if !body.contains_key(required) {
                    anyhow::bail!("--{required} is required (or provide it via --data)");
                }
            }
            // The bootstrap endpoint lives on the build pod and scaffolds +
            // builds the project; the base endpoint only creates the row.
            let path = if bootstrap {
                "/build/api/v1/deployments"
            } else {
                "/api/v1/deployments"
            };
            let res = api.post(path, Some(&util::body(body))).await?;
            output::print_json(&res);
        }
        Cmd::Update {
            deployment,
            name,
            data,
        } => {
            let mut body = util::parse_data(data.as_deref())?;
            util::set(&mut body, "name", &name);
            let res = api
                .put(
                    &format!("/api/v1/deployments/{deployment}"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Delete { deployment } => {
            let res = api
                .delete(&format!("/api/v1/deployments/{deployment}"), None)
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Deleted deployment {deployment}"));
            }
        }
        Cmd::Token { deployment } => {
            let res = api
                .post(&format!("/api/v1/deployments/{deployment}/token"), None)
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                println!("{}", output::field(&res, "cubeApiToken"));
            }
        }
    }
    Ok(())
}
