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
    /// List OIDC token configs
    #[command(alias = "ls")]
    List,
    /// Show an OIDC token config
    Get {
        /// OIDC token config id
        config: i64,
    },
    /// Create an OIDC token config (CreateOidcTokenConfigInput as JSON)
    Create {
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Update an OIDC token config (UpdateOidcTokenConfigInput as JSON)
    Update {
        /// OIDC token config id
        config: i64,
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Delete an OIDC token config
    #[command(alias = "rm")]
    Delete {
        /// OIDC token config id
        config: i64,
    },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List => {
            let res = api.get("/api/v1/oidc-token-configs/", &Vec::new()).await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("NAME", "name"),
                    ("AUDIENCE", "audience"),
                    ("ENABLED", "isEnabled"),
                    ("TARGET ENV", "targetEnvVar"),
                ],
            );
        }
        Cmd::Get { config } => {
            let res = api
                .get(&format!("/api/v1/oidc-token-configs/{config}"), &Vec::new())
                .await?;
            output::print_json(&res);
        }
        Cmd::Create { data } => {
            let body = util::parse_data(Some(&data))?;
            let res = api
                .post("/api/v1/oidc-token-configs/", Some(&util::body(body)))
                .await?;
            output::print_json(&res);
        }
        Cmd::Update { config, data } => {
            let body = util::parse_data(Some(&data))?;
            let res = api
                .put(
                    &format!("/api/v1/oidc-token-configs/{config}"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Delete { config } => {
            api.delete(&format!("/api/v1/oidc-token-configs/{config}"), None)
                .await?;
            output::success(&format!("Deleted OIDC token config {config}"));
        }
    }
    Ok(())
}
