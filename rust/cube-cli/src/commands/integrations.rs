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
    /// List OAuth integrations
    #[command(alias = "ls")]
    List,
    /// Show an OAuth integration
    Get {
        /// OAuth integration id
        integration: i64,
    },
    /// Create an OAuth integration (CreateOAuthIntegrationInput as JSON)
    Create {
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Update an OAuth integration (UpdateOAuthIntegrationInput as JSON)
    Update {
        /// OAuth integration id
        integration: i64,
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Delete an OAuth integration
    #[command(alias = "rm")]
    Delete {
        /// OAuth integration id
        integration: i64,
    },
    /// Manage the current user's OAuth tokens
    Tokens {
        #[command(subcommand)]
        cmd: TokensCmd,
    },
}

#[derive(Subcommand)]
enum TokensCmd {
    /// List the current user's OAuth tokens
    #[command(alias = "ls")]
    List,
    /// Show the user's OAuth token for an integration
    Get {
        /// OAuth integration id
        integration: i64,
    },
    /// Revoke the user's OAuth token for an integration
    Revoke {
        /// OAuth integration id
        integration: i64,
    },
    /// Initiate the OAuth flow for an integration
    Initiate {
        /// OAuth integration id
        integration: i64,
    },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List => {
            let res = api.get("/api/v1/oauth-integrations/", &Vec::new()).await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("NAME", "name"),
                    ("TYPE", "type"),
                    ("CLIENT ID", "clientId"),
                ],
            );
        }
        Cmd::Get { integration } => {
            let res = api
                .get(
                    &format!("/api/v1/oauth-integrations/{integration}"),
                    &Vec::new(),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Create { data } => {
            let body = util::parse_data(Some(&data))?;
            let res = api
                .post("/api/v1/oauth-integrations/", Some(&util::body(body)))
                .await?;
            output::print_json(&res);
        }
        Cmd::Update { integration, data } => {
            let body = util::parse_data(Some(&data))?;
            let res = api
                .put(
                    &format!("/api/v1/oauth-integrations/{integration}"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Delete { integration } => {
            api.delete(&format!("/api/v1/oauth-integrations/{integration}"), None)
                .await?;
            output::success(&format!("Deleted OAuth integration {integration}"));
        }
        Cmd::Tokens { cmd } => match cmd {
            TokensCmd::List => {
                let res = api.get("/api/v1/user-oauth-tokens/", &Vec::new()).await?;
                output::print_list(
                    ctx.json,
                    &res,
                    &[
                        ("ID", "id"),
                        ("INTEGRATION", "integrationId"),
                        ("STATUS", "status"),
                        ("EXPIRES", "accessTokenExpiresAt"),
                    ],
                );
            }
            TokensCmd::Get { integration } => {
                let res = api
                    .get(
                        &format!("/api/v1/user-oauth-tokens/{integration}"),
                        &Vec::new(),
                    )
                    .await?;
                output::print_json(&res);
            }
            TokensCmd::Revoke { integration } => {
                api.delete(&format!("/api/v1/user-oauth-tokens/{integration}"), None)
                    .await?;
                output::success(&format!(
                    "Revoked OAuth token for integration {integration}"
                ));
            }
            TokensCmd::Initiate { integration } => {
                let res = api
                    .post(
                        &format!("/api/v1/user-oauth-tokens/{integration}/initiate"),
                        None,
                    )
                    .await?;
                output::print_json(&res);
            }
        },
    }
    Ok(())
}
