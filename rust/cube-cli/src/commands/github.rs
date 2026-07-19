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
    /// Show GitHub link/install state and browser URLs to complete setup
    Status,
    /// List the user's GitHub App installations
    #[command(alias = "ls")]
    Installations,
    /// List repositories available to an installation
    #[command(alias = "repositories")]
    Repos {
        /// GitHub App installation id (see `cube github installations`)
        installation: i64,
    },
    /// List branches of a repository
    Branches {
        /// Repository as owner/repo, e.g. cube-js/cube
        repo: String,
        /// GitHub App installation id the repository belongs to
        #[arg(long)]
        installation: i64,
    },
    /// Connect a deployment to a GitHub repo: clones it into the
    /// deployment's git storage and triggers the first build
    Connect {
        /// Deployment id
        deployment: i64,
        /// Repository as owner/repo, e.g. cube-js/cube
        repo: String,
        /// GitHub App installation id the repository belongs to
        #[arg(long)]
        installation: i64,
        /// Branch to import (defaults to the repository's default branch)
        #[arg(long)]
        branch: Option<String>,
        /// Extra request body fields as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: Option<String>,
    },
}

/// Split an `owner/repo` argument into its two parts.
fn split_repo(repo: &str) -> Result<(&str, &str)> {
    repo.split_once('/')
        .filter(|(owner, name)| !owner.is_empty() && !name.is_empty() && !name.contains('/'))
        .ok_or_else(|| anyhow::anyhow!("repository must be in owner/repo form, got `{repo}`"))
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::Status => {
            let res = api.get("/api/v1/github/status", &Vec::new()).await?;
            output::print_json(&res);
        }
        Cmd::Installations => {
            let res = api.get("/api/v1/github/installations", &Vec::new()).await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("ACCOUNT", "accountLogin"),
                    ("TYPE", "accountType"),
                ],
            );
        }
        Cmd::Repos { installation } => {
            let res = api
                .get(
                    &format!("/api/v1/github/installations/{installation}/repositories"),
                    &Vec::new(),
                )
                .await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("NAME", "fullName"),
                    ("PRIVATE", "private"),
                    ("DEFAULT BRANCH", "defaultBranch"),
                ],
            );
        }
        Cmd::Branches { repo, installation } => {
            let (owner, name) = split_repo(&repo)?;
            let query = vec![("installationId".to_string(), installation.to_string())];
            let res = api
                .get(
                    &format!("/api/v1/github/repositories/{owner}/{name}/branches"),
                    &query,
                )
                .await?;
            output::print_list(ctx.json, &res, &[("BRANCH", "name")]);
        }
        Cmd::Connect {
            deployment,
            repo,
            installation,
            branch,
            data,
        } => {
            let (owner, name) = split_repo(&repo)?;
            let mut body = serde_json::Map::new();
            body.insert("installationId".into(), serde_json::json!(installation));
            body.insert("owner".into(), serde_json::json!(owner));
            body.insert("repo".into(), serde_json::json!(name));
            util::set(&mut body, "branch", &branch);
            for (k, v) in util::parse_data(data.as_deref())? {
                body.insert(k, v);
            }
            let res = api
                .post(
                    &format!("/build/api/v1/deployments/{deployment}/github/connect"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
    }
    Ok(())
}
