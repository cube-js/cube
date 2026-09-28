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
    /// List users
    #[command(alias = "ls")]
    List {
        /// Page size (cursor pagination)
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Show the current user
    Me,
    /// Create a user (UserCreateInput as JSON, admin only)
    Create {
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Update a user (UserUpdateInput as JSON, admin only)
    Update {
        /// User id
        user: i64,
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Delete a user (admin only)
    #[command(alias = "rm")]
    Delete {
        /// User id
        user: i64,
    },
    /// Show the embed theme for the current user
    EmbedTheme,
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List { first, after } => {
            let mut query = Vec::new();
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api.get("/api/v1/users/", &query).await?;
            output::print_list(
                ctx.json,
                &res,
                &[("ID", "id"), ("EMAIL", "email"), ("NAME", "firstName")],
            );
        }
        Cmd::Me => {
            let res = api.get("/api/v1/users/me", &Vec::new()).await?;
            output::print_json(&res);
        }
        Cmd::Create { data } => {
            let body = util::parse_data(Some(&data))?;
            let res = api.post("/api/v1/users/", Some(&util::body(body))).await?;
            output::print_json(&res);
        }
        Cmd::Update { user, data } => {
            let body = util::parse_data(Some(&data))?;
            let res = api
                .put(&format!("/api/v1/users/{user}"), Some(&util::body(body)))
                .await?;
            output::print_json(&res);
        }
        Cmd::Delete { user } => {
            api.delete(&format!("/api/v1/users/{user}"), None).await?;
            output::success(&format!("Deleted user {user}"));
        }
        Cmd::EmbedTheme => {
            let res = api.get("/api/v1/users/embed-theme", &Vec::new()).await?;
            output::print_json(&res);
        }
    }
    Ok(())
}
