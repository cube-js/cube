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
    /// SCIM v2 users
    Users {
        #[command(subcommand)]
        cmd: ResourceCmd,
    },
    /// SCIM v2 groups
    Groups {
        #[command(subcommand)]
        cmd: ResourceCmd,
    },
    /// Show SCIM resource types
    ResourceTypes,
    /// Show SCIM schemas
    Schemas,
    /// Show the SCIM service provider config
    ServiceProviderConfig,
}

#[derive(Subcommand)]
enum ResourceCmd {
    /// List resources
    #[command(alias = "ls")]
    List {
        /// SCIM filter expression
        #[arg(long)]
        filter: Option<String>,
        #[arg(long)]
        start_index: Option<u64>,
        #[arg(long)]
        count: Option<u64>,
    },
    /// Show a resource
    Get { id: String },
    /// Create a resource (SCIM JSON body)
    Create {
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Patch a resource (SCIM PatchOp JSON body)
    Patch {
        id: String,
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Replace a resource (SCIM JSON body)
    Replace {
        id: String,
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Delete a resource
    #[command(alias = "rm")]
    Delete { id: String },
}

async fn resource(base: &str, cmd: ResourceCmd, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match cmd {
        ResourceCmd::List {
            filter,
            start_index,
            count,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "filter", &filter);
            util::push(&mut query, "startIndex", &start_index);
            util::push(&mut query, "count", &count);
            let res = api.get(base, &query).await?;
            output::print_json(&res);
        }
        ResourceCmd::Get { id } => {
            let res = api.get(&format!("{base}/{id}"), &Vec::new()).await?;
            output::print_json(&res);
        }
        ResourceCmd::Create { data } => {
            let body = util::parse_data(Some(&data))?;
            let res = api.post(base, Some(&util::body(body))).await?;
            output::print_json(&res);
        }
        ResourceCmd::Patch { id, data } => {
            let body = util::parse_data(Some(&data))?;
            let res = api
                .patch(&format!("{base}/{id}"), Some(&util::body(body)))
                .await?;
            output::print_json(&res);
        }
        ResourceCmd::Replace { id, data } => {
            let body = util::parse_data(Some(&data))?;
            let res = api
                .put(&format!("{base}/{id}"), Some(&util::body(body)))
                .await?;
            output::print_json(&res);
        }
        ResourceCmd::Delete { id } => {
            api.delete(&format!("{base}/{id}"), None).await?;
            output::success(&format!("Deleted {id}"));
        }
    }
    Ok(())
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    match args.cmd {
        Cmd::Users { cmd } => resource("/api/scim/v2/Users", cmd, ctx).await,
        Cmd::Groups { cmd } => resource("/api/scim/v2/Groups", cmd, ctx).await,
        Cmd::ResourceTypes => {
            let res = ctx
                .api()?
                .get("/api/scim/v2/ResourceTypes", &Vec::new())
                .await?;
            output::print_json(&res);
            Ok(())
        }
        Cmd::Schemas => {
            let res = ctx.api()?.get("/api/scim/v2/Schemas", &Vec::new()).await?;
            output::print_json(&res);
            Ok(())
        }
        Cmd::ServiceProviderConfig => {
            let res = ctx
                .api()?
                .get("/api/scim/v2/ServiceProviderConfig", &Vec::new())
                .await?;
            output::print_json(&res);
            Ok(())
        }
    }
}
