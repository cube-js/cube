use anyhow::Result;
use clap::Subcommand;
use serde_json::json;

use crate::{output, util, Ctx};

#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List user attribute definitions
    #[command(alias = "ls")]
    List {
        #[arg(long)]
        offset: Option<u64>,
        #[arg(long)]
        limit: Option<u64>,
        #[arg(long)]
        name: Option<String>,
        /// string, number, boolean, string_array, number_array
        #[arg(long = "type")]
        attr_type: Option<String>,
    },
    /// Create a user attribute definition
    Create {
        #[arg(long)]
        name: String,
        /// string, number, boolean, string_array, number_array
        #[arg(long = "type")]
        attr_type: String,
        #[arg(long)]
        display_name: Option<String>,
        #[arg(long)]
        description: Option<String>,
        #[arg(long)]
        default_value: Option<String>,
    },
    /// Update a user attribute definition
    Update {
        attribute: i64,
        #[arg(long)]
        display_name: Option<String>,
        #[arg(long)]
        description: Option<String>,
        #[arg(long)]
        default_value: Option<String>,
    },
    /// Delete a user attribute definition
    #[command(alias = "rm")]
    Delete { attribute: i64 },
    /// Get or set attribute values for users
    Values {
        #[command(subcommand)]
        cmd: ValuesCmd,
    },
}

#[derive(Subcommand)]
enum ValuesCmd {
    /// List attribute values for a user
    Get { user: i64 },
    /// Upsert an attribute value binding for a user
    Set {
        #[arg(long)]
        user: String,
        #[arg(long)]
        attribute: String,
    },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            offset,
            limit,
            name,
            attr_type,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "offset", &offset);
            util::push(&mut query, "limit", &limit);
            util::push(&mut query, "name", &name);
            util::push(&mut query, "type", &attr_type);
            let res = api.get("/api/v1/user-attributes/", &query).await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("NAME", "name"),
                    ("TYPE", "type"),
                    ("DISPLAY NAME", "displayName"),
                    ("DEFAULT", "defaultValue"),
                ],
            );
        }
        Cmd::Create {
            name,
            attr_type,
            display_name,
            description,
            default_value,
        } => {
            let mut body = serde_json::Map::new();
            body.insert("name".to_string(), json!(name));
            body.insert("type".to_string(), json!(attr_type));
            util::set(&mut body, "displayName", &display_name);
            util::set(&mut body, "description", &description);
            util::set(&mut body, "defaultValue", &default_value);
            let res = api
                .post("/api/v1/user-attributes/", Some(&util::body(body)))
                .await?;
            output::print_json(&res);
        }
        Cmd::Update {
            attribute,
            display_name,
            description,
            default_value,
        } => {
            let mut body = serde_json::Map::new();
            util::set(&mut body, "displayName", &display_name);
            util::set(&mut body, "description", &description);
            util::set(&mut body, "defaultValue", &default_value);
            let res = api
                .put(
                    &format!("/api/v1/user-attributes/{attribute}"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Delete { attribute } => {
            api.delete(&format!("/api/v1/user-attributes/{attribute}"), None)
                .await?;
            output::success(&format!("Deleted user attribute {attribute}"));
        }
        Cmd::Values { cmd } => match cmd {
            ValuesCmd::Get { user } => {
                let res = api
                    .get(&format!("/api/v1/user-attribute-values/{user}"), &Vec::new())
                    .await?;
                output::print_json(&res);
            }
            ValuesCmd::Set { user, attribute } => {
                let res = api
                    .post(
                        "/api/v1/user-attribute-values/",
                        Some(&json!({ "userId": user, "userAttributeId": attribute })),
                    )
                    .await?;
                output::print_json(&res);
            }
        },
    }
    Ok(())
}
