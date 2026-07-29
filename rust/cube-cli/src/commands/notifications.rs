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
    /// List scheduled notifications (admin only)
    #[command(alias = "ls")]
    List {
        /// Deployment id
        deployment: i64,
        /// Dashboard
        #[arg(long)]
        dashboard: Option<i64>,
        /// Dashboard public id
        #[arg(long)]
        dashboard_public_id: Option<String>,
        /// Recipient user
        #[arg(long)]
        recipient_user: Option<i64>,
        /// Recipient email
        #[arg(long)]
        recipient_email: Option<String>,
        /// Recipient embed tenant
        #[arg(long)]
        recipient_embed_tenant: Option<String>,
        /// Recipient external id
        #[arg(long)]
        recipient_external_id: Option<String>,
        /// Page size (cursor pagination)
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Show a scheduled notification
    Get {
        /// Deployment id
        deployment: i64,
        /// Notification id
        notification: i64,
    },
    /// Create a scheduled notification (CreateNotificationInput as JSON)
    Create {
        /// Deployment id
        deployment: i64,
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Update a scheduled notification (UpdateNotificationInput as JSON)
    Update {
        /// Deployment id
        deployment: i64,
        /// Notification id
        notification: i64,
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Delete a scheduled notification and all its recipients
    #[command(alias = "rm")]
    Delete {
        /// Deployment id
        deployment: i64,
        /// Notification id
        notification: i64,
    },
    /// Manage notification recipients
    Recipients {
        #[command(subcommand)]
        cmd: RecipientsCmd,
    },
}

#[derive(Subcommand)]
enum RecipientsCmd {
    /// List recipients of a notification
    #[command(alias = "ls")]
    List {
        /// Deployment id
        deployment: i64,
        /// Notification id
        notification: i64,
        /// Page size (cursor pagination)
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Subscribe recipients (AddNotificationRecipientsInput as JSON)
    Add {
        /// Deployment id
        deployment: i64,
        /// Notification id
        notification: i64,
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: String,
    },
    /// Unsubscribe recipients (RemoveNotificationRecipientsInput as JSON)
    Remove {
        /// Deployment id
        deployment: i64,
        /// Notification id
        notification: i64,
        /// Request body as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: String,
    },
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List {
            deployment,
            dashboard,
            dashboard_public_id,
            recipient_user,
            recipient_email,
            recipient_embed_tenant,
            recipient_external_id,
            first,
            after,
        } => {
            let mut query = Vec::new();
            util::push(&mut query, "dashboardId", &dashboard);
            util::push(&mut query, "dashboardPublicId", &dashboard_public_id);
            util::push(&mut query, "recipientUserId", &recipient_user);
            util::push(&mut query, "recipientEmail", &recipient_email);
            util::push(
                &mut query,
                "recipientEmbedTenantName",
                &recipient_embed_tenant,
            );
            util::push(&mut query, "recipientExternalId", &recipient_external_id);
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/notifications"),
                    &query,
                )
                .await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("DASHBOARD", "dashboardId"),
                    ("SCHEDULE", "humanReadableSchedule"),
                    ("TZ", "timezone"),
                    ("FORMAT", "notificationFormat"),
                    ("ENABLED", "isEnabled"),
                ],
            );
        }
        Cmd::Get {
            deployment,
            notification,
        } => {
            let res = api
                .get(
                    &format!("/api/v1/deployments/{deployment}/notifications/{notification}"),
                    &Vec::new(),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Create { deployment, data } => {
            let body = util::parse_data(Some(&data))?;
            let res = api
                .post(
                    &format!("/api/v1/deployments/{deployment}/notifications"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Update {
            deployment,
            notification,
            data,
        } => {
            let body = util::parse_data(Some(&data))?;
            let res = api
                .put(
                    &format!("/api/v1/deployments/{deployment}/notifications/{notification}"),
                    Some(&util::body(body)),
                )
                .await?;
            output::print_json(&res);
        }
        Cmd::Delete {
            deployment,
            notification,
        } => {
            api.delete(
                &format!("/api/v1/deployments/{deployment}/notifications/{notification}"),
                None,
            )
            .await?;
            output::success(&format!("Deleted notification {notification}"));
        }
        Cmd::Recipients { cmd } => match cmd {
            RecipientsCmd::List {
                deployment,
                notification,
                first,
                after,
            } => {
                let mut query = Vec::new();
                util::push(&mut query, "first", &first);
                util::push(&mut query, "after", &after);
                let res = api
                    .get(
                        &format!(
                            "/api/v1/deployments/{deployment}/notifications/{notification}/recipients"
                        ),
                        &query,
                    )
                    .await?;
                output::print_list(
                    ctx.json,
                    &res,
                    &[
                        ("TYPE", "type"),
                        ("USER", "userId"),
                        ("EMAIL", "email"),
                        ("EMBED TENANT", "embedTenantName"),
                        ("EXTERNAL ID", "externalId"),
                        ("CHANNEL", "channelName"),
                    ],
                );
            }
            RecipientsCmd::Add {
                deployment,
                notification,
                data,
            } => {
                let body = util::parse_data(Some(&data))?;
                let res = api
                    .post(
                        &format!(
                            "/api/v1/deployments/{deployment}/notifications/{notification}/recipients"
                        ),
                        Some(&util::body(body)),
                    )
                    .await?;
                output::print_json(&res);
            }
            RecipientsCmd::Remove {
                deployment,
                notification,
                data,
            } => {
                let body = util::parse_data(Some(&data))?;
                api.delete(
                    &format!(
                        "/api/v1/deployments/{deployment}/notifications/{notification}/recipients"
                    ),
                    Some(&util::body(body)),
                )
                .await?;
                output::success("Removed recipients");
            }
        },
    }
    Ok(())
}
