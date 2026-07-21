mod client;
mod commands;
mod config;
mod oauth;
mod output;
mod telemetry;
mod update;
mod util;

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "cube",
    display_name = "Cube CLI",
    version = env!("CUBE_CLI_VERSION"),
    about = "Cube Cloud command line interface"
)]
struct Cli {
    #[command(flatten)]
    global: GlobalArgs,
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Args, Clone)]
pub struct GlobalArgs {
    /// Output raw JSON instead of tables
    #[arg(long, global = true)]
    json: bool,
    /// Cube Cloud API base URL, e.g. https://<tenant>.cubecloud.dev
    #[arg(long, global = true, env = "CUBE_API_URL")]
    api_url: Option<String>,
    /// API token (API key, JWT, or OAuth access token)
    #[arg(long, global = true, env = "CUBE_API_KEY", hide_env_values = true)]
    token: Option<String>,
    /// Named context from the config file to use
    #[arg(long, global = true)]
    context: Option<String>,
}

/// Shared state passed to every command.
pub struct Ctx {
    pub json: bool,
    pub config: config::Config,
    api_url: Option<String>,
    token: Option<String>,
    context: Option<String>,
}

impl Ctx {
    fn new(global: &GlobalArgs) -> Result<Self> {
        Ok(Self {
            json: global.json,
            config: config::Config::load()?,
            api_url: global.api_url.clone(),
            token: global.token.clone(),
            context: global.context.clone(),
        })
    }

    /// Build an authenticated API client from flags, env, or the config file.
    pub fn api(&self) -> Result<client::Client> {
        let ctx = self.config.context(self.context.as_deref());
        if let Some(name) = &self.context {
            if ctx.is_none() {
                bail!("context `{name}` not found in config (run `cube login --context {name}`)");
            }
        }
        let url = self
            .api_url
            .clone()
            .or_else(|| ctx.map(|(_, c)| c.url.clone()));
        // An explicit --token / CUBE_API_KEY wins and disables auto-refresh
        // (it isn't tied to a stored refresh token).
        let token = self
            .token
            .clone()
            .or_else(|| ctx.map(|(_, c)| c.api_key.clone()));
        match (url, token) {
            (Some(url), Some(token)) => {
                // Enable auto-refresh only when using the context's own access
                // token and it has a refresh token saved alongside it.
                let refresh = if self.token.is_none() {
                    ctx.and_then(|(name, c)| {
                        c.refresh_token
                            .as_ref()
                            .map(|rt| (rt.clone(), name.to_string()))
                    })
                } else {
                    None
                };
                match refresh {
                    Some((rt, name)) => client::Client::with_refresh(&url, &token, &rt, Some(name)),
                    None => client::Client::new(&url, &token),
                }
            }
            _ => bail!(
                "not logged in: run `cube login`, or set CUBE_API_URL and CUBE_API_KEY \
                 (or pass --api-url/--token)"
            ),
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Log in to Cube Cloud and save credentials
    Login(commands::login::Args),
    /// Remove saved credentials
    Logout(commands::logout::Args),
    /// Show the currently authenticated user
    Whoami(commands::whoami::Args),
    /// Manage saved contexts (tenants)
    Context(commands::context::Args),

    /// Manage deployments
    #[command(alias = "deployment")]
    Deployments(commands::deployments::Args),
    /// List available deployment regions
    #[command(alias = "region")]
    Regions(commands::regions::Args),
    /// Upload a local project directory to a deployment and build it
    Deploy(commands::deploy::Args),
    /// Tail a deployment's pod logs
    Logs(commands::logs::Args),
    /// GitHub integration: link status, installations, repos, and connect
    #[command(alias = "gh")]
    Github(commands::github::Args),
    /// Manage a deployment's data model files
    #[command(name = "data-model", alias = "dm")]
    DataModel(commands::data_model::Args),
    /// Manage deployment environments and environment tokens
    #[command(alias = "environment", alias = "envs")]
    Environments(commands::environments::Args),
    /// Manage deployment environment variables
    #[command(alias = "vars", alias = "variable")]
    Variables(commands::variables::Args),

    /// Manage workspace folders
    #[command(alias = "folder")]
    Folders(commands::folders::Args),
    /// Manage workbooks and dashboards
    #[command(alias = "workbook")]
    Workbooks(commands::workbooks::Args),
    /// Manage reports
    #[command(alias = "report")]
    Reports(commands::reports::Args),
    /// Browse and organize the deployment workspace
    Workspace(commands::workspace::Args),
    /// Manage scheduled notifications and their recipients
    #[command(alias = "notification")]
    Notifications(commands::notifications::Args),

    /// Manage users
    #[command(alias = "user")]
    Users(commands::users::Args),
    /// Manage user groups
    #[command(alias = "group")]
    Groups(commands::groups::Args),
    /// Manage user attributes and their values
    #[command(alias = "attribute")]
    Attributes(commands::attributes::Args),
    /// Manage resource access policies
    #[command(alias = "policy")]
    Policies(commands::policies::Args),
    /// View and update tenant settings
    Tenant(commands::tenant::Args),

    /// Embed sessions, tokens, dashboards, and embed tenants
    Embed(commands::embed::Args),
    /// Manage OAuth integrations and user OAuth tokens
    #[command(alias = "integration", alias = "oauth")]
    Integrations(commands::integrations::Args),
    /// Manage OIDC token configs
    Oidc(commands::oidc::Args),
    /// Manage API keys for programmatic access
    #[command(name = "api-keys", alias = "api-key")]
    ApiKeys(commands::api_keys::Args),

    /// List agents and agent skills
    #[command(alias = "agent")]
    Agents(commands::agents::Args),
    /// App-level config and theme
    App(commands::app::Args),
    /// Fetch data-model metadata
    Meta(commands::meta::Args),
    /// SCIM v2 user and group provisioning
    Scim(commands::scim::Args),

    /// Make an authenticated raw API request (escape hatch)
    Api(commands::api::Args),
    /// Update the CLI to the latest release
    Update(commands::update::Args),
    /// Generate shell completions
    Completion(commands::completion::Args),
}

/// Expose the clap command tree for `cube completion`.
pub fn cli_command() -> clap::Command {
    use clap::CommandFactory;
    Cli::command()
}

impl Command {
    /// Canonical command-group name, used for telemetry.
    fn name(&self) -> &'static str {
        use Command::*;
        match self {
            Login(_) => "login",
            Logout(_) => "logout",
            Whoami(_) => "whoami",
            Context(_) => "context",
            Deployments(_) => "deployments",
            Regions(_) => "regions",
            Deploy(_) => "deploy",
            Logs(_) => "logs",
            Github(_) => "github",
            DataModel(_) => "data-model",
            Environments(_) => "environments",
            Variables(_) => "variables",
            Folders(_) => "folders",
            Workbooks(_) => "workbooks",
            Reports(_) => "reports",
            Workspace(_) => "workspace",
            Notifications(_) => "notifications",
            Users(_) => "users",
            Groups(_) => "groups",
            Attributes(_) => "attributes",
            Policies(_) => "policies",
            Tenant(_) => "tenant",
            Embed(_) => "embed",
            Integrations(_) => "integrations",
            Oidc(_) => "oidc",
            ApiKeys(_) => "api-keys",
            Agents(_) => "agents",
            App(_) => "app",
            Meta(_) => "meta",
            Scim(_) => "scim",
            Api(_) => "api",
            Update(_) => "update",
            Completion(_) => "completion",
        }
    }
}

#[tokio::main]
async fn main() {
    // Rust ignores SIGPIPE by default, turning `cube ... | head` into a
    // broken-pipe panic. Restore the default disposition so the process
    // exits quietly when the reader goes away, like other CLI tools.
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
    let cli = Cli::parse();
    commands::update::cleanup_stale_binary();

    // Check for a newer release concurrently with the command; the notice
    // (if any) prints after the command output. Skipped for `update` itself
    // and `completion` (whose output is eval'd by shells). Completion also
    // skips telemetry — it runs on shell startup.
    let is_completion = matches!(cli.command, Command::Completion(_));
    let check = match &cli.command {
        Command::Update(_) | Command::Completion(_) => None,
        _ => Some(update::spawn_check()),
    };
    let command_name = cli.command.name();

    let result = run(cli).await;

    if !is_completion {
        let mut props = serde_json::Map::new();
        props.insert("command".into(), serde_json::json!(command_name));
        props.insert("success".into(), serde_json::json!(result.is_ok()));
        telemetry::event("Cube CLI Command", props);
        if let Err(err) = &result {
            let mut props = serde_json::Map::new();
            props.insert("command".into(), serde_json::json!(command_name));
            props.insert("error".into(), serde_json::json!(format!("{err:#}")));
            telemetry::event("Error", props);
        }
        telemetry::flush().await;
    }
    if let Some(check) = check {
        update::print_notice(check).await;
    }
    if let Err(err) = result {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let mut ctx = Ctx::new(&cli.global)?;
    use Command::*;
    match cli.command {
        Login(args) => commands::login::command(args, &mut ctx).await,
        Logout(args) => commands::logout::command(args, &mut ctx).await,
        Whoami(args) => commands::whoami::command(args, &ctx).await,
        Context(args) => commands::context::command(args, &mut ctx).await,
        Deployments(args) => commands::deployments::command(args, &ctx).await,
        Regions(args) => commands::regions::command(args, &ctx).await,
        Deploy(args) => commands::deploy::command(args, &ctx).await,
        Logs(args) => commands::logs::command(args, &ctx).await,
        Github(args) => commands::github::command(args, &ctx).await,
        DataModel(args) => commands::data_model::command(args, &ctx).await,
        Environments(args) => commands::environments::command(args, &ctx).await,
        Variables(args) => commands::variables::command(args, &ctx).await,
        Folders(args) => commands::folders::command(args, &ctx).await,
        Workbooks(args) => commands::workbooks::command(args, &ctx).await,
        Reports(args) => commands::reports::command(args, &ctx).await,
        Workspace(args) => commands::workspace::command(args, &ctx).await,
        Notifications(args) => commands::notifications::command(args, &ctx).await,
        Users(args) => commands::users::command(args, &ctx).await,
        Groups(args) => commands::groups::command(args, &ctx).await,
        Attributes(args) => commands::attributes::command(args, &ctx).await,
        Policies(args) => commands::policies::command(args, &ctx).await,
        Tenant(args) => commands::tenant::command(args, &ctx).await,
        Embed(args) => commands::embed::command(args, &ctx).await,
        Integrations(args) => commands::integrations::command(args, &ctx).await,
        Oidc(args) => commands::oidc::command(args, &ctx).await,
        ApiKeys(args) => commands::api_keys::command(args, &ctx).await,
        Agents(args) => commands::agents::command(args, &ctx).await,
        App(args) => commands::app::command(args, &ctx).await,
        Meta(args) => commands::meta::command(args, &ctx).await,
        Scim(args) => commands::scim::command(args, &ctx).await,
        Api(args) => commands::api::command(args, &ctx).await,
        Update(args) => commands::update::command(args, &ctx).await,
        Completion(args) => commands::completion::command(args),
    }
}
