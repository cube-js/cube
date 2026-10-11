use anyhow::{bail, Result};
use clap::Subcommand;
use serde_json::{json, Map, Value};

use crate::{output, util, Ctx};

#[derive(clap::Args)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List custom roles
    #[command(alias = "ls")]
    List {
        /// Page size (cursor pagination)
        #[arg(long)]
        first: Option<u64>,
        /// Cursor for the next page (from a previous pageInfo.endCursor)
        #[arg(long)]
        after: Option<String>,
    },
    /// Create a custom role, optionally scoped to deployments
    Create {
        /// Role name (a built-in name such as Admin is rejected)
        #[arg(long)]
        name: String,
        /// Role description
        #[arg(long)]
        description: Option<String>,
        /// Deployment id to scope the role to, or All; repeat for several
        #[arg(long = "deployment")]
        deployments: Vec<String>,
        /// Action to grant on those deployments, e.g. DeploymentRead; repeat for several
        #[arg(long = "action")]
        actions: Vec<String>,
        /// Extra request body fields as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: Option<String>,
    },
    /// Show a single custom role
    Get {
        /// Role id
        role: i64,
    },
    /// Update a custom role
    Update {
        /// Role id
        role: i64,
        /// New role name
        #[arg(long)]
        name: Option<String>,
        /// New description
        #[arg(long)]
        description: Option<String>,
        /// Deployment id to scope the role to, or All; repeat for several
        #[arg(long = "deployment")]
        deployments: Vec<String>,
        /// Action to grant on those deployments, e.g. DeploymentRead; repeat for several
        #[arg(long = "action")]
        actions: Vec<String>,
        /// Extra request body fields as JSON (inline, @file, or - for stdin)
        #[arg(long, short = 'd')]
        data: Option<String>,
    },
    /// Delete a custom role
    #[command(alias = "rm")]
    Delete {
        /// Role id
        role: i64,
    },
    /// Grant a role to a user
    Assign {
        /// Role id
        role: i64,
        /// User id
        user: i64,
    },
    /// Revoke a role from a user
    Unassign {
        /// Role id
        role: i64,
        /// User id
        user: i64,
    },
}

const BASE: &str = "/api/v1/roles";

/// The `--deployment`/`--action` pair as one `Deployment` policy.
///
/// The server rejects a policy that grants no action or names no resource, so
/// the two flags only make sense together and are refused apart rather than
/// guessed at: defaulting either one would invent a grant the caller did not
/// ask for.
fn deployment_policy(deployments: &[String], actions: &[String]) -> Result<Option<Value>> {
    match (deployments.is_empty(), actions.is_empty()) {
        (true, true) => Ok(None),
        (true, false) => bail!("--action needs --deployment to say what it is granted on"),
        (false, true) => {
            bail!("--deployment needs --action to say what the role grants, e.g. --action DeploymentRead")
        }
        (false, false) => Ok(Some(json!({
            "resourceType": "Deployment",
            "actions": actions,
            "resources": deployments,
        }))),
    }
}

/// The policies to send, or `None` to leave the field out.
///
/// A policy built from the flags is appended to whatever `--data` carried, so
/// the two compose instead of overwriting each other. `required_empty` is for
/// create, where the server requires `policies`: with nothing to send it
/// becomes `[]`, a role that grants nothing yet. Update omits it instead,
/// because there a `[]` would strip the role's existing grants.
fn policies_for(
    data: &Map<String, Value>,
    deployments: &[String],
    actions: &[String],
    required_empty: bool,
) -> Result<Option<Value>> {
    let mut policies = match data.get("policies") {
        Some(Value::Array(existing)) => existing.clone(),
        Some(_) => bail!("policies in --data must be an array"),
        None => Vec::new(),
    };
    let from_flags = deployment_policy(deployments, actions)?;
    let had_data = data.contains_key("policies");
    if let Some(policy) = from_flags {
        policies.push(policy);
    } else if !had_data {
        return Ok(required_empty.then(|| json!([])));
    }
    Ok(Some(Value::Array(policies)))
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    match args.cmd {
        Cmd::List { first, after } => {
            let mut query = Vec::new();
            util::push(&mut query, "first", &first);
            util::push(&mut query, "after", &after);
            let res = api.get(&format!("{BASE}/"), &query).await?;
            output::print_list(
                ctx.json,
                &res,
                &[
                    ("ID", "id"),
                    ("NAME", "name"),
                    ("DESCRIPTION", "description"),
                ],
            );
        }
        Cmd::Create {
            name,
            description,
            deployments,
            actions,
            data,
        } => {
            let mut body = util::parse_data(data.as_deref())?;
            let policies = policies_for(&body, &deployments, &actions, true)?;
            body.insert("name".to_string(), json!(name));
            util::set(&mut body, "description", &description);
            if let Some(policies) = policies {
                body.insert("policies".to_string(), policies);
            }
            let res = api
                .post(&format!("{BASE}/"), Some(&util::body(body)))
                .await?;
            output::print_json(&res);
        }
        Cmd::Get { role } => {
            let res = api.get(&format!("{BASE}/{role}"), &Vec::new()).await?;
            output::print_json(&res);
        }
        Cmd::Update {
            role,
            name,
            description,
            deployments,
            actions,
            data,
        } => {
            let mut body = util::parse_data(data.as_deref())?;
            let policies = policies_for(&body, &deployments, &actions, false)?;
            util::set(&mut body, "name", &name);
            util::set(&mut body, "description", &description);
            if let Some(policies) = policies {
                body.insert("policies".to_string(), policies);
            }
            let res = api
                .put(&format!("{BASE}/{role}"), Some(&util::body(body)))
                .await?;
            output::print_json(&res);
        }
        Cmd::Delete { role } => {
            let res = api.delete(&format!("{BASE}/{role}"), None).await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Deleted role {role}"));
            }
        }
        Cmd::Assign { role, user } => {
            let res = api
                .put(&format!("{BASE}/{role}/users/{user}"), None)
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Granted role {role} to user {user}"));
            }
        }
        Cmd::Unassign { role, user } => {
            let res = api
                .delete(&format!("{BASE}/{role}/users/{user}"), None)
                .await?;
            if ctx.json {
                output::print_json(&res);
            } else {
                output::success(&format!("Revoked role {role} from user {user}"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(raw: &str) -> Map<String, Value> {
        util::parse_data(Some(raw)).unwrap()
    }

    #[test]
    fn scoping_a_role_to_deployments_builds_one_deployment_policy() {
        let policies = policies_for(
            &Map::new(),
            &["42".to_string(), "43".to_string()],
            &["DeploymentRead".to_string()],
            true,
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            policies,
            json!([{
                "resourceType": "Deployment",
                "actions": ["DeploymentRead"],
                "resources": ["42", "43"],
            }])
        );
    }

    #[test]
    fn deployment_ids_stay_strings_so_all_is_expressible() {
        // `resources` is a string array server-side, and "All" shares it with the
        // numeric ids, so neither can be parsed as a number on the way in.
        let policies = policies_for(
            &Map::new(),
            &["All".to_string()],
            &["DeploymentRead".to_string()],
            true,
        )
        .unwrap()
        .unwrap();

        assert_eq!(policies[0]["resources"], json!(["All"]));
    }

    #[test]
    fn one_flag_without_the_other_is_refused_rather_than_guessed() {
        let deployments = ["42".to_string()];
        let actions = ["DeploymentRead".to_string()];

        assert!(policies_for(&Map::new(), &deployments, &[], true).is_err());
        assert!(policies_for(&Map::new(), &[], &actions, true).is_err());
    }

    #[test]
    fn create_sends_an_empty_list_where_update_leaves_policies_alone() {
        // The server requires `policies` on create, but on update a `[]` would
        // strip the role's grants — so the same absence has to mean each.
        assert_eq!(
            policies_for(&Map::new(), &[], &[], true).unwrap(),
            Some(json!([]))
        );
        assert_eq!(policies_for(&Map::new(), &[], &[], false).unwrap(), None);
    }

    #[test]
    fn a_flag_policy_is_appended_to_the_ones_data_carried() {
        let body = data(
            r#"{"policies": [{"resourceType": "Global", "actions": ["BillingRead"], "resources": ["All"]}]}"#,
        );
        let policies = policies_for(
            &body,
            &["42".to_string()],
            &["DeploymentRead".to_string()],
            true,
        )
        .unwrap()
        .unwrap();

        let policies = policies.as_array().unwrap();
        assert_eq!(policies.len(), 2);
        assert_eq!(policies[0]["resourceType"], "Global");
        assert_eq!(policies[1]["resourceType"], "Deployment");
    }

    #[test]
    fn data_policies_survive_on_update_without_the_flags() {
        let body = data(r#"{"policies": []}"#);

        // An explicit `[]` in --data is a request to clear them, so it has to reach
        // the server rather than be read as "nothing to send".
        assert_eq!(
            policies_for(&body, &[], &[], false).unwrap(),
            Some(json!([]))
        );
    }

    #[test]
    fn a_non_array_policies_in_data_is_rejected() {
        let body = data(r#"{"policies": "DeploymentRead"}"#);

        assert!(policies_for(&body, &[], &[], true).is_err());
    }
}
