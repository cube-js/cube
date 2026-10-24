use anyhow::Result;
use serde_json::json;

use crate::{output, Ctx};

/// Print step-by-step instructions for onboarding onto Cube Cloud with this CLI.
///
/// Written for a coding agent as much as for a person: the Cube Cloud "Get
/// Started" page hands agents a prompt that only says "install the Cube CLI and
/// configure the account", so this is where they learn the order of operations
/// without having to reverse-engineer it from `--help`.
#[derive(clap::Args)]
pub struct Args {}

const PLACEHOLDER_URL: &str = "https://TENANT.cubecloud.dev";

/// The guide, with the tenant URL filled in when a context is already saved.
fn guide(url: Option<&str>) -> String {
    let logged_in = url.is_some();
    let url = url.unwrap_or(PLACEHOLDER_URL);
    let login = if logged_in {
        format!(
            "You are already logged in to {url}. Run `cube whoami` to confirm the account,\n\
             or log in again to switch tenants:\n\n    cube login --url {url}"
        )
    } else {
        format!(
            "Log in with the browser device flow (prints a URL and a code to approve):\n\n    \
             cube login --url {url}\n\n\
             Headless (CI, agents without a browser): create an API key in Cube Cloud and run\n\n    \
             cube login --api-key sk-... --url {url}"
        )
    };

    format!(
        "\
Cube Cloud onboarding
=====================

Follow these steps in order. Every command accepts --json for machine-readable
output, and `cube <command> --help` lists all options.

1. Authenticate

{login}

    cube whoami

2. Create a deployment

    cube regions                                   # pick a region name
    cube deployments create --name my-deployment --region REGION
    cube deployments list                          # note the deployment id

3. Connect the data source

Set the database connection as environment variables. Ask the user for the
credentials; never invent them. See https://docs.cube.dev/admin/connect-to-data
for the variables each database type takes.

    cube variables set DEPLOYMENT_ID \\
      CUBEJS_DB_TYPE=postgres \\
      CUBEJS_DB_HOST=db.example.com \\
      CUBEJS_DB_NAME=mydb \\
      CUBEJS_DB_USER=user \\
      CUBEJS_DB_PASS=secret

4. Add a data model (pick one)

  a) Upload a local project (YAML files under model/cubes and model/views):

    cube deployments update DEPLOYMENT_ID -d '{{\"deployMode\":\"cli\"}}'
    cube deploy DEPLOYMENT_ID --directory ./my-cube-project -m \"initial deploy\"

  b) Connect a GitHub repository:

    cube github status
    cube github installations
    cube github connect DEPLOYMENT_ID OWNER/REPO --installation INSTALLATION_ID --branch main

  c) Import a dbt project: see `cube dbt --help`.

  d) Edit files in place through the data model Git workflow:

    cube data-model dev-mode DEPLOYMENT_ID master        # prints the dev-… branch
    cube data-model put DEPLOYMENT_ID model/cubes/orders.yml --file ./orders.yml
    cube validate DEPLOYMENT_ID --dev-mode
    cube data-model commit DEPLOYMENT_ID -m \"Add orders cube\"

5. Build and verify

    cube deployments build-status DEPLOYMENT_ID --wait
    cube validate DEPLOYMENT_ID                     # compile errors, and the cube count

6. Query and connect AI tools

    cube deployments token DEPLOYMENT_ID           # token for the REST, GraphQL and SQL APIs

  To let an AI agent query Cube, connect the Cube MCP server: open {url}/admin/remote-mcp
  for the endpoint and per-client setup.

Learn more
----------

  Docs:          https://docs.cube.dev/reference/cli
  API reference: cube spec [PATTERN]
"
    )
}

pub async fn command(_args: Args, ctx: &Ctx) -> Result<()> {
    let url = ctx
        .api_url
        .clone()
        .or_else(|| {
            ctx.config
                .context(ctx.context.as_deref())
                .map(|(_, c)| c.url.clone())
        })
        .map(|u| crate::util::normalize_url(&u));
    let text = guide(url.as_deref());
    if ctx.json {
        output::print_json(&json!({ "url": url, "guide": text }));
    } else {
        print!("{text}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_a_saved_context_the_guide_asks_to_log_in_to_a_placeholder() {
        let text = guide(None);
        assert!(text.contains(&format!("cube login --url {PLACEHOLDER_URL}")));
        assert!(text.contains("--api-key"));
        assert!(!text.contains("already logged in"));
    }

    #[test]
    fn a_saved_context_fills_in_the_tenant_url() {
        let text = guide(Some("https://acme.cubecloud.dev"));
        assert!(text.contains("already logged in to https://acme.cubecloud.dev"));
        assert!(text.contains("https://acme.cubecloud.dev/admin/remote-mcp"));
        assert!(!text.contains(PLACEHOLDER_URL));
    }

    #[test]
    fn the_json_body_in_the_deploy_mode_example_is_not_doubled_by_format() {
        // `{{` in the format string must come out as a single brace, or the
        // copied command sends invalid JSON.
        assert!(guide(None).contains(r#"-d '{"deployMode":"cli"}'"#));
    }
}
