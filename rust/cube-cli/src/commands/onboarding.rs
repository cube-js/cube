use anyhow::Result;
use serde_json::json;

use crate::{output, Ctx};

/// Print step-by-step onboarding instructions — the order of operations a coding
/// agent would otherwise have to reverse-engineer from `--help`.
#[derive(clap::Args)]
pub struct Args {}

const PLACEHOLDER_URL: &str = "https://TENANT.cubecloud.dev";

/// The guide, with the tenant URL filled in when it is known. Only a URL together with
/// a token counts as logged in: a bare `CUBE_API_URL` still needs `cube login`.
fn guide(url: Option<&str>, logged_in: bool) -> String {
    let logged_in = logged_in && url.is_some();
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

Before you start: authenticate

{login}

    cube whoami

1. Create an empty data model deployment

Start from a deployment with an empty starter data model; the rest of the guide
connects data to it and fills the model in.

    cube regions                                   # pick a region name
    cube deployments create --name my-deployment --region REGION
    cube deployments list                          # note the deployment id
    cube deployments build-status DEPLOYMENT_ID --wait

  To import the data model from GitHub (step 3b), create it with
  -d '{{\"creationMethod\":\"github\"}}' instead of the starter project, and skip
  the build-status wait above: nothing builds until the repository is connected.

2. Connect the data source

Set the database connection as environment variables. Ask the user for the
credentials; never invent them. See https://docs.cube.dev/admin/connect-to-data
for the variables each database type takes.

    cube variables set DEPLOYMENT_ID \\
      CUBEJS_DB_TYPE=postgres \\
      CUBEJS_DB_HOST=db.example.com \\
      CUBEJS_DB_NAME=mydb \\
      CUBEJS_DB_USER=user \\
      CUBEJS_DB_PASS=secret

3. Add a data model (pick one)

  a) Upload a local project (YAML files under model/cubes and model/views):

    cube deployments update DEPLOYMENT_ID -d '{{\"deployMode\":\"cli\"}}'
    cube deploy DEPLOYMENT_ID --directory ./my-cube-project -m \"initial deploy\"

  b) Connect a GitHub repository:

    cube github status
    cube github installations
    cube github connect DEPLOYMENT_ID OWNER/REPO --installation INSTALLATION_ID --branch main

  c) Import a dbt project: see `cube dbt --help`.

  d) Edit files through the data model Git workflow (a branch, then merge it):

    cube data-model create-branch DEPLOYMENT_ID my-branch
    cube data-model dev-mode DEPLOYMENT_ID my-branch     # prints the dev-… branch: DEV_BRANCH
    cube data-model put DEPLOYMENT_ID model/cubes/orders.yml --file ./orders.yml --branch DEV_BRANCH
    cube validate DEPLOYMENT_ID --dev-mode
    cube data-model commit DEPLOYMENT_ID --branch DEV_BRANCH -m \"Add orders cube\"
    cube data-model exit-dev-mode DEPLOYMENT_ID
    cube data-model delete-branch DEPLOYMENT_ID DEV_BRANCH
    cube data-model merge-to-default DEPLOYMENT_ID --branch my-branch -m \"Add orders cube\"

4. Build and verify

    cube deployments build-status DEPLOYMENT_ID --wait
    cube validate DEPLOYMENT_ID                     # compile errors, and the cube count

5. Query and connect AI tools

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
    let (url, token) = ctx.credentials()?;
    let text = guide(url.as_deref(), token.is_some());
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
        let text = guide(None, false);
        assert!(text.contains(&format!("cube login --url {PLACEHOLDER_URL}")));
        assert!(text.contains("--api-key"));
        assert!(!text.contains("already logged in"));
    }

    #[test]
    fn a_saved_context_fills_in_the_tenant_url() {
        let text = guide(Some("https://acme.cubecloud.dev"), true);
        assert!(text.contains("already logged in to https://acme.cubecloud.dev"));
        assert!(text.contains("https://acme.cubecloud.dev/admin/remote-mcp"));
        assert!(!text.contains(PLACEHOLDER_URL));
    }

    #[test]
    fn the_json_body_in_the_deploy_mode_example_is_not_doubled_by_format() {
        // `{{` in the format string must come out as a single brace, or the
        // copied command sends invalid JSON.
        let text = guide(None, false);
        assert!(text.contains(r#"-d '{"deployMode":"cli"}'"#));
        assert!(text.contains(r#"-d '{"creationMethod":"github"}'"#));
        // A GitHub-created deployment has nothing to build until the repo is
        // connected, so `build-status --wait` would give up with an error.
        assert!(text.contains("skip\n  the build-status wait above"));
    }

    #[test]
    fn a_url_without_a_token_still_asks_to_log_in_but_names_the_tenant() {
        // `CUBE_API_URL` alone: the next command would fail with "not logged in".
        let text = guide(Some("https://acme.cubecloud.dev"), false);
        assert!(!text.contains("already logged in"));
        assert!(text.contains("cube login --url https://acme.cubecloud.dev"));
    }

    #[test]
    fn the_git_workflow_path_ships_the_change_to_the_deploy_branch() {
        // `commit` only reaches `my-branch`; without the merge "Build and verify"
        // checks a deploy branch that never got the model. Order follows the docs.
        let text = guide(None, false);
        let exit = text.find("cube data-model exit-dev-mode").unwrap();
        let merge = text
            .find("cube data-model merge-to-default DEPLOYMENT_ID --branch my-branch")
            .unwrap();
        assert!(exit < merge);
    }

    #[test]
    fn the_first_step_creates_an_empty_data_model_deployment() {
        let text = guide(None, false);
        let first = text.find("1. ").expect("numbered steps");
        assert!(text[first..].starts_with("1. Create an empty data model deployment"));
        // Logging in is a prerequisite, not a numbered step after it.
        assert!(text.find("Before you start").unwrap() < first);
    }
}
