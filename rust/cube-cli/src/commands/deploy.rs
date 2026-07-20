use std::path::{Path, PathBuf};

use anyhow::{bail, Context as _, Result};
use serde_json::Value;
use sha1::{Digest, Sha1};

use crate::{output, util, Ctx};

/// Upload a local project directory to a deployment.
///
/// Mirrors the legacy `cubejs deploy` flow on the public API: diff local
/// content hashes against the server, upload only changed files inside a
/// transaction, then finish with a manifest that prunes deleted files and
/// triggers a single build.
#[derive(clap::Args)]
pub struct Args {
    /// Deployment id
    deployment: i64,
    /// Project directory to upload
    #[arg(long, default_value = ".")]
    directory: PathBuf,
    /// Branch to deploy to (defaults to the active dev-mode branch, else the
    /// deploy branch)
    #[arg(long)]
    branch: Option<String>,
    /// Keep remote files that don't exist locally (default prunes them)
    #[arg(long)]
    keep_missing: bool,
}

/// Collect deployable files as (posix relative path, absolute path).
/// Same filter as the legacy CLI: dotfiles are skipped (except .gitignore),
/// as are node_modules and dashboard-app directories.
fn collect_files(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) -> Result<()> {
    for entry in
        std::fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        let hidden = name.starts_with('.') && name != ".gitignore";
        if hidden || name == "node_modules" || name == "dashboard-app" {
            continue;
        }
        if path.is_dir() {
            collect_files(root, &path, out)?;
        } else {
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            out.push((rel, path));
        }
    }
    Ok(())
}

fn sha1_hex(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let api = ctx.api()?;
    let deployment = args.deployment;
    let base = format!("/build/api/v1/deployments/{deployment}/data-model");

    if !args.directory.is_dir() {
        bail!("{} is not a directory", args.directory.display());
    }
    let mut files = Vec::new();
    collect_files(&args.directory, &args.directory, &mut files)?;
    if files.is_empty() {
        bail!("no deployable files found in {}", args.directory.display());
    }
    files.sort();

    // Hash local files and diff against the server's content hashes.
    let mut query = Vec::new();
    util::push(&mut query, "branchName", &args.branch);
    let upstream = api.get(&format!("{base}/file-hashes"), &query).await?;
    let upstream = upstream.get("files").cloned().unwrap_or(upstream);

    let mut manifest = serde_json::Map::new();
    let mut to_upload = Vec::new();
    for (rel, path) in &files {
        let data =
            std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        let hash = sha1_hex(&data);
        let unchanged = upstream
            .get(rel)
            .and_then(|f| f.get("hash"))
            .and_then(Value::as_str)
            == Some(hash.as_str());
        if !unchanged {
            to_upload.push((rel.clone(), data));
        }
        manifest.insert(rel.clone(), serde_json::json!({ "hash": hash }));
    }

    // Same event name the legacy `cubejs deploy` emitted.
    crate::telemetry::event("Cube Cloud CLI Deploy", serde_json::Map::new());

    // Open the upload transaction.
    let mut body = serde_json::Map::new();
    util::set(&mut body, "branchName", &args.branch);
    let start = api
        .post(&format!("{base}/upload/start"), Some(&util::body(body)))
        .await?;
    let transaction = start.get("transaction").cloned().unwrap_or(start.clone());
    let transaction_str = serde_json::to_string(&transaction)?;

    eprintln!(
        "Uploading {} changed file(s) of {} to deployment {deployment}…",
        to_upload.len(),
        files.len()
    );
    for (rel, data) in &to_upload {
        eprintln!("  {rel}");
        let basename = rel.rsplit('/').next().unwrap_or(rel).to_string();
        let transaction_str = transaction_str.clone();
        let rel_owned = rel.clone();
        let data = data.clone();
        api.post_multipart(&format!("{base}/upload/file"), move || {
            reqwest::multipart::Form::new()
                .text("transaction", transaction_str.clone())
                .text("fileName", rel_owned.clone())
                .part(
                    "file",
                    reqwest::multipart::Part::bytes(data.clone())
                        .file_name(basename.clone())
                        .mime_str("application/octet-stream")
                        .expect("static mime type is valid"),
                )
        })
        .await?;
    }

    // Commit the manifest: prunes files absent from it (unless --keep-missing)
    // and triggers a single build.
    let mut body = serde_json::Map::new();
    body.insert("transaction".into(), transaction);
    body.insert("files".into(), Value::Object(manifest));
    body.insert(
        "autoRemoveFiles".into(),
        serde_json::json!(!args.keep_missing),
    );
    util::set(&mut body, "branchName", &args.branch);
    let res = api
        .post(&format!("{base}/upload/finish"), Some(&util::body(body)))
        .await?;

    crate::telemetry::event("Cube Cloud CLI Deploy Success", serde_json::Map::new());

    if ctx.json {
        output::print_json(&res);
    } else {
        output::success(&format!(
            "Deployed {} file(s) ({} uploaded). Check progress with `cube deployments build-status {deployment}`",
            files.len(),
            to_upload.len()
        ));
    }
    Ok(())
}
