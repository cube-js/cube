use std::path::Path;

use anyhow::{bail, Context as _, Result};
use owo_colors::OwoColorize;

use crate::update::{self, CURRENT_VERSION};
use crate::{output, Ctx};

/// Update the CLI to the latest release.
#[derive(clap::Args)]
pub struct Args {
    /// Only check for a newer release, don't install it
    #[arg(long)]
    check: bool,
}

pub async fn command(args: Args, _ctx: &Ctx) -> Result<()> {
    let http = reqwest::Client::builder()
        .user_agent(concat!("cube-cli/", env!("CARGO_PKG_VERSION")))
        .build()?;

    let release = update::latest_release(&http).await?;
    let latest = release.version();

    if !is_newer(latest, CURRENT_VERSION) {
        output::success(&format!(
            "cube {CURRENT_VERSION} is up to date (latest release: {latest})"
        ));
        return Ok(());
    }
    println!("Current version: {CURRENT_VERSION}");
    println!("Latest release:  {}", latest.bold().green());
    if args.check {
        println!("Run {} to install it.", "cube update".bold().cyan());
        return Ok(());
    }

    let Some(asset) = release.asset_for_this_platform() else {
        bail!(
            "the latest release has no binary for this platform ({}-{}); \
             it may still be building — try again in a few minutes",
            std::env::consts::OS,
            std::env::consts::ARCH
        );
    };

    eprintln!("Downloading {}…", asset.name);
    let bytes = http
        .get(&asset.browser_download_url)
        .send()
        .await?
        .error_for_status()
        .context("download failed")?
        .bytes()
        .await?;

    let new_binary = extract_binary(&bytes)?;
    replace_current_exe(&new_binary)?;

    output::success(&format!("Updated cube {CURRENT_VERSION} → {latest}"));
    Ok(())
}

fn is_newer(candidate: &str, current: &str) -> bool {
    let parse = |v: &str| -> Vec<u64> {
        v.split(['.', '-'])
            .map_while(|s| s.parse::<u64>().ok())
            .collect()
    };
    let (a, b) = (parse(candidate), parse(current));
    if a.is_empty() || b.is_empty() {
        return candidate != current;
    }
    a > b
}

/// Extract the `cube` binary from the release tar.gz archive.
fn extract_binary(archive: &[u8]) -> Result<Vec<u8>> {
    let gz = flate2::read::GzDecoder::new(archive);
    let mut tar = tar::Archive::new(gz);
    for entry in tar.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        if name == "cube" || name == "cube.exe" {
            let mut data = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut data)?;
            return Ok(data);
        }
    }
    bail!("release archive does not contain a cube binary");
}

/// Swap the running executable for the new binary.
///
/// The running file is first renamed aside (allowed on every platform,
/// including Windows, where an in-use file can't be overwritten but can be
/// renamed), then the new binary is written to the original path. The `.old`
/// leftover is cleaned up on the next run (see `cleanup_stale_binary`).
fn replace_current_exe(new_binary: &[u8]) -> Result<()> {
    let exe = std::env::current_exe().context("could not locate the running executable")?;
    let old = exe.with_extension("old");
    let _ = std::fs::remove_file(&old);
    std::fs::rename(&exe, &old)
        .with_context(|| format!("could not move {} aside", exe.display()))?;

    if let Err(e) = write_executable(&exe, new_binary) {
        // Restore the original on failure so the install isn't left broken.
        let _ = std::fs::rename(&old, &exe);
        return Err(e);
    }
    // Best effort: on Windows the old (still running) image can't be
    // deleted; the next invocation cleans it up.
    let _ = std::fs::remove_file(&old);
    Ok(())
}

fn write_executable(path: &Path, data: &[u8]) -> Result<()> {
    std::fs::write(path, data).with_context(|| format!("could not write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))?;
    }
    Ok(())
}

/// Remove the `.old` binary a previous `cube update` left behind (Windows
/// can't delete the running image during the update itself).
pub fn cleanup_stale_binary() {
    if let Ok(exe) = std::env::current_exe() {
        let _ = std::fs::remove_file(exe.with_extension("old"));
    }
}
