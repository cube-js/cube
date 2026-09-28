use std::path::Path;

/// The CLI version tracks the Cube monorepo version. The single source of
/// truth is the repo-root `lerna.json` (bumped by every release commit), so
/// read it at build time instead of keeping Cargo.toml in sync by hand —
/// source builds and release builds then always report the real version.
/// Falls back to the crate version if lerna.json isn't present (e.g. the
/// crate directory built outside the monorepo).
fn main() {
    println!("cargo:rerun-if-changed=../../lerna.json");
    let version = lerna_version().unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
    println!("cargo:rustc-env=CUBE_CLI_VERSION={version}");
}

fn lerna_version() -> Option<String> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").ok()?;
    let lerna = Path::new(&manifest_dir).join("../../lerna.json");
    let raw = std::fs::read_to_string(lerna).ok()?;
    let json: serde_json::Value = serde_json::from_str(&raw).ok()?;
    json.get("version")?.as_str().map(str::to_string)
}
