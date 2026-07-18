use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context as _, Result};
use etcetera::{choose_base_strategy, BaseStrategy};
use serde::{Deserialize, Serialize};

/// A named connection to a Cube Cloud tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextConfig {
    pub url: String,
    /// Bearer token used for API calls: an API key, or an OAuth access token.
    pub api_key: String,
    /// OAuth refresh token, when the context was created via `cube login`
    /// device flow. Absent for API-key contexts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
}

/// On-disk CLI configuration.
///
/// Stored as TOML in the platform config directory:
/// - Linux/macOS: `~/.config/cube/config.toml` (XDG)
/// - Windows: `%APPDATA%\cube\config.toml`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_context: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub contexts: BTreeMap<String, ContextConfig>,
}

pub fn config_path() -> Result<PathBuf> {
    let strategy = choose_base_strategy().context("could not determine config directory")?;
    Ok(strategy.config_dir().join("cube").join("config.toml"))
}

impl Config {
    pub fn load() -> Result<Self> {
        let path = config_path()?;
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path()?;
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = fs::set_permissions(dir, fs::Permissions::from_mode(0o700));
            }
        }
        let raw = toml::to_string_pretty(self)?;

        // Write to a sibling temp file created with restrictive permissions up
        // front (no world-readable window), then atomically rename into place so
        // a crash mid-write can't truncate an existing token file.
        let tmp = path.with_extension("toml.tmp");
        {
            let mut opts = fs::OpenOptions::new();
            opts.write(true).create(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.mode(0o600);
            }
            let mut file = opts
                .open(&tmp)
                .with_context(|| format!("failed to create {}", tmp.display()))?;
            use std::io::Write as _;
            file.write_all(raw.as_bytes())?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &path)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    pub fn context(&self, name: Option<&str>) -> Option<(&str, &ContextConfig)> {
        let name = name.or(self.default_context.as_deref())?;
        self.contexts
            .get_key_value(name)
            .map(|(k, c)| (k.as_str(), c))
    }
}
