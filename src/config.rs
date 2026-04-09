use serde::Deserialize;

use crate::input::cloud::CloudConfig;

#[derive(Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub defaults: DefaultsConfig,

    /// `[s3]` section in config.toml
    pub s3: Option<S3Config>,
    /// `[gcs]` section in config.toml
    pub gcs: Option<GcsConfig>,
    /// `[azure]` section in config.toml
    pub azure: Option<AzureConfig>,
}

#[derive(Debug, Default, Deserialize)]
pub struct S3Config {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub profile: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct GcsConfig {
    pub project: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct AzureConfig {
    pub account: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DefaultsConfig {
    pub format: Option<String>,
    pub color: Option<String>,
    pub theme: Option<String>,
    pub jobs: Option<usize>,
}

impl Config {
    /// Load config from platform config directory or `$PQ_CONFIG`, falling back to defaults.
    ///
    /// Config locations: Linux `~/.config/pq/config.toml`, macOS `~/Library/Application Support/pq/config.toml`,
    /// Windows `%APPDATA%\pq\config.toml`.
    ///
    /// Then override with environment variables (`PQ_DEFAULT_FORMAT`, `PQ_DEFAULT_JOBS`).
    pub fn load() -> Self {
        let config_path = std::env::var("PQ_CONFIG")
            .ok()
            .map(std::path::PathBuf::from)
            .or_else(|| {
                directories::ProjectDirs::from("", "", "pq")
                    .map(|dirs| dirs.config_dir().join("config.toml"))
            });

        let mut config = if let Some(path) = config_path {
            if path.exists() {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    match toml::from_str(&content) {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("warning: invalid config at '{}': {}", path.display(), e);
                            Config::default()
                        }
                    }
                } else {
                    Config::default()
                }
            } else {
                Config::default()
            }
        } else {
            Config::default()
        };

        // env var overrides
        if let Ok(fmt) = std::env::var("PQ_DEFAULT_FORMAT") {
            config.defaults.format = Some(fmt);
        }
        if let Ok(jobs) = std::env::var("PQ_DEFAULT_JOBS")
            && let Ok(n) = jobs.parse()
        {
            config.defaults.jobs = Some(n);
        }
        if let Ok(color) = std::env::var("PQ_COLOR") {
            config.defaults.color = Some(color);
        }
        if let Ok(theme) = std::env::var("PQ_THEME") {
            config.defaults.theme = Some(theme);
        }

        config
    }

    /// Build a [`CloudConfig`] by merging config file sections, env vars, and CLI overrides.
    ///
    /// Priority (highest to lowest): CLI flag > env var > config.toml
    pub fn cloud_config(&self, endpoint_override: Option<&str>) -> CloudConfig {
        let s3_region = std::env::var("PQ_S3_REGION")
            .ok()
            .or_else(|| self.s3.as_ref().and_then(|s| s.region.clone()));

        let s3_endpoint = endpoint_override
            .map(String::from)
            .or_else(|| std::env::var("PQ_S3_ENDPOINT").ok())
            .or_else(|| self.s3.as_ref().and_then(|s| s.endpoint.clone()));

        let s3_profile = std::env::var("AWS_PROFILE")
            .ok()
            .or_else(|| self.s3.as_ref().and_then(|s| s.profile.clone()));

        let gcs_project = std::env::var("PQ_GCS_PROJECT")
            .ok()
            .or_else(|| self.gcs.as_ref().and_then(|g| g.project.clone()));

        let azure_account = std::env::var("PQ_AZURE_ACCOUNT")
            .ok()
            .or_else(|| self.azure.as_ref().and_then(|a| a.account.clone()));

        CloudConfig {
            s3_region,
            s3_endpoint,
            s3_profile,
            gcs_project,
            azure_account,
        }
    }

    /// Apply config defaults to global args that aren't explicitly set.
    pub fn apply_to_args(&self, args: &mut crate::cli::GlobalArgs) {
        if args.format.is_none()
            && let Some(ref fmt) = self.defaults.format
        {
            args.format = match fmt.to_lowercase().as_str() {
                "table" => Some(crate::cli::OutputFormatArg::Table),
                "json" => Some(crate::cli::OutputFormatArg::Json),
                "jsonl" => Some(crate::cli::OutputFormatArg::Jsonl),
                "csv" => Some(crate::cli::OutputFormatArg::Csv),
                "tsv" => Some(crate::cli::OutputFormatArg::Tsv),
                _ => None,
            };
        }

        if args.jobs.is_none() {
            args.jobs = self.defaults.jobs;
        }

        if matches!(args.color, crate::cli::ColorWhen::Auto)
            && let Some(ref color) = self.defaults.color
        {
            args.color = match color.to_lowercase().as_str() {
                "always" => crate::cli::ColorWhen::Always,
                "never" => crate::cli::ColorWhen::Never,
                _ => crate::cli::ColorWhen::Auto,
            };
        }

        if args.theme.is_none()
            && let Some(ref theme) = self.defaults.theme
        {
            args.theme = match theme.to_lowercase().as_str() {
                "dark" => Some(crate::cli::ThemeVariant::Dark),
                "light" => Some(crate::cli::ThemeVariant::Light),
                _ => None,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, DefaultsConfig};
    use crate::cli::{ColorWhen, GlobalArgs, ThemeVariant};

    #[test]
    fn apply_theme_default_when_cli_omits_it() {
        let config = Config {
            defaults: DefaultsConfig {
                theme: Some("light".to_string()),
                ..DefaultsConfig::default()
            },
            ..Config::default()
        };

        let mut args = GlobalArgs {
            output: None,
            format: None,
            quiet: false,
            verbose: false,
            no_color: false,
            color: ColorWhen::Auto,
            theme: None,
            jobs: None,
            endpoint: None,
        };

        config.apply_to_args(&mut args);

        assert_eq!(args.theme, Some(ThemeVariant::Light));
    }

    #[test]
    fn keep_cli_theme_override() {
        let config = Config {
            defaults: DefaultsConfig {
                theme: Some("light".to_string()),
                ..DefaultsConfig::default()
            },
            ..Config::default()
        };

        let mut args = GlobalArgs {
            output: None,
            format: None,
            quiet: false,
            verbose: false,
            no_color: false,
            color: ColorWhen::Auto,
            theme: Some(ThemeVariant::Dark),
            jobs: None,
            endpoint: None,
        };

        config.apply_to_args(&mut args);

        assert_eq!(args.theme, Some(ThemeVariant::Dark));
    }
}
