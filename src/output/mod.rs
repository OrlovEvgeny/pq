pub mod csv_output;
pub mod highlight;
pub mod json;
pub mod symbols;
pub mod table;
pub mod theme;

use crate::cli::{ColorWhen, GlobalArgs, OutputFormatArg, ThemeVariant};
use crate::input::cloud::CloudConfig;
use std::io::{self, IsTerminal, Write};
use std::path::Path;

/// Resolved output format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
    Jsonl,
    Csv,
    Tsv,
    Parquet,
}

/// Whether color is enabled
#[derive(Debug, Clone, Copy)]
pub struct ColorConfig {
    pub enabled: bool,
    pub theme_variant: ThemeVariant,
}

/// Central output configuration resolved from CLI args
pub struct OutputConfig {
    pub format: OutputFormat,
    pub color: ColorConfig,
    pub theme: theme::Theme,
    pub writer: Box<dyn Write>,
    pub is_tty: bool,
    pub quiet: bool,
    pub verbose: bool,
    pub cloud_config: CloudConfig,
    output_path: Option<String>,
    /// If output is a cloud URL, temp file path for text output
    cloud_temp_path: Option<std::path::PathBuf>,
}

impl OutputConfig {
    pub fn from_args(args: &GlobalArgs, cloud_config: CloudConfig) -> miette::Result<Self> {
        let is_tty = io::stdout().is_terminal();

        let format = resolve_format(args.format.as_ref(), args.output.as_deref(), is_tty);

        let color_enabled = match (&args.color, args.no_color) {
            _ if std::env::var_os("NO_COLOR").is_some() => false,
            (_, true) => false,
            (ColorWhen::Always, _) => true,
            (ColorWhen::Never, _) => false,
            (ColorWhen::Auto, _) => is_tty,
        };

        let mut cloud_temp_path: Option<std::path::PathBuf> = None;

        let writer: Box<dyn Write> = if format == OutputFormat::Parquet {
            Box::new(io::BufWriter::new(io::stdout().lock()))
        } else if let Some(ref path) = args.output {
            if crate::input::cloud::is_cloud_url(path) {
                // cloud URL → temp file, upload on finalize()
                let temp = tempfile::NamedTempFile::new()
                    .map_err(|e| miette::miette!("cannot create temp file: {}", e))?;
                let temp_path = temp.into_temp_path().to_path_buf();
                let file = std::fs::File::create(&temp_path)
                    .map_err(|e| miette::miette!("cannot create temp file: {}", e))?;
                cloud_temp_path = Some(temp_path);
                Box::new(io::BufWriter::new(file))
            } else {
                let p = Path::new(path);
                let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
                if ext == "parquet"
                    || ext == "parq"
                    || ext == "pq"
                    || p.is_dir()
                    || (ext.is_empty() && !p.exists())
                {
                    Box::new(io::BufWriter::new(io::stdout().lock()))
                } else {
                    let file = std::fs::File::create(path).map_err(|e| {
                        miette::miette!("cannot create output file '{}': {}", path, e)
                    })?;
                    Box::new(io::BufWriter::new(file))
                }
            }
        } else {
            Box::new(io::BufWriter::new(io::stdout().lock()))
        };

        let color = ColorConfig {
            enabled: color_enabled,
            theme_variant: args.theme.unwrap_or(ThemeVariant::Dark),
        };

        Ok(Self {
            format,
            theme: theme::Theme::new(color),
            color,
            writer,
            is_tty,
            quiet: args.quiet,
            verbose: args.verbose,
            cloud_config,
            output_path: args.output.clone(),
            cloud_temp_path,
        })
    }
}

impl OutputConfig {
    pub fn spinner(&self, msg: &str) -> crate::spinner::Spinner {
        crate::spinner::Spinner::new(self.is_tty, self.quiet, self.color, msg)
    }

    pub fn progress_bar(&self, msg: &str, total: u64) -> crate::spinner::Spinner {
        crate::spinner::Spinner::progress(self.is_tty, self.quiet, self.color, msg, total)
    }

    pub fn output_path(&self) -> Option<&str> {
        self.output_path.as_deref()
    }

    /// Finalize output: if text was written to a temp file for a cloud URL,
    /// flush the writer and upload the temp file to cloud.
    pub fn finalize(&mut self) -> miette::Result<()> {
        self.writer.flush().ok();

        if let (Some(temp_path), Some(cloud_url)) = (&self.cloud_temp_path, &self.output_path)
            && crate::input::cloud::is_cloud_url(cloud_url)
        {
            crate::input::cloud::upload_to_cloud_url(cloud_url, temp_path, &self.cloud_config)?;
            let _ = std::fs::remove_file(temp_path);
        }
        Ok(())
    }
}

fn resolve_format(
    explicit: Option<&OutputFormatArg>,
    output_path: Option<&str>,
    is_tty: bool,
) -> OutputFormat {
    // 1. Explicit -f flag always wins
    if let Some(fmt) = explicit {
        return match fmt {
            OutputFormatArg::Table => OutputFormat::Table,
            OutputFormatArg::Json => OutputFormat::Json,
            OutputFormatArg::Jsonl => OutputFormat::Jsonl,
            OutputFormatArg::Csv => OutputFormat::Csv,
            OutputFormatArg::Tsv => OutputFormat::Tsv,
        };
    }

    // 2. Infer from output file extension
    if let Some(path) = output_path {
        let ext = Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");
        return match ext {
            "json" => OutputFormat::Json,
            "jsonl" | "ndjson" => OutputFormat::Jsonl,
            "csv" => OutputFormat::Csv,
            "tsv" => OutputFormat::Tsv,
            "parquet" | "parq" | "pq" => OutputFormat::Parquet,
            _ => OutputFormat::Json,
        };
    }

    // 3. TTY → table, pipe → JSON
    if is_tty {
        OutputFormat::Table
    } else {
        OutputFormat::Json
    }
}
