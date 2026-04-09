use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "pq", version, about = "The jq of Parquet")]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub command: Option<Command>,

    /// Show all columns (no truncation)
    #[arg(long)]
    pub all: bool,

    /// Only print schema, skip file metadata
    #[arg(long)]
    pub schema_only: bool,

    /// Only print file metadata, skip schema
    #[arg(long)]
    pub meta_only: bool,

    /// Show raw Parquet metadata (thrift-level detail)
    #[arg(long)]
    pub raw: bool,

    /// Sort columns by: name, type, encoding, size, nulls
    #[arg(short, long)]
    pub sort: Option<String>,

    /// Files to inspect (when no subcommand is given)
    #[arg(trailing_var_arg = true)]
    pub files: Vec<String>,
}

#[derive(Debug, clap::Args)]
pub struct GlobalArgs {
    /// Write output to file (default: stdout)
    #[arg(short, long, global = true)]
    pub output: Option<String>,

    /// Output format: table, json, jsonl, csv, tsv
    #[arg(short, long, global = true)]
    pub format: Option<OutputFormatArg>,

    /// Suppress progress, only output data
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Verbose output
    #[arg(short, long, global = true)]
    pub verbose: bool,

    /// Disable colored output
    #[arg(long, global = true)]
    pub no_color: bool,

    /// Color output: auto, always, never
    #[arg(long, global = true, default_value = "auto")]
    pub color: ColorWhen,

    /// Color theme variant: dark or light
    #[arg(long, global = true, env = "PQ_THEME")]
    pub theme: Option<ThemeVariant>,

    /// Parallel jobs (default: num_cpus / 2)
    #[arg(short = 'j', long, global = true)]
    pub jobs: Option<usize>,

    /// Custom S3-compatible endpoint URL (for Cloudflare R2, MinIO, etc.)
    #[arg(long, global = true, env = "PQ_S3_ENDPOINT")]
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormatArg {
    Table,
    Json,
    Jsonl,
    Csv,
    Tsv,
}

#[derive(Debug, Clone, ValueEnum, Default)]
pub enum ColorWhen {
    #[default]
    Auto,
    Always,
    Never,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ThemeVariant {
    Dark,
    Light,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// File overview: metadata, schema, row groups
    #[command(alias = "i")]
    Inspect(InspectArgs),

    /// Schema operations: show, diff, extract
    #[command(alias = "s", alias = "sch")]
    Schema(SchemaArgs),

    /// Column statistics from row group metadata
    #[command(alias = "st")]
    Stats(StatsArgs),

    /// First N rows
    #[command(alias = "h")]
    Head(HeadArgs),

    /// Last N rows
    #[command(alias = "t")]
    Tail(TailArgs),

    /// Random sample of rows
    #[command(alias = "sa")]
    Sample(SampleArgs),

    /// Validate structure and optionally data
    #[command(alias = "c", alias = "validate")]
    Check(CheckArgs),

    /// Merge small files into optimal sizes
    #[command(alias = "cc")]
    Compact(CompactArgs),

    /// Convert between formats (CSV/JSON/Arrow <-> Parquet)
    #[command(alias = "cv")]
    Convert(ConvertArgs),

    /// Extract subsets by rows/columns/row-groups
    #[command(alias = "sl")]
    Slice(SliceArgs),

    /// Rewrite with new compression/encoding/metadata
    #[command(alias = "rw")]
    Rewrite(RewriteArgs),

    /// Concatenate files vertically
    Cat(CatArgs),

    /// Fast row count from metadata
    #[command(alias = "n")]
    Count(CountArgs),

    /// Storage analysis per column
    #[command(alias = "sz")]
    Size(SizeArgs),

    /// Compare schemas or data between files
    #[command(alias = "d")]
    Diff(DiffArgs),

    /// Horizontal join on key columns
    #[command(alias = "m")]
    Merge(MergeArgs),

    /// Generate shell completions
    Completions(CompletionsArgs),
}

// ── Per-command args ──

#[derive(Debug, clap::Args)]
pub struct InspectArgs {
    /// Parquet files, globs, or directories
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Show all columns (no truncation)
    #[arg(long)]
    pub all: bool,

    /// Only print schema, skip file metadata
    #[arg(long)]
    pub schema_only: bool,

    /// Only print file metadata, skip schema
    #[arg(long)]
    pub meta_only: bool,

    /// Show raw Parquet metadata (thrift-level detail)
    #[arg(long)]
    pub raw: bool,

    /// Sort columns by: name, type, encoding, size, nulls
    #[arg(short, long)]
    pub sort: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct SchemaArgs {
    #[command(subcommand)]
    pub subcommand: Option<SchemaSubcommand>,

    /// Parquet files, globs, or directories
    pub files: Vec<String>,

    /// Show nested types fully expanded
    #[arg(long)]
    pub expand: bool,

    /// Arrow schema representation
    #[arg(long)]
    pub arrow: bool,
}

#[derive(Debug, Subcommand)]
pub enum SchemaSubcommand {
    /// Compare schemas between files
    Diff(SchemaDiffArgs),
    /// Export schema as JSON/DDL
    Extract(SchemaExtractArgs),
}

#[derive(Debug, clap::Args)]
pub struct SchemaDiffArgs {
    /// Parquet files to compare
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Reference file for multi-file diff
    #[arg(long = "ref")]
    pub reference: Option<String>,

    /// Only show differences
    #[arg(long)]
    pub changes_only: bool,
}

#[derive(Debug, clap::Args)]
pub struct SchemaExtractArgs {
    /// Parquet files
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Output as DDL for: duckdb, spark
    #[arg(long)]
    pub ddl: Option<String>,

    /// Arrow schema representation
    #[arg(long)]
    pub arrow: bool,
}

#[derive(Debug, clap::Args)]
pub struct StatsArgs {
    /// Parquet files, globs, or directories
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Filter columns (comma-separated names or regex)
    #[arg(short, long)]
    pub columns: Option<String>,

    /// Show per-row-group breakdown
    #[arg(short = 'g', long)]
    pub row_groups: bool,

    /// Show page-level statistics
    #[arg(long)]
    pub pages: bool,

    /// Show Bloom filter info
    #[arg(long)]
    pub bloom: bool,

    /// Only show size/compression info
    #[arg(long)]
    pub size_only: bool,

    /// Sort by: name, nulls, size, ratio, distinct
    #[arg(long)]
    pub sort: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct HeadArgs {
    /// Parquet files, globs, or directories
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Number of rows (default: 10)
    #[arg(short = 'n', long, default_value = "10")]
    pub rows: usize,

    /// Column filter
    #[arg(short, long)]
    pub columns: Option<String>,

    /// Don't truncate columns, show full width
    #[arg(long)]
    pub wide: bool,

    /// Max column width in table mode (default: 40)
    #[arg(long)]
    pub max_width: Option<usize>,
}

#[derive(Debug, clap::Args)]
pub struct TailArgs {
    /// Parquet files, globs, or directories
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Number of rows (default: 10)
    #[arg(short = 'n', long, default_value = "10")]
    pub rows: usize,

    /// Column filter
    #[arg(short, long)]
    pub columns: Option<String>,

    /// Don't truncate columns, show full width
    #[arg(long)]
    pub wide: bool,

    /// Max column width in table mode (default: 40)
    #[arg(long)]
    pub max_width: Option<usize>,
}

#[derive(Debug, clap::Args)]
pub struct SampleArgs {
    /// Parquet files, globs, or directories
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Number of rows (default: 1000)
    #[arg(short = 'n', long, default_value = "1000")]
    pub rows: usize,

    /// Random seed for reproducibility
    #[arg(long)]
    pub seed: Option<u64>,

    /// Column filter
    #[arg(short, long)]
    pub columns: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct CheckArgs {
    /// Parquet files, globs, or directories
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Also validate data pages (slower)
    #[arg(long)]
    pub read_data: bool,

    /// Validate against a contract file
    #[arg(long)]
    pub contract: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct CompactArgs {
    /// Parquet files, globs, or directories
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Target file size (default: 128MB)
    #[arg(long, default_value = "128MB")]
    pub target_size: String,

    /// Re-sort data by these columns
    #[arg(long)]
    pub sort_by: Option<String>,

    /// Output compression: none, snappy, gzip, zstd, lz4, brotli
    #[arg(long)]
    pub compression: Option<String>,

    /// Compression level (codec-dependent)
    #[arg(long)]
    pub compression_level: Option<i32>,

    /// Target rows per row group
    #[arg(long)]
    pub row_group_size: Option<usize>,

    /// Force dictionary encoding where beneficial
    #[arg(long)]
    pub dictionary: bool,

    /// Don't compute column statistics (faster)
    #[arg(long)]
    pub no_stats: bool,

    /// Show plan without executing
    #[arg(long)]
    pub dry_run: bool,

    /// Replace input files (dangerous! use with --backup)
    #[arg(long)]
    pub in_place: bool,

    /// Backup originals before in-place compaction
    #[arg(long)]
    pub backup: Option<String>,

    /// Only compact files smaller than this (default: target/2)
    #[arg(long)]
    pub min_file_size: Option<String>,

    /// Keep Hive partition directory structure
    #[arg(long)]
    pub preserve_partitions: bool,
}

#[derive(Debug, clap::Args)]
pub struct ConvertArgs {
    /// Input files
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Output compression (default: zstd)
    #[arg(long)]
    pub compression: Option<String>,

    /// Compression level
    #[arg(long)]
    pub compression_level: Option<i32>,

    /// Rows per row group (default: 1,000,000)
    #[arg(long)]
    pub row_group_size: Option<usize>,

    /// Rows to sample for schema inference (default: 1000)
    #[arg(long)]
    pub infer_schema_rows: Option<usize>,

    /// Apply explicit schema (JSON or .parquet reference)
    #[arg(long)]
    pub schema: Option<String>,

    /// CSV lacks header row
    #[arg(long)]
    pub no_header: bool,

    /// CSV delimiter (default: auto-detect)
    #[arg(long)]
    pub delimiter: Option<char>,

    /// Enable dictionary encoding
    #[arg(long)]
    pub dictionary: bool,
}

#[derive(Debug, clap::Args)]
pub struct SliceArgs {
    /// Parquet files
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Row range: start:end (0-indexed, exclusive end)
    #[arg(long)]
    pub rows: Option<String>,

    /// Row group indices (comma-separated or range: 0-5)
    #[arg(long)]
    pub row_groups: Option<String>,

    /// Column names (comma-separated) or regex
    #[arg(short, long)]
    pub columns: Option<String>,

    /// Prune row groups using min/max statistics
    #[arg(long)]
    pub prune: Option<String>,

    /// Split into N approximately equal files
    #[arg(long)]
    pub split: Option<usize>,

    /// One output file per row group
    #[arg(long)]
    pub split_row_groups: bool,

    /// Split by unique values of a column (Hive-style)
    #[arg(long)]
    pub split_by: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct RewriteArgs {
    /// Parquet files
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Output compression
    #[arg(long)]
    pub compression: Option<String>,

    /// Rows per row group
    #[arg(long)]
    pub row_group_size: Option<usize>,

    /// Re-sort data
    #[arg(long)]
    pub sort_by: Option<String>,

    /// Force dictionary encoding where beneficial
    #[arg(long)]
    pub dictionary: bool,

    /// Strip all key-value metadata
    #[arg(long)]
    pub strip_metadata: bool,

    /// Strip specific key-value metadata
    #[arg(long)]
    pub strip_kv: Option<String>,

    /// Set key-value metadata
    #[arg(long)]
    pub set_kv: Option<String>,

    /// Upgrade Parquet version (1 or 2)
    #[arg(long = "version")]
    pub parquet_version: Option<i32>,
}

#[derive(Debug, clap::Args)]
pub struct CatArgs {
    /// Parquet files, globs, or directories
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Union by column name (fill missing with nulls)
    #[arg(long)]
    pub union_by_name: bool,

    /// Add source filename column
    #[arg(long)]
    pub add_filename: bool,

    /// Re-sort merged output
    #[arg(long)]
    pub sort_by: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct CountArgs {
    /// Parquet files, globs, or directories
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Only show total across all files
    #[arg(long)]
    pub total_only: bool,
}

#[derive(Debug, clap::Args)]
pub struct SizeArgs {
    /// Parquet files, globs, or directories
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Sort by: name, compressed, uncompressed, ratio, percent
    #[arg(long)]
    pub sort: Option<String>,

    /// Show only N largest columns
    #[arg(long)]
    pub top: Option<usize>,

    /// Show exact byte counts
    #[arg(long)]
    pub bytes: bool,
}

#[derive(Debug, clap::Args)]
pub struct DiffArgs {
    /// Parquet files to compare
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Compare actual data (not just schema)
    #[arg(long)]
    pub data: bool,

    /// Key columns for data diff
    #[arg(long)]
    pub key: Option<String>,

    /// Only show changes
    #[arg(long)]
    pub changes_only: bool,
}

#[derive(Debug, clap::Args)]
pub struct MergeArgs {
    /// Parquet files to join
    #[arg(required = true)]
    pub files: Vec<String>,

    /// Key column(s) for join
    #[arg(long, required = true)]
    pub key: String,

    /// Left join (keep all rows from first file)
    #[arg(long)]
    pub left: bool,
}

#[derive(Debug, clap::Args)]
pub struct CompletionsArgs {
    /// Shell to generate completions for
    #[arg(value_enum)]
    pub shell: CompletionShell,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum CompletionShell {
    Bash,
    Zsh,
    Fish,
    #[value(name = "powershell")]
    PowerShell,
}
