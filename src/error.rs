use miette::Diagnostic;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum PqError {
    #[error("cannot read '{path}'")]
    #[diagnostic(help("Check the file exists and you have read permissions"))]
    FileNotFound { path: PathBuf },

    #[error("not a Parquet file: '{path}'")]
    #[diagnostic(help("Expected a file with PAR1 magic bytes. This may be a different format."))]
    NotParquet { path: PathBuf },

    #[error("no Parquet files found matching '{pattern}'")]
    #[diagnostic(help("Check the path or glob pattern. Use `ls` to verify files exist."))]
    NoFilesFound { pattern: String },

    #[error("invalid glob pattern: '{pattern}'")]
    GlobError {
        pattern: String,
        #[source]
        source: glob::PatternError,
    },

    #[error("{message}")]
    Parquet {
        message: String,
        #[source]
        source: parquet::errors::ParquetError,
    },

    #[error("validation failed: {message}")]
    #[diagnostic(help("{suggestion}"))]
    ValidationFailed { message: String, suggestion: String },

    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("partial failure: {succeeded} succeeded, {failed} failed")]
    #[diagnostic(help("Some files could not be processed. Check the errors above."))]
    PartialFailure { succeeded: usize, failed: usize },

    #[error("cloud storage error: {message}")]
    #[diagnostic(help("{suggestion}"))]
    CloudError { message: String, suggestion: String },

    #[error("SQL error: {message}")]
    #[diagnostic(help("{suggestion}"))]
    Sql { message: String, suggestion: String },

    #[error("{0}")]
    Other(String),
}

impl From<String> for PqError {
    fn from(msg: String) -> Self {
        PqError::Other(msg)
    }
}

impl PqError {
    pub fn is_validation_failure(&self) -> bool {
        matches!(self, PqError::ValidationFailed { .. })
    }

    pub fn is_partial_failure(&self) -> bool {
        matches!(self, PqError::PartialFailure { .. })
    }
}

/// Exit codes
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum ExitCode {
    Success = 0,
    Error = 1,
    Usage = 2,
    ValidationFailed = 3,
    PartialFailure = 4,
}

impl From<ExitCode> for std::process::ExitCode {
    fn from(code: ExitCode) -> Self {
        std::process::ExitCode::from(code as u8)
    }
}
