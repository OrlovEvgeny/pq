use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct LocalSource {
    pub path: PathBuf,
    pub file_size: u64,
}

impl LocalSource {
    pub fn new(path: PathBuf) -> Result<Self, std::io::Error> {
        let metadata = fs::metadata(&path)?;
        Ok(Self {
            path,
            file_size: metadata.len(),
        })
    }

    pub fn display_name(&self) -> String {
        self.path.display().to_string()
    }
}

/// A source backed by a cloud object that has been downloaded to a temp file.
#[derive(Debug, Clone)]
pub struct CloudSource {
    /// The original cloud URL (e.g. `s3://bucket/key.parquet`).
    pub url: String,
    /// Path to the downloaded temporary file on local disk.
    pub local_path: PathBuf,
    /// Size in bytes of the downloaded object.
    pub file_size: u64,
}

#[derive(Debug, Clone)]
pub enum ResolvedSource {
    Local(LocalSource),
    Stdin(LocalSource), // Backed by a temp file
    Cloud(CloudSource),
}

impl ResolvedSource {
    pub fn display_name(&self) -> String {
        match self {
            ResolvedSource::Local(s) => s.display_name(),
            ResolvedSource::Stdin(_) => "<stdin>".to_string(),
            ResolvedSource::Cloud(s) => s.url.clone(),
        }
    }

    pub fn file_size(&self) -> u64 {
        match self {
            ResolvedSource::Local(s) | ResolvedSource::Stdin(s) => s.file_size,
            ResolvedSource::Cloud(s) => s.file_size,
        }
    }

    pub fn path(&self) -> &std::path::Path {
        match self {
            ResolvedSource::Local(s) | ResolvedSource::Stdin(s) => &s.path,
            ResolvedSource::Cloud(s) => &s.local_path,
        }
    }
}
