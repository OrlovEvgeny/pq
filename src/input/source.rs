use std::fs;
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug)]
enum SourcePath {
    Persistent(PathBuf),
    Temporary(tempfile::TempPath),
}

impl SourcePath {
    fn as_path(&self) -> &Path {
        match self {
            SourcePath::Persistent(path) => path.as_path(),
            SourcePath::Temporary(path) => path.as_ref(),
        }
    }
}

#[derive(Debug)]
pub struct LocalSource {
    path: SourcePath,
    pub file_size: u64,
}

impl LocalSource {
    pub fn new(path: PathBuf) -> Result<Self, std::io::Error> {
        let metadata = fs::metadata(&path)?;
        Ok(Self {
            path: SourcePath::Persistent(path),
            file_size: metadata.len(),
        })
    }

    pub fn from_temp(path: tempfile::TempPath, file_size: u64) -> Self {
        Self {
            path: SourcePath::Temporary(path),
            file_size,
        }
    }

    pub fn display_name(&self) -> String {
        self.path().display().to_string()
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
}

/// A source backed by a cloud object that has been downloaded to a temp file.
#[derive(Debug)]
pub struct CloudSource {
    /// The original cloud URL (e.g. `s3://bucket/key.parquet`).
    pub url: String,
    /// Downloaded temporary file on local disk.
    pub local_path: tempfile::TempPath,
    /// Size in bytes of the downloaded object.
    pub file_size: u64,
}

#[derive(Debug)]
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
            ResolvedSource::Local(s) | ResolvedSource::Stdin(s) => s.path(),
            ResolvedSource::Cloud(s) => s.local_path.as_ref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn temp_backed_sources_cleanup_on_drop() {
        let mut temp = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp, "temporary").unwrap();
        let path = temp.path().to_path_buf();
        let source = LocalSource::from_temp(temp.into_temp_path(), 10);

        assert!(source.path().exists());
        drop(source);
        assert!(!path.exists());
    }
}
