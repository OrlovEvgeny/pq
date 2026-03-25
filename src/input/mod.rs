pub mod cloud;
pub mod column_selector;
pub mod source;

use crate::error::PqError;
use source::{CloudSource, LocalSource, ResolvedSource};
use std::path::Path;

/// Resolve a list of user-provided input strings into concrete file sources.
/// Handles plain paths, glob patterns, directories, stdin ("-"), and cloud URLs.
pub fn resolve_inputs(inputs: &[String]) -> miette::Result<Vec<ResolvedSource>> {
    resolve_inputs_with_config(inputs, &cloud::CloudConfig::default())
}

/// Resolve inputs with explicit cloud configuration.
pub fn resolve_inputs_with_config(
    inputs: &[String],
    cloud_config: &cloud::CloudConfig,
) -> miette::Result<Vec<ResolvedSource>> {
    let mut sources = Vec::new();

    for input in inputs {
        let resolved = resolve_single_with_config(input, cloud_config)?;
        if resolved.is_empty() {
            return Err(PqError::NoFilesFound {
                pattern: input.clone(),
            }
            .into());
        }
        sources.extend(resolved);
    }

    if sources.is_empty() {
        return Err(PqError::NoFilesFound {
            pattern: inputs.join(", "),
        }
        .into());
    }

    // Deduplicate by path
    sources.sort_by_key(|a| a.display_name());
    sources.dedup_by(|a, b| a.display_name() == b.display_name());

    Ok(sources)
}

fn resolve_single_with_config(
    input: &str,
    cloud_config: &cloud::CloudConfig,
) -> miette::Result<Vec<ResolvedSource>> {
    if input == "-" {
        return resolve_stdin();
    }

    if cloud::is_cloud_url(input) {
        return resolve_cloud(input, cloud_config);
    }

    let path = Path::new(input);

    if path.is_dir() {
        return resolve_directory(path);
    }

    if input.contains(['*', '?', '[']) {
        return resolve_glob(input);
    }

    if !path.exists() {
        let suggestion = suggest_similar(path);
        if let Some(hint) = suggestion {
            return Err(miette::miette!(
                "cannot read '{}'\n  \u{2192} File not found: {}\n  \u{2192} {}",
                input,
                path.display(),
                hint
            ));
        }
        return Err(PqError::FileNotFound {
            path: path.to_path_buf(),
        }
        .into());
    }

    let source = LocalSource::new(path.to_path_buf()).map_err(PqError::Io)?;
    Ok(vec![ResolvedSource::Local(source)])
}

fn resolve_stdin() -> miette::Result<Vec<ResolvedSource>> {
    // stdin → temp file
    let mut temp = tempfile::NamedTempFile::new().map_err(PqError::Io)?;
    let mut stdin = std::io::stdin().lock();
    std::io::copy(&mut stdin, &mut temp).map_err(PqError::Io)?;

    let path = temp.into_temp_path();
    let persistent_path = path.to_path_buf();
    // Keep the temp file alive — it will be cleaned up when the process exits
    path.keep().map_err(|e| PqError::Io(e.error))?;

    let source = LocalSource::new(persistent_path).map_err(PqError::Io)?;
    Ok(vec![ResolvedSource::Stdin(source)])
}

fn resolve_glob(pattern: &str) -> miette::Result<Vec<ResolvedSource>> {
    let entries = glob::glob(pattern).map_err(|e| PqError::GlobError {
        pattern: pattern.to_string(),
        source: e,
    })?;

    let mut sources = Vec::new();
    for entry in entries {
        let path = entry.map_err(|e| PqError::Io(e.into_error()))?;
        if path.is_file() && has_parquet_extension(&path) {
            let source = LocalSource::new(path).map_err(PqError::Io)?;
            sources.push(ResolvedSource::Local(source));
        }
    }
    Ok(sources)
}

fn resolve_directory(dir: &Path) -> miette::Result<Vec<ResolvedSource>> {
    let mut sources = Vec::new();
    walk_directory(dir, &mut sources)?;
    sources.sort_by_key(|a| a.display_name());
    Ok(sources)
}

fn walk_directory(dir: &Path, sources: &mut Vec<ResolvedSource>) -> miette::Result<()> {
    let entries = std::fs::read_dir(dir).map_err(PqError::Io)?;

    for entry in entries {
        let entry = entry.map_err(PqError::Io)?;
        let path = entry.path();
        if path.is_dir() {
            walk_directory(&path, sources)?;
        } else if path.is_file() && has_parquet_extension(&path) {
            let source = LocalSource::new(path).map_err(PqError::Io)?;
            sources.push(ResolvedSource::Local(source));
        }
    }
    Ok(())
}

fn resolve_cloud(
    input: &str,
    cloud_config: &cloud::CloudConfig,
) -> miette::Result<Vec<ResolvedSource>> {
    let cloud_url = cloud::parse_cloud_url(input).ok_or_else(|| PqError::CloudError {
        message: format!("unrecognised cloud URL: {input}"),
        suggestion: "Supported schemes: s3://, gs://, az://, http://, https://".to_string(),
    })?;

    let store = cloud::build_object_store(&cloud_url, cloud_config)?;
    let obj_path = cloud::object_path(&cloud_url);
    let key_str = obj_path.as_ref();

    let rt = tokio::runtime::Runtime::new().map_err(|e| PqError::CloudError {
        message: format!("failed to create async runtime: {e}"),
        suggestion: "This is an internal error — please report it".to_string(),
    })?;

    if key_str.contains(['*', '?', '[']) {
        return resolve_cloud_glob(input, &cloud_url, &store, key_str, &rt, cloud_config);
    }

    let (local_path, file_size) = cloud::download_to_temp(&store, &obj_path, &rt)?;

    Ok(vec![ResolvedSource::Cloud(CloudSource {
        url: input.to_string(),
        local_path,
        file_size,
    })])
}

/// Resolve a cloud URL with glob pattern (e.g., s3://bucket/prefix/*.parquet).
/// Lists objects under the prefix portion, filters by glob, downloads matches.
fn resolve_cloud_glob(
    _original_input: &str,
    cloud_url: &cloud::CloudUrl,
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    key_pattern: &str,
    rt: &tokio::runtime::Runtime,
    _cloud_config: &cloud::CloudConfig,
) -> miette::Result<Vec<ResolvedSource>> {
    use object_store::path::Path as ObjectPath;

    // prefix = part before first glob char
    let prefix_end = key_pattern
        .find(['*', '?', '['])
        .unwrap_or(key_pattern.len());
    let prefix_str = &key_pattern[..prefix_end];
    // Trim to last '/' to get a clean directory prefix
    let prefix_str = match prefix_str.rfind('/') {
        Some(idx) => &prefix_str[..=idx],
        None => "",
    };

    let prefix = if prefix_str.is_empty() {
        None
    } else {
        Some(ObjectPath::from(prefix_str))
    };

    let glob_pattern = glob::Pattern::new(key_pattern).map_err(|e| PqError::CloudError {
        message: format!("invalid glob pattern: {e}"),
        suggestion: "Check the glob syntax in your cloud URL".to_string(),
    })?;

    let all_objects = cloud::list_objects(store, prefix.as_ref(), rt)?;
    let mut sources = Vec::new();
    for (obj_path, _size) in &all_objects {
        let obj_key = obj_path.as_ref();
        if glob_pattern.matches(obj_key) {
            let has_pq_ext = obj_key.ends_with(".parquet")
                || obj_key.ends_with(".parq")
                || obj_key.ends_with(".pq");
            if has_pq_ext {
                let (local_path, file_size) = cloud::download_to_temp(store, obj_path, rt)?;
                let display_url = match cloud_url {
                    cloud::CloudUrl::S3 { bucket, .. } => format!("s3://{}/{}", bucket, obj_key),
                    cloud::CloudUrl::Gcs { bucket, .. } => format!("gs://{}/{}", bucket, obj_key),
                    cloud::CloudUrl::Azure { container, .. } => {
                        format!("az://{}/{}", container, obj_key)
                    }
                    cloud::CloudUrl::Http { url } => {
                        let base = url.rfind('/').map_or(url.as_str(), |i| &url[..=i]);
                        format!("{}{}", base, obj_key)
                    }
                };
                sources.push(ResolvedSource::Cloud(CloudSource {
                    url: display_url,
                    local_path,
                    file_size,
                }));
            }
        }
    }

    Ok(sources)
}

fn has_parquet_extension(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("parquet" | "parq" | "pq")
    )
}

/// Suggest similar files or directories when a path is not found.
fn suggest_similar(path: &Path) -> Option<String> {
    let parent = path.parent().unwrap_or(Path::new("."));

    if path.extension().is_none() {
        let as_dir = path.to_path_buf();
        if as_dir.is_dir()
            && let Ok(entries) = std::fs::read_dir(&as_dir)
        {
            let pq_count = entries
                .flatten()
                .filter(|e| e.path().is_file() && has_parquet_extension(&e.path()))
                .count();
            if pq_count > 0 {
                return Some(format!(
                    "Did you mean '{}'? (directory with {} .parquet file{})",
                    as_dir.display(),
                    pq_count,
                    if pq_count == 1 { "" } else { "s" }
                ));
            }
        }
    }

    if parent.is_dir()
        && let Ok(entries) = std::fs::read_dir(parent)
    {
        let parquet_files: Vec<String> = entries
            .flatten()
            .filter(|e| e.path().is_file() && has_parquet_extension(&e.path()))
            .map(|e| e.path().display().to_string())
            .take(5)
            .collect();

        if !parquet_files.is_empty() {
            return Some(format!(
                "Found .parquet files in '{}': {}",
                parent.display(),
                parquet_files.join(", ")
            ));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_inputs_plain_file() {
        // Create a temp parquet file
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let inputs = vec![path.to_string_lossy().to_string()];
        let sources = resolve_inputs(&inputs).unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0].file_size(),
            std::fs::metadata(&path).unwrap().len()
        );
    }

    #[test]
    fn test_resolve_inputs_missing_file() {
        let result = resolve_inputs(&["/nonexistent/file.parquet".to_string()]);
        assert!(result.is_err());
    }
}
