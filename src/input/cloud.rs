use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
use object_store::path::Path as ObjectPath;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::PqError;

/// Parsed cloud URL
#[derive(Debug, Clone)]
pub enum CloudUrl {
    S3 { bucket: String, key: String },
    GCS { bucket: String, key: String },
    Azure { container: String, key: String },
    Http { url: String },
}

/// Cloud configuration from config.toml + env vars
#[derive(Debug, Default, Clone)]
pub struct CloudConfig {
    pub s3_region: Option<String>,
    pub s3_endpoint: Option<String>,
    pub s3_profile: Option<String>,
    pub gcs_project: Option<String>,
    pub azure_account: Option<String>,
}

/// Quick check whether a string looks like a cloud URL.
pub fn is_cloud_url(input: &str) -> bool {
    input.starts_with("s3://")
        || input.starts_with("gs://")
        || input.starts_with("az://")
        || input.starts_with("http://")
        || input.starts_with("https://")
}

/// Parse a cloud URL into its structured form.
///
/// Returns `None` if the input is not a recognised cloud URL scheme.
pub fn parse_cloud_url(input: &str) -> Option<CloudUrl> {
    if let Some(rest) = input.strip_prefix("s3://") {
        let (bucket, key) = split_bucket_key(rest);
        Some(CloudUrl::S3 { bucket, key })
    } else if let Some(rest) = input.strip_prefix("gs://") {
        let (bucket, key) = split_bucket_key(rest);
        Some(CloudUrl::GCS { bucket, key })
    } else if let Some(rest) = input.strip_prefix("az://") {
        let (container, key) = split_bucket_key(rest);
        Some(CloudUrl::Azure { container, key })
    } else if input.starts_with("http://") || input.starts_with("https://") {
        Some(CloudUrl::Http {
            url: input.to_string(),
        })
    } else {
        None
    }
}

/// Build an `ObjectStore` implementation for the given cloud URL.
pub fn build_object_store(
    url: &CloudUrl,
    config: &CloudConfig,
) -> miette::Result<Arc<dyn ObjectStore>> {
    match url {
        CloudUrl::S3 { bucket, .. } => {
            let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

            if let Some(ref region) = config.s3_region {
                builder = builder.with_region(region);
            }

            if let Some(ref endpoint) = config.s3_endpoint {
                builder = builder
                    .with_endpoint(endpoint)
                    .with_virtual_hosted_style_request(false);
            }

            // object_store picks up AWS_PROFILE from env, so set it explicitly
            if let Some(ref profile) = config.s3_profile {
                // SAFETY: we set the env var before any multi-threaded work starts
                // and only do so once during store construction.
                unsafe {
                    std::env::set_var("AWS_PROFILE", profile);
                }
            }

            let store = builder.build().map_err(|e| PqError::CloudError {
                message: format!("failed to build S3 client for bucket '{bucket}': {e}"),
                suggestion: "Check AWS credentials (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) \
                             or configure a profile in ~/.aws/credentials"
                    .to_string(),
            })?;
            Ok(Arc::new(store))
        }
        CloudUrl::GCS { bucket, .. } => {
            // GCS needs GOOGLE_CLOUD_PROJECT for billing
            if let Some(ref project) = config.gcs_project {
                unsafe {
                    std::env::set_var("GOOGLE_CLOUD_PROJECT", project);
                }
            }

            let builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);

            let store = builder.build().map_err(|e| PqError::CloudError {
                message: format!("failed to build GCS client for bucket '{bucket}': {e}"),
                suggestion: "Check GCS credentials (GOOGLE_APPLICATION_CREDENTIALS or gcloud auth)"
                    .to_string(),
            })?;
            Ok(Arc::new(store))
        }
        CloudUrl::Azure { container, .. } => {
            let mut builder = MicrosoftAzureBuilder::from_env().with_container_name(container);

            if let Some(ref account) = config.azure_account {
                builder = builder.with_account(account);
            }

            let store = builder.build().map_err(|e| PqError::CloudError {
                message: format!("failed to build Azure client for container '{container}': {e}"),
                suggestion: "Check Azure credentials (AZURE_STORAGE_ACCOUNT_NAME / \
                             AZURE_STORAGE_ACCOUNT_KEY) or set azure.account in config.toml"
                    .to_string(),
            })?;
            Ok(Arc::new(store))
        }
        CloudUrl::Http { url } => {
            let store =
                HttpBuilder::new()
                    .with_url(url)
                    .build()
                    .map_err(|e| PqError::CloudError {
                        message: format!("failed to build HTTP client for '{url}': {e}"),
                        suggestion: "Check the URL is correct and the server is reachable"
                            .to_string(),
                    })?;
            Ok(Arc::new(store))
        }
    }
}

/// Extract the object path (key) portion from a cloud URL.
pub fn object_path(url: &CloudUrl) -> ObjectPath {
    match url {
        CloudUrl::S3 { key, .. } | CloudUrl::GCS { key, .. } | CloudUrl::Azure { key, .. } => {
            ObjectPath::from(key.as_str())
        }
        CloudUrl::Http { url } => {
            if let Ok(parsed) = url::Url::parse(url) {
                ObjectPath::from(parsed.path())
            } else {
                ObjectPath::from(url.as_str())
            }
        }
    }
}

/// Download a remote object to a local temporary file.
///
/// Returns `(temp_path, size_in_bytes)`.
pub fn download_to_temp(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    rt: &tokio::runtime::Runtime,
) -> miette::Result<(PathBuf, u64)> {
    let get_result = rt
        .block_on(store.get(path))
        .map_err(|e| PqError::CloudError {
            message: format!("failed to download '{path}': {e}"),
            suggestion: "Check the object exists and you have read permissions".to_string(),
        })?;

    let bytes = rt
        .block_on(get_result.bytes())
        .map_err(|e| PqError::CloudError {
            message: format!("failed to read bytes for '{path}': {e}"),
            suggestion: "The download may have been interrupted — try again".to_string(),
        })?;

    let size = bytes.len() as u64;

    let mut temp = tempfile::NamedTempFile::new().map_err(PqError::Io)?;
    temp.write_all(&bytes).map_err(PqError::Io)?;
    temp.flush().map_err(PqError::Io)?;

    let temp_path = temp.into_temp_path();
    let persistent_path = temp_path.to_path_buf();
    temp_path.keep().map_err(|e| PqError::Io(e.error))?;

    Ok((persistent_path, size))
}

/// Upload a local file to a remote object store location.
pub fn upload_from_path(
    store: &Arc<dyn ObjectStore>,
    local: &Path,
    remote: &ObjectPath,
    rt: &tokio::runtime::Runtime,
) -> miette::Result<()> {
    let data = std::fs::read(local).map_err(PqError::Io)?;
    let payload = object_store::PutPayload::from(data);

    rt.block_on(store.put(remote, payload))
        .map_err(|e| PqError::CloudError {
            message: format!("failed to upload to '{remote}': {e}"),
            suggestion: "Check you have write permissions on the target bucket/container"
                .to_string(),
        })?;

    Ok(())
}

/// List objects under an optional prefix.
///
/// Returns a vec of `(path, size_in_bytes)`.
pub fn list_objects(
    store: &Arc<dyn ObjectStore>,
    prefix: Option<&ObjectPath>,
    rt: &tokio::runtime::Runtime,
) -> miette::Result<Vec<(ObjectPath, u64)>> {
    let items: Vec<object_store::ObjectMeta> = rt
        .block_on(async { store.list(prefix).try_collect().await })
        .map_err(|e| PqError::CloudError {
            message: format!("failed to list objects: {e}"),
            suggestion: "Check your credentials and that the bucket/container exists".to_string(),
        })?;

    Ok(items
        .into_iter()
        .map(|m| (m.location, m.size as u64))
        .collect())
}

/// High-level: upload a local file to a cloud URL.
///
/// Parses the URL, builds the object store, creates a runtime, and uploads.
pub fn upload_to_cloud_url(
    cloud_url_str: &str,
    local_path: &Path,
    config: &CloudConfig,
) -> miette::Result<()> {
    let cloud_url = parse_cloud_url(cloud_url_str).ok_or_else(|| PqError::CloudError {
        message: format!("not a cloud URL: {cloud_url_str}"),
        suggestion: "Supported: s3://, gs://, az://, http(s)://".to_string(),
    })?;

    let store = build_object_store(&cloud_url, config)?;
    let obj_path = object_path(&cloud_url);

    let rt = tokio::runtime::Runtime::new().map_err(|e| PqError::CloudError {
        message: format!("failed to create async runtime: {e}"),
        suggestion: "This is an internal error — please report it".to_string(),
    })?;

    upload_from_path(&store, local_path, &obj_path, &rt)?;
    Ok(())
}

// ── OutputTarget: cloud-aware output path abstraction ──

/// Represents where command output should be written.
/// If the target is cloud, it writes to a local temp file first,
/// then uploads on `finalize()`.
pub enum OutputTarget {
    /// Write directly to a local path.
    Local(String),
    /// Write to a local temp file, then upload to cloud on finalize.
    Cloud {
        local_path: PathBuf,
        cloud_url: String,
    },
}

impl OutputTarget {
    /// The local filesystem path to write to.
    pub fn local_path(&self) -> &str {
        match self {
            OutputTarget::Local(p) => p,
            OutputTarget::Cloud { local_path, .. } => local_path.to_str().unwrap_or(""),
        }
    }

    /// After writing is complete, upload to cloud if needed.
    pub fn finalize(&self, config: &CloudConfig) -> miette::Result<()> {
        if let OutputTarget::Cloud {
            local_path,
            cloud_url,
        } = self
        {
            upload_to_cloud_url(cloud_url, local_path, config)?;
            let _ = std::fs::remove_file(local_path);
        }
        Ok(())
    }
}

/// Resolve an output path: if cloud URL, create a temp file to write to.
/// Call `finalize()` on the result after writing to upload if needed.
pub fn resolve_output_path(out_path: &str) -> miette::Result<OutputTarget> {
    if is_cloud_url(out_path) {
        let ext = Path::new(out_path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("parquet");
        let temp = tempfile::Builder::new()
            .suffix(&format!(".{}", ext))
            .tempfile()
            .map_err(PqError::Io)?;
        let local_path = temp.into_temp_path().to_path_buf();
        Ok(OutputTarget::Cloud {
            local_path,
            cloud_url: out_path.to_string(),
        })
    } else {
        Ok(OutputTarget::Local(out_path.to_string()))
    }
}

// ── helpers ──

/// Split `bucket/path/to/key` into `("bucket", "path/to/key")`.
fn split_bucket_key(rest: &str) -> (String, String) {
    match rest.find('/') {
        Some(idx) => (rest[..idx].to_string(), rest[idx + 1..].to_string()),
        None => (rest.to_string(), String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url() {
        let url = parse_cloud_url("s3://my-bucket/path/to/file.parquet");
        assert!(url.is_some());
        match url.unwrap() {
            CloudUrl::S3 { bucket, key } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(key, "path/to/file.parquet");
            }
            _ => panic!("expected S3 variant"),
        }
    }

    #[test]
    fn test_parse_s3_url_no_key() {
        let url = parse_cloud_url("s3://my-bucket");
        assert!(url.is_some());
        match url.unwrap() {
            CloudUrl::S3 { bucket, key } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(key, "");
            }
            _ => panic!("expected S3 variant"),
        }
    }

    #[test]
    fn test_parse_gs_url() {
        let url = parse_cloud_url("gs://my-bucket/data/file.parquet");
        assert!(url.is_some());
        match url.unwrap() {
            CloudUrl::GCS { bucket, key } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(key, "data/file.parquet");
            }
            _ => panic!("expected GCS variant"),
        }
    }

    #[test]
    fn test_parse_azure_url() {
        let url = parse_cloud_url("az://my-container/blob/path.parquet");
        assert!(url.is_some());
        match url.unwrap() {
            CloudUrl::Azure { container, key } => {
                assert_eq!(container, "my-container");
                assert_eq!(key, "blob/path.parquet");
            }
            _ => panic!("expected Azure variant"),
        }
    }

    #[test]
    fn test_parse_http_url() {
        let url = parse_cloud_url("https://example.com/data/file.parquet");
        assert!(url.is_some());
        match url.unwrap() {
            CloudUrl::Http { url } => {
                assert_eq!(url, "https://example.com/data/file.parquet");
            }
            _ => panic!("expected Http variant"),
        }
    }

    #[test]
    fn test_parse_plain_path_returns_none() {
        assert!(parse_cloud_url("/local/path/file.parquet").is_none());
        assert!(parse_cloud_url("relative/file.parquet").is_none());
    }

    #[test]
    fn test_is_cloud_url() {
        assert!(is_cloud_url("s3://bucket/key"));
        assert!(is_cloud_url("gs://bucket/key"));
        assert!(is_cloud_url("az://container/key"));
        assert!(is_cloud_url("http://example.com/file"));
        assert!(is_cloud_url("https://example.com/file"));
        assert!(!is_cloud_url("/local/path"));
        assert!(!is_cloud_url("relative/path"));
        assert!(!is_cloud_url("file.parquet"));
    }

    #[test]
    fn test_object_path_s3() {
        let url = CloudUrl::S3 {
            bucket: "b".to_string(),
            key: "path/to/file.parquet".to_string(),
        };
        let p = object_path(&url);
        assert_eq!(p.as_ref(), "path/to/file.parquet");
    }

    #[test]
    fn test_object_path_http() {
        let url = CloudUrl::Http {
            url: "https://example.com/data/file.parquet".to_string(),
        };
        let p = object_path(&url);
        assert_eq!(p.as_ref(), "data/file.parquet");
    }
}
