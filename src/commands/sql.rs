use crate::cli::SqlArgs;
use crate::error::PqError;
use crate::input;
use crate::input::cloud::{self, CloudUrl};
use crate::input::source::ResolvedSource;
use crate::output::{OutputConfig, OutputFormat};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionContext};
use object_store::ObjectStore;
use object_store::http::HttpBuilder;
use object_store::path::Path as ObjectPath;
use std::collections::HashSet;
use std::io::{self, IsTerminal, Read};
use std::path::Path;
use std::sync::Arc;
use tokio::runtime::Runtime;
use url::Url;

pub fn execute(args: &SqlArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let plan = resolve_query_and_sources(args)?;
    if plan.sources.is_empty() && plan.aliases.is_empty() {
        return Err(miette::miette!(
            "pq sql needs at least one Parquet source (file, glob, directory, cloud URL, or --as name=path)"
        ));
    }

    let rt = Runtime::new().map_err(sql_runtime_err)?;
    let cfg = SessionConfig::new()
        .with_batch_size(args.batch_size.unwrap_or(8192))
        .with_target_partitions(default_partitions())
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.reorder_filters", true);
    let ctx = SessionContext::new_with_config(cfg);

    let mut ownership = RegistrationOwnership::default();
    let mut used_names = HashSet::new();
    let spinner = output.spinner("Registering tables");

    register_inputs(
        &ctx,
        &rt,
        output,
        &plan.sources,
        None,
        &mut used_names,
        &mut ownership,
        &|msg| spinner.set_message(msg),
    )?;
    for alias in &plan.aliases {
        register_inputs(
            &ctx,
            &rt,
            output,
            std::slice::from_ref(&alias.path),
            Some(alias.name.as_str()),
            &mut used_names,
            &mut ownership,
            &|msg| spinner.set_message(msg),
        )?;
    }

    let statement = if args.analyze {
        format!("EXPLAIN ANALYZE {}", plan.query)
    } else if args.explain {
        format!("EXPLAIN {}", plan.query)
    } else {
        plan.query
    };

    spinner.set_message("Executing query");
    match output.format {
        OutputFormat::Table | OutputFormat::Json => {
            let batches = rt.block_on(async {
                let df = ctx.sql(&statement).await.map_err(sql_err_to_pq)?;
                df.collect().await.map_err(sql_err_to_pq)
            })?;
            spinner.finish_and_clear();
            crate::commands::util::write_record_batches(&batches, output)?;
        }
        _ => {
            let mut stream = rt.block_on(async {
                let df = ctx.sql(&statement).await.map_err(sql_err_to_pq)?;
                df.execute_stream().await.map_err(sql_err_to_pq)
            })?;
            spinner.finish_and_clear();
            crate::commands::util::stream_record_batches(&mut stream, &rt, output)?;
        }
    }

    drop(ownership);
    Ok(())
}

#[derive(Debug)]
struct QueryPlan {
    query: String,
    sources: Vec<String>,
    aliases: Vec<AliasSpec>,
}

#[derive(Debug)]
struct AliasSpec {
    name: String,
    path: String,
}

#[derive(Default)]
struct RegistrationOwnership {
    local_sources: Vec<ResolvedSource>,
}

#[derive(Clone)]
struct ListingSource {
    table_url: ListingTableUrl,
    object_store_base: Option<Url>,
    object_store: Option<Arc<dyn ObjectStore>>,
}

#[allow(clippy::too_many_arguments)]
fn register_inputs(
    ctx: &SessionContext,
    rt: &Runtime,
    output: &OutputConfig,
    raw_inputs: &[String],
    explicit_name: Option<&str>,
    used_names: &mut HashSet<String>,
    ownership: &mut RegistrationOwnership,
    report: &dyn Fn(&str),
) -> miette::Result<()> {
    for raw_input in raw_inputs {
        let table_name = explicit_name
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| uniquify_table_name(derive_table_name(raw_input), used_names));
        if explicit_name.is_some() && !used_names.insert(table_name.clone()) {
            return Err(miette::miette!("duplicate SQL table name '{}'", table_name));
        }

        report(&format!("Registering {table_name}"));
        let sources = expand_listing_sources(raw_input, output, rt, ownership, report)?;
        register_listing_table(ctx, rt, &table_name, sources)?;
    }

    Ok(())
}

fn expand_listing_sources(
    raw_input: &str,
    output: &OutputConfig,
    rt: &Runtime,
    ownership: &mut RegistrationOwnership,
    report: &dyn Fn(&str),
) -> miette::Result<Vec<ListingSource>> {
    if raw_input == "-" || !cloud::is_cloud_url(raw_input) {
        let before = ownership.local_sources.len();
        let resolved = input::resolve_inputs_report(
            &[raw_input.to_string()],
            &output.cloud_config,
            &mut |msg| report(msg),
        )?;
        ownership.local_sources.extend(resolved);
        return ownership.local_sources[before..]
            .iter()
            .map(listing_source_from_local)
            .collect();
    }

    expand_cloud_listing_sources(raw_input, &output.cloud_config, rt, report)
}

fn expand_cloud_listing_sources(
    raw_input: &str,
    cloud_config: &cloud::CloudConfig,
    rt: &Runtime,
    report: &dyn Fn(&str),
) -> miette::Result<Vec<ListingSource>> {
    let cloud_url = cloud::parse_cloud_url(raw_input).ok_or_else(|| PqError::CloudError {
        message: format!("unrecognised cloud URL: {raw_input}"),
        suggestion: "Supported schemes: s3://, gs://, az://, http://, https://".to_string(),
    })?;

    match &cloud_url {
        CloudUrl::Http { url } => {
            if url.contains(['*', '?', '[']) || url.ends_with('/') {
                return Err(PqError::Sql {
                    message: format!(
                        "HTTP sources do not support glob or directory registration: {url}"
                    ),
                    suggestion: "Use an explicit file URL or download the dataset locally first"
                        .to_string(),
                }
                .into());
            }

            let parsed =
                Url::parse(url).map_err(|e| miette::miette!("invalid URL '{}': {}", url, e))?;
            let host = parsed
                .host_str()
                .ok_or_else(|| miette::miette!("missing host in '{}'", url))?;
            let origin = if let Some(port) = parsed.port() {
                format!("{}://{}:{}", parsed.scheme(), host, port)
            } else {
                format!("{}://{}", parsed.scheme(), host)
            };
            let store = Arc::new(
                HttpBuilder::new()
                    .with_url(origin.as_str())
                    .build()
                    .map_err(|e| miette::miette!("failed to build HTTP object store: {}", e))?,
            ) as Arc<dyn ObjectStore>;
            let table_url = ListingTableUrl::parse(url)
                .map_err(|e| miette::miette!("cannot register '{}': {}", url, e))?;
            Ok(vec![ListingSource {
                table_url,
                object_store_base: Some(Url::parse(&origin).unwrap()),
                object_store: Some(store),
            }])
        }
        CloudUrl::S3 { bucket, key } => expand_bucket_listing_sources(
            raw_input,
            &cloud_url,
            cloud::build_object_store(&cloud_url, cloud_config)?,
            rt,
            &format!("s3://{bucket}"),
            key,
            |obj_key| format!("s3://{bucket}/{obj_key}"),
            report,
        ),
        CloudUrl::Gcs { bucket, key } => expand_bucket_listing_sources(
            raw_input,
            &cloud_url,
            cloud::build_object_store(&cloud_url, cloud_config)?,
            rt,
            &format!("gs://{bucket}"),
            key,
            |obj_key| format!("gs://{bucket}/{obj_key}"),
            report,
        ),
        CloudUrl::Azure { container, key } => expand_bucket_listing_sources(
            raw_input,
            &cloud_url,
            cloud::build_object_store(&cloud_url, cloud_config)?,
            rt,
            &format!("az://{container}"),
            key,
            |obj_key| format!("az://{container}/{obj_key}"),
            report,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn expand_bucket_listing_sources(
    raw_input: &str,
    _cloud_url: &CloudUrl,
    store: Arc<dyn ObjectStore>,
    rt: &Runtime,
    base_url: &str,
    key: &str,
    display_url: impl Fn(&str) -> String,
    report: &dyn Fn(&str),
) -> miette::Result<Vec<ListingSource>> {
    let base = Url::parse(base_url)
        .map_err(|e| miette::miette!("invalid base URL '{}': {}", base_url, e))?;
    let object_keys = if key.contains(['*', '?', '[']) {
        report(&format!("Resolving glob {raw_input}"));
        list_matching_cloud_objects(store.clone(), key, rt)?
    } else if key.is_empty() || key.ends_with('/') {
        report(&format!("Listing {raw_input}"));
        list_cloud_directory(store.clone(), key, rt)?
    } else {
        vec![key.to_string()]
    };

    if object_keys.is_empty() {
        return Err(PqError::NoFilesFound {
            pattern: raw_input.to_string(),
        }
        .into());
    }

    object_keys
        .into_iter()
        .map(|obj_key| {
            let table_url = ListingTableUrl::parse(display_url(&obj_key))
                .map_err(|e| miette::miette!("cannot register '{}': {}", raw_input, e))?;
            Ok(ListingSource {
                table_url,
                object_store_base: Some(base.clone()),
                object_store: Some(store.clone()),
            })
        })
        .collect()
}

fn list_matching_cloud_objects(
    store: Arc<dyn ObjectStore>,
    key_pattern: &str,
    rt: &Runtime,
) -> miette::Result<Vec<String>> {
    let prefix_end = key_pattern
        .find(['*', '?', '['])
        .unwrap_or(key_pattern.len());
    let prefix = key_pattern[..prefix_end]
        .rfind('/')
        .map(|idx| ObjectPath::from(&key_pattern[..=idx]));

    let pattern = glob::Pattern::new(key_pattern).map_err(|e| PqError::CloudError {
        message: format!("invalid cloud glob pattern '{key_pattern}': {e}"),
        suggestion: "Check the glob syntax in the cloud URL".to_string(),
    })?;

    Ok(cloud::list_objects(&store, prefix.as_ref(), rt)?
        .into_iter()
        .map(|(path, _)| path.to_string())
        .filter(|path| {
            pattern.matches_with(
                path,
                glob::MatchOptions {
                    case_sensitive: true,
                    require_literal_separator: true,
                    require_literal_leading_dot: false,
                },
            ) && is_parquet_name(path)
        })
        .collect())
}

fn list_cloud_directory(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    rt: &Runtime,
) -> miette::Result<Vec<String>> {
    let prefix = if prefix.is_empty() {
        None
    } else {
        Some(ObjectPath::from(prefix))
    };

    Ok(cloud::list_objects(&store, prefix.as_ref(), rt)?
        .into_iter()
        .map(|(path, _)| path.to_string())
        .filter(|path| is_parquet_name(path))
        .collect())
}

fn listing_source_from_local(source: &ResolvedSource) -> miette::Result<ListingSource> {
    let path = std::fs::canonicalize(source.path())
        .map_err(|e| miette::miette!("cannot canonicalize '{}': {}", source.display_name(), e))?;
    let url = Url::from_file_path(&path)
        .map_err(|_| miette::miette!("cannot convert '{}' to file URL", path.display()))?;
    let table_url = ListingTableUrl::parse(url.as_str())
        .map_err(|e| miette::miette!("cannot register '{}': {}", path.display(), e))?;
    Ok(ListingSource {
        table_url,
        object_store_base: None,
        object_store: None,
    })
}

fn register_listing_table(
    ctx: &SessionContext,
    rt: &Runtime,
    table_name: &str,
    sources: Vec<ListingSource>,
) -> miette::Result<()> {
    if sources.is_empty() {
        return Err(miette::miette!(
            "no Parquet sources resolved for SQL table '{}'",
            table_name
        ));
    }

    let mut seen_store_bases = HashSet::new();
    for source in &sources {
        if let (Some(base), Some(store)) = (&source.object_store_base, &source.object_store)
            && seen_store_bases.insert(base.as_str().to_string())
        {
            ctx.register_object_store(base, store.clone());
        }
    }

    let table_paths: Vec<ListingTableUrl> = sources
        .iter()
        .map(|source| source.table_url.clone())
        .collect();
    let format = Arc::new(ParquetFormat::default().with_enable_pruning(true));
    let listing_options = ListingOptions::new(format).with_file_extension(".parquet");
    let table_path = table_paths
        .first()
        .cloned()
        .ok_or_else(|| miette::miette!("no table paths resolved for '{}'", table_name))?;
    let schema = rt.block_on(async {
        listing_options
            .infer_schema(&ctx.state(), &table_path)
            .await
            .map_err(sql_err_to_pq)
    })?;
    let config = if table_paths.len() == 1 {
        ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema)
    } else {
        ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(listing_options)
            .with_schema(schema)
    };
    let table = ListingTable::try_new(config).map_err(sql_err_to_pq)?;
    ctx.register_table(table_name.to_string(), Arc::new(table))
        .map_err(sql_err_to_pq)?;
    Ok(())
}

fn resolve_query_and_sources(args: &SqlArgs) -> miette::Result<QueryPlan> {
    let aliases = parse_aliases(&args.aliases)?;

    if let Some(query_file) = args.query_file.as_deref() {
        return Ok(QueryPlan {
            query: read_query_from_source(query_file)?,
            sources: args.inputs.clone(),
            aliases,
        });
    }

    let stdin_is_tty = io::stdin().is_terminal();
    if !stdin_is_tty {
        if !args.inputs.is_empty()
            && looks_like_sql(&args.inputs[0])
            && (args.inputs.len() > 1 || !aliases.is_empty())
        {
            return Ok(QueryPlan {
                query: args.inputs[0].clone(),
                sources: args.inputs[1..].to_vec(),
                aliases,
            });
        }
        return Ok(QueryPlan {
            query: read_query_from_stdin()?,
            sources: args.inputs.clone(),
            aliases,
        });
    }

    if args.inputs.is_empty() {
        return Err(miette::miette!(
            "provide a SQL query and at least one input source, or pipe a query on stdin"
        ));
    }
    if args.inputs.len() == 1 && aliases.is_empty() {
        return Err(miette::miette!(
            "missing input sources after SQL query; pass files, directories, globs, cloud URLs, or --as name=path"
        ));
    }

    Ok(QueryPlan {
        query: args.inputs[0].clone(),
        sources: args.inputs[1..].to_vec(),
        aliases,
    })
}

fn parse_aliases(values: &[String]) -> miette::Result<Vec<AliasSpec>> {
    let mut seen = HashSet::new();
    let mut aliases = Vec::with_capacity(values.len());

    for value in values {
        let (name, path) = value
            .split_once('=')
            .ok_or_else(|| miette::miette!("invalid --as value '{}'; expected NAME=PATH", value))?;
        if !is_valid_sql_ident(name) {
            return Err(miette::miette!(
                "invalid SQL table alias '{}'; use [A-Za-z_][A-Za-z0-9_]*",
                name
            ));
        }
        if !seen.insert(name.to_string()) {
            return Err(miette::miette!("duplicate SQL table alias '{}'", name));
        }
        aliases.push(AliasSpec {
            name: name.to_string(),
            path: path.to_string(),
        });
    }

    Ok(aliases)
}

fn derive_table_name(raw: &str) -> String {
    let name = if let Some(cloud_url) = cloud::parse_cloud_url(raw) {
        match cloud_url {
            CloudUrl::S3 { key, .. } | CloudUrl::Gcs { key, .. } | CloudUrl::Azure { key, .. } => {
                derive_name_from_pathish(&key)
            }
            CloudUrl::Http { url } => {
                let path = Url::parse(&url)
                    .ok()
                    .map(|parsed| parsed.path().to_string())
                    .unwrap_or(url);
                derive_name_from_pathish(&path)
            }
        }
    } else {
        derive_name_from_pathish(raw)
    };

    sanitize_ident(&name)
}

fn derive_name_from_pathish(pathish: &str) -> String {
    let trimmed = pathish.trim_end_matches(['/', '\\']);
    let prefix = if let Some(idx) = trimmed.find(['*', '?', '[']) {
        &trimmed[..idx]
    } else {
        trimmed
    };
    let prefix = prefix.trim_end_matches(['/', '\\']);
    let segment = prefix
        .rsplit(['/', '\\'])
        .find(|segment| !segment.is_empty())
        .unwrap_or("t");
    strip_parquet_extension(segment).to_string()
}

fn strip_parquet_extension(name: &str) -> &str {
    name.strip_suffix(".parquet")
        .or_else(|| name.strip_suffix(".parq"))
        .or_else(|| name.strip_suffix(".pq"))
        .unwrap_or(name)
}

fn sanitize_ident(name: &str) -> String {
    let mut sanitized = String::with_capacity(name.len());
    let mut last_was_underscore = false;
    for ch in name.chars() {
        let out = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '_'
        };
        if out == '_' {
            if !last_was_underscore {
                sanitized.push('_');
                last_was_underscore = true;
            }
        } else {
            sanitized.push(out);
            last_was_underscore = false;
        }
    }
    let sanitized = sanitized.trim_matches('_').to_string();
    if sanitized.is_empty() {
        return "t".to_string();
    }
    if sanitized
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_digit())
    {
        format!("t_{sanitized}")
    } else {
        sanitized
    }
}

fn uniquify_table_name(base: String, used_names: &mut HashSet<String>) -> String {
    if used_names.insert(base.clone()) {
        return base;
    }

    let mut idx = 2;
    loop {
        let candidate = format!("{base}_{idx}");
        if used_names.insert(candidate.clone()) {
            return candidate;
        }
        idx += 1;
    }
}

fn is_valid_sql_ident(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(ch) if ch.is_ascii_alphabetic() || ch == '_' => {}
        _ => return false,
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn looks_like_sql(input: &str) -> bool {
    let trimmed = input.trim_start();
    let upper = trimmed
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "SELECT" | "WITH" | "EXPLAIN" | "SHOW" | "DESCRIBE" | "VALUES" | "TABLE"
    ) || trimmed.contains(char::is_whitespace)
}

fn read_query_from_source(path: &str) -> miette::Result<String> {
    if path == "-" {
        return read_query_from_stdin();
    }

    std::fs::read_to_string(path)
        .map(|query| query.trim().to_string())
        .map_err(|e| miette::miette!("cannot read query file '{}': {}", path, e))
}

fn read_query_from_stdin() -> miette::Result<String> {
    let mut query = String::new();
    io::stdin()
        .read_to_string(&mut query)
        .map_err(|e| miette::miette!("cannot read query from stdin: {}", e))?;
    let query = query.trim().to_string();
    if query.is_empty() {
        return Err(miette::miette!("received an empty SQL query"));
    }
    Ok(query)
}

fn is_parquet_name(name: &str) -> bool {
    matches!(
        Path::new(name).extension().and_then(|ext| ext.to_str()),
        Some("parquet" | "parq" | "pq")
    )
}

fn default_partitions() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .map(|n| (n / 2).max(1))
        .unwrap_or(1)
}

fn sql_runtime_err(err: std::io::Error) -> miette::Report {
    PqError::Sql {
        message: format!("failed to create async runtime: {err}"),
        suggestion: "This is an internal error; try again and report it if it persists".to_string(),
    }
    .into()
}

fn sql_err_to_pq(err: DataFusionError) -> miette::Report {
    let message = err.to_string();
    let suggestion = if message.contains("not found") || message.contains("No field named") {
        "Check table names, run `pq schema <file>` for available columns, or use `--as name=path` to pin table aliases".to_string()
    } else if message.contains("Schema error") || message.contains("schema") {
        "If this came from a glob or directory, split inputs into separate tables with `--as` so mismatched schemas are queried independently".to_string()
    } else {
        "Review the SQL and table aliases, or rerun with `--explain` to inspect the plan"
            .to_string()
    };

    PqError::Sql {
        message,
        suggestion,
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_table_name_handles_common_inputs() {
        let cases = [
            ("demo-data/orders.parquet", "orders"),
            (r"C:\temp\orders.parquet", "orders"),
            (r"C:\temp\logs\*.parquet", "logs"),
            ("s3://bucket/path/users.parquet", "users"),
            ("s3://bucket/logs/*.parquet", "logs"),
            ("data/year=2024/", "year_2024"),
            ("data/", "data"),
            ("123.parquet", "t_123"),
            ("***", "t"),
        ];

        for (input, expected) in cases {
            assert_eq!(derive_table_name(input), expected);
        }
    }

    #[test]
    fn uniquify_table_name_appends_suffixes() {
        let mut used = HashSet::new();
        assert_eq!(
            uniquify_table_name("orders".to_string(), &mut used),
            "orders"
        );
        assert_eq!(
            uniquify_table_name("orders".to_string(), &mut used),
            "orders_2"
        );
        assert_eq!(
            uniquify_table_name("orders".to_string(), &mut used),
            "orders_3"
        );
    }

    #[test]
    fn sql_ident_validation_matches_expected_shapes() {
        assert!(is_valid_sql_ident("orders"));
        assert!(is_valid_sql_ident("_orders_2"));
        assert!(!is_valid_sql_ident("2orders"));
        assert!(!is_valid_sql_ident("orders-test"));
        assert!(!is_valid_sql_ident("orders.test"));
    }

    #[test]
    fn parse_aliases_requires_valid_name_eq_path_pairs() {
        assert!(parse_aliases(&["orders=data/orders.parquet".to_string()]).is_ok());
        assert!(parse_aliases(&["1bad=data/orders.parquet".to_string()]).is_err());
        assert!(parse_aliases(&["missing".to_string()]).is_err());
    }
}
