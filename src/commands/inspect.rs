use crate::cli::InspectArgs;
use crate::input::resolve_inputs_report;
use crate::output::table;
use crate::output::{OutputConfig, OutputFormat};
use crate::parquet_ext::metadata;
use num_format::{Locale, ToFormattedString};
use serde::Serialize;
use std::io::Write;

#[derive(Serialize)]
struct InspectResult {
    file: String,
    size_bytes: u64,
    uncompressed_bytes: i64,
    compression: String,
    compression_ratio: f64,
    num_rows: i64,
    num_columns: usize,
    num_row_groups: usize,
    created_by: Option<String>,
    format_version: String,
    encryption: String,
    schema: Vec<SchemaColumn>,
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    key_value_metadata: std::collections::HashMap<String, String>,
}

#[derive(Serialize)]
struct SchemaColumn {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
    nullable: bool,
    encoding: String,
}

pub fn execute(args: &InspectArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sp = output.spinner("Loading");
    let sources = resolve_inputs_report(&args.files, &output.cloud_config, &mut |msg| {
        sp.set_message(msg);
    })?;
    sp.finish_and_clear();

    if args.raw {
        for source in &sources {
            let meta = metadata::read_metadata(source.path())?;
            writeln!(output.writer, "{:#?}", meta).map_err(|e| miette::miette!("{}", e))?;
        }
        return Ok(());
    }

    let multi_file = sources.len() > 1;

    if multi_file && output.format == OutputFormat::Table {
        return execute_multi_table(args, &sources, output);
    }

    let mut results = Vec::new();

    for source in &sources {
        let meta = metadata::read_metadata(source.path())?;
        let file_size = source.file_size();
        let uncompressed = metadata::total_uncompressed_size(&meta);
        let compressed = metadata::total_compressed_size(&meta);
        let compression = metadata::dominant_compression(&meta);
        let ratio = if compressed > 0 {
            uncompressed as f64 / compressed as f64
        } else {
            1.0
        };
        let rows = metadata::total_rows(&meta);
        let num_columns = meta.file_metadata().schema_descr().num_columns();
        let num_row_groups = meta.num_row_groups();
        let created_by = metadata::created_by(&meta);
        let version = meta.file_metadata().version();
        let kv_meta = metadata::key_value_metadata(&meta);

        let mut columns = metadata::column_info(&meta);
        if let Some(ref sort_field) = args.sort {
            match sort_field.as_str() {
                "name" => columns.sort_by(|a, b| a.name.cmp(&b.name)),
                "type" => columns.sort_by(|a, b| a.type_name.cmp(&b.type_name)),
                "encoding" => columns.sort_by(|a, b| a.encoding.cmp(&b.encoding)),
                _ => {}
            }
        }
        let schema: Vec<SchemaColumn> = columns
            .iter()
            .map(|c| SchemaColumn {
                name: c.name.clone(),
                type_name: c.type_name.clone(),
                nullable: c.nullable,
                encoding: c.encoding.clone(),
            })
            .collect();

        let result = InspectResult {
            file: source.display_name(),
            size_bytes: file_size,
            uncompressed_bytes: uncompressed,
            compression: compression.clone(),
            compression_ratio: (ratio * 100.0).round() / 100.0,
            num_rows: rows,
            num_columns,
            num_row_groups,
            created_by: created_by.clone(),
            format_version: format!("v{}", version),
            encryption: "none".to_string(),
            schema,
            key_value_metadata: kv_meta.clone(),
        };

        if output.format == OutputFormat::Table && !args.schema_only {
            let avg_rows = if num_row_groups > 0 {
                rows / num_row_groups as i64
            } else {
                0
            };

            write!(
                output.writer,
                "{}",
                table::section_header("", &source.display_name(), &output.theme)
            )
            .map_err(|e| miette::miette!("{}", e))?;
            writeln!(output.writer).map_err(|e| miette::miette!("{}", e))?;

            let pairs = build_metadata_pairs(
                file_size,
                uncompressed,
                &compression,
                ratio,
                rows,
                num_columns,
                num_row_groups,
                avg_rows,
                created_by.as_deref(),
                version,
                &kv_meta,
                output.verbose,
                &output.theme,
            );

            writeln!(
                output.writer,
                "{}",
                table::key_value_table(&pairs, &output.theme)
            )
            .map_err(|e| miette::miette!("{}", e))?;
        }

        if output.format == OutputFormat::Table && !args.meta_only {
            writeln!(output.writer).map_err(|e| miette::miette!("{}", e))?;
            writeln!(
                output.writer,
                "{}",
                table::section_header("Schema", "", &output.theme)
            )
            .map_err(|e| miette::miette!("{}", e))?;

            let max_show = if args.all { usize::MAX } else { 20 };

            if output.verbose {
                let detailed = metadata::detailed_column_info(&meta);
                let total_cols = detailed.len();

                let headers = [
                    "#", "Path", "Physical", "Logical", "Rep", "Def", "Rep Lvl", "Encoding",
                ];
                let rows_data: Vec<Vec<String>> = detailed
                    .iter()
                    .take(max_show)
                    .map(|c| {
                        vec![
                            c.index.clone(),
                            c.path.clone(),
                            c.physical_type.clone(),
                            c.logical_type.clone(),
                            c.repetition.clone(),
                            c.max_def_level.to_string(),
                            c.max_rep_level.to_string(),
                            c.encoding.clone(),
                        ]
                    })
                    .collect();

                writeln!(
                    output.writer,
                    "{}",
                    table::data_table(&headers, &rows_data, &output.theme)
                )
                .map_err(|e| miette::miette!("{}", e))?;

                if total_cols > max_show {
                    writeln!(
                        output.writer,
                        "  ... ({} more columns, use --all to show)",
                        total_cols - max_show
                    )
                    .map_err(|e| miette::miette!("{}", e))?;
                }
            } else {
                let mut columns = metadata::column_info(&meta);
                if let Some(ref sort_field) = args.sort {
                    match sort_field.as_str() {
                        "name" => columns.sort_by(|a, b| a.name.cmp(&b.name)),
                        "type" => columns.sort_by(|a, b| a.type_name.cmp(&b.type_name)),
                        "encoding" => columns.sort_by(|a, b| a.encoding.cmp(&b.encoding)),
                        _ => {}
                    }
                }
                let total_cols = columns.len();

                let headers = ["#", "Name", "Type", "Nullable", "Encoding"];
                let rows_data: Vec<Vec<String>> = columns
                    .iter()
                    .take(max_show)
                    .enumerate()
                    .map(|(i, c)| {
                        vec![
                            (i + 1).to_string(),
                            c.name.clone(),
                            c.type_name.clone(),
                            if c.nullable { "yes" } else { "no" }.to_string(),
                            c.encoding.clone(),
                        ]
                    })
                    .collect();

                writeln!(
                    output.writer,
                    "{}",
                    table::data_table(&headers, &rows_data, &output.theme)
                )
                .map_err(|e| miette::miette!("{}", e))?;

                if total_cols > max_show {
                    writeln!(
                        output.writer,
                        "  ... ({} more columns, use --all to show)",
                        total_cols - max_show
                    )
                    .map_err(|e| miette::miette!("{}", e))?;
                }
            }
        }

        results.push(result);
    }

    match output.format {
        OutputFormat::Table => {} // already written above
        OutputFormat::Json | OutputFormat::Parquet => {
            if results.len() == 1 {
                crate::output::json::write_json(&mut output.writer, &results[0])?;
            } else {
                crate::output::json::write_json(&mut output.writer, &results)?;
            }
        }
        OutputFormat::Jsonl => {
            for result in &results {
                crate::output::json::write_jsonl(&mut output.writer, result)?;
            }
        }
        OutputFormat::Csv | OutputFormat::Tsv => {
            let headers = [
                "file",
                "size_bytes",
                "num_rows",
                "num_columns",
                "compression",
                "created_by",
            ];
            let rows: Vec<Vec<String>> = results
                .iter()
                .map(|r| {
                    vec![
                        r.file.clone(),
                        r.size_bytes.to_string(),
                        r.num_rows.to_string(),
                        r.num_columns.to_string(),
                        r.compression.clone(),
                        r.created_by.clone().unwrap_or_default(),
                    ]
                })
                .collect();
            if output.format == OutputFormat::Csv {
                crate::output::csv_output::write_csv(&mut output.writer, &headers, &rows)?;
            } else {
                crate::output::csv_output::write_tsv(&mut output.writer, &headers, &rows)?;
            }
        }
    }

    Ok(())
}

fn execute_multi_table(
    _args: &InspectArgs,
    sources: &[crate::input::source::ResolvedSource],
    output: &mut OutputConfig,
) -> miette::Result<()> {
    let headers = ["File", "Size", "Rows", "Cols", "Compression", "Created by"];
    let mut rows = Vec::new();
    let mut total_size: u64 = 0;
    let mut total_rows: i64 = 0;
    let mut mismatch_count: usize = 0;
    let mut first_num_columns: Option<usize> = None;

    let progress = output.progress_bar("Inspecting", sources.len() as u64);
    for source in sources {
        let meta = metadata::read_metadata(source.path())?;
        let file_size = source.file_size();
        let num_rows = metadata::total_rows(&meta);
        let num_columns = meta.file_metadata().schema_descr().num_columns();
        let compression = metadata::dominant_compression(&meta);
        let created_by = metadata::created_by(&meta).unwrap_or_default();

        total_size += file_size;
        total_rows += num_rows;

        let schema_differs = match first_num_columns {
            None => {
                first_num_columns = Some(num_columns);
                false
            }
            Some(first) => num_columns != first,
        };

        if schema_differs {
            mismatch_count += 1;
        }

        let mut file_label = source.display_name();
        if schema_differs {
            file_label.push_str(&format!(
                " {} schema differs!",
                crate::output::symbols::symbols().larrow
            ));
        }

        rows.push(vec![
            file_label,
            format_size(file_size),
            num_rows.to_formatted_string(&Locale::en),
            num_columns.to_string(),
            compression,
            created_by,
        ]);
        progress.inc(1);
    }
    drop(progress);

    writeln!(
        output.writer,
        "{}",
        table::data_table(&headers, &rows, &output.theme)
    )
    .map_err(|e| miette::miette!("{}", e))?;

    let d = crate::output::symbols::symbols().dash;
    writeln!(output.writer, "  {d}{d}{d}").map_err(|e| miette::miette!("{}", e))?;
    writeln!(
        output.writer,
        "  Total: {} files, {}, {} rows",
        sources.len(),
        format_size(total_size),
        total_rows.to_formatted_string(&Locale::en)
    )
    .map_err(|e| miette::miette!("{}", e))?;

    if mismatch_count > 0 {
        let warn_msg = format!(
            "{}  Schema mismatch in {} file(s) (use `pq schema diff` for details)",
            crate::output::symbols::symbols().warn,
            mismatch_count
        );
        writeln!(
            output.writer,
            "{}",
            output.theme.check.warn.apply_to(&warn_msg)
        )
        .map_err(|e| miette::miette!("{}", e))?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_metadata_pairs(
    file_size: u64,
    uncompressed: i64,
    compression: &str,
    ratio: f64,
    rows: i64,
    num_columns: usize,
    num_row_groups: usize,
    avg_rows: i64,
    created_by: Option<&str>,
    version: i32,
    kv_meta: &std::collections::HashMap<String, String>,
    verbose: bool,
    theme: &crate::output::theme::Theme,
) -> Vec<(&'static str, String)> {
    let mut pairs = Vec::new();

    pairs.push((
        "Size",
        format!(
            "{} (disk)  {}  {} (uncompressed)",
            format_size(file_size),
            crate::output::symbols::symbols().arrow,
            format_size(uncompressed as u64)
        ),
    ));
    pairs.push((
        "Compression",
        format!("{} ({:.1}x ratio)", compression, ratio),
    ));
    pairs.push(("Rows", rows.to_formatted_string(&Locale::en)));
    pairs.push(("Columns", num_columns.to_string()));
    let avg_rg_size = if num_row_groups > 0 {
        file_size / num_row_groups as u64
    } else {
        0
    };
    pairs.push((
        "Row groups",
        format!(
            "{} (avg {} rows, {} each)",
            num_row_groups,
            avg_rows.to_formatted_string(&Locale::en),
            bytesize::ByteSize(avg_rg_size)
        ),
    ));
    if let Some(cb) = created_by {
        pairs.push(("Created by", cb.to_string()));
    }
    pairs.push(("Format", format!("Parquet v{}", version)));
    pairs.push(("Encryption", "none".to_string()));

    if !kv_meta.is_empty() {
        let kv_str = kv_meta
            .iter()
            .map(|(k, v)| {
                let truncated = if !verbose && v.chars().count() > 60 {
                    let t: String = v.chars().take(57).collect();
                    format!("{}...", t)
                } else {
                    v.clone()
                };
                format!("\"{}\": \"{}\"", k, truncated)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let json_str = format!("{{{}}}", kv_str);
        let display = crate::output::highlight::highlight_json(&json_str, theme);
        pairs.push(("Key-value", display));
    }

    pairs
}

fn format_size(bytes: u64) -> String {
    super::util::format_size(bytes as i64, false)
}
