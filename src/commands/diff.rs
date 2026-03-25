use crate::cli::DiffArgs;
use crate::input::resolve_inputs_with_config;
use crate::output::table;
use crate::output::{OutputConfig, OutputFormat};
use crate::parquet_ext::metadata;
use arrow::array::Array;
use num_format::{Locale, ToFormattedString};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;

#[derive(Serialize)]
struct DiffResult {
    schema_diff: Vec<SchemaDiffRow>,
    metadata_diff: MetadataDiff,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_diff: Option<DataDiff>,
}

#[derive(Serialize)]
struct SchemaDiffRow {
    column: String,
    file_a: Option<String>,
    file_b: Option<String>,
    status: String,
}

#[derive(Serialize)]
struct MetadataDiff {
    rows_a: i64,
    rows_b: i64,
    size_a: u64,
    size_b: u64,
    row_groups_a: usize,
    row_groups_b: usize,
    compression_a: String,
    compression_b: String,
}

#[derive(Serialize)]
struct DataDiff {
    added: Vec<Vec<String>>,
    removed: Vec<Vec<String>>,
    changed: Vec<DataChange>,
    total_added: usize,
    total_removed: usize,
    total_changed: usize,
}

#[derive(Serialize)]
struct DataChange {
    row_a: Vec<String>,
    row_b: Vec<String>,
}

pub fn execute(args: &DiffArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;
    if sources.len() < 2 {
        return Err(miette::miette!(
            "diff requires exactly 2 files (got {})",
            sources.len()
        ));
    }

    let meta_a = metadata::read_metadata(sources[0].path())?;
    let meta_b = metadata::read_metadata(sources[1].path())?;

    let cols_a = metadata::column_info(&meta_a);
    let cols_b = metadata::column_info(&meta_b);

    // all columns in order, left first
    let mut all_names: Vec<String> = Vec::new();
    let names_a: std::collections::HashSet<&str> = cols_a.iter().map(|c| c.name.as_str()).collect();
    for c in &cols_a {
        all_names.push(c.name.clone());
    }
    for c in &cols_b {
        if !names_a.contains(c.name.as_str()) {
            all_names.push(c.name.clone());
        }
    }

    let mut schema_diff: Vec<SchemaDiffRow> = Vec::new();
    for name in &all_names {
        let a = cols_a.iter().find(|c| &c.name == name);
        let b = cols_b.iter().find(|c| &c.name == name);

        let (file_a_str, file_b_str, status) = match (a, b) {
            (Some(a), Some(b)) => {
                if a.type_name == b.type_name && a.nullable == b.nullable {
                    if args.changes_only {
                        continue;
                    }
                    (a.type_name.clone(), b.type_name.clone(), "same".to_string())
                } else if a.type_name != b.type_name {
                    (
                        a.type_name.clone(),
                        b.type_name.clone(),
                        "type changed".to_string(),
                    )
                } else {
                    (
                        format!(
                            "{}{}",
                            a.type_name,
                            if a.nullable { "" } else { " NOT NULL" }
                        ),
                        format!(
                            "{}{}",
                            b.type_name,
                            if b.nullable { "" } else { " NOT NULL" }
                        ),
                        "nullability changed".to_string(),
                    )
                }
            }
            (Some(a), None) => (
                a.type_name.clone(),
                "\u{2014}".to_string(),
                "removed".to_string(),
            ),
            (None, Some(b)) => (
                "\u{2014}".to_string(),
                b.type_name.clone(),
                "added".to_string(),
            ),
            (None, None) => unreachable!(),
        };

        schema_diff.push(SchemaDiffRow {
            column: name.clone(),
            file_a: Some(file_a_str),
            file_b: Some(file_b_str),
            status,
        });
    }

    let metadata_diff = MetadataDiff {
        rows_a: metadata::total_rows(&meta_a),
        rows_b: metadata::total_rows(&meta_b),
        size_a: sources[0].file_size(),
        size_b: sources[1].file_size(),
        row_groups_a: meta_a.num_row_groups(),
        row_groups_b: meta_b.num_row_groups(),
        compression_a: metadata::dominant_compression(&meta_a),
        compression_b: metadata::dominant_compression(&meta_b),
    };

    let data_diff = if args.data {
        Some(compute_data_diff(args, &sources)?)
    } else {
        None
    };

    let result = DiffResult {
        schema_diff,
        metadata_diff,
        data_diff,
    };

    match output.format {
        OutputFormat::Table => {
            writeln!(
                output.writer,
                "{}",
                table::section_header("Schema Diff", "", output.color)
            )
            .map_err(|e| miette::miette!("{}", e))?;

            let headers = [
                "Column",
                &sources[0].display_name(),
                &sources[1].display_name(),
                "Status",
            ];
            let rows: Vec<Vec<String>> = result
                .schema_diff
                .iter()
                .map(|r| {
                    vec![
                        r.column.clone(),
                        r.file_a.clone().unwrap_or_default(),
                        r.file_b.clone().unwrap_or_default(),
                        format!("({})", r.status),
                    ]
                })
                .collect();

            writeln!(
                output.writer,
                "{}",
                table::data_table(&headers, &rows, output.color)
            )
            .map_err(|e| miette::miette!("{}", e))?;

            writeln!(output.writer).map_err(|e| miette::miette!("{}", e))?;
            writeln!(
                output.writer,
                "{}",
                table::section_header("Metadata Diff", "", output.color)
            )
            .map_err(|e| miette::miette!("{}", e))?;

            let md = &result.metadata_diff;
            let meta_headers = [
                "",
                &sources[0].display_name(),
                &sources[1].display_name(),
                "",
            ];
            let diff_str = |a: i64, b: i64| -> String {
                let d = b - a;
                if d == 0 {
                    "same".to_string()
                } else if d > 0 {
                    format!("+{}", d.to_formatted_string(&Locale::en))
                } else {
                    d.to_formatted_string(&Locale::en)
                }
            };
            let meta_rows = vec![
                vec![
                    "Rows".to_string(),
                    md.rows_a.to_formatted_string(&Locale::en),
                    md.rows_b.to_formatted_string(&Locale::en),
                    diff_str(md.rows_a, md.rows_b),
                ],
                vec![
                    "Size".to_string(),
                    bytesize::ByteSize(md.size_a).to_string(),
                    bytesize::ByteSize(md.size_b).to_string(),
                    if md.size_a == md.size_b {
                        "same".to_string()
                    } else {
                        format!("{:+}", md.size_b as i64 - md.size_a as i64)
                    },
                ],
                vec![
                    "Row groups".to_string(),
                    md.row_groups_a.to_string(),
                    md.row_groups_b.to_string(),
                    if md.row_groups_a == md.row_groups_b {
                        "same".to_string()
                    } else {
                        "differs".to_string()
                    },
                ],
                vec![
                    "Compression".to_string(),
                    md.compression_a.clone(),
                    md.compression_b.clone(),
                    if md.compression_a == md.compression_b {
                        "same".to_string()
                    } else {
                        "differs".to_string()
                    },
                ],
            ];

            writeln!(
                output.writer,
                "{}",
                table::data_table(&meta_headers, &meta_rows, output.color)
            )
            .map_err(|e| miette::miette!("{}", e))?;

            if let Some(ref dd) = result.data_diff {
                writeln!(output.writer).map_err(|e| miette::miette!("{}", e))?;
                writeln!(
                    output.writer,
                    "{}",
                    table::section_header("Data Diff", "", output.color)
                )
                .map_err(|e| miette::miette!("{}", e))?;

                let mut data_rows: Vec<Vec<String>> = Vec::new();
                for row in &dd.removed {
                    let mut display = vec!["-".to_string()];
                    display.extend(row.iter().cloned());
                    data_rows.push(display);
                }
                for row in &dd.added {
                    let mut display = vec!["+".to_string()];
                    display.extend(row.iter().cloned());
                    data_rows.push(display);
                }
                for change in &dd.changed {
                    let mut display_a = vec!["~".to_string()];
                    display_a.extend(change.row_a.iter().cloned());
                    data_rows.push(display_a);
                    let mut display_b = vec!["~".to_string()];
                    display_b.extend(change.row_b.iter().cloned());
                    data_rows.push(display_b);
                }

                if data_rows.is_empty() {
                    writeln!(output.writer, "  (no data differences)")
                        .map_err(|e| miette::miette!("{}", e))?;
                } else {
                    // header from file A schema
                    let mut dd_headers: Vec<String> = vec!["Op".to_string()];
                    let meta_for_cols = metadata::read_metadata(sources[0].path())?;
                    let schema = meta_for_cols.file_metadata().schema_descr();
                    for col in schema.columns() {
                        dd_headers.push(col.name().to_string());
                    }
                    let dd_header_refs: Vec<&str> = dd_headers.iter().map(|s| s.as_str()).collect();

                    writeln!(
                        output.writer,
                        "{}",
                        table::data_table(&dd_header_refs, &data_rows, output.color)
                    )
                    .map_err(|e| miette::miette!("{}", e))?;
                }

                writeln!(
                    output.writer,
                    "\n  Summary: {} added, {} removed, {} changed",
                    dd.total_added, dd.total_removed, dd.total_changed
                )
                .map_err(|e| miette::miette!("{}", e))?;
            }
        }
        OutputFormat::Json | OutputFormat::Parquet => {
            crate::output::json::write_json(&mut output.writer, &result)?;
        }
        OutputFormat::Jsonl => {
            crate::output::json::write_jsonl(&mut output.writer, &result)?;
        }
        OutputFormat::Csv | OutputFormat::Tsv => {
            let headers = ["column", "file_a", "file_b", "status"];
            let rows: Vec<Vec<String>> = result
                .schema_diff
                .iter()
                .map(|r| {
                    vec![
                        r.column.clone(),
                        r.file_a.clone().unwrap_or_default(),
                        r.file_b.clone().unwrap_or_default(),
                        r.status.clone(),
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

/// Read all rows from a parquet file into a flat vector of string-formatted rows.
fn read_all_rows(
    source: &crate::input::source::ResolvedSource,
) -> miette::Result<(Vec<String>, Vec<Vec<String>>)> {
    let file = File::open(source.path())
        .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;
    let schema = builder.schema().clone();
    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let reader = builder
        .build()
        .map_err(|e| miette::miette!("cannot build reader: {}", e))?;

    let mut rows: Vec<Vec<String>> = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| miette::miette!("error reading data: {}", e))?;
        for row_idx in 0..batch.num_rows() {
            let mut row: Vec<String> = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    row.push("null".to_string());
                } else {
                    row.push(format!(
                        "{}",
                        arrow::util::display::array_value_to_string(col, row_idx)
                            .unwrap_or_else(|_| "?".to_string())
                    ));
                }
            }
            rows.push(row);
        }
    }
    Ok((col_names, rows))
}

/// Build a key string from a row given key column indices.
fn row_key(row: &[String], key_indices: &[usize]) -> String {
    key_indices
        .iter()
        .map(|&i| row.get(i).map(|s| s.as_str()).unwrap_or(""))
        .collect::<Vec<&str>>()
        .join("|")
}

fn compute_data_diff(
    args: &DiffArgs,
    sources: &[crate::input::source::ResolvedSource],
) -> miette::Result<DataDiff> {
    let (_cols_a, rows_a) = read_all_rows(&sources[0])?;
    let (cols_b, rows_b) = read_all_rows(&sources[1])?;

    const MAX_DIFFS: usize = 1000;

    let mut added: Vec<Vec<String>> = Vec::new();
    let mut removed: Vec<Vec<String>> = Vec::new();
    let mut changed: Vec<DataChange> = Vec::new();

    if let Some(ref key_str) = args.key {
        // Key-based comparison
        let key_names: Vec<&str> = key_str.split(',').map(|s| s.trim()).collect();

        let key_indices_a: Vec<usize> = key_names
            .iter()
            .filter_map(|name| _cols_a.iter().position(|c| c == name))
            .collect();
        let key_indices_b: Vec<usize> = key_names
            .iter()
            .filter_map(|name| cols_b.iter().position(|c| c == name))
            .collect();

        if key_indices_a.len() != key_names.len() || key_indices_b.len() != key_names.len() {
            return Err(miette::miette!(
                "key column(s) not found in both files: {}",
                key_str
            ));
        }

        let mut map_a: HashMap<String, &Vec<String>> = HashMap::new();
        for row in &rows_a {
            let key = row_key(row, &key_indices_a);
            map_a.insert(key, row);
        }

        let mut map_b: HashMap<String, &Vec<String>> = HashMap::new();
        for row in &rows_b {
            let key = row_key(row, &key_indices_b);
            map_b.insert(key, row);
        }

        for row in &rows_a {
            if added.len() + removed.len() + changed.len() >= MAX_DIFFS {
                break;
            }
            let key = row_key(row, &key_indices_a);
            match map_b.get(&key) {
                None => removed.push(row.clone()),
                Some(row_b) => {
                    if row != *row_b {
                        changed.push(DataChange {
                            row_a: row.clone(),
                            row_b: (*row_b).clone(),
                        });
                    }
                }
            }
        }

        for row in &rows_b {
            if added.len() + removed.len() + changed.len() >= MAX_DIFFS {
                break;
            }
            let key = row_key(row, &key_indices_b);
            if !map_a.contains_key(&key) {
                added.push(row.clone());
            }
        }
    } else {
        // Position-based comparison
        let max_len = rows_a.len().max(rows_b.len());
        let mut diff_count: usize = 0;
        for i in 0..max_len {
            if diff_count >= MAX_DIFFS {
                break;
            }
            match (rows_a.get(i), rows_b.get(i)) {
                (Some(a), Some(b)) => {
                    if a != b {
                        changed.push(DataChange {
                            row_a: a.clone(),
                            row_b: b.clone(),
                        });
                        diff_count += 1;
                    }
                }
                (Some(a), None) => {
                    removed.push(a.clone());
                    diff_count += 1;
                }
                (None, Some(b)) => {
                    added.push(b.clone());
                    diff_count += 1;
                }
                (None, None) => {}
            }
        }
    }

    let total_added = added.len();
    let total_removed = removed.len();
    let total_changed = changed.len();

    Ok(DataDiff {
        added,
        removed,
        changed,
        total_added,
        total_removed,
        total_changed,
    })
}
