use crate::cli::StatsArgs;
use crate::input::column_selector;
use crate::input::resolve_inputs_report;
use crate::output::table;
use crate::output::{OutputConfig, OutputFormat};
use crate::parquet_ext::metadata;
use num_format::{Locale, ToFormattedString};
use parquet::file::statistics::Statistics;
use serde::Serialize;
use std::io::Write;

/// Build selected column indices from a column filter pattern and Parquet metadata.
fn selected_columns(
    filter: Option<&str>,
    meta: &parquet::file::metadata::ParquetMetaData,
) -> Option<Vec<usize>> {
    let pattern = filter?;
    let num_columns = meta.file_metadata().schema_descr().num_columns();
    let schema = arrow::datatypes::Schema::new(
        (0..num_columns)
            .map(|i| {
                let col = meta.file_metadata().schema_descr().column(i);
                arrow::datatypes::Field::new(col.name(), arrow::datatypes::DataType::Null, true)
            })
            .collect::<Vec<_>>(),
    );
    Some(column_selector::select_columns(pattern, &schema))
}

#[derive(Serialize)]
struct ColumnStats {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
    null_percent: f64,
    min: Option<String>,
    max: Option<String>,
    distinct_count: Option<i64>,
    compressed_bytes: i64,
    uncompressed_bytes: i64,
}

#[derive(Serialize)]
struct RowGroupColumnStats {
    row_group: usize,
    name: String,
    rows: i64,
    null_percent: f64,
    min: Option<String>,
    max: Option<String>,
    compressed_bytes: i64,
}

#[derive(Serialize)]
struct RowGroupStats {
    row_group: usize,
    rows: i64,
    columns: Vec<RowGroupColumnStats>,
}

#[derive(Serialize)]
struct PageInfoRow {
    column: String,
    row_group: usize,
    dictionary_page: bool,
    data_page_offset: i64,
    compressed_size: i64,
}

#[derive(Serialize)]
struct BloomInfoRow {
    column: String,
    has_bloom_filter: bool,
    bloom_filter_size: Option<i64>,
}

pub fn execute(args: &StatsArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sp = output.spinner("Loading");
    let sources = resolve_inputs_report(&args.files, &output.cloud_config, &mut |msg| {
        sp.set_message(msg);
    })?;
    sp.finish_and_clear();

    for source in &sources {
        let meta = metadata::read_metadata(source.path())?;

        if args.pages {
            execute_pages(args, &meta, source, output)?;
        } else if args.bloom {
            execute_bloom(args, &meta, source, output)?;
        } else if args.row_groups {
            execute_row_groups(args, &meta, source, output)?;
        } else {
            execute_aggregated(args, &meta, source, &sources, output)?;
        }
    }

    Ok(())
}

fn execute_pages(
    args: &StatsArgs,
    meta: &parquet::file::metadata::ParquetMetaData,
    source: &crate::input::source::ResolvedSource,
    output: &mut OutputConfig,
) -> miette::Result<()> {
    let num_columns = meta.file_metadata().schema_descr().num_columns();
    let selected = selected_columns(args.columns.as_deref(), meta);
    let mut page_rows: Vec<PageInfoRow> = Vec::new();

    for (rg_idx, rg) in meta.row_groups().iter().enumerate() {
        for col_idx in 0..num_columns {
            if let Some(ref sel) = selected
                && !sel.contains(&col_idx)
            {
                continue;
            }

            let col_desc = meta.file_metadata().schema_descr().column(col_idx);
            let col_name = col_desc.name().to_string();

            let col_meta = &rg.columns()[col_idx];
            let has_dict_page = col_meta.dictionary_page_offset().is_some();
            let data_page_offset = col_meta.data_page_offset();
            let compressed_size = col_meta.compressed_size();

            page_rows.push(PageInfoRow {
                column: col_name,
                row_group: rg_idx,
                dictionary_page: has_dict_page,
                data_page_offset,
                compressed_size,
            });
        }
    }

    match output.format {
        OutputFormat::Table => {
            writeln!(
                output.writer,
                "{}",
                table::section_header("Page Info", &source.display_name(), output.color)
            )
            .map_err(|e| miette::miette!("{}", e))?;

            let headers = [
                "Column",
                "Row Group",
                "Dictionary Page",
                "Data Page Offset",
                "Compressed Size",
            ];
            let rows: Vec<Vec<String>> = page_rows
                .iter()
                .map(|r| {
                    vec![
                        r.column.clone(),
                        r.row_group.to_string(),
                        if r.dictionary_page {
                            "yes".to_string()
                        } else {
                            "no".to_string()
                        },
                        r.data_page_offset.to_string(),
                        bytesize::ByteSize(r.compressed_size.max(0) as u64).to_string(),
                    ]
                })
                .collect();

            writeln!(
                output.writer,
                "{}",
                table::data_table(&headers, &rows, output.color)
            )
            .map_err(|e| miette::miette!("{}", e))?;
        }
        OutputFormat::Json | OutputFormat::Parquet => {
            let wrapper = serde_json::json!({ "page_info": &page_rows });
            crate::output::json::write_json(&mut output.writer, &wrapper)?;
        }
        OutputFormat::Jsonl => {
            for row in &page_rows {
                crate::output::json::write_jsonl(&mut output.writer, row)?;
            }
        }
        OutputFormat::Csv | OutputFormat::Tsv => {
            let headers = [
                "column",
                "row_group",
                "dictionary_page",
                "data_page_offset",
                "compressed_size",
            ];
            let rows: Vec<Vec<String>> = page_rows
                .iter()
                .map(|r| {
                    vec![
                        r.column.clone(),
                        r.row_group.to_string(),
                        r.dictionary_page.to_string(),
                        r.data_page_offset.to_string(),
                        r.compressed_size.to_string(),
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

fn execute_bloom(
    args: &StatsArgs,
    meta: &parquet::file::metadata::ParquetMetaData,
    source: &crate::input::source::ResolvedSource,
    output: &mut OutputConfig,
) -> miette::Result<()> {
    let num_columns = meta.file_metadata().schema_descr().num_columns();
    let selected = selected_columns(args.columns.as_deref(), meta);

    let mut bloom_map: std::collections::BTreeMap<String, (bool, Option<i64>)> =
        std::collections::BTreeMap::new();

    for rg in meta.row_groups() {
        for col_idx in 0..num_columns {
            if let Some(ref sel) = selected
                && !sel.contains(&col_idx)
            {
                continue;
            }

            let col_desc = meta.file_metadata().schema_descr().column(col_idx);
            let col_name = col_desc.name().to_string();

            let col_meta = &rg.columns()[col_idx];
            let has_bloom = col_meta.bloom_filter_offset().is_some();
            let bloom_len = col_meta.bloom_filter_length();

            let entry = bloom_map.entry(col_name).or_insert((false, None));
            if has_bloom {
                entry.0 = true;
                if let Some(len) = bloom_len {
                    let current = entry.1.unwrap_or(0);
                    entry.1 = Some(current + len as i64);
                }
            }
        }
    }

    let bloom_rows: Vec<BloomInfoRow> = bloom_map
        .into_iter()
        .map(|(name, (has, size))| BloomInfoRow {
            column: name,
            has_bloom_filter: has,
            bloom_filter_size: size,
        })
        .collect();

    match output.format {
        OutputFormat::Table => {
            writeln!(
                output.writer,
                "{}",
                table::section_header("Bloom Filters", &source.display_name(), output.color)
            )
            .map_err(|e| miette::miette!("{}", e))?;

            let headers = ["Column", "Has Bloom Filter", "Bloom Filter Size"];
            let rows: Vec<Vec<String>> = bloom_rows
                .iter()
                .map(|r| {
                    vec![
                        r.column.clone(),
                        if r.has_bloom_filter {
                            "yes".to_string()
                        } else {
                            "no".to_string()
                        },
                        r.bloom_filter_size
                            .map(|s| bytesize::ByteSize(s as u64).to_string())
                            .unwrap_or_else(|| {
                                crate::output::symbols::symbols().emdash.to_string()
                            }),
                    ]
                })
                .collect();

            writeln!(
                output.writer,
                "{}",
                table::data_table(&headers, &rows, output.color)
            )
            .map_err(|e| miette::miette!("{}", e))?;
        }
        OutputFormat::Json | OutputFormat::Parquet => {
            crate::output::json::write_json(&mut output.writer, &bloom_rows)?;
        }
        OutputFormat::Jsonl => {
            for row in &bloom_rows {
                crate::output::json::write_jsonl(&mut output.writer, row)?;
            }
        }
        OutputFormat::Csv | OutputFormat::Tsv => {
            let headers = ["column", "has_bloom_filter", "bloom_filter_size"];
            let rows: Vec<Vec<String>> = bloom_rows
                .iter()
                .map(|r| {
                    vec![
                        r.column.clone(),
                        r.has_bloom_filter.to_string(),
                        r.bloom_filter_size
                            .map(|s| s.to_string())
                            .unwrap_or_default(),
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

fn execute_row_groups(
    args: &StatsArgs,
    meta: &parquet::file::metadata::ParquetMetaData,
    source: &crate::input::source::ResolvedSource,
    output: &mut OutputConfig,
) -> miette::Result<()> {
    let num_columns = meta.file_metadata().schema_descr().num_columns();
    let selected = selected_columns(args.columns.as_deref(), meta);
    let mut rg_stats_list: Vec<RowGroupStats> = Vec::new();
    let mut flat_rows: Vec<RowGroupColumnStats> = Vec::new();

    for (rg_idx, rg) in meta.row_groups().iter().enumerate() {
        let rg_rows = rg.num_rows();
        let mut columns = Vec::new();

        for col_idx in 0..num_columns {
            if let Some(ref sel) = selected
                && !sel.contains(&col_idx)
            {
                continue;
            }

            let col_desc = meta.file_metadata().schema_descr().column(col_idx);
            let col_name = col_desc.name().to_string();

            let col_meta = &rg.columns()[col_idx];
            let compressed = col_meta.compressed_size();

            let mut null_count: i64 = 0;
            let mut min_val: Option<String> = None;
            let mut max_val: Option<String> = None;
            let mut has_stats = false;

            if let Some(stats) = col_meta.statistics() {
                has_stats = true;
                if let Some(nc) = stats.null_count_opt() {
                    null_count = nc as i64;
                }
                min_val = format_stat_value(stats, true);
                max_val = format_stat_value(stats, false);
            }

            let null_percent = if rg_rows > 0 {
                null_count as f64 / rg_rows as f64 * 100.0
            } else {
                0.0
            };

            let entry = RowGroupColumnStats {
                row_group: rg_idx,
                name: col_name,
                rows: rg_rows,
                null_percent: (null_percent * 10.0).round() / 10.0,
                min: if has_stats { min_val } else { None },
                max: if has_stats { max_val } else { None },
                compressed_bytes: compressed,
            };

            flat_rows.push(RowGroupColumnStats {
                row_group: entry.row_group,
                name: entry.name.clone(),
                rows: entry.rows,
                null_percent: entry.null_percent,
                min: entry.min.clone(),
                max: entry.max.clone(),
                compressed_bytes: entry.compressed_bytes,
            });
            columns.push(entry);
        }

        rg_stats_list.push(RowGroupStats {
            row_group: rg_idx,
            rows: rg_rows,
            columns,
        });
    }

    match output.format {
        OutputFormat::Table => {
            writeln!(
                output.writer,
                "{}",
                table::section_header(
                    "Stats (per row group)",
                    &source.display_name(),
                    output.color
                )
            )
            .map_err(|e| miette::miette!("{}", e))?;

            let headers = ["Row Group", "Column", "Rows", "Null%", "Min", "Max", "Size"];
            let rows: Vec<Vec<String>> = flat_rows
                .iter()
                .map(|c| {
                    vec![
                        c.row_group.to_string(),
                        c.name.clone(),
                        c.rows.to_string(),
                        format!("{:.1}%", c.null_percent),
                        c.min.clone().unwrap_or_else(|| {
                            crate::output::symbols::symbols().emdash.to_string()
                        }),
                        c.max.clone().unwrap_or_else(|| {
                            crate::output::symbols::symbols().emdash.to_string()
                        }),
                        bytesize::ByteSize(c.compressed_bytes.max(0) as u64).to_string(),
                    ]
                })
                .collect();

            writeln!(
                output.writer,
                "{}",
                table::data_table(&headers, &rows, output.color)
            )
            .map_err(|e| miette::miette!("{}", e))?;
        }
        OutputFormat::Json | OutputFormat::Parquet => {
            crate::output::json::write_json(&mut output.writer, &rg_stats_list)?;
        }
        OutputFormat::Jsonl => {
            for rg_stat in &rg_stats_list {
                crate::output::json::write_jsonl(&mut output.writer, rg_stat)?;
            }
        }
        OutputFormat::Csv | OutputFormat::Tsv => {
            let headers = [
                "row_group",
                "name",
                "rows",
                "null_percent",
                "min",
                "max",
                "compressed_bytes",
            ];
            let rows: Vec<Vec<String>> = flat_rows
                .iter()
                .map(|c| {
                    vec![
                        c.row_group.to_string(),
                        c.name.clone(),
                        c.rows.to_string(),
                        format!("{:.1}", c.null_percent),
                        c.min.clone().unwrap_or_default(),
                        c.max.clone().unwrap_or_default(),
                        c.compressed_bytes.to_string(),
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

fn execute_aggregated(
    args: &StatsArgs,
    meta: &parquet::file::metadata::ParquetMetaData,
    source: &crate::input::source::ResolvedSource,
    sources: &[crate::input::source::ResolvedSource],
    output: &mut OutputConfig,
) -> miette::Result<()> {
    let num_columns = meta.file_metadata().schema_descr().num_columns();
    let total_rows = metadata::total_rows(meta);
    let selected = selected_columns(args.columns.as_deref(), meta);

    let mut col_stats: Vec<ColumnStats> = Vec::new();

    for col_idx in 0..num_columns {
        if let Some(ref sel) = selected
            && !sel.contains(&col_idx)
        {
            continue;
        }

        let col_desc = meta.file_metadata().schema_descr().column(col_idx);
        let col_name = col_desc.name().to_string();

        let type_name = metadata::format_type(&col_desc);

        let mut total_nulls: i64 = 0;
        let mut compressed: i64 = 0;
        let mut uncompressed: i64 = 0;
        let mut global_min: Option<String> = None;
        let mut global_max: Option<String> = None;
        let mut has_stats = false;
        let mut total_distinct: Option<i64> = None;

        for rg in meta.row_groups() {
            let col_meta = &rg.columns()[col_idx];
            compressed += col_meta.compressed_size();
            uncompressed += col_meta.uncompressed_size();

            if let Some(stats) = col_meta.statistics() {
                has_stats = true;
                if let Some(nc) = stats.null_count_opt() {
                    total_nulls += nc as i64;
                }

                if let Some(dc) = stats.distinct_count_opt() {
                    let current = total_distinct.unwrap_or(0);
                    total_distinct = Some(current + dc as i64);
                }

                let min_str = format_stat_value(stats, true);
                let max_str = format_stat_value(stats, false);

                if global_min.is_none() || min_str.as_ref() < global_min.as_ref() {
                    global_min = min_str;
                }
                if global_max.is_none() || max_str.as_ref() > global_max.as_ref() {
                    global_max = max_str;
                }
            }
        }

        let null_percent = if total_rows > 0 {
            total_nulls as f64 / total_rows as f64 * 100.0
        } else {
            0.0
        };

        col_stats.push(ColumnStats {
            name: col_name,
            type_name,
            null_percent: (null_percent * 10.0).round() / 10.0,
            min: if has_stats { global_min } else { None },
            max: if has_stats { global_max } else { None },
            distinct_count: total_distinct,
            compressed_bytes: compressed,
            uncompressed_bytes: uncompressed,
        });
    }

    if let Some(ref sort_field) = args.sort {
        match sort_field.as_str() {
            "name" => col_stats.sort_by(|a, b| a.name.cmp(&b.name)),
            "nulls" => col_stats.sort_by(|a, b| {
                b.null_percent
                    .partial_cmp(&a.null_percent)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            "size" => {
                col_stats.sort_by(|a, b| b.compressed_bytes.cmp(&a.compressed_bytes));
            }
            _ => {}
        }
    }

    match output.format {
        OutputFormat::Table => {
            if sources.len() > 1 {
                writeln!(
                    output.writer,
                    "{}",
                    table::section_header("Stats", &source.display_name(), output.color)
                )
                .map_err(|e| miette::miette!("{}", e))?;
            }

            let headers = if args.size_only {
                vec!["Column", "Type", "Compressed", "Uncompressed"]
            } else {
                vec![
                    "Column",
                    "Type",
                    "Null%",
                    "Min",
                    "Max",
                    "Distinct(est)",
                    "Compressed",
                    "Uncompressed",
                ]
            };

            let rows: Vec<Vec<String>> = col_stats
                .iter()
                .map(|c| {
                    if args.size_only {
                        vec![
                            c.name.clone(),
                            c.type_name.clone(),
                            bytesize::ByteSize(c.compressed_bytes.max(0) as u64).to_string(),
                            bytesize::ByteSize(c.uncompressed_bytes.max(0) as u64).to_string(),
                        ]
                    } else {
                        vec![
                            c.name.clone(),
                            c.type_name.clone(),
                            format!("{:.1}%", c.null_percent),
                            c.min.clone().unwrap_or_else(|| {
                                crate::output::symbols::symbols().emdash.to_string()
                            }),
                            c.max.clone().unwrap_or_else(|| {
                                crate::output::symbols::symbols().emdash.to_string()
                            }),
                            c.distinct_count
                                .map(|d| (d as u64).to_formatted_string(&Locale::en))
                                .unwrap_or_else(|| {
                                    crate::output::symbols::symbols().emdash.to_string()
                                }),
                            bytesize::ByteSize(c.compressed_bytes.max(0) as u64).to_string(),
                            bytesize::ByteSize(c.uncompressed_bytes.max(0) as u64).to_string(),
                        ]
                    }
                })
                .collect();

            let header_refs: Vec<&str> = headers.to_vec();
            writeln!(
                output.writer,
                "{}",
                table::data_table(&header_refs, &rows, output.color)
            )
            .map_err(|e| miette::miette!("{}", e))?;
        }
        OutputFormat::Json | OutputFormat::Parquet => {
            crate::output::json::write_json(&mut output.writer, &col_stats)?;
        }
        OutputFormat::Jsonl => {
            for stat in &col_stats {
                crate::output::json::write_jsonl(&mut output.writer, stat)?;
            }
        }
        OutputFormat::Csv | OutputFormat::Tsv => {
            let headers = [
                "name",
                "type",
                "null_percent",
                "min",
                "max",
                "compressed_bytes",
                "uncompressed_bytes",
            ];
            let rows: Vec<Vec<String>> = col_stats
                .iter()
                .map(|c| {
                    vec![
                        c.name.clone(),
                        c.type_name.clone(),
                        format!("{:.1}", c.null_percent),
                        c.min.clone().unwrap_or_default(),
                        c.max.clone().unwrap_or_default(),
                        c.compressed_bytes.to_string(),
                        c.uncompressed_bytes.to_string(),
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

fn format_stat_value(stats: &Statistics, is_min: bool) -> Option<String> {
    if stats.min_bytes_opt().is_none() || stats.max_bytes_opt().is_none() {
        return None;
    }

    let s = match stats {
        Statistics::Boolean(s) => {
            if is_min {
                s.min_opt().map(|v| v.to_string())
            } else {
                s.max_opt().map(|v| v.to_string())
            }
        }
        Statistics::Int32(s) => {
            if is_min {
                s.min_opt().map(|v| v.to_string())
            } else {
                s.max_opt().map(|v| v.to_string())
            }
        }
        Statistics::Int64(s) => {
            if is_min {
                s.min_opt().map(|v| v.to_string())
            } else {
                s.max_opt().map(|v| v.to_string())
            }
        }
        Statistics::Int96(s) => {
            if is_min {
                s.min_opt().map(|v| format!("{:?}", v))
            } else {
                s.max_opt().map(|v| format!("{:?}", v))
            }
        }
        Statistics::Float(s) => {
            if is_min {
                s.min_opt().map(|v| format!("{:.4}", v))
            } else {
                s.max_opt().map(|v| format!("{:.4}", v))
            }
        }
        Statistics::Double(s) => {
            if is_min {
                s.min_opt().map(|v| format!("{:.4}", v))
            } else {
                s.max_opt().map(|v| format!("{:.4}", v))
            }
        }
        Statistics::ByteArray(s) => {
            if is_min {
                s.min_opt()
                    .map(|v| String::from_utf8_lossy(v.data()).to_string())
            } else {
                s.max_opt()
                    .map(|v| String::from_utf8_lossy(v.data()).to_string())
            }
        }
        Statistics::FixedLenByteArray(s) => {
            if is_min {
                s.min_opt()
                    .map(|v| String::from_utf8_lossy(v.data()).to_string())
            } else {
                s.max_opt()
                    .map(|v| String::from_utf8_lossy(v.data()).to_string())
            }
        }
    };

    s.map(|v| {
        if v.chars().count() > 40 {
            let truncated: String = v.chars().take(37).collect();
            format!("{}...", truncated)
        } else {
            v
        }
    })
}
