use crate::cli::SizeArgs;
use crate::input::resolve_inputs_report;
use crate::output::table;
use crate::output::{OutputConfig, OutputFormat};
use crate::parquet_ext::metadata;
use serde::Serialize;
use std::io::Write;

#[derive(Serialize)]
struct ColumnSizeResult {
    name: String,
    compressed_bytes: i64,
    uncompressed_bytes: i64,
    ratio: f64,
    percent: f64,
}

#[derive(Serialize)]
struct SizeResult {
    file: String,
    columns: Vec<ColumnSizeResult>,
    data_compressed_bytes: i64,
    data_uncompressed_bytes: i64,
    footer_bytes: i64,
    file_size_bytes: u64,
}

pub fn execute(args: &SizeArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sp = output.spinner("Loading");
    let sources = resolve_inputs_report(&args.files, &output.cloud_config, &mut |msg| {
        sp.set_message(msg);
    })?;
    sp.finish_and_clear();

    let mut results = Vec::new();

    for source in &sources {
        let meta = metadata::read_metadata(source.path())?;
        let col_sizes = metadata::column_sizes(&meta);
        let total_compressed: i64 = col_sizes.iter().map(|c| c.compressed).sum();
        let total_uncompressed: i64 = col_sizes.iter().map(|c| c.uncompressed).sum();

        let mut columns: Vec<ColumnSizeResult> = col_sizes
            .iter()
            .map(|c| {
                let ratio = if c.compressed > 0 {
                    c.uncompressed as f64 / c.compressed as f64
                } else {
                    1.0
                };
                let percent = if total_compressed > 0 {
                    c.compressed as f64 / total_compressed as f64 * 100.0
                } else {
                    0.0
                };
                ColumnSizeResult {
                    name: c.name.clone(),
                    compressed_bytes: c.compressed,
                    uncompressed_bytes: c.uncompressed,
                    ratio: (ratio * 100.0).round() / 100.0,
                    percent: (percent * 10.0).round() / 10.0,
                }
            })
            .collect();

        if let Some(ref sort_field) = args.sort {
            match sort_field.as_str() {
                "name" => columns.sort_by(|a, b| a.name.cmp(&b.name)),
                "compressed" => {
                    columns.sort_by_key(|b| std::cmp::Reverse(b.compressed_bytes));
                }
                "uncompressed" => {
                    columns.sort_by_key(|b| std::cmp::Reverse(b.uncompressed_bytes));
                }
                "ratio" => {
                    columns.sort_by(|a, b| {
                        b.ratio
                            .partial_cmp(&a.ratio)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                }
                "percent" => {
                    columns.sort_by(|a, b| {
                        b.percent
                            .partial_cmp(&a.percent)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                }
                _ => {}
            }
        }

        // --top: default sort by size desc
        if let Some(top_n) = args.top {
            if args.sort.is_none() {
                columns.sort_by_key(|b| std::cmp::Reverse(b.compressed_bytes));
            }
            columns.truncate(top_n);
        }

        let footer_bytes = source.file_size() as i64 - total_compressed;

        let result = SizeResult {
            file: source.display_name(),
            columns,
            data_compressed_bytes: total_compressed,
            data_uncompressed_bytes: total_uncompressed,
            footer_bytes,
            file_size_bytes: source.file_size(),
        };

        if output.format == OutputFormat::Table {
            if sources.len() > 1 {
                writeln!(
                    output.writer,
                    "{}",
                    table::section_header("Size", &source.display_name(), &output.theme)
                )
                .map_err(|e| miette::miette!("{}", e))?;
            }

            let headers = ["Column", "Compressed", "Uncompressed", "Ratio", "% of File"];
            let theme = &output.theme;
            let rows: Vec<Vec<String>> = result
                .columns
                .iter()
                .map(|c| {
                    vec![
                        c.name.clone(),
                        theme
                            .kv
                            .value_size
                            .apply_to(format_size(c.compressed_bytes, args.bytes))
                            .to_string(),
                        theme
                            .kv
                            .value_size
                            .apply_to(format_size(c.uncompressed_bytes, args.bytes))
                            .to_string(),
                        table::ratio_chip("RATIO", c.ratio, theme),
                        table::percent_chip("FILE", c.percent, theme),
                    ]
                })
                .collect();

            writeln!(
                output.writer,
                "{}",
                table::data_table(&headers, &rows, &output.theme)
            )
            .map_err(|e| miette::miette!("{}", e))?;

            let data_ratio = if total_compressed > 0 {
                total_uncompressed as f64 / total_compressed as f64
            } else {
                1.0
            };
            let d = crate::output::symbols::symbols().dash;
            writeln!(output.writer, "  {d}{d}{d}").map_err(|e| miette::miette!("{}", e))?;
            writeln!(
                output.writer,
                "  Data total    {} {} {}",
                table::size_chip("ON DISK", format_size(total_compressed, args.bytes), theme),
                table::metric_chip(
                    "RAW",
                    format_size(total_uncompressed, args.bytes),
                    crate::output::theme::Tone::Accent,
                    theme
                ),
                table::ratio_chip("RATIO", data_ratio, theme)
            )
            .map_err(|e| miette::miette!("{}", e))?;
            writeln!(
                output.writer,
                "  Footer        {}",
                table::size_chip("META", format_size(footer_bytes, args.bytes), theme)
            )
            .map_err(|e| miette::miette!("{}", e))?;
            writeln!(
                output.writer,
                "  File total    {}",
                table::size_chip(
                    "FILE",
                    format_size(source.file_size() as i64, args.bytes),
                    theme
                )
            )
            .map_err(|e| miette::miette!("{}", e))?;
        }

        results.push(result);
    }

    match output.format {
        OutputFormat::Table => {} // already written
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
                "column",
                "compressed_bytes",
                "uncompressed_bytes",
                "ratio",
                "percent",
            ];
            let mut all_rows = Vec::new();
            for result in &results {
                for col in &result.columns {
                    all_rows.push(vec![
                        result.file.clone(),
                        col.name.clone(),
                        col.compressed_bytes.to_string(),
                        col.uncompressed_bytes.to_string(),
                        format!("{:.2}", col.ratio),
                        format!("{:.1}", col.percent),
                    ]);
                }
            }
            if output.format == OutputFormat::Csv {
                crate::output::csv_output::write_csv(&mut output.writer, &headers, &all_rows)?;
            } else {
                crate::output::csv_output::write_tsv(&mut output.writer, &headers, &all_rows)?;
            }
        }
    }

    Ok(())
}

fn format_size(bytes: i64, exact: bool) -> String {
    super::util::format_size(bytes, exact)
}
