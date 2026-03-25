use crate::cli::CountArgs;
use crate::input::resolve_inputs_with_config;
use crate::output::{OutputConfig, OutputFormat};
use crate::parquet_ext::metadata;
use num_format::{Locale, ToFormattedString};
use serde::Serialize;

#[derive(Serialize)]
struct CountResult {
    file: String,
    rows: i64,
}

pub fn execute(args: &CountArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;
    let single_file = sources.len() == 1;

    let mut results: Vec<CountResult> = Vec::new();
    let mut total: i64 = 0;

    for source in &sources {
        let meta = metadata::read_metadata(source.path())?;
        let rows = metadata::total_rows(&meta);
        total += rows;
        results.push(CountResult {
            file: source.display_name(),
            rows,
        });
    }

    // -q: raw number for scripting
    if output.quiet {
        if args.total_only || single_file {
            writeln!(output.writer, "{}", total).map_err(|e| miette::miette!("{}", e))?;
        } else {
            for result in &results {
                writeln!(output.writer, "{}\t{}", result.file, result.rows)
                    .map_err(|e| miette::miette!("{}", e))?;
            }
            writeln!(output.writer, "total\t{}", total).map_err(|e| miette::miette!("{}", e))?;
        }
        return Ok(());
    }

    match output.format {
        OutputFormat::Table => {
            if single_file || args.total_only {
                writeln!(output.writer, "{}", total.to_formatted_string(&Locale::en))
                    .map_err(|e| miette::miette!("{}", e))?;
            } else {
                let max_name_len = results.iter().map(|r| r.file.len()).max().unwrap_or(4);

                for result in &results {
                    writeln!(
                        output.writer,
                        "  {:<width$}  {}",
                        result.file,
                        result.rows.to_formatted_string(&Locale::en),
                        width = max_name_len
                    )
                    .map_err(|e| miette::miette!("{}", e))?;
                }
                writeln!(
                    output.writer,
                    "  {:<width$}  {}",
                    "total",
                    total.to_formatted_string(&Locale::en),
                    width = max_name_len
                )
                .map_err(|e| miette::miette!("{}", e))?;
            }
        }
        OutputFormat::Json | OutputFormat::Parquet => {
            if single_file || args.total_only {
                crate::output::json::write_json(&mut output.writer, &total)?;
            } else {
                #[derive(Serialize)]
                struct MultiCountResult {
                    files: Vec<CountResult>,
                    total: i64,
                }
                let result = MultiCountResult {
                    files: results,
                    total,
                };
                crate::output::json::write_json(&mut output.writer, &result)?;
            }
        }
        OutputFormat::Jsonl => {
            for result in &results {
                crate::output::json::write_jsonl(&mut output.writer, result)?;
            }
        }
        OutputFormat::Csv | OutputFormat::Tsv => {
            let headers = ["file", "rows"];
            let rows: Vec<Vec<String>> = results
                .iter()
                .map(|r| vec![r.file.clone(), r.rows.to_string()])
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

use std::io::Write;
