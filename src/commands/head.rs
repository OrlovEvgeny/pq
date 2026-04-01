use crate::cli::HeadArgs;
use crate::input::column_selector::resolve_projection;
use crate::input::resolve_inputs_report;
use crate::output::{OutputConfig, OutputFormat};
use arrow::array::{Array, ArrayRef, AsArray, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

pub fn execute(args: &HeadArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sp = output.spinner("Loading");
    let sources = resolve_inputs_report(&args.files, &output.cloud_config, &mut |msg| {
        sp.set_message(msg);
    })?;
    sp.finish_and_clear();

    for source in &sources {
        let file = File::open(source.path())
            .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        if let Some(indices) = resolve_projection(args.columns.as_deref(), builder.schema()) {
            let mask = parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), indices);
            builder = builder.with_projection(mask);
        }

        builder = builder.with_limit(args.rows);
        builder = builder.with_batch_size(args.rows.min(8192));

        let reader = builder
            .build()
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        let batches: Vec<_> = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| miette::miette!("error reading data: {}", e))?;

        write_batches(&batches, output, args.wide, args.max_width)?;
    }

    Ok(())
}

/// Truncate string columns in a batch to `max_width` characters, appending "..." if truncated.
fn truncate_batch(batch: &RecordBatch, max_width: usize) -> miette::Result<RecordBatch> {
    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx);
        let dt = col.data_type();
        match dt {
            DataType::Utf8 => {
                let str_arr = col.as_string::<i32>();
                let truncated: StringArray = (0..str_arr.len())
                    .map(|i| {
                        if str_arr.is_null(i) {
                            None
                        } else {
                            let val = str_arr.value(i);
                            if val.chars().count() > max_width {
                                let truncated: String =
                                    val.chars().take(max_width.saturating_sub(3)).collect();
                                Some(format!("{}...", truncated))
                            } else {
                                Some(val.to_string())
                            }
                        }
                    })
                    .collect();
                columns.push(Arc::new(truncated) as ArrayRef);
            }
            DataType::LargeUtf8 => {
                let str_arr = col.as_string::<i64>();
                let truncated: StringArray = (0..str_arr.len())
                    .map(|i| {
                        if str_arr.is_null(i) {
                            None
                        } else {
                            let val = str_arr.value(i);
                            if val.chars().count() > max_width {
                                let truncated: String =
                                    val.chars().take(max_width.saturating_sub(3)).collect();
                                Some(format!("{}...", truncated))
                            } else {
                                Some(val.to_string())
                            }
                        }
                    })
                    .collect();
                columns.push(Arc::new(truncated) as ArrayRef);
            }
            _ => {
                columns.push(col.clone());
            }
        }
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|e| miette::miette!("failed to build truncated batch: {}", e))
}

pub fn write_batches(
    batches: &[RecordBatch],
    output: &mut OutputConfig,
    wide: bool,
    max_width: Option<usize>,
) -> miette::Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    match output.format {
        OutputFormat::Table => {
            let display_batches: Vec<RecordBatch> = if !wide && let Some(mw) = max_width {
                batches
                    .iter()
                    .map(|b| truncate_batch(b, mw))
                    .collect::<miette::Result<Vec<_>>>()?
            } else {
                batches.to_vec()
            };

            let formatted = pretty_format_batches(&display_batches);
            let table_str = formatted.map_err(|e| miette::miette!("format error: {}", e))?;
            writeln!(output.writer, "{}", table_str).map_err(|e| miette::miette!("{}", e))?;
        }
        OutputFormat::Parquet => {
            let parquet_path = output.output_path().and_then(|p| {
                let ext = Path::new(p)
                    .extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("");
                match ext {
                    "parquet" | "parq" | "pq" => Some(p.to_string()),
                    _ => None,
                }
            });

            if let Some(out_path) = parquet_path {
                let schema = batches[0].schema();
                let out_file = File::create(&out_path)
                    .map_err(|e| miette::miette!("cannot create '{}': {}", out_path, e))?;
                let mut pq_writer = ArrowWriter::try_new(out_file, schema, None)
                    .map_err(|e| miette::miette!("cannot create Parquet writer: {}", e))?;
                for batch in batches {
                    pq_writer
                        .write(batch)
                        .map_err(|e| miette::miette!("Parquet write error: {}", e))?;
                }
                pq_writer
                    .close()
                    .map_err(|e| miette::miette!("Parquet close error: {}", e))?;
            } else {
                write_json_rows(batches, output)?;
            }
        }
        OutputFormat::Json => {
            write_json_rows(batches, output)?;
        }
        OutputFormat::Jsonl => {
            let buf = Vec::new();
            let mut writer = arrow::json::LineDelimitedWriter::new(buf);
            for batch in batches {
                writer
                    .write(batch)
                    .map_err(|e| miette::miette!("JSON write error: {}", e))?;
            }
            writer
                .finish()
                .map_err(|e| miette::miette!("JSON finish error: {}", e))?;
            let buf = writer.into_inner();
            output
                .writer
                .write_all(&buf)
                .map_err(|e| miette::miette!("{}", e))?;
        }
        OutputFormat::Csv | OutputFormat::Tsv => {
            let delimiter = if output.format == OutputFormat::Tsv {
                b'\t'
            } else {
                b','
            };
            let mut csv_writer = arrow::csv::WriterBuilder::new()
                .with_delimiter(delimiter)
                .with_header(true)
                .build(&mut output.writer);
            for batch in batches {
                csv_writer
                    .write(batch)
                    .map_err(|e| miette::miette!("CSV write error: {}", e))?;
            }
        }
    }

    Ok(())
}

fn write_json_rows(batches: &[RecordBatch], output: &mut OutputConfig) -> miette::Result<()> {
    let mut json_rows = Vec::new();
    let buf = Vec::new();
    let mut writer = arrow::json::LineDelimitedWriter::new(buf);
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| miette::miette!("JSON write error: {}", e))?;
    }
    writer
        .finish()
        .map_err(|e| miette::miette!("JSON finish error: {}", e))?;
    let buf = writer.into_inner();
    for line in buf.split(|&b| b == b'\n') {
        if !line.is_empty()
            && let Ok(val) = serde_json::from_slice::<serde_json::Value>(line)
        {
            json_rows.push(val);
        }
    }
    crate::output::json::write_json(&mut output.writer, &json_rows)?;
    Ok(())
}
