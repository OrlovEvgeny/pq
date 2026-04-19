use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use std::fs::File;
use std::io::Write;
use std::path::Path;

/// Parse a compression string into a `Compression` enum, optionally applying a
/// compression level when one is provided.
pub fn parse_compression(s: &str, level: Option<i32>) -> Compression {
    match s.to_lowercase().as_str() {
        "snappy" => Compression::SNAPPY,
        "gzip" => {
            if let Some(lvl) = level {
                Compression::GZIP(GzipLevel::try_new(lvl as u32).unwrap_or_default())
            } else {
                Compression::GZIP(Default::default())
            }
        }
        "lz4" => Compression::LZ4,
        "zstd" => {
            if let Some(lvl) = level {
                Compression::ZSTD(ZstdLevel::try_new(lvl).unwrap_or_default())
            } else {
                Compression::ZSTD(Default::default())
            }
        }
        "brotli" => {
            if let Some(lvl) = level {
                Compression::BROTLI(BrotliLevel::try_new(lvl as u32).unwrap_or_default())
            } else {
                Compression::BROTLI(Default::default())
            }
        }
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        _ => {
            // Default to zstd, still honouring a level if provided
            if let Some(lvl) = level {
                Compression::ZSTD(ZstdLevel::try_new(lvl).unwrap_or_default())
            } else {
                Compression::ZSTD(Default::default())
            }
        }
    }
}

/// Sort record batches by comma-separated column names.
/// Concatenates all batches, sorts globally, returns a single sorted batch.
pub fn sort_batches(
    batches: &[RecordBatch],
    schema: &arrow::datatypes::SchemaRef,
    sort_by: &str,
) -> miette::Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let col_names: Vec<&str> = sort_by.split(',').map(|s| s.trim()).collect();

    let combined = arrow::compute::concat_batches(schema, batches)
        .map_err(|e| miette::miette!("concat error during sort: {}", e))?;

    let sort_columns: Vec<SortColumn> = col_names
        .iter()
        .map(|name| {
            let idx = schema
                .index_of(name)
                .map_err(|_| miette::miette!("sort column '{}' not found in schema", name))?;
            Ok(SortColumn {
                values: combined.column(idx).clone(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            })
        })
        .collect::<miette::Result<Vec<_>>>()?;

    let indices = lexsort_to_indices(&sort_columns, None)
        .map_err(|e| miette::miette!("sort error: {}", e))?;

    let sorted_columns: Vec<arrow::array::ArrayRef> = combined
        .columns()
        .iter()
        .map(|col| take(col, &indices, None).map_err(|e| miette::miette!("take error: {}", e)))
        .collect::<miette::Result<Vec<_>>>()?;

    let sorted_batch = RecordBatch::try_new(schema.clone(), sorted_columns)
        .map_err(|e| miette::miette!("batch reconstruction error: {}", e))?;

    Ok(vec![sorted_batch])
}

/// Format a byte size as a human-readable string, or exact bytes if `exact` is true.
pub fn format_size(bytes: i64, exact: bool) -> String {
    if exact {
        bytes.to_string()
    } else {
        bytesize::ByteSize(bytes.max(0) as u64).to_string()
    }
}

pub fn write_record_batches(
    batches: &[RecordBatch],
    output: &mut crate::output::OutputConfig,
) -> miette::Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    match output.format {
        crate::output::OutputFormat::Table => {
            let table_str = render_batches_as_table(batches, &output.theme)?;
            writeln!(output.writer, "{}", table_str).map_err(|e| miette::miette!("{}", e))?;
        }
        crate::output::OutputFormat::Parquet => {
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
                let target = crate::input::cloud::resolve_output_path(&out_path)?;
                let schema = batches[0].schema();
                let out_file = File::create(target.local_path())
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
                target.finalize(&output.cloud_config)?;
            } else {
                write_json_rows(batches, output)?;
            }
        }
        crate::output::OutputFormat::Json => {
            write_json_rows(batches, output)?;
        }
        crate::output::OutputFormat::Jsonl => {
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
        crate::output::OutputFormat::Csv | crate::output::OutputFormat::Tsv => {
            let delimiter = if output.format == crate::output::OutputFormat::Tsv {
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

pub fn stream_record_batches(
    stream: &mut SendableRecordBatchStream,
    rt: &tokio::runtime::Runtime,
    output: &mut crate::output::OutputConfig,
) -> miette::Result<()> {
    match output.format {
        crate::output::OutputFormat::Jsonl => {
            let mut writer = arrow::json::LineDelimitedWriter::new(&mut output.writer);
            rt.block_on(async {
                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(|e| miette::miette!("SQL execution error: {}", e))?;
                    writer
                        .write(&batch)
                        .map_err(|e| miette::miette!("JSON write error: {}", e))?;
                }
                writer
                    .finish()
                    .map_err(|e| miette::miette!("JSON finish error: {}", e))
            })?;
        }
        crate::output::OutputFormat::Csv | crate::output::OutputFormat::Tsv => {
            let delimiter = if output.format == crate::output::OutputFormat::Tsv {
                b'\t'
            } else {
                b','
            };
            let mut csv_writer = arrow::csv::WriterBuilder::new()
                .with_delimiter(delimiter)
                .with_header(true)
                .build(&mut output.writer);
            rt.block_on(async {
                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(|e| miette::miette!("SQL execution error: {}", e))?;
                    csv_writer
                        .write(&batch)
                        .map_err(|e| miette::miette!("CSV write error: {}", e))?;
                }
                Ok::<(), miette::Report>(())
            })?;
        }
        crate::output::OutputFormat::Parquet => {
            let out_path = output
                .output_path()
                .ok_or_else(|| miette::miette!("Parquet SQL output requires -o <path.parquet>"))?;
            let target = crate::input::cloud::resolve_output_path(out_path)?;
            let mut pq_writer: Option<ArrowWriter<File>> = None;

            rt.block_on(async {
                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(|e| miette::miette!("SQL execution error: {}", e))?;
                    if pq_writer.is_none() {
                        let file = File::create(target.local_path())
                            .map_err(|e| miette::miette!("cannot create '{}': {}", out_path, e))?;
                        pq_writer =
                            Some(ArrowWriter::try_new(file, batch.schema(), None).map_err(
                                |e| miette::miette!("cannot create Parquet writer: {}", e),
                            )?);
                    }
                    pq_writer
                        .as_mut()
                        .expect("writer initialized")
                        .write(&batch)
                        .map_err(|e| miette::miette!("Parquet write error: {}", e))?;
                }
                Ok::<(), miette::Report>(())
            })?;

            if let Some(writer) = pq_writer {
                writer
                    .close()
                    .map_err(|e| miette::miette!("Parquet close error: {}", e))?;
            }
            target.finalize(&output.cloud_config)?;
        }
        crate::output::OutputFormat::Json | crate::output::OutputFormat::Table => {
            let batches = rt.block_on(async {
                let mut batches = Vec::new();
                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(|e| miette::miette!("SQL execution error: {}", e))?;
                    batches.push(batch);
                }
                Ok::<Vec<RecordBatch>, miette::Report>(batches)
            })?;
            write_record_batches(&batches, output)?;
        }
    }

    Ok(())
}

fn render_batches_as_table(
    batches: &[RecordBatch],
    theme: &crate::output::theme::Theme,
) -> miette::Result<String> {
    if batches.is_empty() {
        return Ok(String::new());
    }

    let schema = batches[0].schema();
    let headers: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let total_rows = batches.iter().map(RecordBatch::num_rows).sum();
    let mut rows = Vec::with_capacity(total_rows);

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row = batch
                .columns()
                .iter()
                .map(|column| {
                    arrow::util::display::array_value_to_string(column.as_ref(), row_idx)
                        .map_err(|e| miette::miette!("format error: {}", e))
                })
                .collect::<miette::Result<Vec<_>>>()?;
            rows.push(row);
        }
    }

    Ok(crate::output::table::data_table(&headers, &rows, theme))
}

fn write_json_rows(
    batches: &[RecordBatch],
    output: &mut crate::output::OutputConfig,
) -> miette::Result<()> {
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
