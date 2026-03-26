use crate::cli::ConvertArgs;
use crate::output::OutputConfig;
use arrow::csv as arrow_csv;
use arrow::datatypes::Schema;
use arrow::ipc;
use arrow::json as arrow_json;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

pub fn execute(args: &ConvertArgs, output: &mut OutputConfig) -> miette::Result<()> {
    if args.files.is_empty() {
        return Err(miette::miette!("no input files specified"));
    }

    let out_path = output
        .output_path()
        .ok_or_else(|| miette::miette!("convert requires an output file (-o <path>)"))?
        .to_string();

    let is_cloud = crate::input::cloud::is_cloud_url(&out_path);
    let out_is_dir = !is_cloud
        && (Path::new(&out_path).is_dir()
            || (args.files.len() > 1 && !Path::new(&out_path).exists()));

    if args.files.len() > 1 || out_is_dir {
        if !is_cloud && !Path::new(&out_path).exists() {
            std::fs::create_dir_all(&out_path)
                .map_err(|e| miette::miette!("cannot create directory '{}': {}", out_path, e))?;
        }

        for input in &args.files {
            let in_ext = Path::new(input)
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("");

            let target_ext = infer_target_extension(in_ext, &out_path)?;

            let in_stem = Path::new(input)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("output");

            let file_out_path = if is_cloud {
                // For cloud URLs, append filename to the cloud prefix
                let sep = if out_path.ends_with('/') { "" } else { "/" };
                format!("{}{}{}.{}", out_path, sep, in_stem, target_ext)
            } else {
                Path::new(&out_path)
                    .join(format!("{}.{}", in_stem, target_ext))
                    .to_string_lossy()
                    .to_string()
            };

            let target = crate::input::cloud::resolve_output_path(&file_out_path)?;
            convert_single(input, target.local_path(), args)?;
            target.finalize(&output.cloud_config)?;
            eprintln!("Converted {} → {}", input, file_out_path);
        }
    } else {
        let target = crate::input::cloud::resolve_output_path(&out_path)?;
        let input = &args.files[0];
        let spinner = if output.is_tty && !output.quiet {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner} Converting {msg}...")
                    .unwrap(),
            );
            pb.set_message(input.clone());
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
            Some(pb)
        } else {
            None
        };
        convert_single(input, target.local_path(), args)?;
        target.finalize(&output.cloud_config)?;
        if let Some(pb) = spinner {
            pb.finish_and_clear();
        }
        eprintln!("Converted {} → {}", input, out_path);
    }

    Ok(())
}

/// Infer the target extension when writing into a directory.
/// We look at the input extension and pick the "opposite" format.
fn infer_target_extension(in_ext: &str, out_dir: &str) -> miette::Result<String> {
    // opposite format: text→parquet, parquet→json
    match in_ext {
        "csv" | "tsv" | "json" | "jsonl" | "ndjson" => Ok("parquet".to_string()),
        "arrow" | "ipc" => Ok("parquet".to_string()),
        "parquet" | "parq" | "pq" => Ok("json".to_string()),
        _ => {
            // Try to infer from the directory name if it has a dot
            let dir_ext = Path::new(out_dir)
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("");
            if !dir_ext.is_empty() {
                Ok(dir_ext.to_string())
            } else {
                Err(miette::miette!(
                    "cannot infer target format for '{}' extension",
                    in_ext
                ))
            }
        }
    }
}

fn convert_single(input: &str, out_path: &str, args: &ConvertArgs) -> miette::Result<()> {
    let in_ext = Path::new(input)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let out_ext = Path::new(out_path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    match (in_ext, out_ext) {
        ("csv" | "tsv", "parquet" | "parq" | "pq") => {
            csv_to_parquet(input, out_path, args)?;
        }
        ("json" | "jsonl" | "ndjson", "parquet" | "parq" | "pq") => {
            json_to_parquet(input, out_path, args)?;
        }
        ("parquet" | "parq" | "pq", "csv") => {
            parquet_to_csv(input, out_path, b',', args)?;
        }
        ("parquet" | "parq" | "pq", "tsv") => {
            parquet_to_csv(input, out_path, b'\t', args)?;
        }
        ("parquet" | "parq" | "pq", "json" | "jsonl" | "ndjson") => {
            parquet_to_json(input, out_path)?;
        }
        ("parquet" | "parq" | "pq", "parquet" | "parq" | "pq") => {
            parquet_to_parquet(input, out_path, args)?;
        }
        ("arrow" | "ipc", "parquet" | "parq" | "pq") => {
            arrow_ipc_to_parquet(input, out_path, args)?;
        }
        ("parquet" | "parq" | "pq", "arrow" | "ipc") => {
            parquet_to_arrow_ipc(input, out_path)?;
        }
        _ => {
            return Err(miette::miette!(
                "unsupported conversion: {} → {}\nSupported: csv/json/arrow → parquet, parquet → csv/json/parquet/arrow",
                in_ext,
                out_ext
            ));
        }
    }

    Ok(())
}

fn resolve_compression(s: Option<&str>, level: Option<i32>) -> Compression {
    match s {
        Some(codec) => super::util::parse_compression(codec, level),
        None => super::util::parse_compression("zstd", level),
    }
}

fn csv_to_parquet(input: &str, output: &str, args: &ConvertArgs) -> miette::Result<()> {
    let file = File::open(input).map_err(|e| miette::miette!("cannot open '{}': {}", input, e))?;
    let reader = BufReader::new(file);

    let delimiter = args.delimiter.map(|c| c as u8);
    let infer_rows = args.infer_schema_rows.unwrap_or(1000);

    let has_header = !args.no_header;

    // TODO: --null-values not supported by arrow csv reader yet

    let format = arrow_csv::reader::Format::default().with_header(has_header);
    let format = if let Some(d) = delimiter {
        format.with_delimiter(d)
    } else {
        format
    };

    let schema = if let Some(ref schema_path) = args.schema {
        Arc::new(load_external_schema(schema_path)?)
    } else {
        let (inferred, _) = format
            .infer_schema(
                BufReader::new(File::open(input).map_err(|e| miette::miette!("{}", e))?),
                Some(infer_rows),
            )
            .map_err(|e| miette::miette!("schema inference failed: {}", e))?;
        Arc::new(inferred)
    };

    let mut csv_builder = arrow_csv::ReaderBuilder::new(schema.clone()).with_format(format);
    if let Some(batch_size) = args.row_group_size {
        csv_builder = csv_builder.with_batch_size(batch_size);
    }

    let csv_reader = csv_builder
        .build(reader)
        .map_err(|e| miette::miette!("CSV read error: {}", e))?;

    let out_file =
        File::create(output).map_err(|e| miette::miette!("cannot create '{}': {}", output, e))?;

    let compression = resolve_compression(args.compression.as_deref(), args.compression_level);
    let mut props_builder =
        parquet::file::properties::WriterProperties::builder().set_compression(compression);

    if args.dictionary {
        props_builder = props_builder.set_dictionary_enabled(true);
    }

    let props = props_builder.build();

    let mut writer = ArrowWriter::try_new(out_file, schema, Some(props))
        .map_err(|e| miette::miette!("cannot create Parquet writer: {}", e))?;

    for batch_result in csv_reader {
        let batch = batch_result.map_err(|e| miette::miette!("CSV read error: {}", e))?;
        writer
            .write(&batch)
            .map_err(|e| miette::miette!("Parquet write error: {}", e))?;
    }

    writer
        .close()
        .map_err(|e| miette::miette!("Parquet close error: {}", e))?;

    Ok(())
}

fn json_to_parquet(input: &str, output: &str, args: &ConvertArgs) -> miette::Result<()> {
    let file = File::open(input).map_err(|e| miette::miette!("cannot open '{}': {}", input, e))?;
    let reader = BufReader::new(file);

    let infer_rows = args.infer_schema_rows.unwrap_or(1000);

    let schema = if let Some(ref schema_path) = args.schema {
        Arc::new(load_external_schema(schema_path)?)
    } else {
        let (inferred_schema, _) = arrow_json::reader::infer_json_schema(
            BufReader::new(File::open(input).map_err(|e| miette::miette!("{}", e))?),
            Some(infer_rows),
        )
        .map_err(|e| miette::miette!("JSON schema inference failed: {}", e))?;
        Arc::new(inferred_schema)
    };

    let json_reader = arrow_json::ReaderBuilder::new(schema.clone())
        .build(reader)
        .map_err(|e| miette::miette!("JSON read error: {}", e))?;

    let out_file =
        File::create(output).map_err(|e| miette::miette!("cannot create '{}': {}", output, e))?;

    let compression = resolve_compression(args.compression.as_deref(), args.compression_level);
    let mut props_builder =
        parquet::file::properties::WriterProperties::builder().set_compression(compression);

    if args.dictionary {
        props_builder = props_builder.set_dictionary_enabled(true);
    }

    let props = props_builder.build();

    let mut writer = ArrowWriter::try_new(out_file, schema, Some(props))
        .map_err(|e| miette::miette!("cannot create Parquet writer: {}", e))?;

    for batch_result in json_reader {
        let batch = batch_result.map_err(|e| miette::miette!("JSON read error: {}", e))?;
        writer
            .write(&batch)
            .map_err(|e| miette::miette!("Parquet write error: {}", e))?;
    }

    writer
        .close()
        .map_err(|e| miette::miette!("Parquet close error: {}", e))?;

    Ok(())
}

fn parquet_to_csv(
    input: &str,
    output: &str,
    delimiter: u8,
    args: &ConvertArgs,
) -> miette::Result<()> {
    let file = File::open(input).map_err(|e| miette::miette!("cannot open '{}': {}", input, e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| miette::miette!("cannot read '{}': {}", input, e))?;
    let reader = builder
        .build()
        .map_err(|e| miette::miette!("cannot read '{}': {}", input, e))?;

    let out_file =
        File::create(output).map_err(|e| miette::miette!("cannot create '{}': {}", output, e))?;

    let has_header = !args.no_header;

    let mut csv_writer = arrow_csv::WriterBuilder::new()
        .with_delimiter(delimiter)
        .with_header(has_header)
        .build(out_file);

    for batch_result in reader {
        let batch = batch_result.map_err(|e| miette::miette!("read error: {}", e))?;
        csv_writer
            .write(&batch)
            .map_err(|e| miette::miette!("CSV write error: {}", e))?;
    }

    Ok(())
}

fn parquet_to_json(input: &str, output: &str) -> miette::Result<()> {
    let file = File::open(input).map_err(|e| miette::miette!("cannot open '{}': {}", input, e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| miette::miette!("cannot read '{}': {}", input, e))?;
    let reader = builder
        .build()
        .map_err(|e| miette::miette!("cannot read '{}': {}", input, e))?;

    let out_file =
        File::create(output).map_err(|e| miette::miette!("cannot create '{}': {}", output, e))?;
    let mut json_writer = arrow_json::LineDelimitedWriter::new(out_file);

    for batch_result in reader {
        let batch = batch_result.map_err(|e| miette::miette!("read error: {}", e))?;
        json_writer
            .write(&batch)
            .map_err(|e| miette::miette!("JSON write error: {}", e))?;
    }

    json_writer
        .finish()
        .map_err(|e| miette::miette!("JSON finish error: {}", e))?;

    Ok(())
}

fn parquet_to_parquet(input: &str, output: &str, args: &ConvertArgs) -> miette::Result<()> {
    let file = File::open(input).map_err(|e| miette::miette!("cannot open '{}': {}", input, e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| miette::miette!("cannot read '{}': {}", input, e))?;
    let schema = builder.schema().clone();
    let reader = builder
        .build()
        .map_err(|e| miette::miette!("cannot read '{}': {}", input, e))?;

    let out_file =
        File::create(output).map_err(|e| miette::miette!("cannot create '{}': {}", output, e))?;

    let compression = resolve_compression(args.compression.as_deref(), args.compression_level);
    let mut props_builder =
        parquet::file::properties::WriterProperties::builder().set_compression(compression);
    if let Some(rg_size) = args.row_group_size {
        props_builder = props_builder.set_max_row_group_size(rg_size);
    }
    if args.dictionary {
        props_builder = props_builder.set_dictionary_enabled(true);
    }
    let props = props_builder.build();

    let mut writer = ArrowWriter::try_new(out_file, schema, Some(props))
        .map_err(|e| miette::miette!("cannot create Parquet writer: {}", e))?;

    for batch_result in reader {
        let batch = batch_result.map_err(|e| miette::miette!("read error: {}", e))?;
        writer
            .write(&batch)
            .map_err(|e| miette::miette!("Parquet write error: {}", e))?;
    }

    writer
        .close()
        .map_err(|e| miette::miette!("Parquet close error: {}", e))?;

    Ok(())
}

fn arrow_ipc_to_parquet(input: &str, output: &str, args: &ConvertArgs) -> miette::Result<()> {
    let file = File::open(input).map_err(|e| miette::miette!("cannot open '{}': {}", input, e))?;
    let ipc_reader = ipc::reader::FileReader::try_new(file, None)
        .map_err(|e| miette::miette!("cannot read Arrow IPC '{}': {}", input, e))?;

    let schema = ipc_reader.schema();

    let compression = resolve_compression(args.compression.as_deref(), args.compression_level);
    let mut props_builder =
        parquet::file::properties::WriterProperties::builder().set_compression(compression);
    if let Some(rg_size) = args.row_group_size {
        props_builder = props_builder.set_max_row_group_size(rg_size);
    }
    if args.dictionary {
        props_builder = props_builder.set_dictionary_enabled(true);
    }
    let props = props_builder.build();

    let out_file =
        File::create(output).map_err(|e| miette::miette!("cannot create '{}': {}", output, e))?;
    let mut writer = ArrowWriter::try_new(out_file, schema.clone(), Some(props))
        .map_err(|e| miette::miette!("cannot create Parquet writer: {}", e))?;

    for batch_result in ipc_reader {
        let batch = batch_result.map_err(|e| miette::miette!("Arrow IPC read error: {}", e))?;
        writer
            .write(&batch)
            .map_err(|e| miette::miette!("Parquet write error: {}", e))?;
    }

    writer
        .close()
        .map_err(|e| miette::miette!("Parquet close error: {}", e))?;

    Ok(())
}

fn parquet_to_arrow_ipc(input: &str, output: &str) -> miette::Result<()> {
    let file = File::open(input).map_err(|e| miette::miette!("cannot open '{}': {}", input, e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| miette::miette!("cannot read '{}': {}", input, e))?;
    let schema = builder.schema().clone();
    let reader = builder
        .build()
        .map_err(|e| miette::miette!("cannot read '{}': {}", input, e))?;

    let out_file =
        File::create(output).map_err(|e| miette::miette!("cannot create '{}': {}", output, e))?;
    let mut ipc_writer = ipc::writer::FileWriter::try_new(out_file, &schema)
        .map_err(|e| miette::miette!("cannot create Arrow IPC writer: {}", e))?;

    for batch_result in reader {
        let batch = batch_result.map_err(|e| miette::miette!("read error: {}", e))?;
        ipc_writer
            .write(&batch)
            .map_err(|e| miette::miette!("Arrow IPC write error: {}", e))?;
    }

    ipc_writer
        .finish()
        .map_err(|e| miette::miette!("Arrow IPC finish error: {}", e))?;

    Ok(())
}

fn load_external_schema(schema_path: &str) -> miette::Result<Schema> {
    if schema_path.ends_with(".parquet")
        || schema_path.ends_with(".parq")
        || schema_path.ends_with(".pq")
    {
        let file = File::open(schema_path)
            .map_err(|e| miette::miette!("cannot open schema file '{}': {}", schema_path, e))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| miette::miette!("cannot read schema from '{}': {}", schema_path, e))?;
        Ok(builder.schema().as_ref().clone())
    } else {
        let contents = std::fs::read_to_string(schema_path)
            .map_err(|e| miette::miette!("cannot read schema file '{}': {}", schema_path, e))?;
        let schema: Schema = serde_json::from_str(&contents).map_err(|e| {
            miette::miette!("cannot parse schema JSON from '{}': {}", schema_path, e)
        })?;
        Ok(schema)
    }
}
