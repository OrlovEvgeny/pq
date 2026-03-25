use crate::cli::RewriteArgs;
use crate::input::resolve_inputs_with_config;
use crate::output::OutputConfig;
use arrow::array::RecordBatch;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::fs::File;

pub fn execute(args: &RewriteArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;
    let out_path = output
        .output_path()
        .ok_or_else(|| miette::miette!("rewrite requires an output file (-o <path>)"))?
        .to_string();
    let target = crate::input::cloud::resolve_output_path(&out_path)?;

    if sources.len() != 1 {
        return Err(miette::miette!(
            "rewrite operates on a single file (got {})",
            sources.len()
        ));
    }

    let source = &sources[0];
    let file = File::open(source.path())
        .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

    let schema = builder.schema().clone();
    let reader = builder
        .build()
        .map_err(|e| miette::miette!("cannot read: {}", e))?;

    let mut props_builder = WriterProperties::builder();

    if let Some(ref compression) = args.compression {
        props_builder =
            props_builder.set_compression(super::util::parse_compression(compression, None));
    }

    if let Some(rg_size) = args.row_group_size {
        props_builder = props_builder.set_max_row_group_size(rg_size);
    }

    if args.dictionary {
        props_builder = props_builder.set_dictionary_enabled(true);
    }

    if let Some(version) = args.parquet_version {
        match version {
            1 => {
                props_builder = props_builder.set_writer_version(WriterVersion::PARQUET_1_0);
            }
            2 => {
                props_builder = props_builder.set_writer_version(WriterVersion::PARQUET_2_0);
            }
            other => {
                return Err(miette::miette!(
                    "unsupported Parquet version: {} (expected 1 or 2)",
                    other
                ));
            }
        }
    }

    let mut kv_meta = Vec::new();
    if !args.strip_metadata {
        // keep existing kv, minus stripped keys
        let orig_meta = crate::parquet_ext::metadata::read_metadata(source.path())?;
        if let Some(kvs) = orig_meta.file_metadata().key_value_metadata() {
            let strip_keys: Vec<&str> = args
                .strip_kv
                .as_deref()
                .map(|s| s.split(',').map(|k| k.trim()).collect())
                .unwrap_or_default();

            for kv in kvs {
                if !strip_keys.contains(&kv.key.as_str()) {
                    kv_meta.push(parquet::format::KeyValue {
                        key: kv.key.clone(),
                        value: kv.value.clone(),
                    });
                }
            }
        }
    }

    if let Some(ref set_kv) = args.set_kv {
        for pair in set_kv.split(',') {
            if let Some((key, value)) = pair.split_once('=') {
                kv_meta.push(parquet::format::KeyValue {
                    key: key.trim().to_string(),
                    value: Some(value.trim().to_string()),
                });
            }
        }
    }

    if !kv_meta.is_empty() {
        props_builder = props_builder.set_key_value_metadata(Some(kv_meta));
    }

    let props = props_builder.build();

    // need all batches in memory for sorting
    let spinner = if output.is_tty && !output.quiet {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner} Rewriting...")
                .unwrap(),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        Some(pb)
    } else {
        None
    };

    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| miette::miette!("read error: {}", e))?;
        batches.push(batch);
    }

    if let Some(ref sort_by) = args.sort_by {
        batches = super::util::sort_batches(&batches, &schema, sort_by)?;
    }

    let out_file = File::create(target.local_path())
        .map_err(|e| miette::miette!("cannot create '{}': {}", out_path, e))?;
    let mut writer = ArrowWriter::try_new(out_file, schema, Some(props))
        .map_err(|e| miette::miette!("cannot create writer: {}", e))?;

    for batch in &batches {
        writer
            .write(batch)
            .map_err(|e| miette::miette!("write error: {}", e))?;
    }

    writer
        .close()
        .map_err(|e| miette::miette!("close error: {}", e))?;
    target.finalize(&output.cloud_config)?;

    if let Some(pb) = spinner {
        pb.finish_and_clear();
    }
    eprintln!("Rewrote {} → {}", source.display_name(), out_path);

    Ok(())
}
