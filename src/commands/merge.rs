use crate::cli::MergeArgs;
use crate::input::resolve_inputs_with_config;
use crate::output::OutputConfig;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::concat_batches;
use arrow::datatypes::{Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

const FLUSH_THRESHOLD: usize = 8192;

pub fn execute(args: &MergeArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;
    let out_path = output
        .output_path()
        .ok_or_else(|| miette::miette!("merge requires an output file (-o <path>)"))?
        .to_string();
    let target = crate::input::cloud::resolve_output_path(&out_path)?;

    if sources.len() != 2 {
        return Err(miette::miette!(
            "merge requires exactly 2 files (got {})",
            sources.len()
        ));
    }

    let key_cols: Vec<&str> = args.key.split(',').map(|s| s.trim()).collect();

    // hash join — both sides in memory
    let left_batches = read_all_batches(&sources[0])?;
    let right_batches = read_all_batches(&sources[1])?;

    if left_batches.is_empty() || right_batches.is_empty() {
        return Err(miette::miette!("one or both files are empty"));
    }

    let left_schema = left_batches[0].schema();
    let right_schema = right_batches[0].schema();

    for key in &key_cols {
        if left_schema.column_with_name(key).is_none() {
            return Err(miette::miette!(
                "key column '{}' not found in {}",
                key,
                sources[0].display_name()
            ));
        }
        if right_schema.column_with_name(key).is_none() {
            return Err(miette::miette!(
                "key column '{}' not found in {}",
                key,
                sources[1].display_name()
            ));
        }
    }

    // output schema = left columns + unique right columns
    let mut output_fields: Vec<Field> = left_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    for field in right_schema.fields() {
        if !left_schema
            .fields()
            .iter()
            .any(|f| f.name() == field.name())
        {
            output_fields.push(field.as_ref().clone());
        }
    }
    let output_schema = Arc::new(Schema::new(output_fields));

    // hash join: index right side by key
    let mut right_map: HashMap<String, Vec<(usize, usize)>> = HashMap::new(); // key -> [(batch_idx, row_idx)]
    for (batch_idx, batch) in right_batches.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            let key = build_key(batch, &key_cols, row_idx)?;
            right_map.entry(key).or_default().push((batch_idx, row_idx));
        }
    }

    let out_file = File::create(target.local_path())
        .map_err(|e| miette::miette!("cannot create '{}': {}", out_path, e))?;
    let mut writer = ArrowWriter::try_new(out_file, output_schema.clone(), None)
        .map_err(|e| miette::miette!("cannot create writer: {}", e))?;

    let mut total_rows = 0usize;
    let mut pending_batches: Vec<RecordBatch> = Vec::new();

    for left_batch in &left_batches {
        for left_row in 0..left_batch.num_rows() {
            let key = build_key(left_batch, &key_cols, left_row)?;

            if let Some(right_matches) = right_map.get(&key) {
                for &(right_batch_idx, right_row) in right_matches {
                    let right_batch = &right_batches[right_batch_idx];
                    let merged = merge_rows(
                        left_batch,
                        left_row,
                        right_batch,
                        right_row,
                        &left_schema,
                        &right_schema,
                        &output_schema,
                    )?;
                    pending_batches.push(merged);
                    total_rows += 1;

                    if pending_batches.len() >= FLUSH_THRESHOLD {
                        let combined = concat_batches(&output_schema, &pending_batches)
                            .map_err(|e| miette::miette!("concat error: {}", e))?;
                        writer
                            .write(&combined)
                            .map_err(|e| miette::miette!("write error: {}", e))?;
                        pending_batches.clear();
                    }
                }
            } else if args.left {
                // left join: nulls for missing right columns
                let merged = merge_rows_left_only(
                    left_batch,
                    left_row,
                    &left_schema,
                    &right_schema,
                    &output_schema,
                )?;
                pending_batches.push(merged);
                total_rows += 1;

                if pending_batches.len() >= FLUSH_THRESHOLD {
                    let combined = concat_batches(&output_schema, &pending_batches)
                        .map_err(|e| miette::miette!("concat error: {}", e))?;
                    writer
                        .write(&combined)
                        .map_err(|e| miette::miette!("write error: {}", e))?;
                    pending_batches.clear();
                }
            }
        }
    }

    if !pending_batches.is_empty() {
        let combined = concat_batches(&output_schema, &pending_batches)
            .map_err(|e| miette::miette!("concat error: {}", e))?;
        writer
            .write(&combined)
            .map_err(|e| miette::miette!("write error: {}", e))?;
    }

    writer
        .close()
        .map_err(|e| miette::miette!("close error: {}", e))?;
    target.finalize(&output.cloud_config)?;

    eprintln!("Merged {} rows → {}", total_rows, out_path);

    Ok(())
}

fn read_all_batches(
    source: &crate::input::source::ResolvedSource,
) -> miette::Result<Vec<RecordBatch>> {
    let file = File::open(source.path())
        .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;
    let reader = builder
        .build()
        .map_err(|e| miette::miette!("cannot read: {}", e))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| miette::miette!("read error: {}", e))
}

fn build_key(batch: &RecordBatch, key_cols: &[&str], row: usize) -> miette::Result<String> {
    let mut parts = Vec::new();
    for col_name in key_cols {
        let col_idx = batch
            .schema()
            .index_of(col_name)
            .map_err(|_| miette::miette!("key column '{}' not found", col_name))?;
        let col = batch.column(col_idx);
        let val = arrow::util::display::ArrayFormatter::try_new(col.as_ref(), &Default::default())
            .map_err(|e| miette::miette!("format error: {}", e))?
            .value(row)
            .to_string();
        parts.push(val);
    }
    Ok(parts.join("|"))
}

fn merge_rows(
    left: &RecordBatch,
    left_row: usize,
    right: &RecordBatch,
    right_row: usize,
    left_schema: &Schema,
    right_schema: &Schema,
    output_schema: &Schema,
) -> miette::Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::new();

    for field in output_schema.fields() {
        if let Some((idx, _)) = left_schema.column_with_name(field.name()) {
            let col = left.column(idx);
            let sliced = col.slice(left_row, 1);
            columns.push(sliced);
        } else if let Some((idx, _)) = right_schema.column_with_name(field.name()) {
            let col = right.column(idx);
            let sliced = col.slice(right_row, 1);
            columns.push(sliced);
        }
    }

    RecordBatch::try_new(Arc::new(output_schema.clone()), columns)
        .map_err(|e| miette::miette!("merge error: {}", e))
}

fn merge_rows_left_only(
    left: &RecordBatch,
    left_row: usize,
    left_schema: &Schema,
    _right_schema: &Schema,
    output_schema: &Schema,
) -> miette::Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::new();

    for field in output_schema.fields() {
        if let Some((idx, _)) = left_schema.column_with_name(field.name()) {
            let col = left.column(idx);
            let sliced = col.slice(left_row, 1);
            columns.push(sliced);
        } else {
            // Right column not present: create null
            let null_arr = arrow::array::new_null_array(field.data_type(), 1);
            columns.push(null_arr);
        }
    }

    RecordBatch::try_new(Arc::new(output_schema.clone()), columns)
        .map_err(|e| miette::miette!("merge error: {}", e))
}
