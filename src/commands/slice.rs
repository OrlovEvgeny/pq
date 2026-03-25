use crate::cli::SliceArgs;
use crate::input::column_selector::resolve_projection;
use crate::input::resolve_inputs_with_config;
use crate::output::OutputConfig;
use arrow::array::{Array, UInt32Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::statistics::Statistics;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

pub fn execute(args: &SliceArgs, output: &mut OutputConfig) -> miette::Result<()> {
    if let Some(n) = args.split {
        return execute_split_n(args, output, n);
    }

    if args.split_row_groups {
        return execute_split_row_groups(args, output);
    }

    if let Some(ref col_name) = args.split_by {
        return execute_split_by(args, output, col_name);
    }

    let sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;

    for source in &sources {
        let file = File::open(source.path())
            .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        if let Some(ref prune_expr) = args.prune {
            let pruned_indices = compute_pruned_row_groups(builder.metadata(), prune_expr)?;
            builder = builder.with_row_groups(pruned_indices);
        }

        if let Some(indices) = resolve_projection(args.columns.as_deref(), builder.schema()) {
            let mask = parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), indices);
            builder = builder.with_projection(mask);
        }

        if let Some(ref rows) = args.rows {
            let parts: Vec<&str> = rows.split(':').collect();
            if parts.len() == 2 {
                let start: usize = parts[0].parse().unwrap_or(0);
                let end: usize = parts[1]
                    .parse()
                    .map_err(|_| miette::miette!("invalid row range: {}", rows))?;
                builder = builder.with_offset(start);
                builder = builder.with_limit(end.saturating_sub(start));
            }
        }

        if let Some(ref rg_str) = args.row_groups {
            let rg_indices: Vec<usize> = parse_row_group_selection(rg_str)?;
            builder = builder.with_row_groups(rg_indices);
        }

        let schema = builder.schema().clone();
        let reader = builder
            .build()
            .map_err(|e| miette::miette!("cannot build reader: {}", e))?;

        if let Some(out_path) = output_parquet_path(output) {
            let target = crate::input::cloud::resolve_output_path(&out_path)?;
            let out_file = File::create(target.local_path())
                .map_err(|e| miette::miette!("cannot create '{}': {}", out_path, e))?;
            let mut writer = ArrowWriter::try_new(out_file, schema, None)
                .map_err(|e| miette::miette!("cannot create writer: {}", e))?;

            for batch_result in reader {
                let batch =
                    batch_result.map_err(|e| miette::miette!("error reading data: {}", e))?;
                writer
                    .write(&batch)
                    .map_err(|e| miette::miette!("error writing: {}", e))?;
            }

            writer
                .close()
                .map_err(|e| miette::miette!("error closing writer: {}", e))?;
            target.finalize(&output.cloud_config)?;
        } else {
            let batches: Vec<_> = reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| miette::miette!("error reading data: {}", e))?;
            crate::commands::head::write_batches(&batches, output, false, None)?;
        }
    }

    Ok(())
}

/// --split N: Read all rows, then write N approximately equal output files.
fn execute_split_n(args: &SliceArgs, output: &OutputConfig, n: usize) -> miette::Result<()> {
    let out_template = output
        .output_path()
        .ok_or_else(|| miette::miette!("--split requires -o <path> flag"))?
        .to_string();

    if n == 0 {
        return Err(miette::miette!("--split N must be >= 1"));
    }

    let sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;

    let mut all_batches = Vec::new();
    let mut schema = None;

    for source in &sources {
        let file = File::open(source.path())
            .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        if let Some(indices) = resolve_projection(args.columns.as_deref(), builder.schema()) {
            let mask = parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), indices);
            builder = builder.with_projection(mask);
        }

        if let Some(ref rows) = args.rows {
            let parts: Vec<&str> = rows.split(':').collect();
            if parts.len() == 2 {
                let start: usize = parts[0].parse().unwrap_or(0);
                let end: usize = parts[1]
                    .parse()
                    .map_err(|_| miette::miette!("invalid row range: {}", rows))?;
                builder = builder.with_offset(start);
                builder = builder.with_limit(end.saturating_sub(start));
            }
        }

        if let Some(ref rg_str) = args.row_groups {
            let rg_indices: Vec<usize> = parse_row_group_selection(rg_str)?;
            builder = builder.with_row_groups(rg_indices);
        }

        if schema.is_none() {
            schema = Some(builder.schema().clone());
        }

        let reader = builder
            .build()
            .map_err(|e| miette::miette!("cannot build reader: {}", e))?;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| miette::miette!("error reading data: {}", e))?;
            all_batches.push(batch);
        }
    }

    let schema = schema.ok_or_else(|| miette::miette!("no data to split"))?;

    // Count total rows
    let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return Err(miette::miette!("no rows to split"));
    }

    let rows_per_split = total_rows.div_ceil(n);

    // walk batches, distributing rows across split files
    let mut split_idx: usize = 0;
    let mut rows_in_current: usize = 0;
    let mut current_writer: Option<ArrowWriter<File>> = None;

    let mut current_target: Option<crate::input::cloud::OutputTarget> = None;

    for batch in &all_batches {
        let mut offset = 0;
        while offset < batch.num_rows() {
            if current_writer.is_none() {
                if split_idx >= n {
                    break;
                }
                let out_path = expand_template(&out_template, split_idx);
                let target = crate::input::cloud::resolve_output_path(&out_path)?;
                let out_file = File::create(target.local_path())
                    .map_err(|e| miette::miette!("cannot create '{}': {}", out_path, e))?;
                let writer = ArrowWriter::try_new(out_file, schema.clone(), None)
                    .map_err(|e| miette::miette!("cannot create writer: {}", e))?;
                current_writer = Some(writer);
                current_target = Some(target);
                rows_in_current = 0;
            }

            let remaining_in_split = rows_per_split - rows_in_current;
            let remaining_in_batch = batch.num_rows() - offset;
            let take = remaining_in_split.min(remaining_in_batch);

            let sliced = batch.slice(offset, take);
            current_writer
                .as_mut()
                .unwrap()
                .write(&sliced)
                .map_err(|e| miette::miette!("error writing: {}", e))?;

            offset += take;
            rows_in_current += take;

            if rows_in_current >= rows_per_split {
                current_writer
                    .take()
                    .unwrap()
                    .close()
                    .map_err(|e| miette::miette!("error closing writer: {}", e))?;
                if let Some(target) = current_target.take() {
                    target.finalize(&output.cloud_config)?;
                }
                split_idx += 1;
            }
        }
    }

    if let Some(writer) = current_writer.take() {
        writer
            .close()
            .map_err(|e| miette::miette!("error closing writer: {}", e))?;
        if let Some(target) = current_target.take() {
            target.finalize(&output.cloud_config)?;
        }
        split_idx += 1;
    }

    eprintln!("Split {} rows into {} files", total_rows, split_idx.min(n));

    Ok(())
}

/// --split-row-groups: Write one output file per row group.
fn execute_split_row_groups(args: &SliceArgs, output: &OutputConfig) -> miette::Result<()> {
    let out_template = output
        .output_path()
        .ok_or_else(|| {
            miette::miette!("--split-row-groups requires -o <path> flag with {{n}} template")
        })?
        .to_string();

    let sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;
    let mut file_idx: usize = 0;

    for source in &sources {
        let file = File::open(source.path())
            .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;

        let meta_builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        let num_row_groups = meta_builder.metadata().num_row_groups();

        for rg_idx in 0..num_row_groups {
            // builder consumes file, so re-open each time
            let file = File::open(source.path())
                .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;

            let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

            // Apply column projection
            if let Some(indices) = resolve_projection(args.columns.as_deref(), builder.schema()) {
                let mask =
                    parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), indices);
                builder = builder.with_projection(mask);
            }

            builder = builder.with_row_groups(vec![rg_idx]);

            let schema = builder.schema().clone();
            let reader = builder
                .build()
                .map_err(|e| miette::miette!("cannot build reader: {}", e))?;

            let out_path = expand_template(&out_template, file_idx);
            let target = crate::input::cloud::resolve_output_path(&out_path)?;
            let out_file = File::create(target.local_path())
                .map_err(|e| miette::miette!("cannot create '{}': {}", out_path, e))?;
            let mut writer = ArrowWriter::try_new(out_file, schema, None)
                .map_err(|e| miette::miette!("cannot create writer: {}", e))?;

            for batch_result in reader {
                let batch =
                    batch_result.map_err(|e| miette::miette!("error reading data: {}", e))?;
                writer
                    .write(&batch)
                    .map_err(|e| miette::miette!("error writing: {}", e))?;
            }

            writer
                .close()
                .map_err(|e| miette::miette!("error closing writer: {}", e))?;
            target.finalize(&output.cloud_config)?;

            file_idx += 1;
        }
    }

    eprintln!("Split into {} files (one per row group)", file_idx);

    Ok(())
}

/// Check if the output path ends in a parquet extension.
fn output_parquet_path(output: &OutputConfig) -> Option<String> {
    let path = output.output_path()?;
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    match ext {
        "parquet" | "parq" | "pq" => Some(path.to_string()),
        _ => None,
    }
}

/// Expand a template path with a file number.
/// If the path contains `{n}`, replace it with the number.
/// Otherwise, insert `_N` before the extension.
fn expand_template(template: &str, idx: usize) -> String {
    if template.contains("{n}") {
        return template.replace("{n}", &idx.to_string());
    }

    // Insert _N before the extension
    let path = Path::new(template);
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("parquet");
    let parent = path.parent().unwrap_or_else(|| Path::new(""));

    let new_name = format!("{}_{}.{}", stem, idx, ext);
    parent.join(new_name).to_string_lossy().to_string()
}

/// Parse a prune expression like "column >= value" and return the operator and parts.
enum PruneOp {
    Ge,
    Le,
    Gt,
    Lt,
}

fn parse_prune_expr(expr: &str) -> miette::Result<(String, PruneOp, String)> {
    // Try operators in order of length (>= before >, <= before <)
    for (op_str, op) in &[
        (">=", PruneOp::Ge),
        ("<=", PruneOp::Le),
        (">", PruneOp::Gt),
        ("<", PruneOp::Lt),
    ] {
        if let Some(pos) = expr.find(op_str) {
            let col = expr[..pos].trim().to_string();
            let val = expr[pos + op_str.len()..].trim().to_string();
            if col.is_empty() || val.is_empty() {
                return Err(miette::miette!(
                    "invalid prune expression: '{}' (expected 'column op value')",
                    expr
                ));
            }
            return Ok((col, *op, val));
        }
    }
    Err(miette::miette!(
        "invalid prune expression: '{}' (supported operators: >=, <=, >, <)",
        expr
    ))
}

impl Clone for PruneOp {
    fn clone(&self) -> Self {
        *self
    }
}

impl Copy for PruneOp {}

/// Extract a stat value as f64 for numeric comparison.
fn stat_as_f64(stats: &Statistics, is_min: bool) -> Option<f64> {
    match stats {
        Statistics::Int32(s) => {
            if is_min {
                s.min_opt().map(|v| *v as f64)
            } else {
                s.max_opt().map(|v| *v as f64)
            }
        }
        Statistics::Int64(s) => {
            if is_min {
                s.min_opt().map(|v| *v as f64)
            } else {
                s.max_opt().map(|v| *v as f64)
            }
        }
        Statistics::Float(s) => {
            if is_min {
                s.min_opt().map(|v| *v as f64)
            } else {
                s.max_opt().map(|v| *v as f64)
            }
        }
        Statistics::Double(s) => {
            if is_min {
                s.min_opt().copied()
            } else {
                s.max_opt().copied()
            }
        }
        _ => None,
    }
}

/// Extract a stat value as String for string comparison.
fn stat_as_string(stats: &Statistics, is_min: bool) -> Option<String> {
    match stats {
        Statistics::ByteArray(s) => {
            if is_min {
                s.min_opt()
                    .and_then(|v| std::str::from_utf8(v.data()).ok())
                    .map(|s| s.to_string())
            } else {
                s.max_opt()
                    .and_then(|v| std::str::from_utf8(v.data()).ok())
                    .map(|s| s.to_string())
            }
        }
        _ => None,
    }
}

/// Given file metadata and a prune expression, return the indices of row groups
/// that could potentially satisfy the condition.
fn compute_pruned_row_groups(
    metadata: &std::sync::Arc<parquet::file::metadata::ParquetMetaData>,
    expr: &str,
) -> miette::Result<Vec<usize>> {
    let (col_name, op, value_str) = parse_prune_expr(expr)?;

    // Find the column index in the parquet schema
    let schema_descr = metadata.file_metadata().schema_descr();
    let col_idx = schema_descr
        .columns()
        .iter()
        .position(|c| c.name() == col_name)
        .ok_or_else(|| miette::miette!("column '{}' not found in schema", col_name))?;

    let num_row_groups = metadata.num_row_groups();
    let mut qualifying = Vec::new();

    // Try parsing the value as f64 for numeric comparison
    let numeric_value = value_str.parse::<f64>().ok();

    for rg_idx in 0..num_row_groups {
        let rg_meta = metadata.row_group(rg_idx);
        let col_meta = rg_meta.column(col_idx);

        let keep = if let Some(stats) = col_meta.statistics() {
            if let Some(num_val) = numeric_value {
                // Numeric comparison
                let rg_min = stat_as_f64(stats, true);
                let rg_max = stat_as_f64(stats, false);

                match op {
                    // column >= value: keep if max >= value
                    PruneOp::Ge => rg_max.map(|max| max >= num_val).unwrap_or(true),
                    // column <= value: keep if min <= value
                    PruneOp::Le => rg_min.map(|min| min <= num_val).unwrap_or(true),
                    // column > value: keep if max > value
                    PruneOp::Gt => rg_max.map(|max| max > num_val).unwrap_or(true),
                    // column < value: keep if min < value
                    PruneOp::Lt => rg_min.map(|min| min < num_val).unwrap_or(true),
                }
            } else {
                // String comparison
                let rg_min = stat_as_string(stats, true);
                let rg_max = stat_as_string(stats, false);

                match op {
                    PruneOp::Ge => rg_max
                        .map(|max| max.as_str() >= value_str.as_str())
                        .unwrap_or(true),
                    PruneOp::Le => rg_min
                        .map(|min| min.as_str() <= value_str.as_str())
                        .unwrap_or(true),
                    PruneOp::Gt => rg_max
                        .map(|max| max.as_str() > value_str.as_str())
                        .unwrap_or(true),
                    PruneOp::Lt => rg_min
                        .map(|min| min.as_str() < value_str.as_str())
                        .unwrap_or(true),
                }
            }
        } else {
            // No statistics available, keep the row group
            true
        };

        if keep {
            qualifying.push(rg_idx);
        }
    }

    Ok(qualifying)
}

/// --split-by column: split output into one file per unique value of the given column.
fn execute_split_by(args: &SliceArgs, output: &OutputConfig, col_name: &str) -> miette::Result<()> {
    let out_template = output
        .output_path()
        .ok_or_else(|| miette::miette!("--split-by requires -o <path> flag with {{n}} template"))?
        .to_string();

    let sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema = None;

    for source in &sources {
        let file = File::open(source.path())
            .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        if let Some(indices) = resolve_projection(args.columns.as_deref(), builder.schema()) {
            let mask = parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), indices);
            builder = builder.with_projection(mask);
        }

        if let Some(ref rows) = args.rows {
            let parts: Vec<&str> = rows.split(':').collect();
            if parts.len() == 2 {
                let start: usize = parts[0].parse().unwrap_or(0);
                let end: usize = parts[1]
                    .parse()
                    .map_err(|_| miette::miette!("invalid row range: {}", rows))?;
                builder = builder.with_offset(start);
                builder = builder.with_limit(end.saturating_sub(start));
            }
        }

        if let Some(ref rg_str) = args.row_groups {
            let rg_indices: Vec<usize> = parse_row_group_selection(rg_str)?;
            builder = builder.with_row_groups(rg_indices);
        }

        if schema.is_none() {
            schema = Some(builder.schema().clone());
        }

        let reader = builder
            .build()
            .map_err(|e| miette::miette!("cannot build reader: {}", e))?;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| miette::miette!("error reading data: {}", e))?;
            all_batches.push(batch);
        }
    }

    let schema = schema.ok_or_else(|| miette::miette!("no data to split"))?;

    let col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == col_name)
        .ok_or_else(|| {
            miette::miette!(
                "column '{}' not found in schema (available: {})",
                col_name,
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;

    // Group rows by the unique values of the split column.
    // Key: stringified column value -> Vec<(batch_idx, row_idx)>
    let mut groups: HashMap<String, Vec<(usize, usize)>> = HashMap::new();

    for (batch_idx, batch) in all_batches.iter().enumerate() {
        let col_array = batch.column(col_idx);
        for row_idx in 0..batch.num_rows() {
            let val = if col_array.is_null(row_idx) {
                "__null__".to_string()
            } else {
                // Use the Display implementation via arrow's array formatting
                arrow::util::display::array_value_to_string(col_array, row_idx)
                    .unwrap_or_else(|_| "__error__".to_string())
            };
            groups.entry(val).or_default().push((batch_idx, row_idx));
        }
    }

    let mut file_count = 0usize;

    for (group_val, row_refs) in &groups {
        let out_path = if out_template.contains("{n}") {
            out_template.replace("{n}", group_val)
        } else {
            expand_template(&out_template, file_count)
        };

        let target = crate::input::cloud::resolve_output_path(&out_path)?;
        let out_file = File::create(target.local_path())
            .map_err(|e| miette::miette!("cannot create '{}': {}", out_path, e))?;
        let mut writer = ArrowWriter::try_new(out_file, schema.clone(), None)
            .map_err(|e| miette::miette!("cannot create writer: {}", e))?;

        // batch rows together for efficient take()
        let mut per_batch: HashMap<usize, Vec<usize>> = HashMap::new();
        for &(batch_idx, row_idx) in row_refs {
            per_batch.entry(batch_idx).or_default().push(row_idx);
        }

        for (batch_idx, row_indices) in &per_batch {
            let batch = &all_batches[*batch_idx];
            let indices =
                UInt32Array::from(row_indices.iter().map(|&i| i as u32).collect::<Vec<u32>>());

            let columns: Vec<arrow::array::ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| miette::miette!("take error: {}", e))?;

            let taken_batch = RecordBatch::try_new(schema.clone(), columns)
                .map_err(|e| miette::miette!("error creating batch: {}", e))?;

            writer
                .write(&taken_batch)
                .map_err(|e| miette::miette!("error writing: {}", e))?;
        }

        writer
            .close()
            .map_err(|e| miette::miette!("error closing writer: {}", e))?;
        target.finalize(&output.cloud_config)?;

        file_count += 1;
    }

    eprintln!("Split into {} files by column '{}'", file_count, col_name);

    Ok(())
}

fn parse_row_group_selection(s: &str) -> miette::Result<Vec<usize>> {
    let mut indices = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if let Some((start, end)) = part.split_once('-') {
            let s: usize = start
                .parse()
                .map_err(|_| miette::miette!("invalid row group index: {}", start))?;
            let e: usize = end
                .parse()
                .map_err(|_| miette::miette!("invalid row group index: {}", end))?;
            indices.extend(s..=e);
        } else {
            let idx: usize = part
                .parse()
                .map_err(|_| miette::miette!("invalid row group index: {}", part))?;
            indices.push(idx);
        }
    }
    Ok(indices)
}
