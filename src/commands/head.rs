use crate::cli::HeadArgs;
use crate::commands::util::write_record_batches;
use crate::input::column_selector::resolve_projection;
use crate::input::resolve_inputs_report;
use crate::output::OutputConfig;
use arrow::array::{Array, ArrayRef, AsArray, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
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
    let display_batches: Vec<RecordBatch> = if !wide && let Some(mw) = max_width {
        batches
            .iter()
            .map(|b| truncate_batch(b, mw))
            .collect::<miette::Result<Vec<_>>>()?
    } else {
        batches.to_vec()
    };

    write_record_batches(&display_batches, output)
}
