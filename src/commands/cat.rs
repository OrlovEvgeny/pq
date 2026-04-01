use crate::cli::CatArgs;
use crate::input::resolve_inputs_report;
use crate::output::OutputConfig;
use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt32Array, new_null_array};
use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;

pub fn execute(args: &CatArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sp = output.spinner("Loading");
    let sources = resolve_inputs_report(&args.files, &output.cloud_config, &mut |msg| {
        sp.set_message(msg);
    })?;
    sp.finish_and_clear();
    let out_path = output
        .output_path()
        .ok_or_else(|| miette::miette!("cat requires an output file (-o <path>)"))?
        .to_string();
    let target = crate::input::cloud::resolve_output_path(&out_path)?;

    if sources.is_empty() {
        return Err(miette::miette!("no files to concatenate"));
    }

    let output_schema: SchemaRef = if args.union_by_name {
        let mut fields: Vec<Arc<Field>> = Vec::new();
        let mut seen_names: Vec<String> = Vec::new();

        for source in &sources {
            let file = File::open(source.path())
                .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;
            let schema = builder.schema();

            for field in schema.fields() {
                if !seen_names.contains(field.name()) {
                    seen_names.push(field.name().clone());
                    fields.push(field.clone());
                }
            }
        }

        Arc::new(Schema::new(fields))
    } else {
        let first_file = File::open(sources[0].path())
            .map_err(|e| miette::miette!("cannot open '{}': {}", sources[0].display_name(), e))?;
        let first_builder = ParquetRecordBatchReaderBuilder::try_new(first_file)
            .map_err(|e| miette::miette!("cannot read '{}': {}", sources[0].display_name(), e))?;
        first_builder.schema().clone()
    };

    let write_schema: SchemaRef = if args.add_filename {
        let mut fields: Vec<Arc<Field>> = output_schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new("_filename", DataType::Utf8, true)));
        Arc::new(Schema::new(fields))
    } else {
        output_schema.clone()
    };

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut total_rows: usize = 0;

    let progress = output.progress_bar("Concatenating", sources.len() as u64);

    for source in &sources {
        let file = File::open(source.path())
            .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        let file_schema = builder.schema().clone();

        if !args.union_by_name && *file_schema != *output_schema {
            return Err(miette::miette!(
                "schema mismatch in '{}' — use --union-by-name for schema evolution",
                source.display_name()
            ));
        }

        let reader = builder
            .build()
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        let filename = source.display_name();

        for batch_result in reader {
            let batch = batch_result.map_err(|e| miette::miette!("read error: {}", e))?;
            let num_rows = batch.num_rows();
            total_rows += num_rows;

            let aligned_batch = if args.union_by_name {
                align_batch_to_schema(&batch, &file_schema, &output_schema)?
            } else {
                batch
            };

            let final_batch = if args.add_filename {
                add_filename_column(&aligned_batch, &filename, &write_schema)?
            } else {
                aligned_batch
            };

            all_batches.push(final_batch);
        }
        progress.inc(1);
    }
    drop(progress);

    let write_batches: Vec<RecordBatch> = if let Some(ref sort_by) = args.sort_by {
        if all_batches.is_empty() {
            all_batches
        } else {
            let combined = concat_batches(&write_schema, &all_batches)
                .map_err(|e| miette::miette!("failed to concatenate batches for sorting: {}", e))?;

            let sort_col_names: Vec<&str> = sort_by.split(',').map(|s| s.trim()).collect();
            let mut sort_columns: Vec<SortColumn> = Vec::new();
            for col_name in &sort_col_names {
                let idx = combined.schema().index_of(col_name).map_err(|_| {
                    miette::miette!("sort column '{}' not found in schema", col_name)
                })?;
                sort_columns.push(SortColumn {
                    values: combined.column(idx).clone(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: true,
                    }),
                });
            }

            let indices: UInt32Array = lexsort_to_indices(&sort_columns, None)
                .map_err(|e| miette::miette!("sort error: {}", e))?;

            let sorted_columns: Vec<ArrayRef> = combined
                .columns()
                .iter()
                .map(|col| take(col.as_ref(), &indices, None))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| miette::miette!("take error during sort: {}", e))?;

            let sorted_batch = RecordBatch::try_new(combined.schema(), sorted_columns)
                .map_err(|e| miette::miette!("failed to build sorted batch: {}", e))?;
            vec![sorted_batch]
        }
    } else {
        all_batches
    };

    let out_file = File::create(target.local_path())
        .map_err(|e| miette::miette!("cannot create '{}': {}", out_path, e))?;
    let mut writer = ArrowWriter::try_new(out_file, write_schema.clone(), None)
        .map_err(|e| miette::miette!("cannot create writer: {}", e))?;

    for batch in &write_batches {
        writer
            .write(batch)
            .map_err(|e| miette::miette!("write error: {}", e))?;
    }

    writer
        .close()
        .map_err(|e| miette::miette!("close error: {}", e))?;
    target.finalize(&output.cloud_config)?;

    eprintln!(
        "Concatenated {} files ({} rows) → {}",
        sources.len(),
        total_rows,
        out_path
    );

    Ok(())
}

fn align_batch_to_schema(
    batch: &RecordBatch,
    file_schema: &SchemaRef,
    target_schema: &SchemaRef,
) -> miette::Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for target_field in target_schema.fields() {
        if let Some((idx, _)) = file_schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == target_field.name())
        {
            columns.push(batch.column(idx).clone());
        } else {
            columns.push(new_null_array(target_field.data_type(), num_rows));
        }
    }

    RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| miette::miette!("failed to build aligned batch: {}", e))
}

fn add_filename_column(
    batch: &RecordBatch,
    filename: &str,
    write_schema: &SchemaRef,
) -> miette::Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let filename_array: ArrayRef = Arc::new(StringArray::from(vec![filename; num_rows]));

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(filename_array);

    RecordBatch::try_new(write_schema.clone(), columns)
        .map_err(|e| miette::miette!("failed to add _filename column: {}", e))
}
