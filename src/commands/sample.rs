use crate::cli::SampleArgs;
use crate::input::column_selector::resolve_projection;
use crate::input::resolve_inputs_report;
use crate::output::OutputConfig;
use crate::parquet_ext::metadata;
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rand::SeedableRng;
use rand::seq::SliceRandom;
use std::fs::File;

pub fn execute(args: &SampleArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sp = output.spinner("Loading");
    let sources = resolve_inputs_report(&args.files, &output.cloud_config, &mut |msg| {
        sp.set_message(msg);
    })?;
    sp.finish_and_clear();

    let sp = output.spinner("Sampling");
    for source in &sources {
        let file = File::open(source.path())
            .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;

        let meta = metadata::read_metadata(source.path())?;
        let total_rows = metadata::total_rows(&meta) as usize;
        let n = args.rows.min(total_rows);

        if n == 0 {
            continue;
        }

        // fast path: read everything
        if n >= total_rows {
            let file = File::open(source.path())
                .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;
            let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

            if let Some(indices) = resolve_projection(args.columns.as_deref(), builder.schema()) {
                let mask =
                    parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), indices);
                builder = builder.with_projection(mask);
            }

            let reader = builder
                .build()
                .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

            let batches: Vec<_> = reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| miette::miette!("error reading data: {}", e))?;

            crate::commands::head::write_batches(&batches, output, false, None)?;
            continue;
        }

        let mut rng: Box<dyn rand::RngCore> = if let Some(seed) = args.seed {
            Box::new(rand::rngs::StdRng::seed_from_u64(seed))
        } else {
            Box::new(rand::rng())
        };

        let mut indices: Vec<usize> = (0..total_rows).collect();
        indices.shuffle(&mut *rng);
        indices.truncate(n);
        indices.sort_unstable();

        // TODO: could read only needed row groups instead of everything
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        if let Some(indices) = resolve_projection(args.columns.as_deref(), builder.schema()) {
            let mask = parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), indices);
            builder = builder.with_projection(mask);
        }

        let reader = builder
            .build()
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        let mut sampled_batches: Vec<RecordBatch> = Vec::new();
        let mut row_offset: usize = 0;
        let mut idx_pos = 0;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| miette::miette!("error reading data: {}", e))?;
            let batch_len = batch.num_rows();
            let batch_end = row_offset + batch_len;

            let mut batch_indices = Vec::new();
            while idx_pos < indices.len() && indices[idx_pos] < batch_end {
                batch_indices.push(indices[idx_pos] - row_offset);
                idx_pos += 1;
            }

            if !batch_indices.is_empty() {
                let indices_array = arrow::array::UInt64Array::from(
                    batch_indices.iter().map(|&i| i as u64).collect::<Vec<_>>(),
                );

                let columns: Vec<arrow::array::ArrayRef> = batch
                    .columns()
                    .iter()
                    .map(|col| arrow::compute::take(col.as_ref(), &indices_array, None))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| miette::miette!("take error: {}", e))?;

                let sampled_batch = RecordBatch::try_new(batch.schema(), columns)
                    .map_err(|e| miette::miette!("batch error: {}", e))?;
                sampled_batches.push(sampled_batch);
            }

            row_offset = batch_end;
            if idx_pos >= indices.len() {
                break;
            }
        }

        crate::commands::head::write_batches(&sampled_batches, output, false, None)?;
    }
    sp.finish_and_clear();

    Ok(())
}
