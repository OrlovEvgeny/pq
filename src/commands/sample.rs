use crate::cli::SampleArgs;
use crate::input::column_selector::resolve_projection;
use crate::input::resolve_inputs_report;
use crate::output::OutputConfig;
use crate::parquet_ext::metadata;
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
use parquet::file::metadata::ParquetMetaData;
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
        let meta = metadata::read_metadata(source.path())?;
        let total_rows = metadata::total_rows(&meta) as usize;
        let n = args.rows.min(total_rows);

        if n == 0 {
            continue;
        }

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
                .with_batch_size(n.min(8192))
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

        let mut sample_indices: Vec<usize> = (0..total_rows).collect();
        sample_indices.shuffle(&mut *rng);
        sample_indices.truncate(n);
        sample_indices.sort_unstable();

        let file = File::open(source.path())
            .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        if let Some(indices) = resolve_projection(args.columns.as_deref(), builder.schema()) {
            let mask = parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), indices);
            builder = builder.with_projection(mask);
        }

        let selection_plan = build_selection_plan(&meta, &sample_indices);
        let reader = builder
            .with_row_groups(selection_plan.row_groups)
            .with_row_selection(selection_plan.selection)
            .with_batch_size(n.min(8192))
            .build()
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        let sampled_batches: Vec<RecordBatch> = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| miette::miette!("error reading data: {}", e))?;

        crate::commands::head::write_batches(&sampled_batches, output, false, None)?;
    }
    sp.finish_and_clear();

    Ok(())
}

#[derive(Debug)]
struct SelectionPlan {
    row_groups: Vec<usize>,
    selection: RowSelection,
}

fn build_selection_plan(meta: &ParquetMetaData, sample_indices: &[usize]) -> SelectionPlan {
    let mut row_groups = Vec::new();
    let mut selectors = Vec::new();
    let mut sample_pos = 0usize;
    let mut row_group_start = 0usize;

    for (row_group_idx, row_group) in meta.row_groups().iter().enumerate() {
        let row_group_rows = row_group.num_rows() as usize;
        let row_group_end = row_group_start + row_group_rows;
        let mut local_ranges = Vec::new();
        let mut range_start: Option<usize> = None;
        let mut previous_index = 0usize;

        while sample_pos < sample_indices.len() && sample_indices[sample_pos] < row_group_end {
            let local_index = sample_indices[sample_pos] - row_group_start;

            match range_start {
                Some(start) if local_index == previous_index + 1 => {
                    range_start = Some(start);
                }
                Some(start) => {
                    local_ranges.push(start..(previous_index + 1));
                    range_start = Some(local_index);
                }
                None => {
                    range_start = Some(local_index);
                }
            }

            previous_index = local_index;
            sample_pos += 1;
        }

        if let Some(start) = range_start {
            local_ranges.push(start..(previous_index + 1));
        }

        if !local_ranges.is_empty() {
            row_groups.push(row_group_idx);
            let group_selectors: Vec<RowSelector> =
                RowSelection::from_consecutive_ranges(local_ranges.into_iter(), row_group_rows)
                    .into();
            selectors.extend(group_selectors);
        }

        row_group_start = row_group_end;
    }

    SelectionPlan {
        row_groups,
        selection: selectors.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    #[test]
    fn build_selection_plan_tracks_sparse_ranges_across_row_groups() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("sample.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let props = WriterProperties::builder()
            .set_max_row_group_size(2)
            .build();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();

        for chunk in [vec![1, 2], vec![3, 4], vec![5, 6]] {
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(chunk))])
                    .unwrap();
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();

        let meta = metadata::read_metadata(&path).unwrap();
        let plan = build_selection_plan(&meta, &[1, 2, 5]);
        let selectors: Vec<RowSelector> = plan.selection.into();

        assert_eq!(plan.row_groups, vec![0, 1, 2]);
        assert_eq!(
            selectors,
            vec![
                RowSelector::skip(1),
                RowSelector::select(2),
                RowSelector::skip(2),
                RowSelector::select(1),
            ]
        );
    }
}
