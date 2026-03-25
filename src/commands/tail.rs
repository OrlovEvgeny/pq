use crate::cli::TailArgs;
use crate::input::column_selector::resolve_projection;
use crate::input::resolve_inputs_with_config;
use crate::output::OutputConfig;
use crate::parquet_ext::metadata;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

pub fn execute(args: &TailArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;

    for source in &sources {
        let file = File::open(source.path())
            .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;

        let meta = metadata::read_metadata(source.path())?;
        let total_rows = metadata::total_rows(&meta) as usize;
        let n = args.rows.min(total_rows);

        let skip_rows = total_rows.saturating_sub(n);

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        if let Some(indices) = resolve_projection(args.columns.as_deref(), builder.schema()) {
            let mask = parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), indices);
            builder = builder.with_projection(mask);
        }

        builder = builder.with_offset(skip_rows);
        builder = builder.with_limit(n);
        builder = builder.with_batch_size(n.min(8192));

        let reader = builder
            .build()
            .map_err(|e| miette::miette!("cannot read '{}': {}", source.display_name(), e))?;

        let batches: Vec<_> = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| miette::miette!("error reading data: {}", e))?;

        crate::commands::head::write_batches(&batches, output, args.wide, args.max_width)?;
    }

    Ok(())
}
