use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};
use arrow::record_batch::RecordBatch;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};

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
        bytesize::ByteSize(bytes as u64).to_string()
    }
}
