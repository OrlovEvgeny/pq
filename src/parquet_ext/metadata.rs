use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::error::PqError;

/// Read only the metadata (footer) from a Parquet file. Never reads data pages.
/// Returns `NotParquet` if the file does not start with the PAR1 magic bytes.
pub fn read_metadata(path: &Path) -> miette::Result<ParquetMetaData> {
    let mut file = File::open(path).map_err(|_| PqError::FileNotFound {
        path: path.to_path_buf(),
    })?;

    let mut magic = [0u8; 4];
    if file.read_exact(&mut magic).is_ok() && &magic != b"PAR1" {
        return Err(PqError::NotParquet {
            path: path.to_path_buf(),
        }
        .into());
    }

    // re-open: SerializedFileReader needs to start from byte 0
    let file = File::open(path).map_err(|_| PqError::FileNotFound {
        path: path.to_path_buf(),
    })?;
    let reader = SerializedFileReader::new(file).map_err(|e| PqError::Parquet {
        message: format!("failed to read '{}'", path.display()),
        source: e,
    })?;
    Ok(reader.metadata().clone())
}

/// Total row count from metadata
pub fn total_rows(meta: &ParquetMetaData) -> i64 {
    meta.row_groups().iter().map(|rg| rg.num_rows()).sum()
}

/// Dominant compression codec across all row groups/columns
pub fn dominant_compression(meta: &ParquetMetaData) -> String {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for rg in meta.row_groups() {
        for col in rg.columns() {
            let codec = format!("{:?}", col.compression());
            *counts.entry(codec).or_default() += 1;
        }
    }
    counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(codec, _)| simplify_compression_name(&codec))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Total compressed size in bytes
pub fn total_compressed_size(meta: &ParquetMetaData) -> i64 {
    meta.row_groups()
        .iter()
        .map(|rg| rg.compressed_size())
        .sum()
}

/// Total uncompressed size in bytes
pub fn total_uncompressed_size(meta: &ParquetMetaData) -> i64 {
    meta.row_groups()
        .iter()
        .flat_map(|rg| rg.columns())
        .map(|col| col.uncompressed_size())
        .sum()
}

/// Created-by string from file metadata
pub fn created_by(meta: &ParquetMetaData) -> Option<String> {
    meta.file_metadata().created_by().map(|s| s.to_string())
}

/// Key-value metadata as a map
pub fn key_value_metadata(meta: &ParquetMetaData) -> HashMap<String, String> {
    meta.file_metadata()
        .key_value_metadata()
        .map(|kvs| {
            kvs.iter()
                .filter_map(|kv| kv.value.as_ref().map(|v| (kv.key.clone(), v.clone())))
                .collect()
        })
        .unwrap_or_default()
}

/// Simplify compression codec names for display
fn simplify_compression_name(name: &str) -> String {
    if name.starts_with("ZSTD") {
        "zstd".to_string()
    } else if name.starts_with("GZIP") {
        "gzip".to_string()
    } else if name.starts_with("BROTLI") {
        "brotli".to_string()
    } else if name.starts_with("LZ4") {
        "lz4".to_string()
    } else {
        name.to_lowercase()
    }
}

/// Format Parquet schema column type as a human-readable string
pub fn format_type(col: &parquet::schema::types::ColumnDescriptor) -> String {
    let physical = format!("{:?}", col.physical_type());

    if let Some(logical) = col.logical_type() {
        format!("{} ({:?})", physical, logical)
    } else if let parquet::basic::ConvertedType::NONE = col.converted_type() {
        physical
    } else {
        format!("{} ({:?})", physical, col.converted_type())
    }
}

/// Get schema column names from metadata
pub fn column_names(meta: &ParquetMetaData) -> Vec<String> {
    let schema = meta.file_metadata().schema_descr();
    schema
        .columns()
        .iter()
        .map(|col| col.name().to_string())
        .collect()
}

/// Get schema type information for display
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub encoding: String,
}

pub fn column_info(meta: &ParquetMetaData) -> Vec<ColumnInfo> {
    let schema = meta.file_metadata().schema_descr();
    let row_groups = meta.row_groups();

    schema
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let type_name = format_schema_type(col);

            // Get encoding from first row group
            let encoding = row_groups
                .first()
                .and_then(|rg| rg.columns().get(i))
                .map(|col_meta| {
                    col_meta
                        .encodings()
                        .iter()
                        .map(|e| format!("{:?}", e))
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();

            let nullable = col.self_type().is_optional();

            ColumnInfo {
                name: col.name().to_string(),
                type_name,
                nullable,
                encoding,
            }
        })
        .collect()
}

fn format_schema_type(col: &parquet::schema::types::ColumnDescriptor) -> String {
    use parquet::basic::{LogicalType, TimeUnit};

    if let Some(logical) = col.logical_type() {
        return match logical {
            LogicalType::String => "VARCHAR".to_string(),
            LogicalType::Integer {
                bit_width,
                is_signed: true,
            } => format!("INT{}", bit_width),
            LogicalType::Integer {
                bit_width,
                is_signed: false,
            } => format!("UINT{}", bit_width),
            LogicalType::Timestamp {
                unit: TimeUnit::MILLIS(_),
                ..
            } => "TIMESTAMP_MS".to_string(),
            LogicalType::Timestamp {
                unit: TimeUnit::MICROS(_),
                ..
            } => "TIMESTAMP_US".to_string(),
            LogicalType::Timestamp {
                unit: TimeUnit::NANOS(_),
                ..
            } => "TIMESTAMP_NS".to_string(),
            LogicalType::Date => "DATE".to_string(),
            LogicalType::Time { .. } => "TIME".to_string(),
            LogicalType::Decimal { scale, precision } => {
                format!("DECIMAL({},{})", precision, scale)
            }
            LogicalType::Json => "JSON".to_string(),
            LogicalType::Bson => "BSON".to_string(),
            LogicalType::Uuid => "UUID".to_string(),
            LogicalType::Enum => "ENUM".to_string(),
            LogicalType::List => "LIST".to_string(),
            LogicalType::Map => "MAP".to_string(),
            other => format!("{:?}", other),
        };
    }

    match col.converted_type() {
        parquet::basic::ConvertedType::NONE => format!("{:?}", col.physical_type()),
        parquet::basic::ConvertedType::UTF8 => "VARCHAR".to_string(),
        parquet::basic::ConvertedType::LIST => "LIST".to_string(),
        parquet::basic::ConvertedType::MAP => "MAP".to_string(),
        converted => format!("{:?}", converted),
    }
}

/// Detailed schema column info including physical/logical types and rep/def levels
pub struct DetailedColumnInfo {
    pub index: String, // "1", "2", "3.1", etc. for nested
    pub path: String,  // dotted path like "tags.list.element"
    pub physical_type: String,
    pub logical_type: String,
    pub repetition: String, // "R" for REQUIRED, "O" for OPTIONAL, "REPEATED" for REPEATED
    pub max_def_level: i16,
    pub max_rep_level: i16,
    pub encoding: String,
}

pub fn detailed_column_info(meta: &ParquetMetaData) -> Vec<DetailedColumnInfo> {
    let schema = meta.file_metadata().schema_descr();
    let row_groups = meta.row_groups();

    let mut result = Vec::new();
    let mut main_counter: usize = 0;
    let mut sub_counter: usize = 0;
    let mut last_top_level: Option<String> = None;

    for (i, col) in schema.columns().iter().enumerate() {
        let parts = col.path().parts();
        let path = parts.join(".");

        let physical_type = format!("{:?}", col.physical_type());

        let logical_type = if let Some(lt) = col.logical_type() {
            format!("{:?}", lt)
        } else {
            match col.converted_type() {
                parquet::basic::ConvertedType::NONE => "\u{2014}".to_string(),
                ct => format!("{:?}", ct),
            }
        };

        let repetition =
            match format!("{:?}", col.self_type().get_basic_info().repetition()).as_str() {
                "REQUIRED" => "R".to_string(),
                "OPTIONAL" => "O".to_string(),
                "REPEATED" => "REPEATED".to_string(),
                other => other.to_string(),
            };

        let max_def_level = col.max_def_level();
        let max_rep_level = col.max_rep_level();

        // Get encoding from first row group
        let encoding = row_groups
            .first()
            .and_then(|rg| rg.columns().get(i))
            .map(|col_meta| {
                col_meta
                    .encodings()
                    .iter()
                    .map(|e| format!("{:?}", e))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();

        // Compute the index
        let index = if parts.len() == 1 {
            main_counter += 1;
            sub_counter = 0;
            last_top_level = Some(parts[0].clone());
            format!("{}", main_counter)
        } else {
            let current_top = &parts[0];
            if last_top_level.as_deref() != Some(current_top) {
                main_counter += 1;
                sub_counter = 0;
                last_top_level = Some(current_top.clone());
            }
            sub_counter += 1;
            format!("{}.{}", main_counter, sub_counter)
        };

        result.push(DetailedColumnInfo {
            index,
            path,
            physical_type,
            logical_type,
            repetition,
            max_def_level,
            max_rep_level,
            encoding,
        });
    }

    result
}

/// Per-column compressed and uncompressed sizes
pub struct ColumnSize {
    pub name: String,
    pub compressed: i64,
    pub uncompressed: i64,
}

pub fn column_sizes(meta: &ParquetMetaData) -> Vec<ColumnSize> {
    let schema = meta.file_metadata().schema_descr();
    let num_columns = schema.num_columns();
    let mut sizes = vec![(0i64, 0i64); num_columns];

    for rg in meta.row_groups() {
        for (i, col) in rg.columns().iter().enumerate() {
            if i < num_columns {
                sizes[i].0 += col.compressed_size();
                sizes[i].1 += col.uncompressed_size();
            }
        }
    }

    schema
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| ColumnSize {
            name: col.name().to_string(),
            compressed: sizes[i].0,
            uncompressed: sizes[i].1,
        })
        .collect()
}
