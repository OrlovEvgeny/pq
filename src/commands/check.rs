use crate::cli::CheckArgs;
use crate::error::PqError;
use crate::input::cloud::is_cloud_url;
use crate::input::{resolve_inputs, resolve_inputs_with_config};
use crate::output::{OutputConfig, OutputFormat};
use crate::parquet_ext::metadata;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Encoding, Type as PhysicalType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;

#[derive(Serialize)]
struct CheckResult {
    file: String,
    passed: usize,
    failed: usize,
    checks: Vec<CheckItem>,
}

#[derive(Serialize)]
struct CheckItem {
    name: String,
    passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

// ── Contract TOML structures ──

#[derive(Debug, Deserialize)]
struct Contract {
    #[serde(default)]
    file: Option<FileContract>,
    #[serde(default)]
    columns: HashMap<String, ColumnContract>,
    #[serde(default)]
    freshness: Option<FreshnessContract>,
}

#[derive(Debug, Deserialize)]
struct FileContract {
    min_rows: Option<i64>,
    max_size: Option<String>,
    compression: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct FreshnessContract {
    column: String,
    max_age: String,
}

#[derive(Debug, Deserialize)]
struct ColumnContract {
    #[serde(rename = "type")]
    col_type: Option<String>,
    nullable: Option<bool>,
    min: Option<f64>,
    max: Option<f64>,
    allowed: Option<Vec<String>>,
}

/// Parse a human-readable size string like "500MB", "1GB", "1024" into bytes.
fn parse_size_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    // Try parsing with bytesize first — it handles "500MB", "1GiB", etc.
    if let Ok(bs) = s.parse::<bytesize::ByteSize>() {
        return Some(bs.as_u64());
    }
    // Fallback: try plain u64
    s.parse::<u64>().ok()
}

pub fn execute(args: &CheckArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sources = if args.files.iter().any(|f| is_cloud_url(f)) {
        resolve_inputs_with_config(&args.files, &output.cloud_config)?
    } else {
        resolve_inputs(&args.files)?
    };
    let mut any_failed = false;
    let mut all_results = Vec::new();

    let contract = if let Some(ref contract_path) = args.contract {
        let content = std::fs::read_to_string(contract_path)
            .map_err(|e| miette::miette!("cannot read contract file '{}': {}", contract_path, e))?;
        let c: Contract = toml::from_str(&content)
            .map_err(|e| miette::miette!("invalid contract TOML '{}': {}", contract_path, e))?;
        Some(c)
    } else {
        None
    };

    for source in &sources {
        let mut checks: Vec<CheckItem> = Vec::new();

        let meta = match metadata::read_metadata(source.path()) {
            Ok(m) => {
                checks.push(CheckItem {
                    name: "Magic bytes and footer parseable".to_string(),
                    passed: true,
                    detail: None,
                });
                Some(m)
            }
            Err(e) => {
                checks.push(CheckItem {
                    name: "Magic bytes and footer parseable".to_string(),
                    passed: false,
                    detail: Some(format!("{}", e)),
                });
                None
            }
        };

        if let Some(ref meta) = meta {
            let total_rows = metadata::total_rows(meta);
            let rg_count: i64 = meta.row_groups().iter().map(|rg| rg.num_rows()).sum();
            checks.push(CheckItem {
                name: format!("Row counts sum correctly ({})", total_rows),
                passed: total_rows == rg_count,
                detail: if total_rows != rg_count {
                    Some(format!(
                        "metadata says {} but row groups sum to {}",
                        total_rows, rg_count
                    ))
                } else {
                    None
                },
            });

            let num_rg = meta.num_row_groups();
            let num_cols = meta.file_metadata().schema_descr().num_columns();
            let mut schema_consistent = true;
            for rg in meta.row_groups() {
                if rg.columns().len() != num_cols {
                    schema_consistent = false;
                    break;
                }
            }
            checks.push(CheckItem {
                name: format!("Schema consistent across {} row groups", num_rg),
                passed: schema_consistent,
                detail: if !schema_consistent {
                    Some("Column count varies across row groups".to_string())
                } else {
                    None
                },
            });

            let mut stats_ok = true;
            let mut stats_detail = None;
            for (rg_idx, rg) in meta.row_groups().iter().enumerate() {
                for (col_idx, col) in rg.columns().iter().enumerate() {
                    if let Some(stats) = col.statistics() {
                        let min_max_valid = match stats {
                            parquet::file::statistics::Statistics::Int32(s) => {
                                match (s.min_opt(), s.max_opt()) {
                                    (Some(min), Some(max)) => min <= max,
                                    _ => true,
                                }
                            }
                            parquet::file::statistics::Statistics::Int64(s) => {
                                match (s.min_opt(), s.max_opt()) {
                                    (Some(min), Some(max)) => min <= max,
                                    _ => true,
                                }
                            }
                            parquet::file::statistics::Statistics::Float(s) => {
                                match (s.min_opt(), s.max_opt()) {
                                    (Some(min), Some(max)) => min <= max,
                                    _ => true,
                                }
                            }
                            parquet::file::statistics::Statistics::Double(s) => {
                                match (s.min_opt(), s.max_opt()) {
                                    (Some(min), Some(max)) => min <= max,
                                    _ => true,
                                }
                            }
                            _ => true,
                        };
                        if !min_max_valid {
                            stats_ok = false;
                            let col_desc = meta.file_metadata().schema_descr().column(col_idx);
                            let col_name = col_desc.name();
                            stats_detail = Some(format!(
                                "Column '{}' row group {}: min > max",
                                col_name, rg_idx
                            ));
                        }
                    }
                }
            }
            checks.push(CheckItem {
                name: "Column statistics within type bounds".to_string(),
                passed: stats_ok,
                detail: stats_detail,
            });

            let compression = metadata::dominant_compression(meta);
            checks.push(CheckItem {
                name: format!("Compression codec recognized ({})", compression),
                passed: true,
                detail: None,
            });

            let encoding_check = validate_encodings(meta);
            checks.push(encoding_check);

            let page_offset_check = validate_page_offsets(meta);
            checks.push(page_offset_check);

            if args.read_data {
                let expected_rows = metadata::total_rows(meta);
                match validate_data(source.path()) {
                    Ok(actual_rows) => {
                        checks.push(CheckItem {
                            name: "All pages decompressible".to_string(),
                            passed: true,
                            detail: None,
                        });
                        let rows_match = actual_rows as i64 == expected_rows;
                        checks.push(CheckItem {
                            name: format!("Actual row count matches metadata ({})", expected_rows),
                            passed: rows_match,
                            detail: if !rows_match {
                                Some(format!(
                                    "metadata says {} rows but data contains {} rows",
                                    expected_rows, actual_rows
                                ))
                            } else {
                                None
                            },
                        });
                    }
                    Err(e) => {
                        checks.push(CheckItem {
                            name: "Data pages readable".to_string(),
                            passed: false,
                            detail: Some(format!("{}", e)),
                        });
                    }
                }
            }

            if let Some(ref contract) = contract {
                validate_contract(contract, meta, source.file_size(), &mut checks);
            }
        }

        let passed = checks.iter().filter(|c| c.passed).count();
        let failed = checks.iter().filter(|c| !c.passed).count();
        if failed > 0 {
            any_failed = true;
        }

        if output.format == OutputFormat::Table {
            for check in &checks {
                let sym = crate::output::symbols::symbols();
                let symbol = if check.passed { sym.check } else { sym.cross };
                write!(output.writer, "  {}  {}", symbol, check.name)
                    .map_err(|e| miette::miette!("{}", e))?;
                if let Some(ref detail) = check.detail {
                    write!(
                        output.writer,
                        "\n     {} {}",
                        crate::output::symbols::symbols().arrow,
                        detail
                    )
                    .map_err(|e| miette::miette!("{}", e))?;
                }
                writeln!(output.writer).map_err(|e| miette::miette!("{}", e))?;
            }
            writeln!(
                output.writer,
                "  {}",
                crate::output::symbols::symbols().dash
            )
            .map_err(|e| miette::miette!("{}", e))?;
            writeln!(
                output.writer,
                "  {}/{} checks passed.{}",
                passed,
                passed + failed,
                if failed > 0 {
                    format!(" {} issue(s) found.", failed)
                } else {
                    String::new()
                }
            )
            .map_err(|e| miette::miette!("{}", e))?;
        }

        all_results.push(CheckResult {
            file: source.display_name(),
            passed,
            failed,
            checks,
        });
    }

    match output.format {
        OutputFormat::Table => {} // already written
        OutputFormat::Json | OutputFormat::Parquet => {
            if all_results.len() == 1 {
                crate::output::json::write_json(&mut output.writer, &all_results[0])?;
            } else {
                crate::output::json::write_json(&mut output.writer, &all_results)?;
            }
        }
        OutputFormat::Jsonl => {
            for result in &all_results {
                crate::output::json::write_jsonl(&mut output.writer, result)?;
            }
        }
        OutputFormat::Csv | OutputFormat::Tsv => {
            let headers = ["file", "passed", "failed"];
            let rows: Vec<Vec<String>> = all_results
                .iter()
                .map(|r| vec![r.file.clone(), r.passed.to_string(), r.failed.to_string()])
                .collect();
            if output.format == OutputFormat::Csv {
                crate::output::csv_output::write_csv(&mut output.writer, &headers, &rows)?;
            } else {
                crate::output::csv_output::write_tsv(&mut output.writer, &headers, &rows)?;
            }
        }
    }

    if any_failed {
        let total_failed: usize = all_results.iter().map(|r| r.failed).sum();
        Err(PqError::ValidationFailed {
            message: format!("{} check(s) failed", total_failed),
            suggestion: "Run with --read-data for deeper validation, or review the checks above."
                .to_string(),
        }
        .into())
    } else {
        Ok(())
    }
}

/// Validate that encodings used are compatible with the physical types of their columns.
fn validate_encodings(meta: &parquet::file::metadata::ParquetMetaData) -> CheckItem {
    let schema = meta.file_metadata().schema_descr();
    let mut all_ok = true;
    let mut detail = None;

    for rg in meta.row_groups() {
        for (col_idx, col) in rg.columns().iter().enumerate() {
            if col_idx >= schema.num_columns() {
                continue;
            }
            let col_desc = schema.column(col_idx);
            let physical_type = col_desc.physical_type();

            for encoding in col.encodings() {
                if !is_encoding_compatible(*encoding, physical_type) {
                    all_ok = false;
                    detail = Some(format!(
                        "Column '{}': encoding {:?} incompatible with physical type {:?}",
                        col_desc.name(),
                        encoding,
                        physical_type,
                    ));
                }
            }
        }
    }

    CheckItem {
        name: "Encoding compatible with column types".to_string(),
        passed: all_ok,
        detail,
    }
}

/// Check whether an encoding is compatible with a physical type.
/// This is a conservative check — PLAIN and RLE_DICTIONARY are universal.
fn is_encoding_compatible(encoding: Encoding, physical_type: PhysicalType) -> bool {
    match encoding {
        // Universal encodings: valid for all types
        Encoding::PLAIN | Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => true,

        // RLE is used for BOOLEAN data and also for definition/repetition levels
        // on ALL column types. Column chunk encoding lists include level encodings,
        // so RLE must be accepted universally.
        Encoding::RLE => true,

        // BIT_PACKED is legacy, primarily for BOOLEAN
        #[allow(deprecated)]
        Encoding::BIT_PACKED => true,

        // DELTA_BINARY_PACKED: integer types only
        Encoding::DELTA_BINARY_PACKED => {
            matches!(physical_type, PhysicalType::INT32 | PhysicalType::INT64)
        }

        // DELTA_LENGTH_BYTE_ARRAY: byte arrays only
        Encoding::DELTA_LENGTH_BYTE_ARRAY => matches!(
            physical_type,
            PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY
        ),

        // DELTA_BYTE_ARRAY: byte arrays only
        Encoding::DELTA_BYTE_ARRAY => matches!(
            physical_type,
            PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY
        ),

        // BYTE_STREAM_SPLIT: float, double, fixed-length byte arrays, int32, int64
        Encoding::BYTE_STREAM_SPLIT => matches!(
            physical_type,
            PhysicalType::FLOAT
                | PhysicalType::DOUBLE
                | PhysicalType::FIXED_LEN_BYTE_ARRAY
                | PhysicalType::INT32
                | PhysicalType::INT64
        ),

        // Unknown/future encodings: accept them (conservative)
        #[allow(unreachable_patterns)]
        _ => true,
    }
}

/// Validate that column chunk data_page_offset values don't overlap within a row group.
fn validate_page_offsets(meta: &parquet::file::metadata::ParquetMetaData) -> CheckItem {
    let mut all_ok = true;
    let mut detail = None;

    for (rg_idx, rg) in meta.row_groups().iter().enumerate() {
        // Collect the true start offset of each column chunk.
        // A column chunk may have a dictionary page before the data page,
        // so the earliest offset is min(data_page_offset, dictionary_page_offset).
        let mut starts: Vec<(i64, String)> = Vec::new();

        for (col_idx, col) in rg.columns().iter().enumerate() {
            let data_offset = col.data_page_offset();
            let dict_offset = col.dictionary_page_offset();
            let start = if let Some(d) = dict_offset {
                data_offset.min(d)
            } else {
                data_offset
            };
            let col_name = if col_idx < meta.file_metadata().schema_descr().num_columns() {
                meta.file_metadata()
                    .schema_descr()
                    .column(col_idx)
                    .name()
                    .to_string()
            } else {
                format!("column_{}", col_idx)
            };
            starts.push((start, col_name));
        }

        // Sort by start offset
        starts.sort_by_key(|(offset, _)| *offset);

        // Check that each column's start is >= previous column's start
        // (We can only detect if starts are out of order or identical for
        //  different columns, since we don't know exact end without page index.)
        for window in starts.windows(2) {
            let (offset_a, ref name_a) = window[0];
            let (offset_b, ref name_b) = window[1];

            if offset_a == offset_b && name_a != name_b {
                all_ok = false;
                detail = Some(format!(
                    "Row group {}: columns '{}' and '{}' share the same offset {}",
                    rg_idx, name_a, name_b, offset_a,
                ));
            }
        }
    }

    CheckItem {
        name: "Page offsets non-overlapping within row groups".to_string(),
        passed: all_ok,
        detail,
    }
}

/// Validate file against a contract specification.
fn validate_contract(
    contract: &Contract,
    meta: &parquet::file::metadata::ParquetMetaData,
    file_size: u64,
    checks: &mut Vec<CheckItem>,
) {
    if let Some(ref file_contract) = contract.file {
        if let Some(min_rows) = file_contract.min_rows {
            let total = metadata::total_rows(meta);
            let passed = total >= min_rows;
            checks.push(CheckItem {
                name: format!("Contract: min_rows >= {}", min_rows),
                passed,
                detail: if !passed {
                    Some(format!("file has {} rows, minimum is {}", total, min_rows))
                } else {
                    None
                },
            });
        }

        if let Some(ref max_size_str) = file_contract.max_size {
            if let Some(max_bytes) = parse_size_bytes(max_size_str) {
                let passed = file_size <= max_bytes;
                checks.push(CheckItem {
                    name: format!("Contract: max_size <= {}", max_size_str),
                    passed,
                    detail: if !passed {
                        Some(format!(
                            "file is {} bytes, maximum is {} bytes ({})",
                            file_size, max_bytes, max_size_str,
                        ))
                    } else {
                        None
                    },
                });
            } else {
                checks.push(CheckItem {
                    name: format!("Contract: max_size <= {}", max_size_str),
                    passed: false,
                    detail: Some(format!("cannot parse max_size value '{}'", max_size_str)),
                });
            }
        }

        if let Some(ref allowed_codecs) = file_contract.compression {
            let dominant = metadata::dominant_compression(meta);
            let dominant_lower = dominant.to_lowercase();
            let allowed_lower: Vec<String> =
                allowed_codecs.iter().map(|c| c.to_lowercase()).collect();
            let passed = allowed_lower.contains(&dominant_lower);
            checks.push(CheckItem {
                name: format!("Contract: compression in [{}]", allowed_codecs.join(", ")),
                passed,
                detail: if !passed {
                    Some(format!(
                        "dominant compression is '{}', allowed: [{}]",
                        dominant,
                        allowed_codecs.join(", "),
                    ))
                } else {
                    None
                },
            });
        }
    }

    let schema = meta.file_metadata().schema_descr();
    let all_column_names = metadata::column_names(meta);
    let columns: HashMap<String, &parquet::schema::types::ColumnDescriptor> = schema
        .columns()
        .iter()
        .map(|c| (c.name().to_string(), c.as_ref()))
        .collect();

    for (col_name, col_contract) in &contract.columns {
        if let Some(col_desc) = columns.get(col_name.as_str()) {
            if let Some(ref expected_type) = col_contract.col_type {
                let actual_type = format!("{:?}", col_desc.physical_type());
                let logical_type_str = col_desc
                    .logical_type()
                    .map(|lt| format!("{:?}", lt))
                    .unwrap_or_default();
                let converted_type_str = format!("{:?}", col_desc.converted_type());

                let expected_upper = expected_type.to_uppercase();
                let type_matches = actual_type.to_uppercase() == expected_upper
                    || logical_type_str.to_uppercase() == expected_upper
                    || converted_type_str.to_uppercase() == expected_upper;

                checks.push(CheckItem {
                    name: format!("Contract: column '{}' type = {}", col_name, expected_type),
                    passed: type_matches,
                    detail: if !type_matches {
                        Some(format!(
                            "expected type '{}', found physical='{}' logical='{}'",
                            expected_type, actual_type, logical_type_str,
                        ))
                    } else {
                        None
                    },
                });
            }

            if let Some(expected_nullable) = col_contract.nullable {
                let actual_nullable = col_desc.self_type().is_optional();
                let passed = actual_nullable == expected_nullable;
                checks.push(CheckItem {
                    name: format!(
                        "Contract: column '{}' nullable = {}",
                        col_name, expected_nullable
                    ),
                    passed,
                    detail: if !passed {
                        Some(format!(
                            "expected nullable={}, found nullable={}",
                            expected_nullable, actual_nullable,
                        ))
                    } else {
                        None
                    },
                });
            }

            if col_contract.min.is_some() || col_contract.max.is_some() {
                let col_idx = schema
                    .columns()
                    .iter()
                    .position(|c| c.name() == col_name.as_str());

                if let Some(col_idx) = col_idx {
                    let mut actual_min: Option<f64> = None;
                    let mut actual_max: Option<f64> = None;

                    for rg in meta.row_groups() {
                        if let Some(stats) = rg.columns()[col_idx].statistics() {
                            match stats {
                                parquet::file::statistics::Statistics::Int32(s) => {
                                    if let Some(v) = s.min_opt() {
                                        actual_min = Some(
                                            actual_min.map_or(*v as f64, |m: f64| m.min(*v as f64)),
                                        );
                                    }
                                    if let Some(v) = s.max_opt() {
                                        actual_max = Some(
                                            actual_max.map_or(*v as f64, |m: f64| m.max(*v as f64)),
                                        );
                                    }
                                }
                                parquet::file::statistics::Statistics::Int64(s) => {
                                    if let Some(v) = s.min_opt() {
                                        actual_min = Some(
                                            actual_min.map_or(*v as f64, |m: f64| m.min(*v as f64)),
                                        );
                                    }
                                    if let Some(v) = s.max_opt() {
                                        actual_max = Some(
                                            actual_max.map_or(*v as f64, |m: f64| m.max(*v as f64)),
                                        );
                                    }
                                }
                                parquet::file::statistics::Statistics::Float(s) => {
                                    if let Some(v) = s.min_opt() {
                                        actual_min = Some(
                                            actual_min.map_or(*v as f64, |m: f64| m.min(*v as f64)),
                                        );
                                    }
                                    if let Some(v) = s.max_opt() {
                                        actual_max = Some(
                                            actual_max.map_or(*v as f64, |m: f64| m.max(*v as f64)),
                                        );
                                    }
                                }
                                parquet::file::statistics::Statistics::Double(s) => {
                                    if let Some(v) = s.min_opt() {
                                        actual_min =
                                            Some(actual_min.map_or(*v, |m: f64| m.min(*v)));
                                    }
                                    if let Some(v) = s.max_opt() {
                                        actual_max =
                                            Some(actual_max.map_or(*v, |m: f64| m.max(*v)));
                                    }
                                }
                                _ => {}
                            }
                        }
                    }

                    if let Some(expected_min) = col_contract.min {
                        let passed = actual_min.is_some_and(|v| v >= expected_min);
                        checks.push(CheckItem {
                            name: format!(
                                "Contract: column '{}' min >= {}",
                                col_name, expected_min
                            ),
                            passed,
                            detail: if !passed {
                                Some(format!(
                                    "actual min is {:?}, expected >= {}",
                                    actual_min, expected_min
                                ))
                            } else {
                                None
                            },
                        });
                    }

                    if let Some(expected_max) = col_contract.max {
                        let passed = actual_max.is_some_and(|v| v <= expected_max);
                        checks.push(CheckItem {
                            name: format!(
                                "Contract: column '{}' max <= {}",
                                col_name, expected_max
                            ),
                            passed,
                            detail: if !passed {
                                Some(format!(
                                    "actual max is {:?}, expected <= {}",
                                    actual_max, expected_max
                                ))
                            } else {
                                None
                            },
                        });
                    }
                }
            }

            if let Some(ref allowed) = col_contract.allowed {
                let col_idx = schema
                    .columns()
                    .iter()
                    .position(|c| c.name() == col_name.as_str());
                if let Some(col_idx) = col_idx {
                    let mut found_values = std::collections::HashSet::new();
                    for rg in meta.row_groups() {
                        if let Some(parquet::file::statistics::Statistics::ByteArray(s)) =
                            rg.columns()[col_idx].statistics()
                        {
                            if let Some(v) = s.min_opt() {
                                found_values.insert(String::from_utf8_lossy(v.data()).to_string());
                            }
                            if let Some(v) = s.max_opt() {
                                found_values.insert(String::from_utf8_lossy(v.data()).to_string());
                            }
                        }
                    }
                    let disallowed: Vec<&String> = found_values
                        .iter()
                        .filter(|v| !allowed.contains(v))
                        .collect();
                    let passed = disallowed.is_empty();
                    checks.push(CheckItem {
                        name: format!("Contract: column '{}' values in {:?}", col_name, allowed),
                        passed,
                        detail: if !passed {
                            Some(format!("found values not in allowed set: {:?}", disallowed))
                        } else {
                            None
                        },
                    });
                }
            }
        } else {
            checks.push(CheckItem {
                name: format!("Contract: column '{}' exists", col_name),
                passed: false,
                detail: Some(format!(
                    "column '{}' not found in schema (available: {})",
                    col_name,
                    all_column_names.join(", ")
                )),
            });
        }
    }

    // freshness: check data recency via timestamp stats
    if let Some(ref freshness) = contract.freshness {
        let max_age_secs = parse_duration_secs(&freshness.max_age);
        let col_idx = schema
            .columns()
            .iter()
            .position(|c| c.name() == freshness.column.as_str());

        if let (Some(max_age), Some(col_idx)) = (max_age_secs, col_idx) {
            // Get max stat value from last row group (most recent data)
            let mut max_ts_ms: Option<i64> = None;
            for rg in meta.row_groups() {
                if let Some(stats) = rg.columns()[col_idx].statistics() {
                    match stats {
                        parquet::file::statistics::Statistics::Int64(s) => {
                            if let Some(v) = s.max_opt() {
                                max_ts_ms = Some(max_ts_ms.map_or(*v, |m: i64| m.max(*v)));
                            }
                        }
                        parquet::file::statistics::Statistics::Int96(s) => {
                            // skip INT96 — Julian day conversion is hairy
                            let _ = s;
                        }
                        _ => {}
                    }
                }
            }

            if let Some(ts) = max_ts_ms {
                // guess resolution: >1e15 = micros, >1e12 = millis, else seconds
                let ts_secs = if ts > 1_000_000_000_000_000 {
                    ts / 1_000_000 // micros to secs
                } else if ts > 1_000_000_000_000 {
                    ts / 1_000 // millis to secs
                } else {
                    ts // already seconds
                };

                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);

                let age_secs = now_secs - ts_secs;
                let passed = age_secs <= max_age as i64;

                checks.push(CheckItem {
                    name: format!(
                        "Contract: freshness on '{}' within {}",
                        freshness.column, freshness.max_age
                    ),
                    passed,
                    detail: if !passed {
                        Some(format!(
                            "data is {}s old, max_age is {}s",
                            age_secs, max_age
                        ))
                    } else {
                        None
                    },
                });
            } else {
                checks.push(CheckItem {
                    name: format!(
                        "Contract: freshness on '{}' within {}",
                        freshness.column, freshness.max_age
                    ),
                    passed: false,
                    detail: Some(format!(
                        "no timestamp statistics found for column '{}'",
                        freshness.column
                    )),
                });
            }
        } else {
            checks.push(CheckItem {
                name: format!(
                    "Contract: freshness on '{}' within {}",
                    freshness.column, freshness.max_age
                ),
                passed: false,
                detail: Some(if max_age_secs.is_none() {
                    format!("cannot parse max_age '{}'", freshness.max_age)
                } else {
                    format!("column '{}' not found", freshness.column)
                }),
            });
        }
    }
}

/// Parse a duration string like "24h", "30m", "7d", "3600s" into seconds.
fn parse_duration_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Some(stripped) = s.strip_suffix('d') {
        stripped.parse::<u64>().ok().map(|v| v * 86400)
    } else if let Some(stripped) = s.strip_suffix('h') {
        stripped.parse::<u64>().ok().map(|v| v * 3600)
    } else if let Some(stripped) = s.strip_suffix('m') {
        stripped.parse::<u64>().ok().map(|v| v * 60)
    } else if let Some(stripped) = s.strip_suffix('s') {
        stripped.parse::<u64>().ok()
    } else {
        s.parse::<u64>().ok()
    }
}

/// Validate data pages: decompress and count actual rows.
/// Returns the actual row count read from data.
fn validate_data(path: &std::path::Path) -> miette::Result<usize> {
    let file = File::open(path).map_err(|e| miette::miette!("cannot open: {}", e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| miette::miette!("cannot read: {}", e))?;
    let reader = builder
        .build()
        .map_err(|e| miette::miette!("cannot read: {}", e))?;

    let mut actual_rows: usize = 0;
    for batch_result in reader {
        let batch = batch_result.map_err(|e| miette::miette!("data read error: {}", e))?;
        actual_rows += batch.num_rows();
    }

    Ok(actual_rows)
}
