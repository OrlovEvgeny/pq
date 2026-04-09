use crate::cli::{SchemaArgs, SchemaSubcommand};
use crate::input::{resolve_inputs_report, resolve_inputs_with_config};
use crate::output::table;
use crate::output::{OutputConfig, OutputFormat};
use crate::parquet_ext::metadata;
use parquet::schema::types::Type;
use serde::Serialize;
use std::io::Write;

#[derive(Serialize)]
struct SchemaField {
    name: String,
    physical_type: Option<String>,
    logical_type: Option<String>,
    repetition: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_rep_level: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_def_level: Option<i16>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    children: Vec<SchemaField>,
}

pub fn execute(args: &SchemaArgs, output: &mut OutputConfig) -> miette::Result<()> {
    if let Some(subcmd) = &args.subcommand {
        return match subcmd {
            SchemaSubcommand::Diff(diff_args) => execute_diff(diff_args, output),
            SchemaSubcommand::Extract(extract_args) => execute_extract(extract_args, output),
        };
    }

    if args.files.is_empty() {
        return Err(miette::miette!("no files specified for schema command"));
    }

    let sp = output.spinner("Loading");
    let sources = resolve_inputs_report(&args.files, &output.cloud_config, &mut |msg| {
        sp.set_message(msg);
    })?;
    sp.finish_and_clear();

    // --arrow: print the Arrow schema representation and return early
    if args.arrow {
        for source in &sources {
            let file = std::fs::File::open(source.path())
                .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;
            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|e| {
                        miette::miette!("cannot read '{}': {}", source.display_name(), e)
                    })?;
            let arrow_schema = builder.schema();

            if sources.len() > 1 {
                writeln!(
                    output.writer,
                    "{}",
                    table::section_header("Arrow Schema", &source.display_name(), &output.theme)
                )
                .map_err(|e| miette::miette!("{}", e))?;
            }
            writeln!(output.writer, "{}", arrow_schema).map_err(|e| miette::miette!("{}", e))?;
        }
        return Ok(());
    }

    for source in &sources {
        let meta = metadata::read_metadata(source.path())?;
        let schema = meta.file_metadata().schema_descr().root_schema().clone();

        match output.format {
            OutputFormat::Table => {
                if sources.len() > 1 {
                    writeln!(
                        output.writer,
                        "{}",
                        table::section_header("Schema", &source.display_name(), &output.theme)
                    )
                    .map_err(|e| miette::miette!("{}", e))?;
                }
                print_schema_table(&schema, &meta, args.expand, output)?;
            }
            OutputFormat::Json | OutputFormat::Parquet => {
                let fields = build_schema_tree(&schema);
                crate::output::json::write_json(&mut output.writer, &fields)?;
            }
            OutputFormat::Jsonl => {
                let fields = build_schema_tree(&schema);
                for field in &fields {
                    crate::output::json::write_jsonl(&mut output.writer, field)?;
                }
            }
            OutputFormat::Csv | OutputFormat::Tsv => {
                let columns = metadata::detailed_column_info(&meta);
                let headers = [
                    "index",
                    "name",
                    "physical_type",
                    "logical_type",
                    "repetition",
                    "max_def_level",
                    "max_rep_level",
                    "encoding",
                ];
                let rows: Vec<Vec<String>> = columns
                    .iter()
                    .map(|c| {
                        vec![
                            c.index.clone(),
                            c.path.clone(),
                            c.physical_type.clone(),
                            c.logical_type.clone(),
                            c.repetition.clone(),
                            c.max_def_level.to_string(),
                            c.max_rep_level.to_string(),
                            c.encoding.clone(),
                        ]
                    })
                    .collect();
                if output.format == OutputFormat::Csv {
                    crate::output::csv_output::write_csv(&mut output.writer, &headers, &rows)?;
                } else {
                    crate::output::csv_output::write_tsv(&mut output.writer, &headers, &rows)?;
                }
            }
        }
    }

    Ok(())
}

fn print_schema_table(
    schema: &Type,
    meta: &parquet::file::metadata::ParquetMetaData,
    expand: bool,
    output: &mut OutputConfig,
) -> miette::Result<()> {
    if expand {
        // Expanded view: show full tree structure
        print_type_tree(schema, 0, output)?;
    } else {
        // Flat view: one row per leaf column with detailed info
        let columns = metadata::detailed_column_info(meta);
        let headers = ["#", "Name", "Physical", "Logical", "Rep", "Def", "Encoding"];
        let rows: Vec<Vec<String>> = columns
            .iter()
            .map(|c| {
                vec![
                    c.index.clone(),
                    c.path.clone(),
                    c.physical_type.clone(),
                    c.logical_type.clone(),
                    c.repetition.clone(),
                    c.max_def_level.to_string(),
                    c.encoding.clone(),
                ]
            })
            .collect();

        writeln!(
            output.writer,
            "{}",
            table::data_table(&headers, &rows, &output.theme)
        )
        .map_err(|e| miette::miette!("{}", e))?;
    }
    Ok(())
}

fn print_type_tree(ty: &Type, depth: usize, output: &mut OutputConfig) -> miette::Result<()> {
    let indent = "  ".repeat(depth);
    match ty {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            let rep = format!("{:?}", basic_info.repetition());
            let logical = basic_info
                .logical_type()
                .map(|l| format!(" ({:?})", l))
                .unwrap_or_default();
            writeln!(
                output.writer,
                "{}{}: {:?}{} [{}]",
                indent,
                basic_info.name(),
                physical_type,
                logical,
                rep
            )
            .map_err(|e| miette::miette!("{}", e))?;
        }
        Type::GroupType {
            basic_info, fields, ..
        } => {
            if depth > 0 {
                let logical = basic_info
                    .logical_type()
                    .map(|l| format!(" ({:?})", l))
                    .unwrap_or_default();
                writeln!(
                    output.writer,
                    "{}{}:{} [{:?}]",
                    indent,
                    basic_info.name(),
                    logical,
                    basic_info.repetition(),
                )
                .map_err(|e| miette::miette!("{}", e))?;
            }
            for field in fields {
                print_type_tree(field, depth + 1, output)?;
            }
        }
    }
    Ok(())
}

fn build_schema_tree(ty: &Type) -> Vec<SchemaField> {
    match ty {
        Type::GroupType { fields, .. } => fields.iter().map(|f| type_to_field(f)).collect(),
        Type::PrimitiveType { .. } => vec![type_to_field(ty)],
    }
}

fn type_to_field(ty: &Type) -> SchemaField {
    match ty {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => SchemaField {
            name: basic_info.name().to_string(),
            physical_type: Some(format!("{:?}", physical_type)),
            logical_type: basic_info.logical_type().map(|l| format!("{:?}", l)),
            repetition: format!("{:?}", basic_info.repetition()),
            max_rep_level: None,
            max_def_level: None,
            children: vec![],
        },
        Type::GroupType {
            basic_info, fields, ..
        } => SchemaField {
            name: basic_info.name().to_string(),
            physical_type: None,
            logical_type: basic_info.logical_type().map(|l| format!("{:?}", l)),
            repetition: format!("{:?}", basic_info.repetition()),
            max_rep_level: None,
            max_def_level: None,
            children: fields.iter().map(|f| type_to_field(f)).collect(),
        },
    }
}

fn execute_diff(
    args: &crate::cli::SchemaDiffArgs,
    output: &mut OutputConfig,
) -> miette::Result<()> {
    let sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;
    if sources.len() < 2 {
        return Err(miette::miette!(
            "schema diff requires at least 2 files (got {})",
            sources.len()
        ));
    }

    // When more than 2 files are provided, use the first (or --ref) as reference
    // and diff each subsequent file against it, showing a summary.
    if sources.len() > 2 {
        return execute_multi_diff(args, output);
    }

    diff_two_files(
        &sources[0].display_name(),
        sources[0].path(),
        &sources[1].display_name(),
        sources[1].path(),
        args.changes_only,
        output,
    )
}

/// Diff more than two files: use the first (or --ref) as reference and
/// compare each subsequent file against it.
fn execute_multi_diff(
    args: &crate::cli::SchemaDiffArgs,
    output: &mut OutputConfig,
) -> miette::Result<()> {
    let mut sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;

    // Determine the reference file
    let (ref_name, ref_path, compare_sources) = if let Some(ref ref_file) = args.reference {
        let ref_sources =
            resolve_inputs_with_config(std::slice::from_ref(ref_file), &output.cloud_config)?;
        if ref_sources.is_empty() {
            return Err(miette::miette!("reference file not found: {}", ref_file));
        }
        (
            ref_sources[0].display_name(),
            ref_sources[0].path().to_path_buf(),
            sources,
        )
    } else {
        let reference = sources.remove(0);
        let ref_name = reference.display_name();
        let ref_path = reference.path().to_path_buf();
        (ref_name, ref_path, sources)
    };

    let ref_meta = metadata::read_metadata(&ref_path)?;
    let ref_cols = metadata::column_info(&ref_meta);
    let ref_names: std::collections::HashSet<&str> =
        ref_cols.iter().map(|c| c.name.as_str()).collect();

    writeln!(
        output.writer,
        "Reference: {} ({} columns)",
        ref_name,
        ref_cols.len()
    )
    .map_err(|e| miette::miette!("{}", e))?;
    writeln!(output.writer).map_err(|e| miette::miette!("{}", e))?;

    for source in &compare_sources {
        let meta = metadata::read_metadata(source.path())?;
        let cols = metadata::column_info(&meta);
        let other_names: std::collections::HashSet<&str> =
            cols.iter().map(|c| c.name.as_str()).collect();

        let added: Vec<&str> = other_names
            .iter()
            .filter(|n| !ref_names.contains(**n))
            .copied()
            .collect();
        let removed: Vec<&str> = ref_names
            .iter()
            .filter(|n| !other_names.contains(**n))
            .copied()
            .collect();

        let mut type_changed = 0usize;
        for rc in &ref_cols {
            if let Some(oc) = cols.iter().find(|c| c.name == rc.name)
                && (rc.type_name != oc.type_name || rc.nullable != oc.nullable)
            {
                type_changed += 1;
            }
        }

        let status = if added.is_empty() && removed.is_empty() && type_changed == 0 {
            "identical"
        } else {
            "differs"
        };

        let theme = &output.theme;
        writeln!(
            output.writer,
            "  {} vs {}: {} {} {} {}",
            ref_name,
            source.display_name(),
            table::diff_status_chip(status, theme),
            theme.value_chip("ADDED", added.len(), crate::output::theme::Tone::Success),
            theme.value_chip("REMOVED", removed.len(), crate::output::theme::Tone::Danger),
            theme.value_chip("CHANGED", type_changed, crate::output::theme::Tone::Warn)
        )
        .map_err(|e| miette::miette!("{}", e))?;

        if !added.is_empty() {
            writeln!(
                output.writer,
                "    {} {}",
                theme.chip("Added", crate::output::theme::Tone::Success),
                theme.diff.added.apply_to(added.join(", "))
            )
            .map_err(|e| miette::miette!("{}", e))?;
        }
        if !removed.is_empty() {
            writeln!(
                output.writer,
                "    {} {}",
                theme.chip("Removed", crate::output::theme::Tone::Danger),
                theme.diff.removed.apply_to(removed.join(", "))
            )
            .map_err(|e| miette::miette!("{}", e))?;
        }
        if type_changed > 0 {
            writeln!(
                output.writer,
                "    {} {}",
                theme.chip("Changed", crate::output::theme::Tone::Warn),
                theme.diff.changed.apply_to(format!(
                    "{type_changed} column(s) have type/nullability differences"
                ))
            )
            .map_err(|e| miette::miette!("{}", e))?;
        }
    }

    Ok(())
}

/// Diff exactly two files (the original behaviour).
fn diff_two_files(
    name_a: &str,
    path_a: &std::path::Path,
    name_b: &str,
    path_b: &std::path::Path,
    changes_only: bool,
    output: &mut OutputConfig,
) -> miette::Result<()> {
    let meta_a = metadata::read_metadata(path_a)?;
    let meta_b = metadata::read_metadata(path_b)?;

    let cols_a = metadata::column_info(&meta_a);
    let cols_b = metadata::column_info(&meta_b);

    let names_a: std::collections::HashSet<&str> = cols_a.iter().map(|c| c.name.as_str()).collect();
    let _names_b: std::collections::HashSet<&str> =
        cols_b.iter().map(|c| c.name.as_str()).collect();

    // Collect all unique column names in order
    let mut all_names: Vec<&str> = Vec::new();
    for c in &cols_a {
        all_names.push(&c.name);
    }
    for c in &cols_b {
        if !names_a.contains(c.name.as_str()) {
            all_names.push(&c.name);
        }
    }

    #[derive(Serialize)]
    struct DiffRow {
        column: String,
        file_a: Option<String>,
        file_b: Option<String>,
        status: String,
    }

    let mut diff_rows: Vec<DiffRow> = Vec::new();
    let mut table_rows: Vec<Vec<String>> = Vec::new();

    for name in &all_names {
        let a = cols_a.iter().find(|c| c.name == *name);
        let b = cols_b.iter().find(|c| c.name == *name);

        let (file_a_str, file_b_str, status) = match (a, b) {
            (Some(a), Some(b)) => {
                if a.type_name == b.type_name && a.nullable == b.nullable {
                    if changes_only {
                        continue;
                    }
                    (a.type_name.clone(), b.type_name.clone(), "same".to_string())
                } else if a.type_name != b.type_name {
                    (
                        a.type_name.clone(),
                        b.type_name.clone(),
                        "type changed".to_string(),
                    )
                } else {
                    (
                        format!(
                            "{}{}",
                            a.type_name,
                            if a.nullable { "" } else { " NOT NULL" }
                        ),
                        format!(
                            "{}{}",
                            b.type_name,
                            if b.nullable { "" } else { " NOT NULL" }
                        ),
                        "nullability changed".to_string(),
                    )
                }
            }
            (Some(a), None) => (
                a.type_name.clone(),
                crate::output::symbols::symbols().emdash.to_string(),
                "removed".to_string(),
            ),
            (None, Some(b)) => (
                crate::output::symbols::symbols().emdash.to_string(),
                b.type_name.clone(),
                "added".to_string(),
            ),
            (None, None) => unreachable!(),
        };

        diff_rows.push(DiffRow {
            column: name.to_string(),
            file_a: Some(file_a_str.clone()),
            file_b: Some(file_b_str.clone()),
            status: status.clone(),
        });
        let styled_status = table::diff_status_chip(&status, &output.theme);
        table_rows.push(vec![
            name.to_string(),
            file_a_str,
            file_b_str,
            styled_status,
        ]);
    }

    match output.format {
        OutputFormat::Table => {
            let headers = ["Column", name_a, name_b, "Status"];
            writeln!(
                output.writer,
                "{}",
                table::data_table(&headers, &table_rows, &output.theme)
            )
            .map_err(|e| miette::miette!("{}", e))?;
        }
        OutputFormat::Json | OutputFormat::Parquet => {
            crate::output::json::write_json(&mut output.writer, &diff_rows)?;
        }
        OutputFormat::Jsonl => {
            for row in &diff_rows {
                crate::output::json::write_jsonl(&mut output.writer, row)?;
            }
        }
        OutputFormat::Csv | OutputFormat::Tsv => {
            let headers = ["column", "file_a", "file_b", "status"];
            let csv_rows: Vec<Vec<String>> = diff_rows
                .iter()
                .map(|r| {
                    vec![
                        r.column.clone(),
                        r.file_a.clone().unwrap_or_default(),
                        r.file_b.clone().unwrap_or_default(),
                        r.status.clone(),
                    ]
                })
                .collect();
            if output.format == OutputFormat::Csv {
                crate::output::csv_output::write_csv(&mut output.writer, &headers, &csv_rows)?;
            } else {
                crate::output::csv_output::write_tsv(&mut output.writer, &headers, &csv_rows)?;
            }
        }
    }

    Ok(())
}

fn execute_extract(
    args: &crate::cli::SchemaExtractArgs,
    output: &mut OutputConfig,
) -> miette::Result<()> {
    let sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;

    // --arrow: print the Arrow schema representation and return early
    if args.arrow {
        for source in &sources {
            let file = std::fs::File::open(source.path())
                .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;
            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|e| {
                        miette::miette!("cannot read '{}': {}", source.display_name(), e)
                    })?;
            let arrow_schema = builder.schema();

            if sources.len() > 1 {
                writeln!(
                    output.writer,
                    "{}",
                    table::section_header("Arrow Schema", &source.display_name(), &output.theme)
                )
                .map_err(|e| miette::miette!("{}", e))?;
            }
            writeln!(output.writer, "{}", arrow_schema).map_err(|e| miette::miette!("{}", e))?;
        }
        return Ok(());
    }

    let meta = metadata::read_metadata(sources[0].path())?;
    let schema = meta.file_metadata().schema_descr().root_schema().clone();

    // --ddl: generate DDL output instead of JSON
    if let Some(ref ddl_dialect) = args.ddl {
        let columns = metadata::column_info(&meta);
        let table_name = sources[0]
            .path()
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("table_name");

        let ddl_text = match ddl_dialect.to_lowercase().as_str() {
            "duckdb" => generate_duckdb_ddl(table_name, &schema, &columns),
            "spark" => generate_spark_schema(&schema, &columns),
            "clickhouse" | "ch" => generate_clickhouse_ddl(table_name, &schema, &columns),
            "postgres" | "postgresql" | "pg" => {
                generate_postgres_ddl(table_name, &schema, &columns)
            }
            "bigquery" | "bq" => generate_bigquery_ddl(table_name, &schema, &columns),
            "snowflake" | "sf" => generate_snowflake_ddl(table_name, &schema, &columns),
            "redshift" | "rs" => generate_redshift_ddl(table_name, &schema, &columns),
            "mysql" => generate_mysql_ddl(table_name, &schema, &columns),
            other => {
                return Err(miette::miette!(
                    "unsupported DDL dialect: '{}' (supported: duckdb, spark, clickhouse, postgres, bigquery, snowflake, redshift, mysql)",
                    other
                ));
            }
        };
        let display = crate::output::highlight::highlight_sql(&ddl_text, &output.theme);
        writeln!(output.writer, "{}", display).map_err(|e| miette::miette!("{}", e))?;
        return Ok(());
    }

    let fields = build_schema_tree(&schema);
    crate::output::json::write_json(&mut output.writer, &fields)?;
    Ok(())
}

/// Map a Parquet column to a DuckDB SQL type.
fn parquet_to_duckdb_type(col_type: &Type) -> String {
    match col_type {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            // Check logical type first for more precise mapping
            if let Some(logical) = basic_info.logical_type() {
                return match logical {
                    parquet::basic::LogicalType::String => "VARCHAR".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 8,
                        is_signed: true,
                    } => "TINYINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 16,
                        is_signed: true,
                    } => "SMALLINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 32,
                        is_signed: true,
                    } => "INTEGER".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 64,
                        is_signed: true,
                    } => "BIGINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        is_signed: false, ..
                    } => "BIGINT".to_string(),
                    parquet::basic::LogicalType::Timestamp { .. } => "TIMESTAMP".to_string(),
                    parquet::basic::LogicalType::Date => "DATE".to_string(),
                    parquet::basic::LogicalType::Time { .. } => "TIME".to_string(),
                    parquet::basic::LogicalType::Decimal { precision, scale } => {
                        format!("DECIMAL({}, {})", precision, scale)
                    }
                    parquet::basic::LogicalType::Uuid => "UUID".to_string(),
                    parquet::basic::LogicalType::Json => "VARCHAR".to_string(),
                    parquet::basic::LogicalType::Bson => "BLOB".to_string(),
                    parquet::basic::LogicalType::Enum => "VARCHAR".to_string(),
                    _ => physical_to_duckdb(*physical_type),
                };
            }
            physical_to_duckdb(*physical_type)
        }
        Type::GroupType { .. } => "STRUCT".to_string(),
    }
}

/// Map a Parquet physical type to a DuckDB SQL type.
fn physical_to_duckdb(physical: parquet::basic::Type) -> String {
    match physical {
        parquet::basic::Type::BOOLEAN => "BOOLEAN".to_string(),
        parquet::basic::Type::INT32 => "INTEGER".to_string(),
        parquet::basic::Type::INT64 => "BIGINT".to_string(),
        parquet::basic::Type::INT96 => "TIMESTAMP".to_string(),
        parquet::basic::Type::FLOAT => "FLOAT".to_string(),
        parquet::basic::Type::DOUBLE => "DOUBLE".to_string(),
        parquet::basic::Type::BYTE_ARRAY => "BLOB".to_string(),
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => "BLOB".to_string(),
    }
}

/// Generate a DuckDB-compatible CREATE TABLE statement.
fn generate_duckdb_ddl(
    table_name: &str,
    schema: &Type,
    columns: &[metadata::ColumnInfo],
) -> String {
    let mut ddl = format!("CREATE TABLE {} (\n", table_name);

    if let Type::GroupType { fields, .. } = schema {
        let col_defs: Vec<String> = fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let sql_type = parquet_to_duckdb_type(field.as_ref());
                let nullable = columns.get(i).map(|c| c.nullable).unwrap_or(true);
                let not_null = if !nullable { " NOT NULL" } else { "" };
                let name = match &**field {
                    Type::PrimitiveType { basic_info, .. } | Type::GroupType { basic_info, .. } => {
                        basic_info.name()
                    }
                };
                format!("  {} {}{}", name, sql_type, not_null)
            })
            .collect();

        ddl.push_str(&col_defs.join(",\n"));
    }

    ddl.push_str("\n);");
    ddl
}

/// Map a Parquet column to a PySpark type string.
fn parquet_to_spark_type(col_type: &Type) -> String {
    match col_type {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            if let Some(logical) = basic_info.logical_type() {
                return match logical {
                    parquet::basic::LogicalType::String => "StringType()".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 8,
                        is_signed: true,
                    } => "ByteType()".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 16,
                        is_signed: true,
                    } => "ShortType()".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 32,
                        is_signed: true,
                    } => "IntegerType()".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 64,
                        is_signed: true,
                    } => "LongType()".to_string(),
                    parquet::basic::LogicalType::Integer {
                        is_signed: false, ..
                    } => "LongType()".to_string(),
                    parquet::basic::LogicalType::Timestamp { .. } => "TimestampType()".to_string(),
                    parquet::basic::LogicalType::Date => "DateType()".to_string(),
                    parquet::basic::LogicalType::Decimal { precision, scale } => {
                        format!("DecimalType({}, {})", precision, scale)
                    }
                    parquet::basic::LogicalType::Json => "StringType()".to_string(),
                    parquet::basic::LogicalType::Enum => "StringType()".to_string(),
                    _ => physical_to_spark(*physical_type),
                };
            }
            physical_to_spark(*physical_type)
        }
        Type::GroupType { .. } => "StructType()".to_string(),
    }
}

/// Map a Parquet physical type to a PySpark type string.
fn physical_to_spark(physical: parquet::basic::Type) -> String {
    match physical {
        parquet::basic::Type::BOOLEAN => "BooleanType()".to_string(),
        parquet::basic::Type::INT32 => "IntegerType()".to_string(),
        parquet::basic::Type::INT64 => "LongType()".to_string(),
        parquet::basic::Type::INT96 => "TimestampType()".to_string(),
        parquet::basic::Type::FLOAT => "FloatType()".to_string(),
        parquet::basic::Type::DOUBLE => "DoubleType()".to_string(),
        parquet::basic::Type::BYTE_ARRAY => "BinaryType()".to_string(),
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => "BinaryType()".to_string(),
    }
}

/// Generate a PySpark StructType schema definition.
fn generate_spark_schema(schema: &Type, columns: &[metadata::ColumnInfo]) -> String {
    let mut out = "StructType([\n".to_string();

    if let Type::GroupType { fields, .. } = schema {
        let field_defs: Vec<String> = fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let spark_type = parquet_to_spark_type(field.as_ref());
                let nullable = columns.get(i).map(|c| c.nullable).unwrap_or(true);
                let name = match &**field {
                    Type::PrimitiveType { basic_info, .. } | Type::GroupType { basic_info, .. } => {
                        basic_info.name()
                    }
                };
                format!(
                    "  StructField(\"{}\", {}, {})",
                    name,
                    spark_type,
                    if nullable { "True" } else { "False" }
                )
            })
            .collect();

        out.push_str(&field_defs.join(",\n"));
        out.push('\n');
    }

    out.push_str("])");
    out
}

// ---------------------------------------------------------------------------
// ClickHouse
// ---------------------------------------------------------------------------

/// Map a Parquet column to a ClickHouse SQL type.
fn parquet_to_clickhouse_type(col_type: &Type) -> String {
    match col_type {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            if let Some(logical) = basic_info.logical_type() {
                return match logical {
                    parquet::basic::LogicalType::String => "String".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 8,
                        is_signed: true,
                    } => "Int8".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 16,
                        is_signed: true,
                    } => "Int16".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 32,
                        is_signed: true,
                    } => "Int32".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 64,
                        is_signed: true,
                    } => "Int64".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 8,
                        is_signed: false,
                    } => "UInt8".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 16,
                        is_signed: false,
                    } => "UInt16".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 32,
                        is_signed: false,
                    } => "UInt32".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 64,
                        is_signed: false,
                    } => "UInt64".to_string(),
                    parquet::basic::LogicalType::Timestamp {
                        unit: parquet::basic::TimeUnit::MILLIS(_),
                        ..
                    } => "DateTime64(3)".to_string(),
                    parquet::basic::LogicalType::Timestamp {
                        unit: parquet::basic::TimeUnit::MICROS(_),
                        ..
                    } => "DateTime64(6)".to_string(),
                    parquet::basic::LogicalType::Timestamp {
                        unit: parquet::basic::TimeUnit::NANOS(_),
                        ..
                    } => "DateTime64(9)".to_string(),
                    parquet::basic::LogicalType::Date => "Date32".to_string(),
                    parquet::basic::LogicalType::Time { .. } => "DateTime64(6)".to_string(),
                    parquet::basic::LogicalType::Decimal { precision, scale } => {
                        format!("Decimal({}, {})", precision, scale)
                    }
                    parquet::basic::LogicalType::Uuid => "UUID".to_string(),
                    parquet::basic::LogicalType::Json => "String".to_string(),
                    parquet::basic::LogicalType::List => "Array(String)".to_string(),
                    parquet::basic::LogicalType::Map => "Map(String, String)".to_string(),
                    _ => physical_to_clickhouse(*physical_type),
                };
            }
            physical_to_clickhouse(*physical_type)
        }
        Type::GroupType { .. } => "Tuple()".to_string(),
    }
}

/// Map a Parquet physical type to a ClickHouse SQL type.
fn physical_to_clickhouse(physical: parquet::basic::Type) -> String {
    match physical {
        parquet::basic::Type::BOOLEAN => "Bool".to_string(),
        parquet::basic::Type::INT32 => "Int32".to_string(),
        parquet::basic::Type::INT64 => "Int64".to_string(),
        parquet::basic::Type::INT96 => "DateTime64(9)".to_string(),
        parquet::basic::Type::FLOAT => "Float32".to_string(),
        parquet::basic::Type::DOUBLE => "Float64".to_string(),
        parquet::basic::Type::BYTE_ARRAY => "String".to_string(),
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => "FixedString(16)".to_string(),
    }
}

/// Generate a ClickHouse-compatible CREATE TABLE statement.
fn generate_clickhouse_ddl(
    table_name: &str,
    schema: &Type,
    columns: &[metadata::ColumnInfo],
) -> String {
    let mut ddl = format!("CREATE TABLE {} (\n", table_name);

    if let Type::GroupType { fields, .. } = schema {
        let col_defs: Vec<String> = fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let sql_type = parquet_to_clickhouse_type(field.as_ref());
                let nullable = columns.get(i).map(|c| c.nullable).unwrap_or(true);
                let name = match &**field {
                    Type::PrimitiveType { basic_info, .. } | Type::GroupType { basic_info, .. } => {
                        basic_info.name()
                    }
                };
                let col_type = if nullable {
                    format!("Nullable({})", sql_type)
                } else {
                    sql_type
                };
                format!("  {} {}", name, col_type)
            })
            .collect();

        ddl.push_str(&col_defs.join(",\n"));
    }

    ddl.push_str("\n) ENGINE = MergeTree()\nORDER BY tuple();");
    ddl
}

// ---------------------------------------------------------------------------
// PostgreSQL
// ---------------------------------------------------------------------------

/// Map a Parquet column to a PostgreSQL SQL type.
fn parquet_to_postgres_type(col_type: &Type) -> String {
    match col_type {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            if let Some(logical) = basic_info.logical_type() {
                return match logical {
                    parquet::basic::LogicalType::String => "TEXT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 8,
                        is_signed: true,
                    } => "SMALLINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 16,
                        is_signed: true,
                    } => "SMALLINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 32,
                        is_signed: true,
                    } => "INTEGER".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 64,
                        is_signed: true,
                    } => "BIGINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        is_signed: false, ..
                    } => "BIGINT".to_string(),
                    parquet::basic::LogicalType::Timestamp { .. } => "TIMESTAMP".to_string(),
                    parquet::basic::LogicalType::Date => "DATE".to_string(),
                    parquet::basic::LogicalType::Time { .. } => "TIME".to_string(),
                    parquet::basic::LogicalType::Decimal { precision, scale } => {
                        format!("NUMERIC({}, {})", precision, scale)
                    }
                    parquet::basic::LogicalType::Uuid => "UUID".to_string(),
                    parquet::basic::LogicalType::Json => "JSONB".to_string(),
                    parquet::basic::LogicalType::List => "JSONB".to_string(),
                    parquet::basic::LogicalType::Map => "JSONB".to_string(),
                    _ => physical_to_postgres(*physical_type),
                };
            }
            physical_to_postgres(*physical_type)
        }
        Type::GroupType { .. } => "JSONB".to_string(),
    }
}

/// Map a Parquet physical type to a PostgreSQL SQL type.
fn physical_to_postgres(physical: parquet::basic::Type) -> String {
    match physical {
        parquet::basic::Type::BOOLEAN => "BOOLEAN".to_string(),
        parquet::basic::Type::INT32 => "INTEGER".to_string(),
        parquet::basic::Type::INT64 => "BIGINT".to_string(),
        parquet::basic::Type::INT96 => "TIMESTAMP".to_string(),
        parquet::basic::Type::FLOAT => "REAL".to_string(),
        parquet::basic::Type::DOUBLE => "DOUBLE PRECISION".to_string(),
        parquet::basic::Type::BYTE_ARRAY => "BYTEA".to_string(),
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => "BYTEA".to_string(),
    }
}

/// Generate a PostgreSQL-compatible CREATE TABLE statement.
fn generate_postgres_ddl(
    table_name: &str,
    schema: &Type,
    columns: &[metadata::ColumnInfo],
) -> String {
    let mut ddl = format!("CREATE TABLE {} (\n", table_name);

    if let Type::GroupType { fields, .. } = schema {
        let col_defs: Vec<String> = fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let sql_type = parquet_to_postgres_type(field.as_ref());
                let nullable = columns.get(i).map(|c| c.nullable).unwrap_or(true);
                let not_null = if !nullable { " NOT NULL" } else { "" };
                let name = match &**field {
                    Type::PrimitiveType { basic_info, .. } | Type::GroupType { basic_info, .. } => {
                        basic_info.name()
                    }
                };
                format!("  {} {}{}", name, sql_type, not_null)
            })
            .collect();

        ddl.push_str(&col_defs.join(",\n"));
    }

    ddl.push_str("\n);");
    ddl
}

// ---------------------------------------------------------------------------
// BigQuery
// ---------------------------------------------------------------------------

/// Map a Parquet column to a BigQuery SQL type.
fn parquet_to_bigquery_type(col_type: &Type) -> String {
    match col_type {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            if let Some(logical) = basic_info.logical_type() {
                return match logical {
                    parquet::basic::LogicalType::String => "STRING".to_string(),
                    parquet::basic::LogicalType::Integer { .. } => "INT64".to_string(),
                    parquet::basic::LogicalType::Timestamp { .. } => "TIMESTAMP".to_string(),
                    parquet::basic::LogicalType::Date => "DATE".to_string(),
                    parquet::basic::LogicalType::Time { .. } => "TIME".to_string(),
                    parquet::basic::LogicalType::Decimal { .. } => "NUMERIC".to_string(),
                    parquet::basic::LogicalType::Uuid => "STRING".to_string(),
                    parquet::basic::LogicalType::Json => "JSON".to_string(),
                    parquet::basic::LogicalType::List => "JSON".to_string(),
                    parquet::basic::LogicalType::Map => "JSON".to_string(),
                    _ => physical_to_bigquery(*physical_type),
                };
            }
            physical_to_bigquery(*physical_type)
        }
        Type::GroupType { .. } => "STRUCT".to_string(),
    }
}

/// Map a Parquet physical type to a BigQuery SQL type.
fn physical_to_bigquery(physical: parquet::basic::Type) -> String {
    match physical {
        parquet::basic::Type::BOOLEAN => "BOOL".to_string(),
        parquet::basic::Type::INT32 => "INT64".to_string(),
        parquet::basic::Type::INT64 => "INT64".to_string(),
        parquet::basic::Type::INT96 => "TIMESTAMP".to_string(),
        parquet::basic::Type::FLOAT => "FLOAT64".to_string(),
        parquet::basic::Type::DOUBLE => "FLOAT64".to_string(),
        parquet::basic::Type::BYTE_ARRAY => "BYTES".to_string(),
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => "BYTES".to_string(),
    }
}

/// Generate a BigQuery-compatible CREATE TABLE statement.
fn generate_bigquery_ddl(
    table_name: &str,
    schema: &Type,
    columns: &[metadata::ColumnInfo],
) -> String {
    let mut ddl = format!("CREATE TABLE {} (\n", table_name);

    if let Type::GroupType { fields, .. } = schema {
        let col_defs: Vec<String> = fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let sql_type = parquet_to_bigquery_type(field.as_ref());
                let nullable = columns.get(i).map(|c| c.nullable).unwrap_or(true);
                let not_null = if !nullable { " NOT NULL" } else { "" };
                let name = match &**field {
                    Type::PrimitiveType { basic_info, .. } | Type::GroupType { basic_info, .. } => {
                        basic_info.name()
                    }
                };
                format!("  {} {}{}", name, sql_type, not_null)
            })
            .collect();

        ddl.push_str(&col_defs.join(",\n"));
    }

    ddl.push_str("\n);");
    ddl
}

// ---------------------------------------------------------------------------
// Snowflake
// ---------------------------------------------------------------------------

/// Map a Parquet column to a Snowflake SQL type.
fn parquet_to_snowflake_type(col_type: &Type) -> String {
    match col_type {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            if let Some(logical) = basic_info.logical_type() {
                return match logical {
                    parquet::basic::LogicalType::String => "VARCHAR".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 8,
                        is_signed: true,
                    } => "INTEGER".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 16,
                        is_signed: true,
                    } => "INTEGER".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 32,
                        is_signed: true,
                    } => "INTEGER".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 64,
                        is_signed: true,
                    } => "BIGINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        is_signed: false, ..
                    } => "BIGINT".to_string(),
                    parquet::basic::LogicalType::Timestamp { .. } => "TIMESTAMP_NTZ".to_string(),
                    parquet::basic::LogicalType::Date => "DATE".to_string(),
                    parquet::basic::LogicalType::Time { .. } => "TIME".to_string(),
                    parquet::basic::LogicalType::Decimal { precision, scale } => {
                        format!("NUMBER({}, {})", precision, scale)
                    }
                    parquet::basic::LogicalType::Uuid => "VARCHAR".to_string(),
                    parquet::basic::LogicalType::Json => "VARIANT".to_string(),
                    parquet::basic::LogicalType::List => "ARRAY".to_string(),
                    parquet::basic::LogicalType::Map => "OBJECT".to_string(),
                    _ => physical_to_snowflake(*physical_type),
                };
            }
            physical_to_snowflake(*physical_type)
        }
        Type::GroupType { .. } => "OBJECT".to_string(),
    }
}

/// Map a Parquet physical type to a Snowflake SQL type.
fn physical_to_snowflake(physical: parquet::basic::Type) -> String {
    match physical {
        parquet::basic::Type::BOOLEAN => "BOOLEAN".to_string(),
        parquet::basic::Type::INT32 => "INTEGER".to_string(),
        parquet::basic::Type::INT64 => "BIGINT".to_string(),
        parquet::basic::Type::INT96 => "TIMESTAMP_NTZ".to_string(),
        parquet::basic::Type::FLOAT => "FLOAT".to_string(),
        parquet::basic::Type::DOUBLE => "DOUBLE".to_string(),
        parquet::basic::Type::BYTE_ARRAY => "BINARY".to_string(),
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => "BINARY".to_string(),
    }
}

/// Generate a Snowflake-compatible CREATE TABLE statement.
fn generate_snowflake_ddl(
    table_name: &str,
    schema: &Type,
    columns: &[metadata::ColumnInfo],
) -> String {
    let mut ddl = format!("CREATE TABLE {} (\n", table_name);

    if let Type::GroupType { fields, .. } = schema {
        let col_defs: Vec<String> = fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let sql_type = parquet_to_snowflake_type(field.as_ref());
                let nullable = columns.get(i).map(|c| c.nullable).unwrap_or(true);
                let not_null = if !nullable { " NOT NULL" } else { "" };
                let name = match &**field {
                    Type::PrimitiveType { basic_info, .. } | Type::GroupType { basic_info, .. } => {
                        basic_info.name()
                    }
                };
                format!("  {} {}{}", name, sql_type, not_null)
            })
            .collect();

        ddl.push_str(&col_defs.join(",\n"));
    }

    ddl.push_str("\n);");
    ddl
}

// ---------------------------------------------------------------------------
// Redshift
// ---------------------------------------------------------------------------

/// Map a Parquet column to a Redshift SQL type.
fn parquet_to_redshift_type(col_type: &Type) -> String {
    match col_type {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            if let Some(logical) = basic_info.logical_type() {
                return match logical {
                    parquet::basic::LogicalType::String => "VARCHAR(65535)".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 8,
                        is_signed: true,
                    } => "SMALLINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 16,
                        is_signed: true,
                    } => "SMALLINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 32,
                        is_signed: true,
                    } => "INTEGER".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 64,
                        is_signed: true,
                    } => "BIGINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        is_signed: false, ..
                    } => "BIGINT".to_string(),
                    parquet::basic::LogicalType::Timestamp { .. } => "TIMESTAMP".to_string(),
                    parquet::basic::LogicalType::Date => "DATE".to_string(),
                    parquet::basic::LogicalType::Time { .. } => "VARCHAR(20)".to_string(),
                    parquet::basic::LogicalType::Decimal { precision, scale } => {
                        format!("DECIMAL({}, {})", precision, scale)
                    }
                    parquet::basic::LogicalType::Uuid => "VARCHAR(36)".to_string(),
                    parquet::basic::LogicalType::Json => "VARCHAR(65535)".to_string(),
                    parquet::basic::LogicalType::List => "VARCHAR(65535)".to_string(),
                    parquet::basic::LogicalType::Map => "VARCHAR(65535)".to_string(),
                    _ => physical_to_redshift(*physical_type),
                };
            }
            physical_to_redshift(*physical_type)
        }
        Type::GroupType { .. } => "VARCHAR(65535)".to_string(),
    }
}

/// Map a Parquet physical type to a Redshift SQL type.
fn physical_to_redshift(physical: parquet::basic::Type) -> String {
    match physical {
        parquet::basic::Type::BOOLEAN => "BOOLEAN".to_string(),
        parquet::basic::Type::INT32 => "INTEGER".to_string(),
        parquet::basic::Type::INT64 => "BIGINT".to_string(),
        parquet::basic::Type::INT96 => "TIMESTAMP".to_string(),
        parquet::basic::Type::FLOAT => "REAL".to_string(),
        parquet::basic::Type::DOUBLE => "DOUBLE PRECISION".to_string(),
        parquet::basic::Type::BYTE_ARRAY => "VARCHAR(65535)".to_string(),
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => "VARCHAR(65535)".to_string(),
    }
}

/// Generate a Redshift-compatible CREATE TABLE statement.
fn generate_redshift_ddl(
    table_name: &str,
    schema: &Type,
    columns: &[metadata::ColumnInfo],
) -> String {
    let mut ddl = format!("CREATE TABLE {} (\n", table_name);

    if let Type::GroupType { fields, .. } = schema {
        let col_defs: Vec<String> = fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let sql_type = parquet_to_redshift_type(field.as_ref());
                let nullable = columns.get(i).map(|c| c.nullable).unwrap_or(true);
                let not_null = if !nullable { " NOT NULL" } else { "" };
                let name = match &**field {
                    Type::PrimitiveType { basic_info, .. } | Type::GroupType { basic_info, .. } => {
                        basic_info.name()
                    }
                };
                format!("  {} {}{}", name, sql_type, not_null)
            })
            .collect();

        ddl.push_str(&col_defs.join(",\n"));
    }

    ddl.push_str("\n);");
    ddl
}

// ---------------------------------------------------------------------------
// MySQL
// ---------------------------------------------------------------------------

/// Map a Parquet column to a MySQL SQL type.
fn parquet_to_mysql_type(col_type: &Type) -> String {
    match col_type {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            if let Some(logical) = basic_info.logical_type() {
                return match logical {
                    parquet::basic::LogicalType::String => "TEXT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 8,
                        is_signed: true,
                    } => "TINYINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 16,
                        is_signed: true,
                    } => "SMALLINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 32,
                        is_signed: true,
                    } => "INT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        bit_width: 64,
                        is_signed: true,
                    } => "BIGINT".to_string(),
                    parquet::basic::LogicalType::Integer {
                        is_signed: false, ..
                    } => "BIGINT".to_string(),
                    parquet::basic::LogicalType::Timestamp { .. } => "DATETIME".to_string(),
                    parquet::basic::LogicalType::Date => "DATE".to_string(),
                    parquet::basic::LogicalType::Time { .. } => "TIME".to_string(),
                    parquet::basic::LogicalType::Decimal { precision, scale } => {
                        format!("DECIMAL({}, {})", precision, scale)
                    }
                    parquet::basic::LogicalType::Uuid => "CHAR(36)".to_string(),
                    parquet::basic::LogicalType::Json => "JSON".to_string(),
                    parquet::basic::LogicalType::List => "JSON".to_string(),
                    parquet::basic::LogicalType::Map => "JSON".to_string(),
                    _ => physical_to_mysql(*physical_type),
                };
            }
            physical_to_mysql(*physical_type)
        }
        Type::GroupType { .. } => "JSON".to_string(),
    }
}

/// Map a Parquet physical type to a MySQL SQL type.
fn physical_to_mysql(physical: parquet::basic::Type) -> String {
    match physical {
        parquet::basic::Type::BOOLEAN => "TINYINT(1)".to_string(),
        parquet::basic::Type::INT32 => "INT".to_string(),
        parquet::basic::Type::INT64 => "BIGINT".to_string(),
        parquet::basic::Type::INT96 => "DATETIME".to_string(),
        parquet::basic::Type::FLOAT => "FLOAT".to_string(),
        parquet::basic::Type::DOUBLE => "DOUBLE".to_string(),
        parquet::basic::Type::BYTE_ARRAY => "BLOB".to_string(),
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => "BLOB".to_string(),
    }
}

/// Generate a MySQL-compatible CREATE TABLE statement.
fn generate_mysql_ddl(table_name: &str, schema: &Type, columns: &[metadata::ColumnInfo]) -> String {
    let mut ddl = format!("CREATE TABLE {} (\n", table_name);

    if let Type::GroupType { fields, .. } = schema {
        let col_defs: Vec<String> = fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let sql_type = parquet_to_mysql_type(field.as_ref());
                let nullable = columns.get(i).map(|c| c.nullable).unwrap_or(true);
                let not_null = if !nullable { " NOT NULL" } else { "" };
                let name = match &**field {
                    Type::PrimitiveType { basic_info, .. } | Type::GroupType { basic_info, .. } => {
                        basic_info.name()
                    }
                };
                format!("  {} {}{}", name, sql_type, not_null)
            })
            .collect();

        ddl.push_str(&col_defs.join(",\n"));
    }

    ddl.push_str("\n) ENGINE=InnoDB;");
    ddl
}
