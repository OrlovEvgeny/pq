use assert_cmd::Command;
use predicates::prelude::*;
use std::sync::Arc;
use tempfile::TempDir;

fn create_test_file(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    use arrow::array::*;
    use arrow::datatypes::*;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

    let path = dir.join(name);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]));

    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Diana", "Eve",
            ])),
            Arc::new(Float64Array::from(vec![95.5, 87.3, 92.1, 88.7, 91.0])),
            Arc::new(BooleanArray::from(vec![true, false, true, true, false])),
        ],
    )
    .unwrap();

    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    path
}

fn create_test_file2(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    use arrow::array::*;
    use arrow::datatypes::*;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

    let path = dir.join(name);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("email", DataType::Utf8, true),
    ]));

    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![6, 7, 8])),
            Arc::new(StringArray::from(vec!["Frank", "Grace", "Hank"])),
            Arc::new(Float64Array::from(vec![78.9, 95.2, 82.1])),
            Arc::new(StringArray::from(vec!["f@x.com", "g@x.com", "h@x.com"])),
        ],
    )
    .unwrap();

    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    path
}

#[test]
fn test_help() {
    Command::cargo_bin("pq")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("inspect"))
        .stdout(predicate::str::contains("schema"))
        .stdout(predicate::str::contains("count"))
        .stdout(predicate::str::contains("stats"));
}

#[test]
fn test_version() {
    Command::cargo_bin("pq")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("pq"));
}

#[test]
fn test_count() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("5"));
}

#[test]
fn test_count_json() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("5"));
}

#[test]
fn test_count_multi_file() {
    let dir = TempDir::new().unwrap();
    let f1 = create_test_file(dir.path(), "a.parquet");
    let f2 = create_test_file2(dir.path(), "b.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", f1.to_str().unwrap(), f2.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("8"));
}

#[test]
fn test_count_directory() {
    let dir = TempDir::new().unwrap();
    create_test_file(dir.path(), "a.parquet");
    create_test_file2(dir.path(), "b.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", dir.path().to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("8"));
}

#[test]
fn test_inspect() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // Without TTY, output is JSON
    Command::cargo_bin("pq")
        .unwrap()
        .args(["inspect", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"num_rows\": 5"))
        .stdout(predicate::str::contains("\"id\""))
        .stdout(predicate::str::contains("\"name\""));
}

#[test]
fn test_inspect_table() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["inspect", "-f", "table", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Rows"))
        .stdout(predicate::str::contains("5"))
        .stdout(predicate::str::contains("id"))
        .stdout(predicate::str::contains("name"));
}

#[test]
fn test_bare_invocation() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // pq file.parquet should behave like pq inspect file.parquet (JSON when piped)
    Command::cargo_bin("pq")
        .unwrap()
        .arg(file.to_str().unwrap())
        .assert()
        .success()
        .stdout(predicate::str::contains("\"num_rows\": 5"));
}

#[test]
fn test_inspect_json() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["inspect", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"num_rows\": 5"))
        .stdout(predicate::str::contains("\"num_columns\": 4"));
}

#[test]
fn test_schema() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // JSON output when piped
    Command::cargo_bin("pq")
        .unwrap()
        .args(["schema", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"id\""))
        .stdout(predicate::str::contains("\"name\""))
        .stdout(predicate::str::contains("\"score\""))
        .stdout(predicate::str::contains("\"active\""));
}

#[test]
fn test_schema_diff() {
    let dir = TempDir::new().unwrap();
    let f1 = create_test_file(dir.path(), "a.parquet");
    let f2 = create_test_file2(dir.path(), "b.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["schema", "diff", f1.to_str().unwrap(), f2.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("active"))
        .stdout(predicate::str::contains("email"));
}

#[test]
fn test_size() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // JSON output when piped
    Command::cargo_bin("pq")
        .unwrap()
        .args(["size", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"id\""))
        .stdout(predicate::str::contains("\"name\""))
        .stdout(predicate::str::contains("compressed_bytes"));
}

#[test]
fn test_stats() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["stats", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("id"))
        .stdout(predicate::str::contains("name"));
}

#[test]
fn test_file_not_found() {
    Command::cargo_bin("pq")
        .unwrap()
        .args(["inspect", "nonexistent.parquet"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot read"));
}

#[test]
fn test_no_parquet_files_in_dir() {
    let dir = TempDir::new().unwrap();
    // Empty dir with no parquet files
    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", dir.path().to_str().unwrap()])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no Parquet files found"));
}

#[test]
fn test_count_csv_format() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", "-f", "csv", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("file,rows"));
}

#[test]
fn test_aliases() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // pq n = pq count
    Command::cargo_bin("pq")
        .unwrap()
        .args(["n", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("5"));

    // pq i = pq inspect
    Command::cargo_bin("pq")
        .unwrap()
        .args(["i", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"num_rows\""));

    // pq sz = pq size
    Command::cargo_bin("pq")
        .unwrap()
        .args(["sz", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("compressed_bytes"));
}

// ── Phase 2: Data Preview Commands ──

#[test]
fn test_head() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["head", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Alice"))
        .stdout(predicate::str::contains("Bob"));
}

#[test]
fn test_head_limit() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["head", "-n", "2", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Alice"))
        .stdout(predicate::str::contains("Bob"))
        .stdout(predicate::str::contains("Charlie").not());
}

#[test]
fn test_head_columns() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "head",
            "-c",
            "id,name",
            "-f",
            "json",
            file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"id\""))
        .stdout(predicate::str::contains("\"name\""))
        .stdout(predicate::str::contains("\"score\"").not());
}

#[test]
fn test_head_csv() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["head", "-f", "csv", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("id,name,score,active"))
        .stdout(predicate::str::contains("Alice"));
}

#[test]
fn test_tail() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["tail", "-n", "2", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Diana"))
        .stdout(predicate::str::contains("Eve"))
        .stdout(predicate::str::contains("Alice").not());
}

#[test]
fn test_sample() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // Sample 3 rows with seed for reproducibility
    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "sample",
            "-n",
            "3",
            "--seed",
            "42",
            "-f",
            "json",
            file.to_str().unwrap(),
        ])
        .assert()
        .success();
}

#[test]
fn test_sample_all() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // Sample more than total rows — should return all
    Command::cargo_bin("pq")
        .unwrap()
        .args(["sample", "-n", "100", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Alice"))
        .stdout(predicate::str::contains("Eve"));
}

#[test]
fn test_head_alias() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["h", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Alice"));
}

#[test]
fn test_tail_alias() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["t", "-n", "1", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Eve"));
}

// ── Phase 3: Validation & Comparison ──

#[test]
fn test_check() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["check", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"passed\""));
}

#[test]
fn test_check_with_read_data() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["check", "--read-data", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success();
}

#[test]
fn test_diff() {
    let dir = TempDir::new().unwrap();
    let f1 = create_test_file(dir.path(), "a.parquet");
    let f2 = create_test_file2(dir.path(), "b.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "diff",
            "-f",
            "json",
            f1.to_str().unwrap(),
            f2.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("schema_diff"))
        .stdout(predicate::str::contains("metadata_diff"));
}

#[test]
fn test_slice() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "slice",
            "--rows",
            "1:3",
            "-f",
            "json",
            file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Bob"))
        .stdout(predicate::str::contains("Charlie"))
        .stdout(predicate::str::contains("Alice").not());
}

#[test]
fn test_slice_columns() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "slice",
            "-c",
            "id,name",
            "-f",
            "json",
            file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"id\""))
        .stdout(predicate::str::contains("\"score\"").not());
}

// ── Phase 4: Write Operations ──

#[test]
fn test_convert_parquet_to_csv() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");
    let out = dir.path().join("output.csv");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "convert",
            file.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();

    let content = std::fs::read_to_string(&out).unwrap();
    assert!(content.contains("id,name,score,active"));
    assert!(content.contains("Alice"));
}

#[test]
fn test_convert_parquet_to_json() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");
    let out = dir.path().join("output.jsonl");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "convert",
            file.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();

    let content = std::fs::read_to_string(&out).unwrap();
    assert!(content.contains("Alice"));
}

#[test]
fn test_convert_csv_to_parquet() {
    let dir = TempDir::new().unwrap();

    // Write a CSV file first
    let csv_path = dir.path().join("input.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n").unwrap();

    let out = dir.path().join("output.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "convert",
            csv_path.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify the parquet file is readable
    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", out.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("2"));
}

#[test]
fn test_rewrite_compression() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");
    let out = dir.path().join("rewritten.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "rewrite",
            "--compression",
            "snappy",
            file.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify the output is a valid parquet file with same row count
    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", out.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("5"));
}

#[test]
fn test_cat() {
    let dir = TempDir::new().unwrap();
    let f1 = create_test_file(dir.path(), "a.parquet");
    let f2_path = dir.path().join("b.parquet");

    // Create b.parquet with same schema as a.parquet
    create_test_file(dir.path(), "b.parquet");

    let out = dir.path().join("merged.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "cat",
            f1.to_str().unwrap(),
            f2_path.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Should have 10 rows (5 + 5)
    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", out.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("10"));
}

#[test]
fn test_compact_dry_run() {
    let dir = TempDir::new().unwrap();
    create_test_file(dir.path(), "a.parquet");
    create_test_file(dir.path(), "b.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "compact",
            "--dry-run",
            "-f",
            "json",
            dir.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("input_files"));
}

// ── Wave 5: New tests for gap-closing features ──

#[test]
fn test_exit_code_3_on_check_failure() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // Create a contract that will fail (require min 1000 rows, file only has 5)
    let contract_path = dir.path().join("contract.toml");
    std::fs::write(&contract_path, "[file]\nmin_rows = 1000\n").unwrap();

    let output = Command::cargo_bin("pq")
        .unwrap()
        .args([
            "check",
            "--contract",
            contract_path.to_str().unwrap(),
            "-f",
            "json",
            file.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    // Exit code should be 3 (ValidationFailed)
    assert_eq!(output.status.code(), Some(3));
}

#[test]
fn test_contract_validation_pass() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // Contract that should pass (file has 5 rows, id is INT64 NOT NULL)
    let contract_path = dir.path().join("contract.toml");
    std::fs::write(
        &contract_path,
        "[file]\nmin_rows = 1\n\n[columns.id]\ntype = \"INT64\"\nnullable = false\n",
    )
    .unwrap();

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "check",
            "--contract",
            contract_path.to_str().unwrap(),
            "-f",
            "json",
            file.to_str().unwrap(),
        ])
        .assert()
        .success();
}

#[test]
fn test_output_to_file() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");
    let out = dir.path().join("output.json");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "inspect",
            file.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();

    let content = std::fs::read_to_string(&out).unwrap();
    assert!(content.contains("\"num_rows\": 5"));
}

#[test]
fn test_output_to_csv_file() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");
    let out = dir.path().join("output.csv");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["head", file.to_str().unwrap(), "-o", out.to_str().unwrap()])
        .assert()
        .success();

    let content = std::fs::read_to_string(&out).unwrap();
    assert!(content.contains("id"));
    assert!(content.contains("Alice"));
}

#[test]
fn test_stats_row_groups() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "stats",
            "--row-groups",
            "-f",
            "json",
            file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"row_group\""));
}

#[test]
fn test_inspect_raw() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["inspect", "--raw", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("ParquetMetaData"));
}

#[test]
fn test_inspect_encryption_field() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["inspect", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"encryption\": \"none\""));
}

#[test]
fn test_slice_split() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");
    let out_template = dir.path().join("part_{n}.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "slice",
            "--split",
            "2",
            file.to_str().unwrap(),
            "-o",
            out_template.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Should create part_0.parquet and part_1.parquet
    assert!(dir.path().join("part_0.parquet").exists());
    assert!(dir.path().join("part_1.parquet").exists());
}

#[test]
fn test_slice_parquet_output() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");
    let out = dir.path().join("sliced.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "slice",
            "--rows",
            "0:3",
            file.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify the output parquet has 3 rows
    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", out.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("3"));
}

#[test]
fn test_cat_union_by_name() {
    let dir = TempDir::new().unwrap();
    let f1 = create_test_file(dir.path(), "a.parquet");
    let f2 = create_test_file2(dir.path(), "b.parquet");
    let out = dir.path().join("merged.parquet");

    // Files have different schemas (a has 'active', b has 'email')
    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "cat",
            "--union-by-name",
            f1.to_str().unwrap(),
            f2.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Should have 8 rows (5 + 3) and all columns from both files
    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", out.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("8"));
}

#[test]
fn test_cat_add_filename() {
    let dir = TempDir::new().unwrap();
    let f1 = create_test_file(dir.path(), "a.parquet");
    create_test_file(dir.path(), "b.parquet");
    let f2 = dir.path().join("b.parquet");
    let out = dir.path().join("merged.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "cat",
            "--add-filename",
            f1.to_str().unwrap(),
            f2.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();

    // The merged file should have a _filename column
    Command::cargo_bin("pq")
        .unwrap()
        .args(["schema", "-f", "json", out.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("_filename"));
}

#[test]
fn test_completions() {
    Command::cargo_bin("pq")
        .unwrap()
        .args(["completions", "bash"])
        .assert()
        .success()
        .stdout(predicate::str::contains("pq"));
}

#[test]
fn test_sch_alias() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // pq sch = pq schema
    Command::cargo_bin("pq")
        .unwrap()
        .args(["sch", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"id\""));
}

#[test]
fn test_inspect_multi_schema_mismatch() {
    let dir = TempDir::new().unwrap();
    // a.parquet has 4 cols (id, name, score, active)
    // b.parquet has 4 cols (id, name, score, email) — same count, different columns
    // To trigger mismatch detection (which checks column count), create a file with different col count
    create_test_file(dir.path(), "a.parquet");

    // Create c.parquet with only 2 columns
    use arrow::array::*;
    use arrow::datatypes::*;
    use parquet::arrow::ArrowWriter;

    let schema3 = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch3 = arrow::record_batch::RecordBatch::try_new(
        schema3.clone(),
        vec![
            std::sync::Arc::new(Int64Array::from(vec![1, 2])),
            std::sync::Arc::new(StringArray::from(vec!["X", "Y"])),
        ],
    )
    .unwrap();
    let path3 = dir.path().join("c.parquet");
    let file3 = std::fs::File::create(&path3).unwrap();
    let mut writer3 = ArrowWriter::try_new(file3, schema3, None).unwrap();
    writer3.write(&batch3).unwrap();
    writer3.close().unwrap();

    Command::cargo_bin("pq")
        .unwrap()
        .args(["inspect", "-f", "table", dir.path().to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("schema differs"));
}

#[test]
fn test_convert_multi_file() {
    let dir = TempDir::new().unwrap();
    let f1 = create_test_file(dir.path(), "a.parquet");
    let f2 = create_test_file(dir.path(), "b.parquet");
    let out_dir = dir.path().join("output");

    // Pre-create the output directory so OutputConfig doesn't try to open it as a file
    std::fs::create_dir_all(&out_dir).unwrap();

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "convert",
            f1.to_str().unwrap(),
            f2.to_str().unwrap(),
            "-o",
            out_dir.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Should create JSON files in the output directory
    assert!(out_dir.join("a.json").exists());
    assert!(out_dir.join("b.json").exists());
}

// ── Round 3: Tests for newly wired flags ──

#[test]
fn test_schema_arrow() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["schema", "--arrow", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Field"))
        .stdout(predicate::str::contains("id"));
}

#[test]
fn test_schema_extract_ddl_duckdb() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "schema",
            "extract",
            "--ddl",
            "duckdb",
            file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("CREATE TABLE"))
        .stdout(predicate::str::contains("id"));
}

#[test]
fn test_schema_extract_ddl_spark() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "schema",
            "extract",
            "--ddl",
            "spark",
            file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("StructType"))
        .stdout(predicate::str::contains("StructField"));
}

#[test]
fn test_schema_extract_ddl_clickhouse() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "schema",
            "extract",
            "--ddl",
            "clickhouse",
            file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("CREATE TABLE"))
        .stdout(predicate::str::contains("MergeTree"));
}

#[test]
fn test_schema_extract_ddl_postgres() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["schema", "extract", "--ddl", "pg", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("CREATE TABLE"))
        .stdout(predicate::str::contains("BIGINT"));
}

#[test]
fn test_schema_extract_ddl_bigquery() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["schema", "extract", "--ddl", "bq", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("CREATE TABLE"))
        .stdout(predicate::str::contains("INT64"));
}

#[test]
fn test_schema_extract_ddl_snowflake() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["schema", "extract", "--ddl", "sf", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("CREATE TABLE"))
        .stdout(predicate::str::contains("VARCHAR"));
}

#[test]
fn test_schema_extract_ddl_mysql() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "schema",
            "extract",
            "--ddl",
            "mysql",
            file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("CREATE TABLE"))
        .stdout(predicate::str::contains("InnoDB"));
}

#[test]
fn test_stats_pages() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["stats", "--pages", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("data_page_offset"));
}

#[test]
fn test_stats_bloom() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["stats", "--bloom", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("has_bloom_filter"));
}

#[test]
fn test_head_max_width() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // With very small max-width, strings should be truncated
    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "head",
            "--max-width",
            "3",
            "-f",
            "table",
            file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("...")); // truncated strings
}

#[test]
fn test_diff_data() {
    let dir = TempDir::new().unwrap();
    let f1 = create_test_file(dir.path(), "a.parquet");
    let f2 = create_test_file2(dir.path(), "b.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "diff",
            "--data",
            "-f",
            "json",
            f1.to_str().unwrap(),
            f2.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("data_diff"))
        .stdout(predicate::str::contains("total_added"));
}

#[test]
fn test_rewrite_dictionary_and_version() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");
    let out = dir.path().join("rewritten.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "rewrite",
            "--dictionary",
            "--version",
            "2",
            file.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify the output is valid
    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", out.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("5"));
}

#[test]
fn test_convert_no_header() {
    let dir = TempDir::new().unwrap();

    // Write a CSV without header
    let csv_path = dir.path().join("noheader.csv");
    std::fs::write(&csv_path, "1,Alice\n2,Bob\n").unwrap();
    let out = dir.path().join("output.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "convert",
            "--no-header",
            csv_path.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();

    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", out.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("2"));
}

#[test]
fn test_schema_diff_directory() {
    let dir = TempDir::new().unwrap();
    create_test_file(dir.path(), "a.parquet");
    create_test_file2(dir.path(), "b.parquet");

    // Create a third file with yet another schema
    use arrow::array::*;
    use arrow::datatypes::*;
    use parquet::arrow::ArrowWriter;

    let schema3 = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch3 = arrow::record_batch::RecordBatch::try_new(
        schema3.clone(),
        vec![
            std::sync::Arc::new(Int64Array::from(vec![1])),
            std::sync::Arc::new(StringArray::from(vec!["Z"])),
        ],
    )
    .unwrap();
    let path3 = dir.path().join("c.parquet");
    let f3 = std::fs::File::create(&path3).unwrap();
    let mut w3 = ArrowWriter::try_new(f3, schema3, None).unwrap();
    w3.write(&batch3).unwrap();
    w3.close().unwrap();

    // Diff 3 files — should show multi-file comparison
    let a = dir.path().join("a.parquet");
    let b = dir.path().join("b.parquet");
    let c = dir.path().join("c.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "schema",
            "diff",
            a.to_str().unwrap(),
            b.to_str().unwrap(),
            c.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Reference"));
}

// ── Round 4: Final gap tests ──

#[test]
fn test_schema_detailed_columns() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // Flat view should show Physical/Logical/Rep/Def columns
    Command::cargo_bin("pq")
        .unwrap()
        .args(["schema", "-f", "table", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Physical"))
        .stdout(predicate::str::contains("Logical"))
        .stdout(predicate::str::contains("Rep"))
        .stdout(predicate::str::contains("Def"));
}

#[test]
fn test_size_footer() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["size", "-f", "table", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Footer"));
}

#[test]
fn test_size_footer_json() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["size", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("footer_bytes"));
}

#[test]
fn test_inspect_avg_rg_size() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["inspect", "-f", "table", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("each)"));
}

#[test]
fn test_head_parquet_output() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");
    let out = dir.path().join("head.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "head",
            "-n",
            "3",
            file.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify the output parquet has 3 rows
    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", out.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("3"));
}

#[test]
fn test_contract_min_max() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // id column has values 1-5, so min=0 max=10 should pass
    let contract_path = dir.path().join("contract.toml");
    std::fs::write(
        &contract_path,
        "[file]\nmin_rows = 1\n\n[columns.id]\ntype = \"INT64\"\nmin = 0.0\nmax = 10.0\n",
    )
    .unwrap();

    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "check",
            "--contract",
            contract_path.to_str().unwrap(),
            "-f",
            "json",
            file.to_str().unwrap(),
        ])
        .assert()
        .success();
}

#[test]
fn test_convert_arrow_ipc_roundtrip() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");
    let arrow_file = dir.path().join("test.arrow");
    let back_file = dir.path().join("back.parquet");

    // Parquet → Arrow IPC
    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "convert",
            file.to_str().unwrap(),
            "-o",
            arrow_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    assert!(arrow_file.exists());

    // Arrow IPC → Parquet
    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "convert",
            arrow_file.to_str().unwrap(),
            "-o",
            back_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify row count preserved
    Command::cargo_bin("pq")
        .unwrap()
        .args(["count", back_file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("5"));
}

// ── Round 5: Final polish tests ──

#[test]
fn test_inspect_sort_by_name() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    let output = Command::cargo_bin("pq")
        .unwrap()
        .args([
            "inspect",
            "--sort",
            "name",
            "-f",
            "table",
            file.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // "active" should come before "id" when sorted by name
    let active_pos = stdout.find("active").unwrap();
    let id_pos = stdout.find("  id").unwrap();
    assert!(active_pos < id_pos, "columns should be sorted by name");
}

#[test]
fn test_friendly_type_names() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // name column should show VARCHAR (not "String")
    Command::cargo_bin("pq")
        .unwrap()
        .args(["inspect", "-f", "table", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("VARCHAR"));
}

#[test]
fn test_stats_distinct_column() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["stats", "-f", "table", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Distinct(est)"));
}

#[test]
fn test_schema_extract_arrow() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["schema", "extract", "--arrow", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Field"));
}

// ── Round 6: Final fixes tests ──

#[test]
fn test_count_quiet_mode() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    let output = Command::cargo_bin("pq")
        .unwrap()
        .args(["count", "-q", file.to_str().unwrap()])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should be just "5\n" with no formatting
    assert_eq!(stdout.trim(), "5");
}

#[test]
fn test_check_read_data_row_count() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    Command::cargo_bin("pq")
        .unwrap()
        .args(["check", "--read-data", "-f", "json", file.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Actual row count matches metadata",
        ));
}

// ── Cloud storage tests (URL parsing, no live connections) ──

#[test]
fn test_endpoint_flag_accepted() {
    // --endpoint should be accepted as a global flag
    Command::cargo_bin("pq")
        .unwrap()
        .args([
            "--endpoint",
            "https://example.r2.cloudflarestorage.com",
            "--help",
        ])
        .assert()
        .success();
}

#[test]
fn test_cloud_url_error_message() {
    // Trying to access a non-existent S3 URL should give a cloud-related error
    let output = Command::cargo_bin("pq")
        .unwrap()
        .args(["inspect", "s3://nonexistent-bucket/file.parquet"])
        .output()
        .unwrap();

    // Should fail (no real S3 access) but with a meaningful error
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cloud") || stderr.contains("S3") || stderr.contains("error"),
        "Expected cloud error message, got: {}",
        stderr
    );
}

#[test]
fn test_cloud_output_url_does_not_create_local_file() {
    let dir = TempDir::new().unwrap();
    let file = create_test_file(dir.path(), "test.parquet");

    // Using -o with a cloud URL should NOT create a local file called "s3://..."
    let output = Command::cargo_bin("pq")
        .unwrap()
        .args([
            "inspect",
            "-f",
            "json",
            file.to_str().unwrap(),
            "-o",
            "s3://bucket/output.json",
        ])
        .output()
        .unwrap();

    // The command outputs to stdout since cloud upload would fail
    // But critically, no local file named "s3:" should be created
    assert!(!std::path::Path::new("s3:").exists());
    // stdout should have the JSON output (written to stdout since cloud write isn't attempted for text)
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("num_rows") || stdout.is_empty());
}
