# pq

[![CI](https://github.com/OrlovEvgeny/pq/workflows/CI/badge.svg)](https://github.com/OrlovEvgeny/pq/actions)

A fast, feature-rich CLI for Parquet files. Inspect, transform, validate - without leaving the terminal.

<p align="center">
  <img src="doc/demo.gif" alt="pq demo" width="800">
</p>

A fast, feature-rich CLI for Parquet inspection, validation, and transformation. 16 commands with instant metadata reads from file footer, DDL generation for major databases, cloud storage support, smart output formatting, and Unix composability.

## Quick Start

```bash
pq data.parquet
```

```
─── data.parquet ──────────────────────────────────────────
  Size          148.2 MB (disk)  →  412.6 MB (uncompressed)
  Compression   zstd (2.8x ratio)
  Rows          4,218,903
  Columns       23
  Row groups    12 (avg 351K rows, 12.4 MB each)
  Created by    DuckDB v1.5.0
  Format        Parquet v2
  Encryption    none

  ─── Schema ────────────────────────────────────────────
  #   Name        Type        Nullable  Encoding
  1   id          INT64       no        DELTA_BINARY_PACKED
  2   name        VARCHAR     yes       DELTA_BYTE_ARRAY
  3   amount      DOUBLE      yes       BYTE_STREAM_SPLIT
  4   status      VARCHAR     no        RLE_DICTIONARY
  ...             (19 more columns, use --all to show)
```

```bash
pq head -n 5 data.parquet                          # preview first rows
pq count 'data/*.parquet'                          # instant row count from metadata
pq schema extract --ddl postgres data.parquet      # generate CREATE TABLE
pq check --contract contract.toml data.parquet     # validate against spec
pq size --top 5 data.parquet                       # largest columns by size
```

## Installation

```bash
# Cargo
cargo install pq

# From source
git clone https://github.com/OrlovEvgeny/pq.git && cd pq
cargo build --release
# Binary: ./target/release/pq
```

## Commands

| Command | Alias | Description |
|---------|-------|-------------|
| `inspect` | `i` | File overview: metadata, schema, row groups |
| `schema` | `s` | Schema operations: show, diff, extract DDL |
| `stats` | `st` | Column statistics from row group metadata |
| `count` | `n` | Fast row count from metadata |
| `size` | `sz` | Per-column storage analysis |
| `head` | `h` | First N rows |
| `tail` | `t` | Last N rows |
| `sample` | `sa` | Random sample of rows |
| `check` | `c` | Structural and contract validation |
| `diff` | `d` | Compare schemas or data between files |
| `slice` | `sl` | Extract subsets by rows, columns, row groups |
| `rewrite` | `rw` | Change compression, encoding, metadata |
| `convert` | `cv` | Convert between CSV, JSON, Arrow, Parquet |
| `compact` | `cc` | Merge small files into optimal sizes |
| `cat` | | Concatenate files vertically |
| `merge` | `m` | Horizontal join on key columns |

Bare invocation is inspect: `pq data.parquet` = `pq inspect data.parquet`

### Inspection

```bash
pq inspect data.parquet                           # metadata + schema
pq inspect --schema-only data.parquet             # schema only
pq inspect --raw data.parquet                     # raw Parquet metadata dump
pq inspect data/                                  # inspect all .parquet in directory
```

Multi-file view detects schema mismatches:
```
  File                  Size     Rows     Cols  Compression
  data/2024-01.parquet  12.4 MB  351,241  23    zstd
  data/2024-02.parquet  11.8 MB  338,102  23    zstd
  data/2024-03.parquet  14.1 MB  412,060  24    zstd  ← schema differs!
  ───
  Total: 3 files, 38.3 MB, 1,101,403 rows
```

```bash
pq schema data.parquet                            # detailed schema
pq schema diff a.parquet b.parquet                # compare schemas
pq schema extract --ddl duckdb data.parquet       # DuckDB CREATE TABLE
pq schema extract --ddl postgres data.parquet     # PostgreSQL
```

Supported DDL dialects: `duckdb`, `spark`, `clickhouse` (`ch`), `postgres` (`pg`), `bigquery` (`bq`), `snowflake` (`sf`), `redshift` (`rs`), `mysql`.

```bash
pq stats data.parquet                             # all columns
pq stats -c id,name data.parquet                  # specific columns
pq stats --row-groups data.parquet                # per-row-group breakdown
```

```
  Column  Type     Null%  Min     Max        Distinct(est)  Compressed  Uncompressed
  id      INT64    0.0%   1       4218903    4,218,903      8.1 MB      32.2 MB
  name    VARCHAR  0.3%   Aaron   Zuzana     1,842,091      42.3 MB     98.7 MB
  amount  DOUBLE   1.2%   0.01    999999.99  -              16.8 MB     33.8 MB
```

```bash
pq count data.parquet                             # 4218903
pq count 'data/*.parquet'                         # per-file + total
ROWS=$(pq count -q data.parquet)                  # quiet mode for scripting
```

```bash
pq size data.parquet                              # per-column breakdown
pq size --sort compressed data.parquet            # sort by size
pq size --top 5 data.parquet                      # top 5 largest columns
```

```
  Column  Compressed  Uncompressed  Ratio  % of File
  name    42.3 MB     98.7 MB       2.3x   28.5%
  email   38.9 MB     94.1 MB       2.4x   26.2%
  ...
  ───
  Data total  148.2 MB   412.6 MB  2.8x
  Footer      0.04 MB
  File total  148.2 MB
```

### Data Preview

```bash
pq head data.parquet                              # first 10 rows
pq head -n 50 -c id,name data.parquet             # 50 rows, specific columns
pq tail -n 5 data.parquet                         # last 5 rows
pq sample -n 1000 data.parquet                    # random 1000 rows
pq sample --seed 42 -o sample.parquet data.parquet  # reproducible sample to file
```

### Validation

```bash
pq check data.parquet                             # structural validation
pq check --read-data data.parquet                 # + data quality checks
pq check --contract contract.toml data.parquet    # contract validation
```

```
  ✓  Magic bytes and footer parseable
  ✓  Row counts sum correctly (4,218,903)
  ✓  Schema consistent across 12 row groups
  ✓  Column statistics within type bounds
  ✓  Encoding compatible with column types
  ✓  Page offsets non-overlapping
  ✓  Compression codec recognized (zstd)
  ─
  7/7 checks passed.
```

Contract file (`contract.toml`):
```toml
[file]
min_rows = 1000
max_size = "500MB"
compression = ["zstd", "snappy"]

[columns.id]
type = "INT64"
nullable = false

[columns.amount]
type = "DOUBLE"
min = 0.0
max = 1000000.0
```

```bash
pq diff a.parquet b.parquet                       # schema + metadata diff
pq diff --data --key id a.parquet b.parquet       # key-based data comparison
```

### Transformation

```bash
# Convert between formats
pq convert data.csv -o data.parquet               # CSV → Parquet
pq convert data.jsonl -o data.parquet             # JSON → Parquet
pq convert data.parquet -o data.csv               # Parquet → CSV
pq convert 'raw/*.csv' -o parquet/                # batch convert

# Extract subsets
pq slice data.parquet --rows 0:1000 -o subset.parquet
pq slice data.parquet --columns id,name -o projection.parquet
pq slice data.parquet --split-by region -o 'partitions/{n}.parquet'

# Rewrite with new properties
pq rewrite --compression zstd data.parquet -o optimized.parquet
pq rewrite --sort-by id data.parquet -o sorted.parquet
pq rewrite --strip-metadata data.parquet -o clean.parquet

# Merge and concatenate
pq cat a.parquet b.parquet -o merged.parquet
pq cat --union-by-name 'data/*.parquet' -o all.parquet
pq compact data/ -o compacted/                    # merge small files
pq merge --key id left.parquet right.parquet -o joined.parquet
```

## Cloud Storage

Read from and write to S3, GCS, Azure Blob Storage, and Cloudflare R2:

```bash
pq inspect s3://bucket/path/data.parquet
pq head s3://bucket/data.parquet
pq count 's3://bucket/prefix/*.parquet'
pq inspect gs://bucket/data.parquet               # GCS
pq inspect az://container/data.parquet            # Azure
pq compact data/ -o s3://bucket/compacted/        # write to cloud
```

Credentials are resolved automatically via standard cloud SDK chains:

| Provider | Resolution order |
|----------|-----------------|
| **S3** | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` → `~/.aws/credentials` → IAM role |
| **GCS** | `GOOGLE_APPLICATION_CREDENTIALS` → `gcloud auth application-default login` |
| **Azure** | `AZURE_STORAGE_ACCOUNT` / `AZURE_STORAGE_ACCESS_KEY` → SAS token → `az login` |
| **R2** | Same as S3 with `--endpoint` or `PQ_S3_ENDPOINT` |

## Output Formats

Output format is auto-detected:

| Condition | Format |
|-----------|--------|
| stdout is TTY | `table` (human-readable) |
| stdout is piped | `json` (machine-readable) |
| `-o file.csv` | `csv` |
| `-o file.json` | `json` |
| `-o file.parquet` | `parquet` (binary) |
| `-f` flag | explicit override |

```bash
pq stats data.parquet                             # table on TTY
pq stats data.parquet | jq '.[]'                  # JSON when piped
pq stats -f csv data.parquet                      # force CSV
pq head data.parquet -o sample.parquet            # write Parquet
```

## Column Selection

The `-c` / `--columns` flag supports:

```bash
-c id,name,email           # exact names
-c '/^user_.*/'            # regex
-c '!metadata,!tags'       # exclusion
-c '#0-5'                  # first 6 columns
-c '#-3'                   # last 3 columns
```

## Unix Composability

```bash
# Find columns consuming most space
pq size data.parquet -f json | jq '.columns | sort_by(.compressed_bytes) | reverse | .[0:5]'

# Count rows across partitioned dataset
pq count 'data/**/*.parquet' -f csv | sort -t, -k2 -rn | head -20

# Validate in CI, fail on issues
pq check --contract contract.toml data/ || exit 1

# Convert all CSVs then compact
pq convert 'raw/*.csv' -o staging/ && pq compact staging/ -o production/

# Chain with DuckDB
pq slice data.parquet --columns id,name,amount -f csv | \
  duckdb -c "SELECT name, sum(amount) FROM read_csv('/dev/stdin') GROUP BY 1"
```

## Configuration

Shell completions:
```bash
pq completions bash >> ~/.bashrc
pq completions zsh >> ~/.zshrc
pq completions fish > ~/.config/fish/completions/pq.fish
```

Optional config file at `~/.config/pq/config.toml` (or `$PQ_CONFIG`):

```toml
[defaults]
format = "table"
jobs = 4

[s3]
region = "eu-west-1"
endpoint = "https://account.r2.cloudflarestorage.com"
```

Environment variables: `PQ_DEFAULT_FORMAT`, `PQ_DEFAULT_JOBS`, `PQ_S3_ENDPOINT`, `PQ_S3_REGION`, `PQ_GCS_PROJECT`, `PQ_AZURE_ACCOUNT`.

Priority: CLI flag > env var > config file > default.

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid arguments |
| 3 | Validation failed (`pq check`) |
| 4 | Partial failure (some files ok, some failed) |

## License

[MIT](LICENSE)
