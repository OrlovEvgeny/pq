# pq - the Swiss Knife of Parquet

> Inspect, transform, and operate on Parquet files from your terminal.

`pq` is a **file operations tool** for Parquet - like `jq` is for JSON, `csvkit` for CSV, or `imagemagick` for images. It answers the question: *"What's inside this Parquet file, is it healthy, and how do I fix it?"*

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

## Install

```bash
# From source
cargo install --path .

# Or build locally
cargo build --release
```

## Quick Start

```bash
# Inspect a file (default command)
pq data.parquet

# Count rows (instant - reads only metadata)
pq count data.parquet

# Preview first 10 rows
pq head data.parquet

# Check file health
pq check data.parquet

# See column sizes
pq size data.parquet
```

## Commands

### Inspect - file overview

```bash
pq inspect data.parquet              # metadata + schema
pq inspect --schema-only data.parquet # schema only
pq inspect --raw data.parquet         # raw Parquet metadata dump
pq inspect --sort name data.parquet   # sort columns by name
pq inspect data/                      # inspect all .parquet in directory
```

Multi-file view detects schema mismatches:
```
  File                  Size     Rows     Cols  Compression
  data/2024-01.parquet  12.4 MB  351,241  23    zstd
  data/2024-02.parquet  11.8 MB  338,102  23    zstd
  data/2024-03.parquet  14.1 MB  412,060  24    zstd  ← schema differs!
  ───
  Total: 3 files, 38.3 MB, 1,101,403 rows
  ⚠  Schema mismatch in 1 file(s) (use `pq schema diff` for details)
```

### Schema - schema operations

```bash
pq schema data.parquet                        # detailed: Physical, Logical, Rep, Def, Encoding
pq schema --arrow data.parquet                # Arrow schema representation
pq schema --expand data.parquet               # nested types fully expanded
pq schema diff a.parquet b.parquet            # compare schemas
pq schema diff --changes-only a.parquet b.parquet  # only differences
pq schema extract --ddl duckdb data.parquet      # DuckDB CREATE TABLE
pq schema extract --ddl spark data.parquet       # PySpark StructType
pq schema extract --ddl clickhouse data.parquet  # ClickHouse MergeTree
pq schema extract --ddl postgres data.parquet    # PostgreSQL
pq schema extract --ddl bigquery data.parquet    # BigQuery
pq schema extract --ddl snowflake data.parquet   # Snowflake
pq schema extract --ddl redshift data.parquet    # Redshift
pq schema extract --ddl mysql data.parquet       # MySQL InnoDB
```

Supported DDL dialects: `duckdb`, `spark`, `clickhouse` (alias: `ch`), `postgres` (aliases: `postgresql`, `pg`), `bigquery` (`bq`), `snowflake` (`sf`), `redshift` (`rs`), `mysql`.

DuckDB DDL output:
```sql
CREATE TABLE data (
  id BIGINT NOT NULL,
  name VARCHAR,
  amount DOUBLE,
  status VARCHAR NOT NULL
);
```

### Stats - column statistics

```bash
pq stats data.parquet                    # all columns
pq stats -c id,name data.parquet         # specific columns
pq stats --row-groups data.parquet       # per-row-group breakdown
pq stats --pages data.parquet            # page-level info
pq stats --bloom data.parquet            # bloom filter info
pq stats --size-only data.parquet        # only sizes
```

```
  Column  Type     Null%  Min     Max        Distinct(est)  Compressed  Uncompressed
  id      INT64    0.0%   1       4218903    4,218,903      8.1 MB      32.2 MB
  name    VARCHAR  0.3%   Aaron   Zuzana     1,842,091      42.3 MB     98.7 MB
  amount  DOUBLE   1.2%   0.01    999999.99  -              16.8 MB     33.8 MB
```

### Head / Tail / Sample - data preview

```bash
pq head data.parquet                  # first 10 rows
pq head -n 50 -c id,name data.parquet # 50 rows, specific columns
pq tail -n 5 data.parquet            # last 5 rows
pq sample -n 1000 data.parquet       # random 1000 rows
pq sample --seed 42 data.parquet     # reproducible sample
pq sample -n 5000 data.parquet -o sample.parquet  # save as Parquet
```

### Count - fast row count

```bash
pq count data.parquet          # 4218903
pq count 'data/*.parquet'      # per-file + total
pq count --total-only data/    # just the total
ROWS=$(pq count -q data.parquet)  # quiet mode for scripting
```

### Size - storage analysis

```bash
pq size data.parquet           # per-column breakdown
pq size --sort compressed data.parquet  # sort by size
pq size --top 5 data.parquet   # top 5 largest columns
pq size --bytes data.parquet   # exact byte counts
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

### Check - validate files

```bash
pq check data.parquet                    # structural validation
pq check --read-data data.parquet        # + data quality checks
pq check --contract contract.toml data.parquet  # contract validation
pq check data/                           # validate entire directory
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

[columns.status]
allowed = ["active", "inactive", "suspended"]
```

### Compact - merge small files

```bash
pq compact data/ -o compacted/                    # merge small files
pq compact --target-size 256MB data/ -o out/       # custom target size
pq compact --compression zstd --compression-level 3 data/ -o out/
pq compact --sort-by created_at,id data/ -o out/   # sort during merge
pq compact --dry-run data/                         # preview plan
```

### Convert - format conversion

```bash
pq convert data.csv -o data.parquet        # CSV → Parquet
pq convert data.jsonl -o data.parquet      # JSON → Parquet
pq convert data.parquet -o data.csv        # Parquet → CSV
pq convert data.parquet -o data.jsonl      # Parquet → JSONL
pq convert data.arrow -o data.parquet      # Arrow IPC → Parquet
pq convert 'raw/*.csv' -o parquet/         # batch convert directory
pq convert data.csv -o data.parquet --compression zstd --no-header
```

### Slice - extract subsets

```bash
pq slice data.parquet --rows 0:1000 -o subset.parquet
pq slice data.parquet --columns id,name -o projection.parquet
pq slice data.parquet --row-groups 0,1,2 -o first_three.parquet
pq slice data.parquet --split 10 -o 'chunks/part_{n}.parquet'
pq slice data.parquet --split-row-groups -o 'groups/rg_{n}.parquet'
pq slice data.parquet --split-by region -o 'partitions/{n}.parquet'
pq slice data.parquet --prune "created_at >= 2024-01-01" -o recent.parquet
```

### Rewrite - change physical properties

```bash
pq rewrite --compression zstd data.parquet -o optimized.parquet
pq rewrite --dictionary --version 2 data.parquet -o v2.parquet
pq rewrite --sort-by id data.parquet -o sorted.parquet
pq rewrite --strip-metadata data.parquet -o clean.parquet
pq rewrite --set-kv 'pipeline.version=2.0' data.parquet -o tagged.parquet
```

### Cat - concatenate files

```bash
pq cat a.parquet b.parquet -o merged.parquet
pq cat --union-by-name 'data/*.parquet' -o all.parquet  # schema evolution
pq cat --add-filename 'data/*.parquet' -o merged.parquet # track source
pq cat --sort-by timestamp 'data/*.parquet' -o sorted.parquet
```

### Diff - compare files

```bash
pq diff a.parquet b.parquet              # schema + metadata diff
pq diff --data a.parquet b.parquet       # full data comparison
pq diff --data --key id a.parquet b.parquet  # key-based diff
```

### Merge - horizontal join

```bash
pq merge --key id left.parquet right.parquet -o joined.parquet
pq merge --left --key id left.parquet right.parquet -o joined.parquet
```

## Cloud Storage

Read from and write to S3, GCS, Azure Blob Storage, and Cloudflare R2:

```bash
# Read from S3
pq inspect s3://bucket/path/data.parquet
pq head s3://bucket/data.parquet
pq count 's3://bucket/prefix/*.parquet'

# Read from GCS
pq inspect gs://bucket/data.parquet

# Read from Azure
pq inspect az://container/data.parquet

# Write to cloud
pq compact data/ -o s3://bucket/compacted/
pq convert data.csv -o s3://bucket/data.parquet
pq rewrite --compression zstd data.parquet -o gs://bucket/optimized.parquet

# Cloudflare R2 (S3-compatible)
pq inspect s3://bucket/data.parquet --endpoint https://ACCOUNT.r2.cloudflarestorage.com
# or set PQ_S3_ENDPOINT environment variable
```

### Credentials

Credentials are resolved automatically via standard cloud SDK chains:

| Provider | Resolution order |
|----------|-----------------|
| **S3** | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` → `~/.aws/credentials` → IAM role |
| **GCS** | `GOOGLE_APPLICATION_CREDENTIALS` → `gcloud auth application-default login` |
| **Azure** | `AZURE_STORAGE_ACCOUNT` / `AZURE_STORAGE_ACCESS_KEY` → SAS token → `az login` |
| **R2** | Same as S3 with `--endpoint` or `PQ_S3_ENDPOINT` |

### Cloud config

```toml
# ~/.config/pq/config.toml
[s3]
region = "us-east-1"
endpoint = "https://ACCOUNT.r2.cloudflarestorage.com"  # for R2/MinIO

[gcs]
project = "my-project"

[azure]
account = "myaccount"
```

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
pq stats data.parquet                    # table on TTY
pq stats data.parquet | jq '.[]'         # JSON when piped
pq stats -f csv data.parquet             # force CSV
pq inspect data.parquet -o report.json   # write JSON to file
pq head data.parquet -o sample.parquet   # write Parquet
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

## Column Selection

The `-c` / `--columns` flag supports:

```bash
-c id,name,email           # exact names
-c '/^user_.*/'            # regex
-c '!metadata,!tags'       # exclusion
-c '#0-5'                  # first 6 columns
-c '#-3'                   # last 3 columns
```

## Aliases

Every command has a short alias:

```
pq i   = pq inspect       pq cc  = pq compact
pq s   = pq schema        pq cv  = pq convert
pq st  = pq stats         pq sl  = pq slice
pq h   = pq head          pq rw  = pq rewrite
pq t   = pq tail          pq n   = pq count
pq sa  = pq sample        pq sz  = pq size
pq c   = pq check         pq d   = pq diff
                           pq m   = pq merge
```

Bare invocation is inspect: `pq data.parquet` = `pq inspect data.parquet`

## Shell Completions

```bash
pq completions bash >> ~/.bashrc
pq completions zsh >> ~/.zshrc
pq completions fish > ~/.config/fish/completions/pq.fish
```

## Configuration

Optional config file at `~/.config/pq/config.toml` (or `$PQ_CONFIG`):

```toml
[defaults]
format = "table"
jobs = 4

[s3]
region = "eu-west-1"
endpoint = "https://account.r2.cloudflarestorage.com"

[gcs]
project = "my-project"
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
