use crate::cli::CompactArgs;
use crate::error::PqError;
use crate::input::resolve_inputs_with_config;
use crate::output::{OutputConfig, OutputFormat};
use crate::parquet_ext::metadata;
use arrow::record_batch::RecordBatch;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Serialize)]
struct CompactPlan {
    input_files: usize,
    input_size_bytes: u64,
    input_rows: i64,
    output_files: usize,
    groups: Vec<CompactGroup>,
}

#[derive(Serialize)]
struct CompactGroup {
    files: Vec<String>,
    total_bytes: u64,
}

/// Build `WriterProperties` from CLI flags and resolved compression codec.
fn build_writer_props(args: &CompactArgs, compression: Compression) -> WriterProperties {
    let mut builder = WriterProperties::builder().set_compression(compression);

    if let Some(rg_size) = args.row_group_size {
        builder = builder.set_max_row_group_size(rg_size);
    }

    if args.dictionary {
        builder = builder.set_dictionary_enabled(true);
    }

    if args.no_stats {
        builder = builder.set_statistics_enabled(EnabledStatistics::None);
    }

    builder.build()
}

/// Resolve the compression codec, applying an optional compression level.
fn resolve_compression(args: &CompactArgs) -> Compression {
    match args.compression.as_deref() {
        Some(s) => super::util::parse_compression(s, args.compression_level),
        None => {
            // Default to zstd, honouring level if provided
            super::util::parse_compression("zstd", args.compression_level)
        }
    }
}

/// Create an optional progress bar. Returns `None` when stdout is not a TTY
/// (piped output) so that machine-readable output stays clean.
fn make_progress_bar(output: &OutputConfig, total: u64) -> Option<ProgressBar> {
    if !output.is_tty || output.quiet {
        return None;
    }

    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} groups ({eta})")
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("##-"),
    );
    Some(pb)
}

pub fn execute(args: &CompactArgs, output: &mut OutputConfig) -> miette::Result<()> {
    let sources = resolve_inputs_with_config(&args.files, &output.cloud_config)?;
    if sources.is_empty() {
        return Err(miette::miette!("no files to compact"));
    }

    let target_size = parse_size(&args.target_size)?;
    let compression = resolve_compression(args);

    // files above this threshold are skipped (default: target/2)
    let min_file_size: u64 = match args.min_file_size {
        Some(ref s) => parse_size(s)?,
        None => target_size / 2,
    };

    // ── Partition sources ──────────────────────────────────────────────
    let partitions: Vec<(String, Vec<usize>)> = if args.preserve_partitions {
        let mut by_dir: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (i, source) in sources.iter().enumerate() {
            let parent = source
                .path()
                .parent()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| ".".to_string());
            by_dir.entry(parent).or_default().push(i);
        }
        by_dir.into_iter().collect()
    } else {
        let all_indices: Vec<usize> = (0..sources.len()).collect();
        vec![(".".to_string(), all_indices)]
    };

    // ── Build compaction groups (within each partition) ────────────────
    let mut groups: Vec<Vec<usize>> = Vec::new();

    for (_partition_dir, indices) in &partitions {
        let mut current_group: Vec<usize> = Vec::new();
        let mut current_size: u64 = 0;

        for &i in indices {
            let file_size = sources[i].file_size();

            if file_size >= min_file_size && current_group.is_empty() {
                continue;
            }

            current_group.push(i);
            current_size += file_size;

            if current_size >= target_size {
                groups.push(current_group.clone());
                current_group.clear();
                current_size = 0;
            }
        }

        if !current_group.is_empty() {
            groups.push(current_group);
        }
    }

    if groups.is_empty() {
        writeln!(
            output.writer,
            "All files already at target size, nothing to compact."
        )
        .map_err(|e| miette::miette!("{}", e))?;
        return Ok(());
    }

    // ── Compute plan summary ──────────────────────────────────────────
    let plan = CompactPlan {
        input_files: sources.len(),
        input_size_bytes: sources.iter().map(|s| s.file_size()).sum(),
        input_rows: {
            let mut total = 0i64;
            for s in &sources {
                let m = metadata::read_metadata(s.path())?;
                total += metadata::total_rows(&m);
            }
            total
        },
        output_files: groups.len(),
        groups: groups
            .iter()
            .map(|g| CompactGroup {
                files: g.iter().map(|&i| sources[i].display_name()).collect(),
                total_bytes: g.iter().map(|&i| sources[i].file_size()).sum(),
            })
            .collect(),
    };

    // ── Dry-run output ────────────────────────────────────────────────
    if args.dry_run {
        match output.format {
            OutputFormat::Table => {
                let d = crate::output::symbols::symbols().dash;
                writeln!(output.writer, "{d}{d}{d} Compaction Plan {d}{d}{d}")
                    .map_err(|e| miette::miette!("{}", e))?;
                writeln!(
                    output.writer,
                    "  Input:   {} files, {}, {} rows",
                    plan.input_files,
                    bytesize::ByteSize(plan.input_size_bytes),
                    plan.input_rows
                )
                .map_err(|e| miette::miette!("{}", e))?;
                writeln!(
                    output.writer,
                    "  Output:  {} files (~{} target)",
                    plan.output_files, args.target_size
                )
                .map_err(|e| miette::miette!("{}", e))?;
                writeln!(output.writer).map_err(|e| miette::miette!("{}", e))?;
                for (i, group) in plan.groups.iter().enumerate() {
                    writeln!(
                        output.writer,
                        "  Group {}: {} files -> 1 ({})",
                        i + 1,
                        group.files.len(),
                        bytesize::ByteSize(group.total_bytes)
                    )
                    .map_err(|e| miette::miette!("{}", e))?;
                    for f in &group.files {
                        writeln!(output.writer, "    {}", f)
                            .map_err(|e| miette::miette!("{}", e))?;
                    }
                }
                writeln!(output.writer).map_err(|e| miette::miette!("{}", e))?;
                writeln!(output.writer, "  Run without --dry-run to execute.")
                    .map_err(|e| miette::miette!("{}", e))?;
            }
            _ => {
                crate::output::json::write_json(&mut output.writer, &plan)?;
            }
        }
        return Ok(());
    }

    // ── Backup originals (when --in-place --backup <dir>) ─────────────
    if args.in_place
        && let Some(ref backup_dir) = args.backup
    {
        std::fs::create_dir_all(backup_dir).map_err(|e| {
            miette::miette!("cannot create backup directory '{}': {}", backup_dir, e)
        })?;

        for source in &sources {
            let src_path = source.path();
            let file_name = src_path
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("unknown"));
            let dst_path = Path::new(backup_dir).join(file_name);
            std::fs::copy(src_path, &dst_path).map_err(|e| {
                miette::miette!(
                    "cannot backup '{}' to '{}': {}",
                    src_path.display(),
                    dst_path.display(),
                    e
                )
            })?;
        }
    }

    // ── Determine output directory ────────────────────────────────────
    let out_dir = output
        .output_path()
        .map(|s| s.to_string())
        .unwrap_or_else(|| ".".to_string());

    let is_cloud_out = crate::input::cloud::is_cloud_url(&out_dir);
    if !is_cloud_out {
        std::fs::create_dir_all(&out_dir)
            .map_err(|e| miette::miette!("cannot create output directory '{}': {}", out_dir, e))?;
    }

    // ── Progress bar ──────────────────────────────────────────────────
    let progress = make_progress_bar(output, groups.len() as u64);

    // ── Execute compaction ────────────────────────────────────────────
    let mut succeeded: usize = 0;
    let mut failed: usize = 0;

    for (group_idx, group) in groups.iter().enumerate() {
        let result = compact_group(
            group_idx,
            group,
            &sources,
            args,
            compression,
            &out_dir,
            is_cloud_out,
            output,
        );

        match result {
            Ok(out_path) => {
                if args.in_place {
                    for &file_idx in group {
                        let orig = sources[file_idx].path();
                        if orig != Path::new(&out_path) {
                            std::fs::remove_file(orig).map_err(|e| {
                                miette::miette!("cannot remove '{}': {}", orig.display(), e)
                            })?;
                        }
                    }
                }
                succeeded += 1;
                if let Some(ref pb) = progress {
                    pb.inc(1);
                }
                writeln!(
                    output.writer,
                    "  Group {}: {} files -> {}",
                    group_idx + 1,
                    group.len(),
                    out_path
                )
                .map_err(|e| miette::miette!("{}", e))?;
            }
            Err(e) => {
                failed += 1;
                if let Some(ref pb) = progress {
                    pb.inc(1);
                }
                writeln!(output.writer, "  Group {}: FAILED — {}", group_idx + 1, e)
                    .map_err(|e| miette::miette!("{}", e))?;
            }
        }
    }

    if let Some(ref pb) = progress {
        pb.finish_and_clear();
    }

    writeln!(
        output.writer,
        "Compacted {} files -> {} files in {}",
        sources.len(),
        groups.len(),
        out_dir
    )
    .map_err(|e| miette::miette!("{}", e))?;

    if failed > 0 {
        return Err(PqError::PartialFailure { succeeded, failed }.into());
    }

    Ok(())
}

/// Process a single compaction group: read all files, optionally sort, write output.
#[allow(clippy::too_many_arguments)]
fn compact_group(
    group_idx: usize,
    group: &[usize],
    sources: &[crate::input::source::ResolvedSource],
    args: &CompactArgs,
    compression: Compression,
    out_dir: &str,
    is_cloud_out: bool,
    output: &OutputConfig,
) -> miette::Result<String> {
    let out_path = if args.preserve_partitions {
        let first_parent = sources[group[0]]
            .path()
            .parent()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| out_dir.to_string());
        if !is_cloud_out {
            std::fs::create_dir_all(&first_parent).map_err(|e| {
                miette::miette!("cannot create directory '{}': {}", first_parent, e)
            })?;
        }
        format!("{}/compacted_{:04}.parquet", first_parent, group_idx)
    } else {
        format!("{}/compacted_{:04}.parquet", out_dir, group_idx)
    };

    let target = crate::input::cloud::resolve_output_path(&out_path)?;

    let first_source = &sources[group[0]];
    let first_file =
        File::open(first_source.path()).map_err(|e| miette::miette!("cannot open: {}", e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(first_file)
        .map_err(|e| miette::miette!("cannot read: {}", e))?;
    let schema = builder.schema().clone();

    let props = build_writer_props(args, compression);

    let mut batches: Vec<RecordBatch> = Vec::new();
    for &file_idx in group {
        let source = &sources[file_idx];
        let file = File::open(source.path())
            .map_err(|e| miette::miette!("cannot open '{}': {}", source.display_name(), e))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| miette::miette!("cannot read: {}", e))?;
        let reader = builder
            .build()
            .map_err(|e| miette::miette!("cannot read: {}", e))?;

        for batch in reader {
            let batch = batch.map_err(|e| miette::miette!("read error: {}", e))?;
            batches.push(batch);
        }
    }

    let batches = if let Some(ref sort_by) = args.sort_by {
        super::util::sort_batches(&batches, &schema, sort_by)?
    } else {
        batches
    };

    let out_file = File::create(target.local_path())
        .map_err(|e| miette::miette!("cannot create '{}': {}", out_path, e))?;
    let mut writer = ArrowWriter::try_new(out_file, schema, Some(props))
        .map_err(|e| miette::miette!("cannot create writer: {}", e))?;

    for batch in &batches {
        writer
            .write(batch)
            .map_err(|e| miette::miette!("write error: {}", e))?;
    }

    writer
        .close()
        .map_err(|e| miette::miette!("close error: {}", e))?;
    target.finalize(&output.cloud_config)?;

    Ok(out_path)
}

fn parse_size(s: &str) -> miette::Result<u64> {
    let s = s.trim();
    let (num_str, unit) =
        if let Some(stripped) = s.strip_suffix("GB").or_else(|| s.strip_suffix("gb")) {
            (stripped, 1_000_000_000u64)
        } else if let Some(stripped) = s.strip_suffix("GiB") {
            (stripped, 1_073_741_824u64)
        } else if let Some(stripped) = s.strip_suffix("MB").or_else(|| s.strip_suffix("mb")) {
            (stripped, 1_000_000u64)
        } else if let Some(stripped) = s.strip_suffix("MiB") {
            (stripped, 1_048_576u64)
        } else if let Some(stripped) = s.strip_suffix("KB").or_else(|| s.strip_suffix("kb")) {
            (stripped, 1_000u64)
        } else if let Some(stripped) = s.strip_suffix("KiB") {
            (stripped, 1_024u64)
        } else {
            (s, 1u64)
        };

    let num: u64 = num_str
        .trim()
        .parse()
        .map_err(|_| miette::miette!("invalid size: {}", s))?;

    Ok(num * unit)
}
