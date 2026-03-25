use std::io::Write;

/// Write rows as CSV (with header)
pub fn write_csv<W: Write>(
    writer: &mut W,
    headers: &[&str],
    rows: &[Vec<String>],
) -> miette::Result<()> {
    let mut csv_writer = csv::Writer::from_writer(writer);
    csv_writer
        .write_record(headers)
        .map_err(|e| miette::miette!("CSV write failed: {}", e))?;
    for row in rows {
        csv_writer
            .write_record(row)
            .map_err(|e| miette::miette!("CSV write failed: {}", e))?;
    }
    csv_writer
        .flush()
        .map_err(|e| miette::miette!("CSV flush failed: {}", e))?;
    Ok(())
}

/// Write rows as TSV (with header)
pub fn write_tsv<W: Write>(
    writer: &mut W,
    headers: &[&str],
    rows: &[Vec<String>],
) -> miette::Result<()> {
    let mut csv_writer = csv::WriterBuilder::new()
        .delimiter(b'\t')
        .from_writer(writer);
    csv_writer
        .write_record(headers)
        .map_err(|e| miette::miette!("TSV write failed: {}", e))?;
    for row in rows {
        csv_writer
            .write_record(row)
            .map_err(|e| miette::miette!("TSV write failed: {}", e))?;
    }
    csv_writer
        .flush()
        .map_err(|e| miette::miette!("TSV flush failed: {}", e))?;
    Ok(())
}
