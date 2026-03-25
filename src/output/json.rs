use serde::Serialize;
use std::io::Write;

/// Write a single JSON value (pretty-printed)
pub fn write_json<W: Write, T: Serialize>(writer: &mut W, value: &T) -> miette::Result<()> {
    serde_json::to_writer_pretty(&mut *writer, value)
        .map_err(|e| miette::miette!("JSON serialization failed: {}", e))?;
    writeln!(writer).map_err(|e| miette::miette!("write failed: {}", e))?;
    Ok(())
}

/// Write a value as a single JSONL line
pub fn write_jsonl<W: Write, T: Serialize>(writer: &mut W, value: &T) -> miette::Result<()> {
    serde_json::to_writer(&mut *writer, value)
        .map_err(|e| miette::miette!("JSON serialization failed: {}", e))?;
    writeln!(writer).map_err(|e| miette::miette!("write failed: {}", e))?;
    Ok(())
}
