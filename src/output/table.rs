use comfy_table::{ContentArrangement, Table};
use console::Style;

use super::ColorConfig;

/// Build a key-value display (like inspect output)
pub fn key_value_table(pairs: &[(&str, String)], color: ColorConfig) -> String {
    let label_style = if color.enabled {
        Style::new().bold()
    } else {
        Style::new()
    };

    let mut lines = Vec::new();
    for (key, value) in pairs {
        lines.push(format!("  {:<18}{}", label_style.apply_to(key), value));
    }
    lines.join("\n")
}

/// Build a columnar data table
pub fn data_table(headers: &[&str], rows: &[Vec<String>], color: ColorConfig) -> String {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.load_preset(comfy_table::presets::NOTHING);

    if color.enabled {
        let styled_headers: Vec<comfy_table::Cell> = headers
            .iter()
            .map(|h| comfy_table::Cell::new(h).add_attribute(comfy_table::Attribute::Bold))
            .collect();
        table.set_header(styled_headers);
    } else {
        table.set_header(headers.iter().copied());
    }

    for row in rows {
        table.add_row(row.iter().map(|c| c.as_str()));
    }

    table.to_string()
}

/// Build a section header
pub fn section_header(title: &str, file: &str, color: ColorConfig) -> String {
    let width = console::Term::stdout().size().1 as usize;
    let width = width.min(80);

    let d = crate::output::symbols::symbols().dash;
    let dash = d.repeat(3);
    let trail = d.repeat(width.saturating_sub(title.len() + file.len() + 8));

    if color.enabled {
        let style = Style::new().bold();
        format!(
            "{} {} {} {}",
            dash,
            style.apply_to(file),
            style.apply_to(title),
            trail
        )
    } else {
        format!("{} {} {} {}", dash, file, title, trail)
    }
}
