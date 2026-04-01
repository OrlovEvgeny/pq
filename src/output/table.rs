use comfy_table::{ContentArrangement, Table};

use super::theme::Theme;

/// Build a key-value display (like inspect output)
pub fn key_value_table(pairs: &[(&str, String)], theme: &Theme) -> String {
    let mut lines = Vec::new();
    for (key, value) in pairs {
        let styled_key = theme.kv.label.apply_to(key);
        let styled_value = style_value(value, theme);
        lines.push(format!("  {:<18}{}", styled_key, styled_value));
    }
    lines.join("\n")
}

/// Build a columnar data table
pub fn data_table(headers: &[&str], rows: &[Vec<String>], theme: &Theme) -> String {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.load_preset(comfy_table::presets::NOTHING);

    if theme.color_enabled {
        let styled_headers: Vec<comfy_table::Cell> = headers
            .iter()
            .map(|h| {
                comfy_table::Cell::new(h)
                    .add_attribute(comfy_table::Attribute::Bold)
                    .fg(comfy_table::Color::Cyan)
            })
            .collect();
        table.set_header(styled_headers);
    } else {
        table.set_header(headers.iter().copied());
    }

    // Detect type column indices for special coloring
    let type_col_indices: Vec<usize> = if theme.color_enabled {
        headers
            .iter()
            .enumerate()
            .filter(|(_, h)| matches!(*h, &"Type" | &"Physical" | &"Logical"))
            .map(|(i, _)| i)
            .collect()
    } else {
        Vec::new()
    };

    for row in rows {
        if type_col_indices.is_empty() {
            table.add_row(row.iter().map(|c| c.as_str()));
        } else {
            let styled_cells: Vec<comfy_table::Cell> = row
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    if type_col_indices.contains(&i) {
                        comfy_table::Cell::new(c).fg(comfy_table::Color::Green)
                    } else {
                        comfy_table::Cell::new(c)
                    }
                })
                .collect();
            table.add_row(styled_cells);
        }
    }

    table.to_string()
}

/// Build a section header
pub fn section_header(title: &str, file: &str, theme: &Theme) -> String {
    let width = console::Term::stdout().size().1 as usize;
    let width = width.min(80);

    let d = crate::output::symbols::symbols().dash;
    let dash = d.repeat(3);
    let trail = d.repeat(width.saturating_sub(title.len() + file.len() + 8));

    if theme.color_enabled {
        format!(
            "{} {} {} {}",
            theme.section.dash.apply_to(&dash),
            theme.section.filename.apply_to(file),
            theme.section.header.apply_to(title),
            theme.section.dash.apply_to(&trail)
        )
    } else {
        format!("{} {} {} {}", dash, file, title, trail)
    }
}

/// Apply heuristic value styling based on content
fn style_value(value: &str, theme: &Theme) -> String {
    if !theme.color_enabled {
        return value.to_string();
    }

    // Size values: contains byte units
    if value.contains("MB")
        || value.contains("KB")
        || value.contains("GB")
        || value.contains("TB")
        || value.contains(" B ")
        || value.contains(" B)")
        || (value.ends_with(" B") && value.len() > 2)
    {
        return theme.kv.value_size.apply_to(value).to_string();
    }

    // Ratio values
    if value.contains("x ratio") || value.ends_with("x)") {
        return theme.kv.value_ratio.apply_to(value).to_string();
    }

    // Pure number values (digits, commas, spaces)
    let trimmed = value.trim();
    if !trimmed.is_empty() && trimmed.chars().all(|c| c.is_ascii_digit() || c == ',') {
        return theme.kv.value_number.apply_to(value).to_string();
    }

    value.to_string()
}
