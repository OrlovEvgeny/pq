use comfy_table::{ContentArrangement, Table};
use std::fmt::Display;

use super::theme::{Theme, Tone};

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
    if crate::output::symbols::supports_unicode() {
        table
            .load_preset(comfy_table::presets::UTF8_BORDERS_ONLY)
            .apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS);
    } else {
        table.load_preset(comfy_table::presets::ASCII_BORDERS_ONLY_CONDENSED);
    }

    if theme.color_enabled {
        let styled_headers: Vec<comfy_table::Cell> = headers
            .iter()
            .map(|h| {
                comfy_table::Cell::new(h)
                    .add_attribute(comfy_table::Attribute::Bold)
                    .fg(theme.table.header_color)
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
            .filter(|(_, h)| matches!(**h, "Type" | "Physical" | "Logical"))
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
                        comfy_table::Cell::new(c).fg(theme.table.type_col_color)
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
    let unicode = crate::output::symbols::supports_unicode();
    let (left, right, rule) = if unicode {
        ("╭", "╮", "─")
    } else {
        ("+", "+", "-")
    };

    let mut plain_parts = Vec::new();
    let mut styled_parts = Vec::new();

    if !title.is_empty() {
        plain_parts.push(format!(" {} ", title));
        styled_parts.push(theme.chip(title, Tone::Accent));
    }
    if !file.is_empty() {
        plain_parts.push(file.to_string());
        styled_parts.push(theme.section.filename.apply_to(file).to_string());
    }
    if plain_parts.is_empty() {
        plain_parts.push(" Section ".to_string());
        styled_parts.push(theme.chip("Section", Tone::Accent));
    }

    let plain_content = plain_parts.join(" ");
    let styled_content = styled_parts.join(" ");
    let trail = rule.repeat(width.saturating_sub(plain_content.chars().count() + 5));

    if theme.color_enabled {
        format!(
            "{}{} {} {}{}",
            theme.section.dash.apply_to(left),
            theme.section.dash.apply_to(rule),
            styled_content,
            theme.section.dash.apply_to(&trail),
            theme.section.dash.apply_to(right)
        )
    } else {
        format!("{left}{rule} {plain_content} {trail}{right}")
    }
}

pub fn diff_status_chip(status: &str, theme: &Theme) -> String {
    let tone = match status {
        "same" | "identical" => Tone::Muted,
        "added" => Tone::Success,
        "removed" => Tone::Danger,
        _ => Tone::Warn,
    };
    theme.chip(status, tone)
}

pub fn check_status_chip(passed: bool, theme: &Theme) -> String {
    if passed {
        theme.chip("PASS", Tone::Success)
    } else {
        theme.chip("FAIL", Tone::Danger)
    }
}

pub fn metric_chip(label: &str, value: impl Display, tone: Tone, theme: &Theme) -> String {
    theme.value_chip(label, value, tone)
}

pub fn compression_chip(codec: &str, theme: &Theme) -> String {
    theme.chip(codec, Tone::Info)
}

pub fn size_chip(label: &str, value: impl Display, theme: &Theme) -> String {
    metric_chip(label, value, Tone::Success, theme)
}

pub fn count_chip(label: &str, value: impl Display, theme: &Theme) -> String {
    metric_chip(label, value, Tone::Warn, theme)
}

pub fn ratio_chip(label: &str, ratio: f64, theme: &Theme) -> String {
    let tone = if ratio >= 2.5 {
        Tone::Success
    } else if ratio >= 1.25 {
        Tone::Accent
    } else {
        Tone::Warn
    };
    metric_chip(label, format!("{ratio:.1}x"), tone, theme)
}

pub fn percent_chip(label: &str, percent: f64, theme: &Theme) -> String {
    let tone = if percent >= 50.0 {
        Tone::Warn
    } else {
        Tone::Accent
    };
    metric_chip(label, format!("{percent:.1}%"), tone, theme)
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

    theme.kv.value_default.apply_to(value).to_string()
}
