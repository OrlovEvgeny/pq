use arrow::datatypes::Schema;

/// Parses and applies column selection syntax.
///
/// Supported formats:
/// - Exact names: `id,name,email`
/// - Regex: `/^user_.*/`
/// - Exclusion: `!metadata,!tags`
/// - Index ranges: `#0-5`, `#-3`
/// - Combined: `id,name,/^user_/,!user_internal`
pub fn select_columns(pattern: &str, schema: &Schema) -> Vec<usize> {
    let all_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let num_cols = all_names.len();
    let mut selected: Vec<bool> = vec![false; num_cols];
    let mut excluded: Vec<bool> = vec![false; num_cols];
    let mut has_positive = false;

    for part in pattern.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some(excl) = part.strip_prefix('!') {
            // Exclusion
            for (i, name) in all_names.iter().enumerate() {
                if *name == excl {
                    excluded[i] = true;
                }
            }
        } else if let Some(range) = part.strip_prefix('#') {
            // Index range
            has_positive = true;
            if let Some(neg) = range.strip_prefix('-') {
                // Last N columns
                if let Ok(n) = neg.parse::<usize>() {
                    for item in &mut selected[num_cols.saturating_sub(n)..num_cols] {
                        *item = true;
                    }
                }
            } else if let Some((start, end)) = range.split_once('-') {
                // Range: start-end
                if let (Ok(s), Ok(e)) = (start.parse::<usize>(), end.parse::<usize>()) {
                    for item in &mut selected[s..=e.min(num_cols.saturating_sub(1))] {
                        *item = true;
                    }
                }
            } else if let Ok(idx) = range.parse::<usize>()
                && idx < num_cols
            {
                selected[idx] = true;
            }
        } else if part.starts_with('/') && part.ends_with('/') && part.len() > 2 {
            // Regex
            has_positive = true;
            let regex_str = &part[1..part.len() - 1];
            if let Ok(re) = regex::Regex::new(regex_str) {
                for (i, name) in all_names.iter().enumerate() {
                    if re.is_match(name) {
                        selected[i] = true;
                    }
                }
            }
        } else {
            // Exact name
            has_positive = true;
            for (i, name) in all_names.iter().enumerate() {
                if *name == part {
                    selected[i] = true;
                }
            }
        }
    }

    // If no positive selections, start with all columns
    if !has_positive {
        selected = vec![true; num_cols];
    }

    // Apply exclusions
    (0..num_cols)
        .filter(|&i| selected[i] && !excluded[i])
        .collect()
}

/// Get projection indices from an optional column pattern
pub fn resolve_projection(columns: Option<&str>, schema: &Schema) -> Option<Vec<usize>> {
    columns.map(|pattern| select_columns(pattern, schema))
}
