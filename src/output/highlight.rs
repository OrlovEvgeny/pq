use regex::Regex;
use std::sync::OnceLock;

use super::theme::Theme;

struct SqlPatterns {
    keywords: Regex,
    types: Regex,
    strings: Regex,
    numbers: Regex,
    punctuation: Regex,
}

fn sql_patterns() -> &'static SqlPatterns {
    static PATTERNS: OnceLock<SqlPatterns> = OnceLock::new();
    PATTERNS.get_or_init(|| SqlPatterns {
        keywords: Regex::new(r"(?i)\b(CREATE|TABLE|NOT|NULL|DEFAULT|PRIMARY|KEY|IF|EXISTS|ENGINE|ORDER|BY|PARTITION|CLUSTER|OPTIONS|COMMENT|USING|LOCATION|ROW\s+FORMAT|STORED\s+AS|TBLPROPERTIES)\b").unwrap(),
        types: Regex::new(concat!(
            r"(?i)\b(",
            // Standard SQL types
            r"VARCHAR|CHAR|INTEGER|INT|BIGINT|SMALLINT|TINYINT|BOOLEAN|BOOL",
            r"|TIMESTAMP|TIMESTAMP_NTZ|TIMESTAMP_LTZ|DATE|TIME|DATETIME",
            r"|TEXT|FLOAT|DOUBLE|REAL|DECIMAL|NUMERIC|NUMBER",
            r"|UUID|BLOB|BINARY|VARBINARY|BYTEA|BYTES",
            r"|STRING|VARIANT|OBJECT|ARRAY|MAP|STRUCT|RECORD",
            r"|JSONB|JSON|SUPER|GEOMETRY|GEOGRAPHY",
            // ClickHouse types
            r"|Int8|Int16|Int32|Int64|Int128|Int256",
            r"|UInt8|UInt16|UInt32|UInt64|UInt128|UInt256",
            r"|Float32|Float64|DateTime64|Date32|FixedString",
            r"|Nullable|LowCardinality|Enum8|Enum16",
            // Spark/PySpark types
            r"|StructType|StructField|StringType|IntegerType|LongType",
            r"|DoubleType|FloatType|BooleanType|TimestampType|DateType",
            r"|DecimalType|BinaryType|ArrayType|MapType|ShortType|ByteType",
            r")\b",
        )).unwrap(),
        strings: Regex::new(r#""[^"]*"|'[^']*'"#).unwrap(),
        numbers: Regex::new(r"\b\d+\b").unwrap(),
        punctuation: Regex::new(r"[(),;]").unwrap(),
    })
}

/// Highlight a SQL DDL string with theme colors.
pub fn highlight_sql(ddl: &str, theme: &Theme) -> String {
    if !theme.color_enabled {
        return ddl.to_string();
    }

    let patterns = sql_patterns();
    let mut result = String::with_capacity(ddl.len() * 2);
    let mut pos = 0;
    let bytes = ddl.as_bytes();

    // Collect all matches with their positions and types
    let mut spans: Vec<(usize, usize, SpanType)> = Vec::new();

    for m in patterns.strings.find_iter(ddl) {
        spans.push((m.start(), m.end(), SpanType::SqlString));
    }
    for m in patterns.keywords.find_iter(ddl) {
        if !overlaps_any(&spans, m.start(), m.end()) {
            spans.push((m.start(), m.end(), SpanType::SqlKeyword));
        }
    }
    for m in patterns.types.find_iter(ddl) {
        if !overlaps_any(&spans, m.start(), m.end()) {
            spans.push((m.start(), m.end(), SpanType::SqlType));
        }
    }
    for m in patterns.numbers.find_iter(ddl) {
        if !overlaps_any(&spans, m.start(), m.end()) {
            spans.push((m.start(), m.end(), SpanType::SqlNumber));
        }
    }
    for m in patterns.punctuation.find_iter(ddl) {
        if !overlaps_any(&spans, m.start(), m.end()) {
            spans.push((m.start(), m.end(), SpanType::SqlPunctuation));
        }
    }

    spans.sort_by_key(|s| s.0);

    for (start, end, span_type) in &spans {
        // Append unmatched text before this span
        if *start > pos {
            result.push_str(&ddl[pos..*start]);
        }
        if *start < pos {
            continue; // skip overlapping
        }

        let text = &ddl[*start..*end];
        let styled = match span_type {
            SpanType::SqlKeyword => theme.sql.keyword.apply_to(text).to_string(),
            SpanType::SqlType => theme.sql.type_name.apply_to(text).to_string(),
            SpanType::SqlString => theme.sql.string_literal.apply_to(text).to_string(),
            SpanType::SqlNumber => theme.sql.number.apply_to(text).to_string(),
            SpanType::SqlPunctuation => theme.sql.punctuation.apply_to(text).to_string(),
        };
        result.push_str(&styled);
        pos = *end;
    }

    // Append remaining text
    if pos < bytes.len() {
        result.push_str(&ddl[pos..]);
    }

    result
}

/// Highlight inline JSON with theme colors.
pub fn highlight_json(json: &str, theme: &Theme) -> String {
    if !theme.color_enabled {
        return json.to_string();
    }

    let mut result = String::with_capacity(json.len() * 2);
    let chars: Vec<char> = json.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        match chars[i] {
            '{' | '}' | '[' | ']' | ':' | ',' => {
                result.push_str(&theme.json.punctuation.apply_to(chars[i]).to_string());
                i += 1;
            }
            '"' => {
                // Collect the full string
                let start = i;
                i += 1;
                while i < len && chars[i] != '"' {
                    if chars[i] == '\\' {
                        i += 1; // skip escaped char
                    }
                    i += 1;
                }
                if i < len {
                    i += 1; // closing quote
                }
                let s: String = chars[start..i].iter().collect();

                // Look ahead to see if this is a key (followed by ':')
                let mut j = i;
                while j < len && chars[j].is_whitespace() {
                    j += 1;
                }
                if j < len && chars[j] == ':' {
                    result.push_str(&theme.json.key.apply_to(&s).to_string());
                } else {
                    result.push_str(&theme.json.string.apply_to(&s).to_string());
                }
            }
            c if c.is_ascii_digit() || c == '-' => {
                let start = i;
                i += 1;
                while i < len
                    && (chars[i].is_ascii_digit()
                        || chars[i] == '.'
                        || chars[i] == 'e'
                        || chars[i] == 'E'
                        || chars[i] == '+'
                        || chars[i] == '-')
                {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                result.push_str(&theme.json.number.apply_to(&s).to_string());
            }
            't' if i + 4 <= len && chars[i..i + 4].iter().collect::<String>() == "true" => {
                result.push_str(&theme.json.boolean.apply_to("true").to_string());
                i += 4;
            }
            'f' if i + 5 <= len && chars[i..i + 5].iter().collect::<String>() == "false" => {
                result.push_str(&theme.json.boolean.apply_to("false").to_string());
                i += 5;
            }
            'n' if i + 4 <= len && chars[i..i + 4].iter().collect::<String>() == "null" => {
                result.push_str(&theme.json.null.apply_to("null").to_string());
                i += 4;
            }
            c => {
                result.push(c);
                i += 1;
            }
        }
    }

    result
}

#[derive(Clone, Copy)]
#[allow(clippy::enum_variant_names)]
enum SpanType {
    SqlKeyword,
    SqlType,
    SqlString,
    SqlNumber,
    SqlPunctuation,
}

fn overlaps_any(spans: &[(usize, usize, SpanType)], start: usize, end: usize) -> bool {
    spans.iter().any(|(s, e, _)| start < *e && end > *s)
}
