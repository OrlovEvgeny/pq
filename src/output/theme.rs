use console::Style;

use super::ColorConfig;

#[allow(dead_code)]
pub struct SectionStyles {
    pub header: Style,
    pub filename: Style,
    pub dash: Style,
}

#[allow(dead_code)]
pub struct KvStyles {
    pub label: Style,
    pub value_number: Style,
    pub value_size: Style,
    pub value_ratio: Style,
    pub value_default: Style,
}

#[allow(dead_code)]
pub struct TableStyles {
    pub header: Style,
    pub type_col: Style,
}

pub struct CheckStyles {
    pub pass: Style,
    pub fail: Style,
    pub warn: Style,
    pub detail: Style,
}

pub struct DiffStyles {
    pub added: Style,
    pub removed: Style,
    pub changed: Style,
    pub same: Style,
}

#[allow(dead_code)]
pub struct SqlStyles {
    pub keyword: Style,
    pub type_name: Style,
    pub identifier: Style,
    pub punctuation: Style,
    pub string_literal: Style,
    pub number: Style,
}

pub struct JsonStyles {
    pub key: Style,
    pub string: Style,
    pub number: Style,
    pub boolean: Style,
    pub null: Style,
    pub punctuation: Style,
}

#[allow(dead_code)]
pub struct Theme {
    pub section: SectionStyles,
    pub kv: KvStyles,
    pub table: TableStyles,
    pub check: CheckStyles,
    pub diff: DiffStyles,
    pub sql: SqlStyles,
    pub json: JsonStyles,
    pub color_enabled: bool,
}

impl Theme {
    pub fn new(color: ColorConfig) -> Self {
        if color.enabled {
            Self::colored()
        } else {
            Self::plain()
        }
    }

    fn colored() -> Self {
        Self {
            section: SectionStyles {
                header: Style::new().bold().cyan(),
                filename: Style::new().bold().white(),
                dash: Style::new().dim(),
            },
            kv: KvStyles {
                label: Style::new().bold().cyan(),
                value_number: Style::new().yellow(),
                value_size: Style::new().green(),
                value_ratio: Style::new().magenta(),
                value_default: Style::new(),
            },
            table: TableStyles {
                header: Style::new().bold().cyan(),
                type_col: Style::new().green().dim(),
            },
            check: CheckStyles {
                pass: Style::new().green(),
                fail: Style::new().red(),
                warn: Style::new().yellow(),
                detail: Style::new().dim(),
            },
            diff: DiffStyles {
                added: Style::new().green(),
                removed: Style::new().red(),
                changed: Style::new().yellow(),
                same: Style::new().dim(),
            },
            sql: SqlStyles {
                keyword: Style::new().bold().blue(),
                type_name: Style::new().cyan(),
                identifier: Style::new(),
                punctuation: Style::new().dim(),
                string_literal: Style::new().green(),
                number: Style::new().yellow(),
            },
            json: JsonStyles {
                key: Style::new().cyan(),
                string: Style::new().green(),
                number: Style::new().yellow(),
                boolean: Style::new().magenta(),
                null: Style::new().dim(),
                punctuation: Style::new().dim(),
            },
            color_enabled: true,
        }
    }

    fn plain() -> Self {
        let s = Style::new;
        Self {
            section: SectionStyles {
                header: s(),
                filename: s(),
                dash: s(),
            },
            kv: KvStyles {
                label: s(),
                value_number: s(),
                value_size: s(),
                value_ratio: s(),
                value_default: s(),
            },
            table: TableStyles {
                header: s(),
                type_col: s(),
            },
            check: CheckStyles {
                pass: s(),
                fail: s(),
                warn: s(),
                detail: s(),
            },
            diff: DiffStyles {
                added: s(),
                removed: s(),
                changed: s(),
                same: s(),
            },
            sql: SqlStyles {
                keyword: s(),
                type_name: s(),
                identifier: s(),
                punctuation: s(),
                string_literal: s(),
                number: s(),
            },
            json: JsonStyles {
                key: s(),
                string: s(),
                number: s(),
                boolean: s(),
                null: s(),
                punctuation: s(),
            },
            color_enabled: false,
        }
    }
}
