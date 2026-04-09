use comfy_table::Color as TableColor;
use console::{Color, Style};
use std::fmt::Display;

use crate::cli::ThemeVariant;

use super::ColorConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rgb {
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

impl Rgb {
    pub const fn new(r: u8, g: u8, b: u8) -> Self {
        Self { r, g, b }
    }

    pub const fn to_table(self) -> TableColor {
        TableColor::Rgb {
            r: self.r,
            g: self.g,
            b: self.b,
        }
    }

    pub fn to_style(self) -> Style {
        Style::new().fg(Color::Color256(rgb_to_ansi256(self)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Palette {
    pub accent: Rgb,
    pub info: Rgb,
    pub success: Rgb,
    pub warn: Rgb,
    pub danger: Rgb,
    pub muted: Rgb,
    pub text: Rgb,
}

impl Palette {
    pub const fn for_variant(variant: ThemeVariant) -> Self {
        match variant {
            ThemeVariant::Dark => Self {
                accent: Rgb::new(203, 166, 247),
                info: Rgb::new(137, 220, 235),
                success: Rgb::new(166, 227, 161),
                warn: Rgb::new(249, 226, 175),
                danger: Rgb::new(243, 139, 168),
                muted: Rgb::new(108, 112, 134),
                text: Rgb::new(205, 214, 244),
            },
            ThemeVariant::Light => Self {
                accent: Rgb::new(124, 58, 237),
                info: Rgb::new(15, 118, 110),
                success: Rgb::new(21, 128, 61),
                warn: Rgb::new(180, 83, 9),
                danger: Rgb::new(190, 18, 60),
                muted: Rgb::new(107, 114, 128),
                text: Rgb::new(31, 41, 55),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Accent,
    Info,
    Success,
    Warn,
    Danger,
    Muted,
}

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
    pub header_color: TableColor,
    pub type_col: Style,
    pub type_col_color: TableColor,
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
    pub variant: ThemeVariant,
    pub palette: Palette,
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
        let palette = Palette::for_variant(color.theme_variant);
        if color.enabled {
            Self::colored(color.theme_variant, palette)
        } else {
            Self::plain(color.theme_variant, palette)
        }
    }

    fn colored(variant: ThemeVariant, palette: Palette) -> Self {
        Self {
            variant,
            palette,
            section: SectionStyles {
                header: palette.accent.to_style().bold(),
                filename: palette.text.to_style().bold(),
                dash: palette.muted.to_style().dim(),
            },
            kv: KvStyles {
                label: palette.muted.to_style(),
                value_number: palette.warn.to_style(),
                value_size: palette.success.to_style(),
                value_ratio: palette.accent.to_style(),
                value_default: palette.text.to_style(),
            },
            table: TableStyles {
                header: palette.accent.to_style().bold(),
                header_color: palette.accent.to_table(),
                type_col: palette.info.to_style(),
                type_col_color: palette.info.to_table(),
            },
            check: CheckStyles {
                pass: palette.success.to_style(),
                fail: palette.danger.to_style(),
                warn: palette.warn.to_style(),
                detail: palette.muted.to_style().dim(),
            },
            diff: DiffStyles {
                added: palette.success.to_style(),
                removed: palette.danger.to_style(),
                changed: palette.warn.to_style(),
            },
            sql: SqlStyles {
                keyword: palette.info.to_style().bold(),
                type_name: palette.accent.to_style(),
                identifier: palette.text.to_style(),
                punctuation: palette.muted.to_style().dim(),
                string_literal: palette.success.to_style(),
                number: palette.warn.to_style(),
            },
            json: JsonStyles {
                key: palette.info.to_style(),
                string: palette.text.to_style(),
                number: palette.warn.to_style(),
                boolean: palette.accent.to_style(),
                null: palette.muted.to_style().dim(),
                punctuation: palette.muted.to_style().dim(),
            },
            color_enabled: true,
        }
    }

    fn plain(variant: ThemeVariant, palette: Palette) -> Self {
        let s = Style::new;
        Self {
            variant,
            palette,
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
                header_color: palette.accent.to_table(),
                type_col: s(),
                type_col_color: palette.info.to_table(),
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

    pub fn chip(&self, label: &str, tone: Tone) -> String {
        if !self.color_enabled {
            return format!("[{}]", label);
        }

        self.chip_style(tone)
            .apply_to(format!(" {} ", label))
            .to_string()
    }

    pub fn value_chip(&self, label: &str, value: impl Display, tone: Tone) -> String {
        self.chip(&format!("{} {}", label, value), tone)
    }

    fn chip_style(&self, tone: Tone) -> Style {
        let background = self.tone_color(tone);
        let foreground = readable_foreground(background);
        Style::new()
            .bg(Color::Color256(rgb_to_ansi256(background)))
            .fg(Color::Color256(rgb_to_ansi256(foreground)))
            .bold()
    }

    fn tone_color(&self, tone: Tone) -> Rgb {
        match tone {
            Tone::Accent => self.palette.accent,
            Tone::Info => self.palette.info,
            Tone::Success => self.palette.success,
            Tone::Warn => self.palette.warn,
            Tone::Danger => self.palette.danger,
            Tone::Muted => self.palette.muted,
        }
    }
}

fn rgb_to_ansi256(rgb: Rgb) -> u8 {
    let cube_r = cube_index(rgb.r);
    let cube_g = cube_index(rgb.g);
    let cube_b = cube_index(rgb.b);

    let cube = Rgb::new(
        CUBE_LEVELS[cube_r],
        CUBE_LEVELS[cube_g],
        CUBE_LEVELS[cube_b],
    );
    let cube_code = 16 + 36 * cube_r as u8 + 6 * cube_g as u8 + cube_b as u8;

    let avg = ((rgb.r as u16 + rgb.g as u16 + rgb.b as u16) / 3) as u8;
    let gray_index = if avg <= 8 {
        0
    } else if avg >= 238 {
        23
    } else {
        ((avg - 8) / 10) as usize
    };
    let gray_level = 8 + gray_index as u8 * 10;
    let gray = Rgb::new(gray_level, gray_level, gray_level);
    let gray_code = 232 + gray_index as u8;

    if color_distance_sq(rgb, gray) < color_distance_sq(rgb, cube) {
        gray_code
    } else {
        cube_code
    }
}

const CUBE_LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];

fn cube_index(channel: u8) -> usize {
    match channel {
        0..=47 => 0,
        48..=114 => 1,
        _ => ((channel as usize - 35) / 40).min(5),
    }
}

fn color_distance_sq(a: Rgb, b: Rgb) -> u32 {
    let dr = a.r as i32 - b.r as i32;
    let dg = a.g as i32 - b.g as i32;
    let db = a.b as i32 - b.b as i32;
    (dr * dr + dg * dg + db * db) as u32
}

fn readable_foreground(background: Rgb) -> Rgb {
    if perceived_brightness(background) >= 150 {
        Rgb::new(17, 24, 39)
    } else {
        Rgb::new(248, 250, 252)
    }
}

fn perceived_brightness(color: Rgb) -> u16 {
    let brightness = 299u32 * color.r as u32 + 587u32 * color.g as u32 + 114u32 * color.b as u32;
    (brightness / 1000) as u16
}

#[cfg(test)]
mod tests {
    use super::{Palette, Rgb, Theme, perceived_brightness};
    use crate::cli::ThemeVariant;
    use crate::output::ColorConfig;

    #[test]
    fn palette_matches_requested_dark_values() {
        let palette = Palette::for_variant(ThemeVariant::Dark);
        assert_eq!(palette.accent.r, 203);
        assert_eq!(palette.accent.g, 166);
        assert_eq!(palette.accent.b, 247);
        assert_eq!(palette.text.r, 205);
        assert_eq!(palette.text.g, 214);
        assert_eq!(palette.text.b, 244);
    }

    #[test]
    fn theme_preserves_variant_even_when_colors_are_disabled() {
        let theme = Theme::new(ColorConfig {
            enabled: false,
            theme_variant: ThemeVariant::Light,
        });

        assert_eq!(theme.variant, ThemeVariant::Light);
        assert!(!theme.color_enabled);
        assert_eq!(theme.palette.accent.r, 124);
    }

    #[test]
    fn perceived_brightness_handles_full_white_without_overflow() {
        assert_eq!(perceived_brightness(Rgb::new(255, 255, 255)), 255);
    }
}
