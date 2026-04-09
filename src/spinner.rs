use indicatif::{ProgressBar, ProgressStyle};
use std::io::IsTerminal;
use std::time::Duration;

use crate::cli::ThemeVariant;
use crate::output::ColorConfig;

const BRAILLE_FRAMES: &[&str] = &["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"];
const ANSI_RESET: &str = "\x1b[0m";

pub struct Spinner(Option<ProgressBar>);

impl Spinner {
    fn should_show(is_tty: bool, quiet: bool) -> bool {
        is_tty && !quiet && std::io::stderr().is_terminal()
    }

    pub fn new(is_tty: bool, quiet: bool, color: ColorConfig, msg: &str) -> Self {
        if !Self::should_show(is_tty, quiet) {
            return Self(None);
        }

        let pb = ProgressBar::new_spinner();

        if color.enabled {
            let colored_frames: Vec<String> = BRAILLE_FRAMES
                .iter()
                .zip(spinner_gradient(color.theme_variant).iter())
                .map(|(frame, rgb)| paint(frame, *rgb))
                .collect();
            let tick_strs: Vec<&str> = colored_frames.iter().map(String::as_str).collect();
            let template = format!(
                "{{spinner}} {}{{msg}}{}...",
                ansi_fg(message_color(color.theme_variant)),
                ANSI_RESET
            );

            pb.set_style(
                ProgressStyle::with_template(&template)
                    .unwrap_or_else(|_| ProgressStyle::default_spinner())
                    .tick_strings(&tick_strs),
            );
        } else {
            pb.set_style(
                ProgressStyle::with_template("{spinner} {msg}...")
                    .unwrap_or_else(|_| ProgressStyle::default_spinner())
                    .tick_strings(BRAILLE_FRAMES),
            );
        }

        pb.set_message(msg.to_string());
        pb.enable_steady_tick(Duration::from_millis(80));
        Self(Some(pb))
    }

    pub fn progress(is_tty: bool, quiet: bool, color: ColorConfig, msg: &str, total: u64) -> Self {
        if !Self::should_show(is_tty, quiet) {
            return Self(None);
        }

        let pb = ProgressBar::new(total);
        let template = if color.enabled {
            format!(
                "  {}{{msg}}{} [{}{{bar:20}}{}] {{pos}}/{{len}}",
                ansi_fg(message_color(color.theme_variant)),
                ANSI_RESET,
                ansi_fg(bar_color(color.theme_variant)),
                ANSI_RESET
            )
        } else {
            "  {msg} [{bar:20}] {pos}/{len}".to_string()
        };

        pb.set_style(
            ProgressStyle::with_template(&template)
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("█░"),
        );
        pb.set_message(msg.to_string());
        Self(Some(pb))
    }

    pub fn set_message(&self, msg: &str) {
        if let Some(ref pb) = self.0 {
            pb.set_message(msg.to_string());
        }
    }

    pub fn inc(&self, n: u64) {
        if let Some(ref pb) = self.0 {
            pb.inc(n);
        }
    }

    pub fn finish_and_clear(&self) {
        if let Some(ref pb) = self.0 {
            pb.finish_and_clear();
        }
    }
}

impl Drop for Spinner {
    fn drop(&mut self) {
        self.finish_and_clear();
    }
}

fn ansi_fg((r, g, b): (u8, u8, u8)) -> String {
    format!("\x1b[38;2;{};{};{}m", r, g, b)
}

fn paint(text: &str, rgb: (u8, u8, u8)) -> String {
    format!("{}{}{}", ansi_fg(rgb), text, ANSI_RESET)
}

fn message_color(variant: ThemeVariant) -> (u8, u8, u8) {
    match variant {
        ThemeVariant::Dark => (137, 220, 235),
        ThemeVariant::Light => (15, 118, 110),
    }
}

fn bar_color(variant: ThemeVariant) -> (u8, u8, u8) {
    match variant {
        ThemeVariant::Dark => (203, 166, 247),
        ThemeVariant::Light => (124, 58, 237),
    }
}

fn spinner_gradient(variant: ThemeVariant) -> &'static [(u8, u8, u8)] {
    match variant {
        ThemeVariant::Dark => &[
            (137, 220, 235),
            (170, 208, 241),
            (203, 166, 247),
            (223, 153, 208),
            (243, 139, 168),
            (223, 153, 208),
            (203, 166, 247),
            (170, 208, 241),
        ],
        ThemeVariant::Light => &[
            (15, 118, 110),
            (70, 88, 174),
            (124, 58, 237),
            (157, 38, 149),
            (190, 18, 60),
            (157, 38, 149),
            (124, 58, 237),
            (70, 88, 174),
        ],
    }
}
