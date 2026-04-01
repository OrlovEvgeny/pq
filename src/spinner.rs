use indicatif::{ProgressBar, ProgressStyle};
use std::io::IsTerminal;
use std::time::Duration;

const BRAILLE_FRAMES: &[&str] = &["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"];

const GRADIENT: &[(u8, u8, u8)] = &[
    (0, 210, 255),  // cyan
    (0, 150, 255),  // deep sky blue
    (60, 90, 255),  // blue
    (120, 50, 255), // blue-violet
    (170, 50, 230), // violet
    (120, 50, 255), // blue-violet
    (60, 90, 255),  // blue
    (0, 150, 255),  // deep sky blue
];

pub struct Spinner(Option<ProgressBar>);

impl Spinner {
    fn should_show(is_tty: bool, quiet: bool) -> bool {
        is_tty && !quiet && std::io::stderr().is_terminal()
    }

    pub fn new(is_tty: bool, quiet: bool, color: bool, msg: &str) -> Self {
        if !Self::should_show(is_tty, quiet) {
            return Self(None);
        }

        let pb = ProgressBar::new_spinner();

        if color {
            let colored_frames: Vec<String> = BRAILLE_FRAMES
                .iter()
                .zip(GRADIENT.iter())
                .map(|(frame, (r, g, b))| format!("\x1b[38;2;{};{};{}m{}\x1b[0m", r, g, b, frame))
                .collect();
            let tick_strs: Vec<&str> = colored_frames.iter().map(|s| s.as_str()).collect();

            pb.set_style(
                ProgressStyle::with_template("{spinner} {msg:.cyan}...")
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

    pub fn progress(is_tty: bool, quiet: bool, color: bool, msg: &str, total: u64) -> Self {
        if !Self::should_show(is_tty, quiet) {
            return Self(None);
        }

        let pb = ProgressBar::new(total);
        let template = if color {
            format!("  {{msg:.cyan}} [{{bar:20.cyan/blue}}] {{pos}}/{{len}}")
        } else {
            format!("  {msg} [{{bar:20}}] {{pos}}/{{len}}")
        };
        pb.set_style(
            ProgressStyle::with_template(&template)
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("█░"),
        );
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
