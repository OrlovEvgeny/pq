use indicatif::{ProgressBar, ProgressStyle};
use std::io::IsTerminal;
use std::time::Duration;

const BRAILLE_FRAMES: &[&str] = &["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"];

pub struct Spinner(Option<ProgressBar>);

impl Spinner {
    fn should_show(is_tty: bool, quiet: bool) -> bool {
        is_tty && !quiet && std::io::stderr().is_terminal()
    }

    pub fn new(is_tty: bool, quiet: bool, msg: &str) -> Self {
        if !Self::should_show(is_tty, quiet) {
            return Self(None);
        }

        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("{spinner} {msg}...")
                .unwrap_or_else(|_| ProgressStyle::default_spinner())
                .tick_strings(BRAILLE_FRAMES),
        );
        pb.set_message(msg.to_string());
        pb.enable_steady_tick(Duration::from_millis(80));
        Self(Some(pb))
    }

    pub fn progress(is_tty: bool, quiet: bool, msg: &str, total: u64) -> Self {
        if !Self::should_show(is_tty, quiet) {
            return Self(None);
        }

        let pb = ProgressBar::new(total);
        pb.set_style(
            ProgressStyle::with_template(&format!("  {msg} [{{bar:20}}] {{pos}}/{{len}}"))
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
