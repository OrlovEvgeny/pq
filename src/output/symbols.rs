use std::sync::OnceLock;

pub struct Symbols {
    pub check: &'static str,
    pub cross: &'static str,
    pub arrow: &'static str,
    pub larrow: &'static str,
    pub dash: &'static str,
    pub warn: &'static str,
    pub emdash: &'static str,
}

const UNICODE: Symbols = Symbols {
    check: "\u{2713}",
    cross: "\u{2717}",
    arrow: "\u{2192}",
    larrow: "\u{2190}",
    dash: "\u{2500}",
    warn: "\u{26a0}",
    emdash: "\u{2014}",
};

const ASCII: Symbols = Symbols {
    check: "[ok]",
    cross: "[FAIL]",
    arrow: "->",
    larrow: "<-",
    dash: "-",
    warn: "[!]",
    emdash: "--",
};

pub fn symbols() -> &'static Symbols {
    static INSTANCE: OnceLock<&'static Symbols> = OnceLock::new();
    INSTANCE.get_or_init(|| if supports_unicode() { &UNICODE } else { &ASCII })
}

fn supports_unicode() -> bool {
    #[cfg(not(target_os = "windows"))]
    {
        true
    }
    #[cfg(target_os = "windows")]
    {
        // Windows Terminal sets WT_SESSION
        // ConEmu sets ConEmuPID
        // Modern VSCode terminal sets TERM_PROGRAM
        std::env::var("WT_SESSION").is_ok()
            || std::env::var("ConEmuPID").is_ok()
            || std::env::var("TERM_PROGRAM").is_ok()
    }
}
