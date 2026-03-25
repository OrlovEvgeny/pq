mod cli;
mod commands;
mod config;
mod error;
mod input;
mod output;
mod parquet_ext;

use clap::{CommandFactory, Parser};
use cli::{Cli, Command, CompletionShell, InspectArgs};
use error::{ExitCode, PqError};
use output::OutputConfig;

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();

    let command = cli.command.unwrap_or_else(|| {
        if cli.files.is_empty() {
            eprintln!("error: no files specified\n\nUsage: pq <COMMAND> [FILES...]\n\nFor more info, try '--help'");
            std::process::exit(ExitCode::Usage as i32);
        }
        Command::Inspect(InspectArgs {
            files: cli.files,
            all: false,
            schema_only: false,
            meta_only: false,
            raw: false,
            sort: None,
        })
    });

    let config = config::Config::load();
    let mut global = cli.global;
    config.apply_to_args(&mut global);

    let cloud_config = config.cloud_config(global.endpoint.as_deref());
    let result = run(command, &global, cloud_config);

    match result {
        Ok(()) => ExitCode::Success.into(),
        Err(err) => {
            let pq_err = err.downcast_ref::<PqError>();
            let is_validation = pq_err.is_some_and(|e| e.is_validation_failure());
            let is_partial = pq_err.is_some_and(|e| e.is_partial_failure());

            eprintln!("{:?}", err);
            if is_validation {
                ExitCode::ValidationFailed.into()
            } else if is_partial {
                ExitCode::PartialFailure.into()
            } else {
                ExitCode::Error.into()
            }
        }
    }
}

fn run(
    command: Command,
    global: &cli::GlobalArgs,
    cloud_config: input::cloud::CloudConfig,
) -> miette::Result<()> {
    if let Command::Completions(args) = &command {
        let mut cmd = Cli::command();
        let shell = match args.shell {
            CompletionShell::Bash => clap_complete::Shell::Bash,
            CompletionShell::Zsh => clap_complete::Shell::Zsh,
            CompletionShell::Fish => clap_complete::Shell::Fish,
            CompletionShell::PowerShell => clap_complete::Shell::PowerShell,
        };
        clap_complete::generate(shell, &mut cmd, "pq", &mut std::io::stdout());
        return Ok(());
    }

    let mut output = OutputConfig::from_args(global, cloud_config)?;

    let result = match &command {
        Command::Inspect(args) => commands::inspect::execute(args, &mut output),
        Command::Schema(args) => commands::schema::execute(args, &mut output),
        Command::Stats(args) => commands::stats::execute(args, &mut output),
        Command::Count(args) => commands::count::execute(args, &mut output),
        Command::Size(args) => commands::size::execute(args, &mut output),

        Command::Head(args) => commands::head::execute(args, &mut output),
        Command::Tail(args) => commands::tail::execute(args, &mut output),
        Command::Sample(args) => commands::sample::execute(args, &mut output),
        Command::Check(args) => commands::check::execute(args, &mut output),
        Command::Compact(args) => commands::compact::execute(args, &mut output),
        Command::Convert(args) => commands::convert::execute(args, &mut output),
        Command::Slice(args) => commands::slice::execute(args, &mut output),
        Command::Rewrite(args) => commands::rewrite::execute(args, &mut output),
        Command::Cat(args) => commands::cat::execute(args, &mut output),
        Command::Diff(args) => commands::diff::execute(args, &mut output),
        Command::Merge(args) => commands::merge::execute(args, &mut output),
        Command::Completions(_) => unreachable!(), // handled above
    };

    if result.is_ok() {
        output.finalize()?;
    }

    result
}
