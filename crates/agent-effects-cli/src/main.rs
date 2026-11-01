//! The `agent-effects` operator CLI. See the crate's README.

use std::io::{BufRead, IsTerminal, Write};
use std::process::ExitCode;

use agent_effects_cli::cli::Cli;
use agent_effects_cli::{Confirm, Io, exit, run};
use clap::Parser;

/// Asks on the terminal; refuses to guess when input is piped.
struct Terminal;

impl Confirm for Terminal {
    fn confirm(&mut self, prompt: &str) -> Option<bool> {
        let stdin = std::io::stdin();
        if !stdin.is_terminal() {
            return None;
        }
        let mut err = std::io::stderr();
        let _ = write!(err, "{prompt}");
        let _ = err.flush();
        let mut answer = String::new();
        stdin.lock().read_line(&mut answer).ok()?;
        Some(answer.trim() == "yes")
    }
}

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            let _ = e.print();
            return ExitCode::from(if e.use_stderr() {
                exit::USAGE
            } else {
                exit::OK
            });
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("error: cannot start: {e}");
            return ExitCode::from(exit::ERROR);
        }
    };
    let (mut out, mut err) = (std::io::stdout(), std::io::stderr());
    let code = runtime.block_on(run(
        cli,
        &mut Io {
            out: &mut out,
            err: &mut err,
            confirm: &mut Terminal,
        },
    ));
    let _ = out.flush();
    ExitCode::from(code)
}
