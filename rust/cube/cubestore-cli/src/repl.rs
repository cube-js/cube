use anyhow::Result;
use cubestore_ws_transport::Client;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::Editor;

use crate::exec;

const HELP: &str = "\
Meta-commands:
  \\q, \\quit    quit the REPL
  \\h, \\?       this help
  \\timing      toggle elapsed-time display

Statements are terminated with `;`.";

pub async fn run(client: &Client, mut timing: bool) -> Result<()> {
    let history_path = dirs::home_dir().map(|p| p.join(".cubestore_history"));

    let mut rl: Editor<(), DefaultHistory> = Editor::new()?;
    if let Some(ref p) = history_path {
        let _ = rl.load_history(p);
    }

    let mut buffer = String::new();

    loop {
        let prompt = if buffer.is_empty() {
            "cubestore=> "
        } else {
            "cubestore-> "
        };

        let line = match rl.readline(prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C: clear current buffer, continue.
                buffer.clear();
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D at empty buffer: exit.
                if buffer.is_empty() {
                    break;
                }
                buffer.clear();
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        let trimmed = line.trim();

        // Meta commands only apply on an empty buffer.
        if buffer.is_empty() && trimmed.starts_with('\\') {
            let _ = rl.add_history_entry(line.as_str());
            match trimmed {
                "\\q" | "\\quit" => break,
                "\\h" | "\\?" => {
                    println!("{HELP}");
                }
                "\\timing" => {
                    timing = !timing;
                    println!("Timing is {}", if timing { "on" } else { "off" });
                }
                other => {
                    eprintln!("Unknown meta-command: {other}");
                }
            }
            continue;
        }

        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(&line);

        // Submit when the accumulated buffer ends with `;` outside any string literal.
        if statement_complete(&buffer) {
            let stmt = buffer.trim().trim_end_matches(';').trim().to_string();
            let _ = rl.add_history_entry(buffer.trim_end().to_string());
            buffer.clear();
            if stmt.is_empty() {
                continue;
            }
            exec::run_one(client, &stmt, timing).await?;
        }
    }

    if let Some(ref p) = history_path {
        let _ = rl.save_history(p);
    }

    // Make sure autowrap is on when we hand the terminal back.
    print!("\x1b[?7h");

    Ok(())
}

fn statement_complete(s: &str) -> bool {
    let mut in_single = false;
    let mut in_double = false;
    let mut last_non_ws: Option<char> = None;

    for c in s.chars() {
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            _ => {}
        }
        if !c.is_whitespace() {
            last_non_ws = Some(c);
        }
    }

    !in_single && !in_double && last_non_ws == Some(';')
}
