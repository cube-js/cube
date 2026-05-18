use std::time::Instant;

use anyhow::Result;
use cubestore_ws_transport::Client;

use crate::format;

/// Run a single statement and print the result. Returns Ok even on a query error,
/// matching `psql` behavior in script mode (errors print but don't kill the process)
/// — caller decides whether to bail.
pub async fn run_one(client: &Client, sql: &str, show_timing: bool) -> Result<bool> {
    let started = Instant::now();
    match client.query(sql).await {
        Ok(result) => {
            let elapsed_ms = started.elapsed().as_millis();
            let footer = make_footer(&result, show_timing, elapsed_ms);
            // `\x1b[?7h` = DECAWM on. rustyline may turn autowrap off for its own
            // cursor accounting and not restore it; without this, long table rows
            // get clipped at the right edge of the terminal instead of wrapping.
            print!("\x1b[?7h");
            match format::render_table(&result) {
                Some(table) => println!("{table}\n{footer}"),
                None => println!("{footer}"),
            }
            Ok(true)
        }
        Err(e) => {
            print!("\x1b[?7h");
            eprintln!("ERROR: {e}");
            Ok(false)
        }
    }
}

fn make_footer(
    result: &cubestore_ws_transport::QueryResult,
    show_timing: bool,
    elapsed_ms: u128,
) -> String {
    let has_table = !result.columns.is_empty();
    let n = result.rows.len();
    let rows_part = if has_table {
        format!("{n} {}", if n == 1 { "row" } else { "rows" })
    } else {
        "OK".to_string()
    };
    if show_timing {
        format!("({rows_part}, Time: {elapsed_ms} ms)")
    } else {
        format!("({rows_part})")
    }
}

/// Split a script into SQL statements on `;` boundaries that are outside string
/// literals. Empty statements are skipped. Same level of rigor as `psql -f`.
pub fn split_statements(input: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut buf = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            // Inside a single-quoted string, '' is an escaped quote — stay inside.
            '\'' if in_single => {
                buf.push(c);
                if chars.peek() == Some(&'\'') {
                    buf.push(chars.next().unwrap());
                } else {
                    in_single = false;
                }
            }
            '\'' if !in_double => {
                buf.push(c);
                in_single = true;
            }
            // Same rule for double-quoted identifiers: "" is an escaped ".
            '"' if in_double => {
                buf.push(c);
                if chars.peek() == Some(&'"') {
                    buf.push(chars.next().unwrap());
                } else {
                    in_double = false;
                }
            }
            '"' if !in_single => {
                buf.push(c);
                in_double = true;
            }
            ';' if !in_single && !in_double => {
                let trimmed = buf.trim().to_string();
                if !trimmed.is_empty() {
                    out.push(trimmed);
                }
                buf.clear();
            }
            _ => buf.push(c),
        }
    }

    let trailing = buf.trim();
    if !trailing.is_empty() {
        out.push(trailing.to_string());
    }
    out
}

pub async fn run_script(client: &Client, script: &str, show_timing: bool) -> Result<bool> {
    let stmts = split_statements(script);
    let mut all_ok = true;
    for stmt in stmts {
        let ok = run_one(client, &stmt, show_timing).await?;
        all_ok &= ok;
    }
    Ok(all_ok)
}
