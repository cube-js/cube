use owo_colors::OwoColorize;
use serde_json::Value;

/// Pretty-print a JSON value to stdout.
pub fn print_json(value: &Value) {
    println!("{}", serde_json::to_string_pretty(value).unwrap_or_default());
}

/// Extract the list payload from a response. The public API wraps lists as
/// `{items: [...]}` and/or `{data: [...]}`; older endpoints return bare arrays.
pub fn items(value: &Value) -> Vec<Value> {
    for key in ["items", "data"] {
        if let Some(arr) = value.get(key).and_then(Value::as_array) {
            if !arr.is_empty() || value.get("items").is_some() || value.get("data").is_some() {
                return arr.clone();
            }
        }
    }
    match value {
        Value::Array(arr) => arr.clone(),
        Value::Null => vec![],
        other => vec![other.clone()],
    }
}

/// Render a value as a table cell.
fn stringify(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Look up a (possibly dotted) field on an object and stringify it.
pub fn field(obj: &Value, path: &str) -> String {
    let mut cur = obj;
    for part in path.split('.') {
        match cur.get(part) {
            Some(v) => cur = v,
            None => return String::new(),
        }
    }
    stringify(cur)
}

/// Print a simple aligned table.
pub fn table(headers: &[&str], rows: Vec<Vec<String>>) {
    if rows.is_empty() {
        eprintln!("{}", "No results".dimmed());
        return;
    }
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
    }
    let header_line = headers
        .iter()
        .enumerate()
        .map(|(i, h)| format!("{:<width$}", h, width = widths[i]))
        .collect::<Vec<_>>()
        .join("  ");
    println!("{}", header_line.bold());
    for row in rows {
        let line = row
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{:<width$}", c, width = widths.get(i).copied().unwrap_or(0)))
            .collect::<Vec<_>>()
            .join("  ");
        println!("{}", line.trim_end());
    }
}

/// Print a list response: raw JSON in `--json` mode, otherwise a table with
/// the given columns (header, field path).
pub fn print_list(json: bool, response: &Value, columns: &[(&str, &str)]) {
    if json {
        print_json(response);
        return;
    }
    let headers: Vec<&str> = columns.iter().map(|(h, _)| *h).collect();
    let rows = items(response)
        .iter()
        .map(|item| columns.iter().map(|(_, f)| field(item, f)).collect())
        .collect();
    table(&headers, rows);
}

pub fn success(message: &str) {
    println!("{} {}", "✓".green(), message);
}
