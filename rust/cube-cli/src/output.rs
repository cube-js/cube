use owo_colors::OwoColorize;
use serde_json::Value;

/// Pretty-print a JSON value to stdout.
pub fn print_json(value: &Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

/// Extract the list payload from a response. The public API wraps lists as
/// `{items: [...]}` and/or `{data: [...]}`; older endpoints return bare arrays.
pub fn items(value: &Value) -> Vec<Value> {
    for key in ["items", "data"] {
        if let Some(arr) = value.get(key).and_then(Value::as_array) {
            return arr.clone();
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
    print_list_from(json, response, None, columns);
}

/// Like [`print_list`], but reads the rows from `key` when the response
/// carries it. Needed where `items` and the deprecated `data` are two
/// *different* pages of the same list: on the endpoints that still accept
/// `offset`/`limit`, only `data` honors it while `items` is the cursor page.
pub fn print_list_from(json: bool, response: &Value, key: Option<&str>, columns: &[(&str, &str)]) {
    if json {
        print_json(response);
        return;
    }
    let headers: Vec<&str> = columns.iter().map(|(h, _)| *h).collect();
    let rows = rows_from(response, key)
        .iter()
        .map(|item| columns.iter().map(|(_, f)| field(item, f)).collect())
        .collect();
    table(&headers, rows);
}

/// The rows a list response should be rendered from: `key` when it names an
/// array the response carries, otherwise the usual `items`/`data` resolution.
fn rows_from(response: &Value, key: Option<&str>) -> Vec<Value> {
    key.and_then(|k| response.get(k))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| items(response))
}

pub fn success(message: &str) {
    println!("{} {}", "✓".green(), message);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn items_prefers_the_canonical_field_over_the_deprecated_one() {
        // Lists that kept `data` as a deprecated alias return both; `items` wins.
        let both = json!({ "items": [{"id": 1}], "data": [{"id": 2}] });
        assert_eq!(items(&both), vec![json!({"id": 1})]);
        // Endpoints already migrated off `data` (e.g. deployment versions).
        assert_eq!(
            items(&json!({ "items": [{"id": 1}] })),
            vec![json!({"id": 1})]
        );
        // Released shapes that only ever had `data`, and bare arrays.
        assert_eq!(
            items(&json!({ "data": [{"id": 2}] })),
            vec![json!({"id": 2})]
        );
        assert_eq!(items(&json!([{"id": 3}])), vec![json!({"id": 3})]);
        assert!(items(&Value::Null).is_empty());
    }

    #[test]
    fn a_requested_key_overrides_the_canonical_field() {
        // `--offset/--limit` slice only `data`; `items` is the whole cursor page.
        let res = json!({ "items": [{"id": 1}, {"id": 2}], "data": [{"id": 2}] });
        assert_eq!(rows_from(&res, Some("data")), vec![json!({"id": 2})]);
        assert_eq!(
            rows_from(&res, None),
            vec![json!({"id": 1}), json!({"id": 2})]
        );
        // A field the response doesn't carry falls back to the usual resolution.
        assert_eq!(rows_from(&res, Some("nope")).len(), 2);
    }
}
