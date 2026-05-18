use cubestore_ws_transport::QueryResult;

const NULL_RENDER: &str = "NULL";

/// psql-style aligned text table. Returns `None` for empty (DDL/INSERT) results.
pub fn render_table(result: &QueryResult) -> Option<String> {
    if result.columns.is_empty() {
        return None;
    }

    let ncols = result.columns.len();
    let mut widths: Vec<usize> = result.columns.iter().map(|c| c.chars().count()).collect();
    for row in &result.rows {
        for (i, cell) in row.iter().enumerate() {
            if i >= ncols {
                break;
            }
            let len = match cell.as_deref() {
                Some(s) => s.chars().count(),
                None => NULL_RENDER.chars().count(),
            };
            if len > widths[i] {
                widths[i] = len;
            }
        }
    }

    let mut out = String::new();

    // Header
    for (i, name) in result.columns.iter().enumerate() {
        if i > 0 {
            out.push('|');
        }
        out.push(' ');
        push_center(&mut out, name, widths[i]);
        out.push(' ');
    }
    out.push('\n');

    // Separator
    for (i, w) in widths.iter().enumerate() {
        if i > 0 {
            out.push('+');
        }
        for _ in 0..(w + 2) {
            out.push('-');
        }
    }

    // Data rows
    for row in &result.rows {
        out.push('\n');
        for i in 0..ncols {
            if i > 0 {
                out.push('|');
            }
            out.push(' ');
            let cell = match row.get(i) {
                Some(Some(s)) => s.as_str(),
                Some(None) | None => NULL_RENDER,
            };
            push_left(&mut out, cell, widths[i]);
            out.push(' ');
        }
    }

    Some(out)
}

fn push_left(out: &mut String, s: &str, width: usize) {
    out.push_str(s);
    let pad = width.saturating_sub(s.chars().count());
    for _ in 0..pad {
        out.push(' ');
    }
}

fn push_center(out: &mut String, s: &str, width: usize) {
    let len = s.chars().count();
    if len >= width {
        out.push_str(s);
        return;
    }
    let total = width - len;
    let left = total / 2;
    let right = total - left;
    for _ in 0..left {
        out.push(' ');
    }
    out.push_str(s);
    for _ in 0..right {
        out.push(' ');
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_all_rows_without_truncation() {
        let result = QueryResult {
            columns: vec!["table_catalog".to_string(), "table_schema".to_string()],
            rows: vec![
                vec![
                    Some("ovr".to_string()),
                    Some("information_schema".to_string()),
                ],
                vec![
                    Some("ovr".to_string()),
                    Some("information_schema".to_string()),
                ],
                vec![
                    Some("ovr".to_string()),
                    Some("information_schema".to_string()),
                ],
                vec![Some("ovr".to_string()), Some("public".to_string())],
                vec![Some("ovr".to_string()), Some("public".to_string())],
            ],
        };
        let out = render_table(&result).expect("table");

        // header line + separator + 5 data rows = 7 lines
        assert_eq!(out.lines().count(), 7);

        // every data cell present
        assert_eq!(out.matches("information_schema").count(), 3);
        assert_eq!(out.matches("public").count(), 2);
        assert_eq!(out.matches(" ovr").count(), 5);

        // long-value column width is honored (header centered to data width 18)
        let header = out.lines().next().unwrap();
        assert!(
            header.contains("    table_schema    "),
            "header centered to data width: {header:?}"
        );
    }

    #[test]
    fn preserves_long_cell_values() {
        let long = "x".repeat(500);
        let result = QueryResult {
            columns: vec!["v".to_string()],
            rows: vec![vec![Some(long.clone())]],
        };
        let out = render_table(&result).expect("table");
        assert!(out.contains(&long), "long value preserved in output");
    }
}
