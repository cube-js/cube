use cubestore_ws_transport::arrow::array::RecordBatch;
use cubestore_ws_transport::arrow::error::ArrowError;
use cubestore_ws_transport::arrow::util::display::{ArrayFormatter, FormatOptions};
use cubestore_ws_transport::{QueryResult, ResultData};

use crate::CliError;

const NULL_RENDER: &str = "NULL";

/// psql-style aligned text table. Returns `Ok(None)` for empty (DDL/INSERT) results.
pub fn render_table(result: &QueryResult) -> Result<Option<String>, CliError> {
    let columns = result.get_columns();
    if columns.is_empty() {
        return Ok(None);
    }

    let table = match &result.data {
        ResultData::Legacy { rows, .. } => render_legacy_rows(&columns, rows),
        ResultData::Arrow { batches, .. } => render_arrow_batches(&columns, batches)?,
        // Completed carries no columns, so the guard above already returned.
        ResultData::Completed => return Ok(None),
    };
    Ok(Some(table))
}

fn render_legacy_rows(columns: &[String], rows: &[Vec<Option<String>>]) -> String {
    let ncols = columns.len();
    let mut widths = header_widths(columns);
    for row in rows {
        for (i, cell) in row.iter().enumerate().take(ncols) {
            observe_width(&mut widths, i, cell_len(cell.as_deref()));
        }
    }

    let mut out = String::new();
    write_header(&mut out, columns, &widths);
    write_separator(&mut out, &widths);
    for row in rows {
        out.push('\n');
        for (i, &width) in widths.iter().enumerate() {
            let cell = row.get(i).and_then(|c| c.as_deref());
            write_cell(&mut out, i, cell, width);
        }
    }
    out
}

/// Render Arrow batches directly. Each cell is formatted twice — once to size
/// the columns, once to emit the row — which trades a bit of CPU for not
/// materializing the entire result into row based format
fn render_arrow_batches(columns: &[String], batches: &[RecordBatch]) -> Result<String, ArrowError> {
    let ncols = columns.len();
    let fmt_options = FormatOptions::default().with_display_error(true);
    let mut widths = header_widths(columns);

    for batch in batches {
        let formatters = batch_formatters(batch, &fmt_options)?;
        let batch_cols = formatters.len().min(ncols);
        for row_idx in 0..batch.num_rows() {
            for col_idx in 0..batch_cols {
                let cell = arrow_cell(batch, &formatters, col_idx, row_idx);
                observe_width(&mut widths, col_idx, cell_len(cell.as_deref()));
            }
        }
    }

    let mut out = String::new();
    write_header(&mut out, columns, &widths);
    write_separator(&mut out, &widths);
    for batch in batches {
        let formatters = batch_formatters(batch, &fmt_options)?;
        let batch_cols = formatters.len().min(ncols);
        for row_idx in 0..batch.num_rows() {
            out.push('\n');
            for (i, &width) in widths.iter().enumerate() {
                let cell = if i < batch_cols {
                    arrow_cell(batch, &formatters, i, row_idx)
                } else {
                    None
                };
                write_cell(&mut out, i, cell.as_deref(), width);
            }
        }
    }
    Ok(out)
}

fn arrow_cell(
    batch: &RecordBatch,
    formatters: &[ArrayFormatter],
    col_idx: usize,
    row_idx: usize,
) -> Option<String> {
    if batch.column(col_idx).is_null(row_idx) {
        None
    } else {
        Some(formatters[col_idx].value(row_idx).to_string())
    }
}

fn batch_formatters<'a>(
    batch: &'a RecordBatch,
    options: &'a FormatOptions,
) -> Result<Vec<ArrayFormatter<'a>>, ArrowError> {
    batch
        .columns()
        .iter()
        .map(|col| ArrayFormatter::try_new(col.as_ref(), options))
        .collect()
}

fn header_widths(columns: &[String]) -> Vec<usize> {
    columns.iter().map(|c| c.chars().count()).collect()
}

fn cell_len(cell: Option<&str>) -> usize {
    cell.unwrap_or(NULL_RENDER).chars().count()
}

fn observe_width(widths: &mut [usize], col: usize, len: usize) {
    if len > widths[col] {
        widths[col] = len;
    }
}

fn write_header(out: &mut String, columns: &[String], widths: &[usize]) {
    for (i, name) in columns.iter().enumerate() {
        if i > 0 {
            out.push('|');
        }
        out.push(' ');
        push_center(out, name, widths[i]);
        out.push(' ');
    }
    out.push('\n');
}

fn write_separator(out: &mut String, widths: &[usize]) {
    for (i, w) in widths.iter().enumerate() {
        if i > 0 {
            out.push('+');
        }
        for _ in 0..(w + 2) {
            out.push('-');
        }
    }
}

fn write_cell(out: &mut String, col: usize, cell: Option<&str>, width: usize) {
    if col > 0 {
        out.push('|');
    }
    out.push(' ');
    push_left(out, cell.unwrap_or(NULL_RENDER), width);
    out.push(' ');
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
    fn renders_all_rows_without_truncation() -> Result<(), CliError> {
        let result = QueryResult {
            data: ResultData::Legacy {
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
            },
        };
        let out = render_table(&result)?.expect("table");

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

        Ok(())
    }

    #[test]
    fn preserves_long_cell_values() -> Result<(), CliError> {
        let long = "x".repeat(500);
        let result = QueryResult {
            data: ResultData::Legacy {
                columns: vec!["v".to_string()],
                rows: vec![vec![Some(long.clone())]],
            },
        };
        let out = render_table(&result)?.expect("table");
        assert!(out.contains(&long), "long value preserved in output");

        Ok(())
    }
}
