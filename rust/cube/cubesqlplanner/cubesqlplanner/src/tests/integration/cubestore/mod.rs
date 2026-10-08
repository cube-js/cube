mod basic;
mod partitioned_rollup;
mod switch_rolling;

/// Engine-independent form of a result table: CubeStore timestamps rewritten to
/// Postgres' shape, numbers rounded (f64 vs NUMERIC differ in the last digit).
pub(super) fn normalize(table: &str) -> String {
    fn normalize_cell(cell: &str) -> String {
        let cell = cell.trim();
        // Only rewrite cells shaped like CubeStore's `2024-05-01T00:00:00.000Z`,
        // so string values carrying a `T` or `Z` — the `YTD` calc group here —
        // survive untouched.
        let cell = match cell.strip_suffix('Z') {
            Some(timestamp) if timestamp.contains('T') => {
                timestamp.replacen('T', " ", 1).replace(".000", "")
            }
            _ => cell.to_string(),
        };
        match cell.parse::<f64>() {
            Ok(value) => format!("{value:.10}"),
            Err(_) => cell,
        }
    }

    table
        .lines()
        .filter(|line| {
            // the `---+---` separator, not a data row whose first cell is negative
            !line
                .trim()
                .chars()
                .all(|c| matches!(c, '-' | '+' | ' ' | '|'))
        })
        .map(|line| {
            line.split('|')
                .map(normalize_cell)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>()
        .join("\n")
}
