mod basic;
mod partitioned_rollup;
mod switch_rolling;

/// Engine-independent form of a result table. CubeStore renders timestamps as
/// `...T00:00:00.000Z` where Postgres uses `... 00:00:00` — matched on that
/// shape, so a string cell like the `YTD` calc-group value is left alone — and
/// ratios are
/// computed in f64 against Postgres' NUMERIC, so the two differ in the last
/// digit (`2.4285714285714284` vs `...86`); numbers are therefore compared
/// rounded. Everything else must match cell for cell.
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
