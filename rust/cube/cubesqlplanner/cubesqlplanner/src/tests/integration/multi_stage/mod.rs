mod add_group_by;
mod bucketing;
mod calculated;
mod case_switch;
mod dimension_deps;
mod dimensions;
mod edge_cases;
mod filter_directive;
mod filters;
mod granularities;
mod group_by;
mod include_switch;
mod joins;
mod multi_fact;
mod multiple_measures;
mod order_limit;
mod plan_optimizers;
mod rank;
mod reduce_by;
mod rolling_window_grain;
mod time_shift_basic;
mod time_shift_mixed;
mod timezone;
mod ungrouped;
mod views;
mod with_rolling_window;

/// PARTITION BY clause bodies of every window function in `sql`.
pub(super) fn partition_by_clauses(sql: &str) -> Vec<String> {
    sql.match_indices("PARTITION BY")
        .map(|(i, _)| {
            let rest = &sql[i + "PARTITION BY".len()..];
            // The clause ends at the window's own `ORDER BY` or at the `)`
            // closing `OVER (`, whichever comes first.
            let mut depth = 0;
            let end = rest
                .char_indices()
                .find(|&(j, c)| match c {
                    '(' => {
                        depth += 1;
                        false
                    }
                    ')' if depth == 0 => true,
                    ')' => {
                        depth -= 1;
                        false
                    }
                    _ => depth == 0 && rest[j..].starts_with("ORDER BY"),
                })
                .map_or(rest.len(), |(j, _)| j);
            rest[..end].to_string()
        })
        .collect()
}
