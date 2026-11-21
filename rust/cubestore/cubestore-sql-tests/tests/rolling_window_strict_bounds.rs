//! https://github.com/cube-js/cube/issues/8580
//!
//! A rolling window with `offset: start` is rendered by Cube as a range join whose upper bound is
//! strict: `dim >= date_from - trailing AND dim < date_from`. The rolling window optimizer turns
//! such a join into a `RollingWindowAggregate`, but `extract_dimension_and_bounds` maps `<` and
//! `<=` (and `>` / `>=`) to the same bound, which `meets_upper_bound` then applies inclusively.
//! So rows sitting exactly on `date_from` leak into the window and every bucket of a
//! pre-aggregated `offset: start` measure reads one period too far.
use cubestore::config::Config;
use cubestore::table::TableValue;
use cubestore_sql_tests::to_rows;

#[test]
fn rolling_window_respects_a_strict_upper_bound() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .thread_stack_size(4 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(Config::run_test(
        "rolling_window_respects_a_strict_upper_bound",
        |services| async move {
            let service = services.sql_service;
            service.exec_query("CREATE SCHEMA s").await?;
            service
                .exec_query("CREATE TABLE s.data(day int, n int)")
                .await?;
            service
                .exec_query("INSERT INTO s.data(day, n) VALUES (2, 2), (4, 4), (6, 6), (8, 8)")
                .await?;

            // The shape Cube emits for `trailing: 4, offset: start` over a series stepping by 2.
            let r = service
                .exec_query(
                    "SELECT q_0.d, q_0.s FROM (
                       SELECT series.date_from d, sum(base.n) s
                       FROM (
                         SELECT date_from AS date_from, date_from + 2 AS date_to
                         FROM (select unnest(generate_series(0, 10, 2))) AS series(date_from)
                       ) AS series
                       LEFT JOIN (SELECT day, n FROM s.data) AS base
                         ON base.day >= series.date_from - 4 AND base.day < series.date_from
                       GROUP BY 1
                     ) AS q_0
                     ORDER BY 1",
                )
                .await?;

            // Window for `d` is [d - 4, d): the row at `d` itself must not be counted.
            // Today Cube Store returns 0 NULL, 2 2, 4 6, 6 12, 8 18, 10 14.
            let expected = [
                (0, None),
                (2, None),
                (4, Some(2)),
                (6, Some(6)),
                (8, Some(10)),
                (10, Some(14)),
            ]
            .iter()
            .map(|(d, s)| {
                vec![
                    TableValue::Int(*d),
                    s.map(TableValue::Int).unwrap_or(TableValue::Null),
                ]
            })
            .collect::<Vec<_>>();
            assert_eq!(to_rows(&r), expected);
            Ok(())
        },
    ));
}
