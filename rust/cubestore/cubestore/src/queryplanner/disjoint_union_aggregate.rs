//! Pushes an aggregate below a `UNION ALL` of tables when no group can take rows from two
//! branches.
//!
//! The proof uses the min/max rows the metastore keeps for every partition and chunk. A group key
//! `f(c)`, where `c` is the leading sort column of each branch's index and `f` is the identity or
//! `date_trunc`, separates the branches when the intervals `[f(min c), f(max c)]` do not overlap.
//! Every group then lives in exactly one branch, so aggregating each branch on its own gives the
//! final value and the aggregate above the union is not needed.

use crate::queryplanner::optimizations::rewrite_plan::{rewrite_plan, PlanRewriter};
use crate::queryplanner::planning::ChooseIndexContext;
use crate::queryplanner::serialized_plan::IndexSnapshot;
use crate::queryplanner::CubeTableLogical;
use crate::table::{Row, TableValue, TimestampValue};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::DefaultTableSource;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Aggregate, ColumnarValue, Expr, Filter, LogicalPlan, Projection, ScalarFunctionArgs, ScalarUDF,
    SubqueryAlias, TableScan, Union,
};
use std::cmp::Ordering;
use std::sync::Arc;

/// `indices` must be in the order `choose_index_ext` collected them: one entry per cube table
/// scan, in plan traversal order. The rewrite keeps the scans in that order.
pub fn push_aggregate_into_disjoint_union(
    p: LogicalPlan,
    indices: &[IndexSnapshot],
    limit_pushdown: bool,
    min_rows_per_branch: u64,
) -> Result<LogicalPlan, DataFusionError> {
    let mut r = DisjointUnionAggregate {
        indices,
        limit_pushdown,
        min_rows_per_branch,
        next_scan: 0,
    };
    rewrite_plan(p, &Scope::default(), &mut r)
}

struct DisjointUnionAggregate<'a> {
    indices: &'a [IndexSnapshot],
    limit_pushdown: bool,
    min_rows_per_branch: u64,
    next_scan: usize,
}

#[derive(Clone, Default)]
struct Scope {
    // Ordinal of the first cube table scan below the nearest aggregate.
    first_scan: usize,
    // What `ChooseIndex` will see here, so the two agree on whether the `LIMIT` reaches the workers.
    choose_index: ChooseIndexContext,
}

impl PlanRewriter for DisjointUnionAggregate<'_> {
    type Context = Scope;

    fn enter_node(&mut self, n: &LogicalPlan, c: &Scope) -> Option<Scope> {
        let first_scan = match n {
            LogicalPlan::Aggregate(_) => self.next_scan,
            _ => c.first_scan,
        };
        let choose_index = c.choose_index.enter(n);
        if choose_index.is_none() && first_scan == c.first_scan {
            return None;
        }
        Some(Scope {
            first_scan,
            choose_index: choose_index.unwrap_or_else(|| c.choose_index.clone()),
        })
    }

    fn rewrite(&mut self, n: LogicalPlan, c: &Scope) -> Result<LogicalPlan, DataFusionError> {
        match n {
            LogicalPlan::TableScan(ref scan) => {
                if is_cube_table_scan(scan) {
                    self.next_scan += 1;
                }
                Ok(n)
            }
            LogicalPlan::Aggregate(agg) => {
                // A worker that stops after the first `n` groups beats aggregating every branch.
                if self.limit_pushdown && c.choose_index.group_limit_reaches_workers() {
                    return Ok(LogicalPlan::Aggregate(agg));
                }
                let Some(scans) = self.indices.get(c.first_scan..self.next_scan) else {
                    return Ok(LogicalPlan::Aggregate(agg));
                };
                match try_push_down(&agg, scans, self.min_rows_per_branch)? {
                    Some(p) => Ok(p),
                    None => Ok(LogicalPlan::Aggregate(agg)),
                }
            }
            n => Ok(n),
        }
    }
}

// Same test `CollectConstraints` and `ChooseIndex` use to pair a scan with an index snapshot.
fn is_cube_table_scan(scan: &TableScan) -> bool {
    scan.source
        .as_any()
        .downcast_ref::<DefaultTableSource>()
        .map_or(false, |s| {
            s.table_provider
                .as_any()
                .downcast_ref::<CubeTableLogical>()
                .is_some()
        })
}

fn try_push_down(
    agg: &Aggregate,
    scans: &[IndexSnapshot],
    min_rows_per_branch: u64,
) -> Result<Option<LogicalPlan>, DataFusionError> {
    if agg.group_expr.is_empty() {
        return Ok(None);
    }
    let mut input = agg.input.as_ref();
    while let LogicalPlan::SubqueryAlias(SubqueryAlias { input: i, .. }) = input {
        input = i.as_ref();
    }
    let LogicalPlan::Union(Union {
        inputs: branches, ..
    }) = input
    else {
        return Ok(None);
    };
    if branches.len() < 2
        || branches.len() != scans.len()
        || !branches.iter().all(|b| is_single_cube_scan_branch(b))
    {
        return Ok(None);
    }
    // Every branch becomes its own `ClusterSend`; that only pays off on large enough branches.
    let rows: u64 = scans.iter().map(row_count).sum();
    if rows < min_rows_per_branch.saturating_mul(scans.len() as u64) {
        return Ok(None);
    }
    let mut ranges = Vec::with_capacity(scans.len());
    for s in scans {
        match leading_range(s) {
            Some(r) => ranges.push(r),
            None => return Ok(None),
        }
    }

    let input_schema = agg.input.schema();
    let separates = agg.group_expr.iter().any(|g| {
        let Some((column, trunc)) = group_key_on_column(g) else {
            return false;
        };
        let Ok(pos) = input_schema.index_of_column(&column) else {
            return false;
        };
        branches_disjoint(branches, scans, &ranges, pos, trunc)
    });
    if !separates {
        return Ok(None);
    }

    let mut pushed = Vec::with_capacity(branches.len());
    for b in branches {
        let to_branch = |e: Expr| -> Result<Expr, DataFusionError> {
            e.transform(|e| match e {
                Expr::Column(c) => {
                    let idx = input_schema.index_of_column(&c)?;
                    let (q, f) = b.schema().qualified_field(idx);
                    Ok(Transformed::yes(Expr::Column(Column::from((q, f)))))
                }
                e => Ok(Transformed::no(e)),
            })
            .map(|t| t.data)
        };
        let group = agg
            .group_expr
            .iter()
            .cloned()
            .map(to_branch)
            .collect::<Result<Vec<_>, _>>()?;
        let aggr = agg
            .aggr_expr
            .iter()
            .cloned()
            .map(to_branch)
            .collect::<Result<Vec<_>, _>>()?;
        pushed.push(Arc::new(LogicalPlan::Aggregate(Aggregate::try_new(
            b.clone(),
            group,
            aggr,
        )?)));
    }
    let union = LogicalPlan::Union(Union::try_new_with_loose_types(pushed)?);
    let exprs = (0..agg.schema.fields().len())
        .map(|i| {
            let (uq, uf) = union.schema().qualified_field(i);
            let (oq, of) = agg.schema.qualified_field(i);
            Expr::Column(Column::from((uq, uf))).alias_qualified(oq.cloned(), of.name())
        })
        .collect();
    Ok(Some(LogicalPlan::Projection(Projection::try_new(
        exprs,
        Arc::new(union),
    )?)))
}

fn is_single_cube_scan_branch(p: &LogicalPlan) -> bool {
    match p {
        LogicalPlan::TableScan(scan) => is_cube_table_scan(scan),
        LogicalPlan::Projection(Projection { input, .. })
        | LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. })
        | LogicalPlan::Filter(Filter { input, .. }) => is_single_cube_scan_branch(input),
        _ => false,
    }
}

// `col` or `date_trunc(<literal>, col)`.
fn group_key_on_column(e: &Expr) -> Option<(Column, Option<(&Arc<ScalarUDF>, &Expr)>)> {
    match e {
        Expr::Column(c) => Some((c.clone(), None)),
        Expr::Alias(a) => group_key_on_column(&a.expr),
        Expr::ScalarFunction(ScalarFunction { func, args }) if func.name() == "date_trunc" => {
            match args.as_slice() {
                [g @ Expr::Literal(_), Expr::Column(c)] => Some((c.clone(), Some((func, g)))),
                _ => None,
            }
        }
        _ => None,
    }
}

// The scanned table column that output `pos` of `p` passes through unchanged.
fn passthrough_column(p: &LogicalPlan, pos: usize) -> Option<String> {
    match p {
        LogicalPlan::TableScan(TableScan {
            projected_schema, ..
        }) => Some(projected_schema.field(pos).name().clone()),
        LogicalPlan::Projection(Projection { expr, input, .. }) => {
            let mut e = expr.get(pos)?;
            while let Expr::Alias(a) = e {
                e = &a.expr;
            }
            let Expr::Column(c) = e else {
                return None;
            };
            passthrough_column(input, input.schema().index_of_column(c).ok()?)
        }
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. })
        | LogicalPlan::Filter(Filter { input, .. }) => passthrough_column(input, pos),
        _ => None,
    }
}

fn branches_disjoint(
    branches: &[Arc<LogicalPlan>],
    scans: &[IndexSnapshot],
    ranges: &[Option<(TableValue, TableValue)>],
    pos: usize,
    trunc: Option<(&Arc<ScalarUDF>, &Expr)>,
) -> bool {
    let mut mapped = Vec::with_capacity(ranges.len());
    for ((b, s), r) in branches.iter().zip(scans).zip(ranges) {
        let Some(name) = passthrough_column(b, pos) else {
            return false;
        };
        let columns = s.index.get_row().get_columns();
        if columns.first().map(|c| c.get_name()) != Some(&name) {
            return false;
        }
        let Some((min, max)) = r else {
            continue;
        };
        let data_type = b.schema().field(pos).data_type();
        let (Some(min), Some(max)) = (apply(trunc, min, data_type), apply(trunc, max, data_type))
        else {
            return false;
        };
        mapped.push((min, max));
    }
    // Empty branches hold no group; with fewer than two loaded ones there is nothing to split.
    if mapped.len() < 2 {
        return false;
    }
    let mut failed = false;
    mapped.sort_by(|a, b| {
        cmp_values(&a.0, &b.0).unwrap_or_else(|| {
            failed = true;
            Ordering::Equal
        })
    });
    !failed
        && mapped
            .windows(2)
            .all(|w| cmp_values(&w[0].1, &w[1].0) == Some(Ordering::Less))
}

fn row_count(s: &IndexSnapshot) -> u64 {
    s.partitions
        .iter()
        .map(|p| {
            p.partition.get_row().main_table_row_count()
                + p.chunks
                    .iter()
                    .map(|c| c.get_row().get_row_count())
                    .sum::<u64>()
        })
        .sum()
}

// Min and max of the leading sort column over the scanned partitions and their chunks. `Some(None)`
// for an empty snapshot, `None` when a bound is unknown, NULL or not comparable.
fn leading_range(s: &IndexSnapshot) -> Option<Option<(TableValue, TableValue)>> {
    let mut range: Option<(TableValue, TableValue)> = None;
    let mut add = |lo: &Option<Row>, hi: &Option<Row>| -> Option<()> {
        let lo = lo.as_ref()?.values().first()?;
        let hi = hi.as_ref()?.values().first()?;
        cmp_values(lo, hi)?;
        range = Some(match range.take() {
            None => (lo.clone(), hi.clone()),
            Some((min, max)) => (
                if cmp_values(lo, &min)? == Ordering::Less {
                    lo.clone()
                } else {
                    min
                },
                if cmp_values(hi, &max)? == Ordering::Greater {
                    hi.clone()
                } else {
                    max
                },
            ),
        });
        Some(())
    };
    for p in &s.partitions {
        let row = p.partition.get_row();
        if row.main_table_row_count() > 0 {
            add(row.get_min(), row.get_max())?;
        }
        for c in &p.chunks {
            let c = c.get_row();
            add(c.min(), c.max())?;
        }
    }
    Some(range)
}

// Orders two values of the same comparable variant. NULLs and mixed or unsupported variants give
// `None`, which makes the rewrite back off.
fn cmp_values(a: &TableValue, b: &TableValue) -> Option<Ordering> {
    Some(match (a, b) {
        (TableValue::String(a), TableValue::String(b)) => a.cmp(b),
        (TableValue::Int(a), TableValue::Int(b)) => a.cmp(b),
        (TableValue::Decimal(a), TableValue::Decimal(b)) => a.cmp(b),
        (TableValue::Float(a), TableValue::Float(b)) => a.cmp(b),
        (TableValue::Bytes(a), TableValue::Bytes(b)) => a.cmp(b),
        (TableValue::Timestamp(a), TableValue::Timestamp(b)) => a.cmp(b),
        (TableValue::Boolean(a), TableValue::Boolean(b)) => a.cmp(b),
        _ => return None,
    })
}

// Evaluates `date_trunc` with the very UDF the query calls, so bucket boundaries match execution.
fn apply(
    trunc: Option<(&Arc<ScalarUDF>, &Expr)>,
    v: &TableValue,
    data_type: &DataType,
) -> Option<TableValue> {
    let Some((func, Expr::Literal(granularity))) = trunc else {
        return trunc.is_none().then(|| v.clone());
    };
    let (TableValue::Timestamp(t), DataType::Timestamp(unit, tz)) = (v, data_type) else {
        return None;
    };
    let nanos = t.get_time_stamp();
    let arg = match unit {
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(nanos), tz.clone()),
        TimeUnit::Microsecond => {
            ScalarValue::TimestampMicrosecond(Some(nanos.div_euclid(1_000)), tz.clone())
        }
        TimeUnit::Millisecond => {
            ScalarValue::TimestampMillisecond(Some(nanos.div_euclid(1_000_000)), tz.clone())
        }
        TimeUnit::Second => {
            ScalarValue::TimestampSecond(Some(nanos.div_euclid(1_000_000_000)), tz.clone())
        }
    };
    let return_type = func
        .return_type(&[granularity.data_type(), data_type.clone()])
        .ok()?;
    let result = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(granularity.clone()),
                ColumnarValue::Scalar(arg),
            ],
            number_rows: 1,
            return_type: &return_type,
        })
        .ok()?;
    let ColumnarValue::Scalar(result) = result else {
        return None;
    };
    let nanos = match result {
        ScalarValue::TimestampNanosecond(Some(v), _) => v,
        ScalarValue::TimestampMicrosecond(Some(v), _) => v.checked_mul(1_000)?,
        ScalarValue::TimestampMillisecond(Some(v), _) => v.checked_mul(1_000_000)?,
        ScalarValue::TimestampSecond(Some(v), _) => v.checked_mul(1_000_000_000)?,
        _ => return None,
    };
    Some(TableValue::Timestamp(TimestampValue::new(nanos)))
}

#[cfg(test)]
mod tests {
    use super::cmp_values;
    use crate::config::{Config, CubeServices};
    use crate::sql::{timestamp_from_string, SqlService};
    use crate::table::{Row, TableValue};
    use crate::util::decimal::{Decimal, Decimal96};
    use crate::util::int96::Int96;
    use crate::CubeError;
    use std::future::Future;
    use std::sync::Arc;

    async fn exec(service: &Arc<dyn SqlService>, sql: &str) -> Result<Vec<Row>, CubeError> {
        Ok(service
            .exec_query(sql)
            .await?
            .collect()
            .await?
            .get_rows()
            .clone())
    }

    async fn logical_plan(service: &Arc<dyn SqlService>, sql: &str) -> String {
        match &exec(service, &format!("EXPLAIN {}", sql)).await.unwrap()[0].values()[0] {
            TableValue::String(s) => s.clone(),
            v => panic!("unexpected EXPLAIN output: {:?}", v),
        }
    }

    // The rewrite turns `Aggregate <- Union` into `Union <- Aggregate`.
    fn pushed_down(plan: &str) -> bool {
        let union = plan.find("Union").expect(plan);
        plan[union..].contains("Aggregate")
    }

    fn ts(s: &str) -> TableValue {
        TableValue::Timestamp(timestamp_from_string(s).unwrap())
    }

    fn row(m: &str, k: &str, v: i64) -> Row {
        Row::new(vec![
            ts(m),
            TableValue::String(k.to_string()),
            TableValue::Int(v),
        ])
    }

    async fn run<F, Fut>(name: &str, test: F)
    where
        F: FnOnce(Arc<dyn SqlService>) -> Fut + Send,
        Fut: Future<Output = Result<(), CubeError>> + Send,
    {
        Config::test(name)
            .update_config(|mut c| {
                c.partition_split_threshold = 2;
                c.disjoint_union_aggregate = true;
                c.disjoint_union_aggregate_min_rows_per_branch = 0;
                c
            })
            .start_test(async move |services: CubeServices| test(services.sql_service).await)
            .await;
    }

    async fn create_month_tables(service: &Arc<dyn SqlService>) -> Result<(), CubeError> {
        exec(service, "CREATE SCHEMA s").await?;
        for (t, month) in [("t1", "2024-01"), ("t2", "2024-02"), ("t3", "2024-03")] {
            exec(
                service,
                &format!("CREATE TABLE s.{} (m timestamp, k text, v int)", t),
            )
            .await?;
            for chunk in 0..2 {
                exec(
                    service,
                    &format!(
                        "INSERT INTO s.{t} (m, k, v) VALUES \
                         ('{month}-01T00:00:00.000Z', 'a', {a}), \
                         ('{month}-01T00:00:00.000Z', 'b', {b}), \
                         ('{month}-15T00:00:00.000Z', 'a', 100)",
                        a = 1 + chunk,
                        b = 10 + chunk,
                    ),
                )
                .await?;
            }
        }
        Ok(())
    }

    const UNION3: &str =
        "SELECT * FROM s.t1 UNION ALL SELECT * FROM s.t2 UNION ALL SELECT * FROM s.t3";

    #[tokio::test]
    async fn disjoint_month_trunc() {
        run(
            "disjoint_union_aggregate_month_trunc",
            |service| async move {
                create_month_tables(&service).await?;
                let sql = format!(
                "SELECT date_trunc('month', m), k, sum(v) FROM ({}) t GROUP BY 1, 2 ORDER BY 1, 2",
                UNION3
            );
                assert!(pushed_down(&logical_plan(&service, &sql).await));
                assert_eq!(
                    exec(&service, &sql).await?,
                    vec![
                        row("2024-01-01T00:00:00.000Z", "a", 203),
                        row("2024-01-01T00:00:00.000Z", "b", 21),
                        row("2024-02-01T00:00:00.000Z", "a", 203),
                        row("2024-02-01T00:00:00.000Z", "b", 21),
                        row("2024-03-01T00:00:00.000Z", "a", 203),
                        row("2024-03-01T00:00:00.000Z", "b", 21),
                    ]
                );
                Ok(())
            },
        )
        .await;
    }

    #[tokio::test]
    async fn disjoint_raw_key_top_k_and_having() {
        run("disjoint_union_aggregate_raw_key", |service| async move {
            create_month_tables(&service).await?;
            let top = format!(
                "SELECT m, k, sum(v) s FROM ({}) t GROUP BY 1, 2 ORDER BY 3 DESC, 1 LIMIT 2",
                UNION3
            );
            assert!(pushed_down(&logical_plan(&service, &top).await));
            assert_eq!(
                exec(&service, &top).await?,
                vec![
                    row("2024-01-15T00:00:00.000Z", "a", 200),
                    row("2024-02-15T00:00:00.000Z", "a", 200),
                ]
            );
            let having = format!(
                "SELECT m, k, sum(v) s FROM ({}) t GROUP BY 1, 2 HAVING sum(v) < 5 ORDER BY 1",
                UNION3
            );
            assert!(pushed_down(&logical_plan(&service, &having).await));
            assert_eq!(
                exec(&service, &having).await?,
                vec![
                    row("2024-01-01T00:00:00.000Z", "a", 3),
                    row("2024-02-01T00:00:00.000Z", "a", 3),
                    row("2024-03-01T00:00:00.000Z", "a", 3),
                ]
            );
            Ok(())
        })
        .await;
    }

    #[tokio::test]
    async fn limit_on_group_order_stays_with_workers() {
        run(
            "disjoint_union_aggregate_group_order_limit",
            |service| async move {
                create_month_tables(&service).await?;
                for order in ["ORDER BY 1, 2", "ORDER BY 2", ""] {
                    let sql = format!(
                        "SELECT m, k, sum(v) s FROM ({}) t GROUP BY 1, 2 {} LIMIT 2",
                        UNION3, order
                    );
                    assert!(!pushed_down(&logical_plan(&service, &sql).await), "{}", sql);
                }
                let sql = format!(
                    "SELECT m, k, sum(v) s FROM ({}) t GROUP BY 1, 2 ORDER BY 1, 2 LIMIT 2",
                    UNION3
                );
                assert_eq!(
                    exec(&service, &sql).await?,
                    vec![
                        row("2024-01-01T00:00:00.000Z", "a", 3),
                        row("2024-01-01T00:00:00.000Z", "b", 21),
                    ]
                );
                let having = format!(
                    "SELECT m, k, sum(v) s FROM ({}) t GROUP BY 1, 2 HAVING sum(v) < 5 \
                 ORDER BY 1 LIMIT 2",
                    UNION3
                );
                assert!(pushed_down(&logical_plan(&service, &having).await));
                Ok(())
            },
        )
        .await;
    }

    #[tokio::test]
    async fn small_branches_keep_union_aggregate() {
        Config::test("disjoint_union_aggregate_small_branches")
            .update_config(|mut c| {
                c.partition_split_threshold = 2;
                c.disjoint_union_aggregate = true;
                c.disjoint_union_aggregate_min_rows_per_branch = 7;
                c
            })
            .start_test(async move |services: CubeServices| {
                let service = services.sql_service;
                create_month_tables(&service).await?;
                let sql = |n: usize| {
                    let tables = ["s.t1", "s.t2", "s.t3"][..n]
                        .iter()
                        .map(|t| format!("SELECT * FROM {}", t))
                        .collect::<Vec<_>>()
                        .join(" UNION ALL ");
                    format!("SELECT m, k, sum(v) FROM ({}) t GROUP BY 1, 2", tables)
                };
                // Six rows per table: below the threshold of seven.
                assert!(!pushed_down(&logical_plan(&service, &sql(3)).await));
                exec(
                    &service,
                    "INSERT INTO s.t1 (m, k, v) VALUES ('2024-01-02T00:00:00.000Z', 'a', 1), \
                     ('2024-01-03T00:00:00.000Z', 'a', 1)",
                )
                .await?;
                exec(
                    &service,
                    "INSERT INTO s.t2 (m, k, v) VALUES ('2024-02-02T00:00:00.000Z', 'a', 1)",
                )
                .await?;
                // Eight and seven rows: seven and a half on average.
                assert!(pushed_down(&logical_plan(&service, &sql(2)).await));
                Ok(())
            })
            .await;
    }

    #[tokio::test]
    async fn overlapping_branches_keep_union_aggregate() {
        run("disjoint_union_aggregate_overlap", |service| async move {
            create_month_tables(&service).await?;
            exec(
                &service,
                "INSERT INTO s.t3 (m, k, v) VALUES ('2024-02-01T00:00:00.000Z', 'a', 1000)",
            )
            .await?;
            let sql = format!(
                "SELECT date_trunc('month', m), k, sum(v) FROM ({}) t GROUP BY 1, 2 ORDER BY 1, 2",
                UNION3
            );
            assert!(!pushed_down(&logical_plan(&service, &sql).await));
            assert_eq!(
                exec(&service, &sql).await?[2],
                row("2024-02-01T00:00:00.000Z", "a", 1203)
            );

            let same_table = "SELECT k, sum(v) FROM \
                (SELECT * FROM s.t1 UNION ALL SELECT * FROM s.t1) t GROUP BY 1 ORDER BY 1";
            assert!(!pushed_down(&logical_plan(&service, same_table).await));

            exec(
                &service,
                "CREATE TABLE s.empty (m timestamp, k text, v int)",
            )
            .await?;
            let one_loaded = "SELECT m, sum(v) FROM \
                (SELECT * FROM s.t1 UNION ALL SELECT * FROM s.empty) t GROUP BY 1 ORDER BY 1";
            assert!(!pushed_down(&logical_plan(&service, one_loaded).await));
            Ok(())
        })
        .await;
    }

    #[tokio::test]
    async fn coarse_trunc_merges_branches() {
        run("disjoint_union_aggregate_year", |service| async move {
            create_month_tables(&service).await?;
            let sql = format!(
                "SELECT date_trunc('year', m), sum(v) FROM ({}) t GROUP BY 1",
                UNION3
            );
            assert!(!pushed_down(&logical_plan(&service, &sql).await));
            assert_eq!(
                exec(&service, &sql).await?,
                vec![Row::new(vec![
                    ts("2024-01-01T00:00:00.000Z"),
                    TableValue::Int(672)
                ])]
            );
            Ok(())
        })
        .await;
    }

    // DDL `decimal96` keeps its min/max as plain `Decimal`, so the ranges still prove disjointness.
    #[tokio::test]
    async fn decimal96_leading_column() {
        run("disjoint_union_aggregate_decimal96", |service| async move {
            exec(&service, "CREATE SCHEMA s").await?;
            for (t, base) in [("a", 1), ("b", 100)] {
                exec(
                    &service,
                    &format!("CREATE TABLE s.{t} (d decimal96(2), v int)"),
                )
                .await?;
                exec(
                    &service,
                    &format!(
                        "INSERT INTO s.{t} (d, v) VALUES ({base}, 1), ({base}, 2), ({}, 3)",
                        base + 1
                    ),
                )
                .await?;
            }
            let sql = "SELECT d, sum(v) FROM \
                       (SELECT * FROM s.a UNION ALL SELECT * FROM s.b) t GROUP BY 1 ORDER BY 1";
            assert!(pushed_down(&logical_plan(&service, sql).await));
            let sums = exec(&service, sql)
                .await?
                .iter()
                .map(|r| r.values()[1].clone())
                .collect::<Vec<_>>();
            assert_eq!(sums, vec![TableValue::Int(3); 4]);
            Ok(())
        })
        .await;
    }

    #[test]
    fn unordered_values_back_off() {
        let int96 = TableValue::Int96(Int96::new(1));
        let decimal96 = TableValue::Decimal96(Decimal96::new(1));
        assert_eq!(cmp_values(&int96, &int96), None);
        assert_eq!(cmp_values(&decimal96, &decimal96), None);
        assert_eq!(cmp_values(&TableValue::Null, &TableValue::Int(1)), None);
        assert_eq!(
            cmp_values(&TableValue::Int(1), &TableValue::Decimal(Decimal::new(1))),
            None
        );
    }
}
