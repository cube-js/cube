//! Verifies `IsNotDistinctFromJoinKeysRule`: `IS NOT DISTINCT FROM` predicates
//! in a JOIN's ON clause are lifted into equi-keys with `null_equals_null = true`,
//! so DF's physical planner picks `HashJoinExec` instead of `NestedLoopJoinExec`
//! and NULL keys still match.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::queryplanner::QueryPlannerImpl;

fn make_table(rows: &[(Option<i32>, Option<i32>)]) -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]));
    let col_a = Int32Array::from(rows.iter().map(|(a, _)| *a).collect::<Vec<_>>());
    let col_b = Int32Array::from(rows.iter().map(|(_, b)| *b).collect::<Vec<_>>());
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(col_a), Arc::new(col_b)]).unwrap();
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

/// Table with a non-null `tag` column. A successful LEFT JOIN match against
/// a NULL-bearing row in this table is visible in the result because `tag`
/// remains non-null, distinguishing it from LEFT-padding.
fn make_tagged_table(tag: i32, rows: &[(Option<i32>, Option<i32>)]) -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
        Field::new("tag", DataType::Int32, false),
    ]));
    let col_a = Int32Array::from(rows.iter().map(|(a, _)| *a).collect::<Vec<_>>());
    let col_b = Int32Array::from(rows.iter().map(|(_, b)| *b).collect::<Vec<_>>());
    let col_tag = Int32Array::from(vec![tag; rows.len()]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(col_a), Arc::new(col_b), Arc::new(col_tag)],
    )
    .unwrap();
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

async fn optimized_logical(ctx: &SessionContext, sql: &str) -> LogicalPlan {
    let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
    ctx.state().optimize(&plan).unwrap()
}

async fn physical(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    let opt = optimized_logical(ctx, sql).await;
    ctx.state().create_physical_plan(&opt).await.unwrap()
}

async fn run(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    pretty_format_batches(&batches).unwrap().to_string()
}

fn count_exec(plan: &dyn ExecutionPlan, needle: &str) -> usize {
    displayable(plan)
        .indent(true)
        .to_string()
        .matches(needle)
        .count()
}

fn assert_plan_str(actual: String, expected: &str) {
    let actual = actual.trim();
    let expected = expected.trim();
    assert_eq!(
        actual, expected,
        "\nexpected:\n{expected}\n\nactual:\n{actual}"
    );
}

fn ab_context() -> SessionContext {
    let ctx = QueryPlannerImpl::make_execution_context(SessionConfig::new());
    let rows = &[
        (Some(1), Some(10)),
        (None, Some(20)),
        (Some(3), None),
        (None, None),
        (Some(4), Some(40)),
    ];
    let b_rows = &[
        (Some(1), Some(10)),
        (None, Some(20)),
        (Some(3), None),
        (None, None),
        (Some(5), Some(50)),
    ];
    ctx.register_table("a", make_table(rows)).unwrap();
    ctx.register_table("b", make_table(b_rows)).unwrap();
    ctx
}

const ORDER: &str = "ORDER BY a_a NULLS LAST, a_b NULLS LAST";

#[tokio::test]
async fn equality_join_uses_hash_join_and_excludes_null_keys() {
    let ctx = ab_context();
    let sql = &format!(
        "SELECT a.a AS a_a, a.b AS a_b, b.a AS b_a, b.b AS b_b \
         FROM a LEFT JOIN b ON a.a = b.a AND a.b = b.b {ORDER}"
    );

    let logical = optimized_logical(&ctx, sql).await;
    assert!(
        logical
            .display_indent()
            .to_string()
            .contains("Left Join: a.a = b.a, a.b = b.b"),
        "expected equi-keys lifted into ON, got:\n{}",
        logical.display_indent()
    );

    let physical = physical(&ctx, sql).await;
    assert_eq!(count_exec(physical.as_ref(), "HashJoinExec"), 1);
    assert_eq!(count_exec(physical.as_ref(), "NestedLoopJoinExec"), 0);

    assert_plan_str(
        run(&ctx, sql).await,
        "\
+-----+-----+-----+-----+
| a_a | a_b | b_a | b_b |
+-----+-----+-----+-----+
| 1   | 10  | 1   | 10  |
| 3   |     |     |     |
| 4   | 40  |     |     |
|     | 20  |     |     |
|     |     |     |     |
+-----+-----+-----+-----+",
    );
}

#[tokio::test]
async fn is_not_distinct_from_folds_into_hash_join_keys() {
    let ctx = ab_context();
    let sql = &format!(
        "SELECT a.a AS a_a, a.b AS a_b, b.a AS b_a, b.b AS b_b \
         FROM a LEFT JOIN b \
         ON (a.a IS NOT DISTINCT FROM b.a) AND (a.b IS NOT DISTINCT FROM b.b) \
         {ORDER}"
    );

    let logical = optimized_logical(&ctx, sql).await;
    assert!(
        logical
            .display_indent()
            .to_string()
            .contains("Left Join: a.a = b.a, a.b = b.b"),
        "expected IS NOT DISTINCT FROM predicates lifted into ON, got:\n{}",
        logical.display_indent()
    );

    let physical = physical(&ctx, sql).await;
    assert_eq!(
        count_exec(physical.as_ref(), "HashJoinExec"),
        1,
        "expected one HashJoinExec, got plan:\n{}",
        displayable(physical.as_ref()).indent(true)
    );
    assert_eq!(
        count_exec(physical.as_ref(), "NestedLoopJoinExec"),
        0,
        "rule failed to lift IS NOT DISTINCT FROM, plan:\n{}",
        displayable(physical.as_ref()).indent(true)
    );

    assert_plan_str(
        run(&ctx, sql).await,
        "\
+-----+-----+-----+-----+
| a_a | a_b | b_a | b_b |
+-----+-----+-----+-----+
| 1   | 10  | 1   | 10  |
| 3   |     | 3   |     |
| 4   | 40  |     |     |
|     | 20  |     | 20  |
|     |     |     |     |
+-----+-----+-----+-----+",
    );
}

#[tokio::test]
async fn is_not_distinct_from_lifts_many_predicates() {
    let ctx = QueryPlannerImpl::make_execution_context(SessionConfig::new());

    let schema = Arc::new(Schema::new(vec![
        Field::new("k1", DataType::Int32, true),
        Field::new("k2", DataType::Int32, true),
        Field::new("k3", DataType::Int32, true),
        Field::new("k4", DataType::Int32, true),
        Field::new("k5", DataType::Int32, true),
    ]));
    let rows: Vec<(
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
        Option<i32>,
    )> = vec![
        (Some(1), None, Some(3), None, Some(5)),
        (None, None, None, None, None),
        (Some(1), Some(2), Some(3), Some(4), Some(5)),
        (None, Some(2), Some(3), Some(4), Some(5)),
    ];
    let int_col = |idx: fn(
        &(
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
        ),
    ) -> Option<i32>|
     -> Int32Array { Int32Array::from(rows.iter().map(idx).collect::<Vec<_>>()) };
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(int_col(|r| r.0)),
            Arc::new(int_col(|r| r.1)),
            Arc::new(int_col(|r| r.2)),
            Arc::new(int_col(|r| r.3)),
            Arc::new(int_col(|r| r.4)),
        ],
    )
    .unwrap();
    let make = || Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap());
    ctx.register_table("a", make()).unwrap();
    ctx.register_table("b", make()).unwrap();

    let sql = "SELECT a.k1, a.k2, a.k3, a.k4, a.k5, \
                      b.k1 AS b1, b.k2 AS b2, b.k3 AS b3, b.k4 AS b4, b.k5 AS b5 \
               FROM a LEFT JOIN b \
               ON (a.k1 IS NOT DISTINCT FROM b.k1) \
              AND (a.k2 IS NOT DISTINCT FROM b.k2) \
              AND (a.k3 IS NOT DISTINCT FROM b.k3) \
              AND (a.k4 IS NOT DISTINCT FROM b.k4) \
              AND (a.k5 IS NOT DISTINCT FROM b.k5) \
               ORDER BY a.k1 NULLS LAST, a.k2 NULLS LAST";

    let logical = optimized_logical(&ctx, sql).await;
    let logical_str = logical.display_indent().to_string();
    assert!(
        logical_str
            .contains("Left Join: a.k1 = b.k1, a.k2 = b.k2, a.k3 = b.k3, a.k4 = b.k4, a.k5 = b.k5"),
        "expected all 5 IS NOT DISTINCT FROM predicates lifted, got:\n{logical_str}"
    );

    let physical = physical(&ctx, sql).await;
    assert_eq!(count_exec(physical.as_ref(), "HashJoinExec"), 1);
    assert_eq!(count_exec(physical.as_ref(), "NestedLoopJoinExec"), 0);

    assert_plan_str(
        run(&ctx, sql).await,
        "\
+----+----+----+----+----+----+----+----+----+----+
| k1 | k2 | k3 | k4 | k5 | b1 | b2 | b3 | b4 | b5 |
+----+----+----+----+----+----+----+----+----+----+
| 1  | 2  | 3  | 4  | 5  | 1  | 2  | 3  | 4  | 5  |
| 1  |    | 3  |    | 5  | 1  |    | 3  |    | 5  |
|    | 2  | 3  | 4  | 5  |    | 2  | 3  | 4  | 5  |
|    |    |    |    |    |    |    |    |    |    |
+----+----+----+----+----+----+----+----+----+----+",
    );
}

#[tokio::test]
async fn full_outer_via_driver_chain_uses_hash_joins() {
    // FULL OUTER emulation:
    //   keys = SELECT DISTINCT (a, b) FROM (A UNION ALL B UNION ALL C)
    //   keys LEFT JOIN A LEFT JOIN B LEFT JOIN C, all on keys.k IS NOT DISTINCT FROM X.k
    // Every ON references the driver, so LEFT-padded NULLs never feed into a
    // downstream ON.
    let ctx = QueryPlannerImpl::make_execution_context(SessionConfig::new());

    let a = make_tagged_table(1, &[(Some(1), Some(10)), (None, None)]);
    let b = make_tagged_table(2, &[(None, Some(20)), (None, None)]);
    let c = make_tagged_table(3, &[(Some(3), None), (None, None)]);
    ctx.register_table("a", a).unwrap();
    ctx.register_table("b", b).unwrap();
    ctx.register_table("c", c).unwrap();

    let sql = "WITH keys AS ( \
                  SELECT DISTINCT a, b FROM ( \
                      SELECT a, b FROM a \
                      UNION ALL SELECT a, b FROM b \
                      UNION ALL SELECT a, b FROM c \
                  ) \
               ) \
               SELECT keys.a AS k_a, keys.b AS k_b, \
                      a.tag AS in_a, b.tag AS in_b, c.tag AS in_c \
               FROM keys \
               LEFT JOIN a \
                 ON (keys.a IS NOT DISTINCT FROM a.a) AND (keys.b IS NOT DISTINCT FROM a.b) \
               LEFT JOIN b \
                 ON (keys.a IS NOT DISTINCT FROM b.a) AND (keys.b IS NOT DISTINCT FROM b.b) \
               LEFT JOIN c \
                 ON (keys.a IS NOT DISTINCT FROM c.a) AND (keys.b IS NOT DISTINCT FROM c.b) \
               ORDER BY k_a NULLS LAST, k_b NULLS LAST";

    let logical = optimized_logical(&ctx, sql).await;
    let logical_str = logical.display_indent().to_string();
    let lifted_joins = logical_str.matches("Left Join: ").count();
    assert_eq!(
        lifted_joins, 3,
        "expected all 3 LEFT JOINs to carry equi-keys, got:\n{logical_str}"
    );
    // None of the LEFT JOINs should still hang IS NOT DISTINCT FROM in `Filter:`.
    assert!(
        !logical_str.contains("Filter: keys.a IS NOT DISTINCT FROM"),
        "rule failed to lift one of the chained joins, got:\n{logical_str}"
    );

    let physical = physical(&ctx, sql).await;
    assert_eq!(
        count_exec(physical.as_ref(), "HashJoinExec"),
        3,
        "expected 3 HashJoinExec in the chain, got plan:\n{}",
        displayable(physical.as_ref()).indent(true)
    );
    assert_eq!(count_exec(physical.as_ref(), "NestedLoopJoinExec"), 0);

    // The `(NULL,NULL)` driver row must match the `(NULL,NULL)` rows in A, B,
    // and C — so all three `in_*` tags appear on the last row.
    assert_plan_str(
        run(&ctx, sql).await,
        "\
+-----+-----+------+------+------+
| k_a | k_b | in_a | in_b | in_c |
+-----+-----+------+------+------+
| 1   | 10  | 1    |      |      |
| 3   |     |      |      | 3    |
|     | 20  |      | 2    |      |
|     |     | 1    | 2    | 3    |
+-----+-----+------+------+------+",
    );
}

/// Documents the conservative scope: when DF has already lifted an `=` key
/// into ON, the rule keeps its hands off (flipping `null_equals_null` would
/// silently make existing `=` keys null-safe). Mixed predicates fall back to
/// a NestedLoopJoin filter for the `IS NOT DISTINCT FROM` side.
#[tokio::test]
async fn mixed_eq_and_is_not_distinct_from_is_left_alone() {
    let ctx = ab_context();
    let sql = "SELECT a.a AS a_a, a.b AS a_b, b.a AS b_a, b.b AS b_b \
               FROM a LEFT JOIN b \
               ON a.a = b.a AND (a.b IS NOT DISTINCT FROM b.b)";

    let logical = optimized_logical(&ctx, sql).await;
    let logical_str = logical.display_indent().to_string();
    assert!(
        logical_str.contains("Left Join: a.a = b.a")
            && logical_str.contains("Filter: a.b IS NOT DISTINCT FROM b.b"),
        "expected only `=` lifted and IS NOT DISTINCT FROM kept in filter, got:\n{logical_str}"
    );

    let physical = physical(&ctx, sql).await;
    // The `=` key is hashable, so DF still chooses a HashJoinExec — with the
    // IS NOT DISTINCT FROM clause attached as a residual filter on the join.
    assert_eq!(count_exec(physical.as_ref(), "HashJoinExec"), 1);
    assert_eq!(count_exec(physical.as_ref(), "NestedLoopJoinExec"), 0);
}
