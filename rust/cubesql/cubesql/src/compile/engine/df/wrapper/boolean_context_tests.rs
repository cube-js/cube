use super::*;
use crate::compile::test::{mssql_boolean_templates, sql_generator};
use datafusion::{
    arrow::{
        array::{Array, BooleanArray},
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    execution::context::SessionContext,
};

fn render(expr: Expr, predicate: bool, mssql: bool) -> String {
    WrappedSelectNode::generate_sql_for_expr_context(
        SqlQuery::new(String::new(), vec![]),
        sql_generator(if mssql {
            mssql_boolean_templates()
        } else {
            vec![]
        }),
        expr,
        None,
        &HashMap::new(),
        predicate,
    )
    .unwrap()
    .0
}

async fn values(ctx: &SessionContext, sql: &str) -> Vec<Option<bool>> {
    ctx.sql(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            (0..values.len())
                .map(|i| {
                    if values.is_null(i) {
                        None
                    } else {
                        Some(values.value(i))
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

// Evaluate all nine combinations, including UNKNOWN, through an independent
// native-boolean engine. Only the dialect's BIT type name is translated back;
// CASE branches, comparisons and logical operators are the actual emitted SQL.
#[tokio::test]
async fn boolean_context_truth_tables() {
    let ctx = SessionContext::new();
    let inputs = [Some(true), Some(false), None];
    let b = inputs.iter().flat_map(|b| [*b; 3]).collect::<Vec<_>>();
    let c = inputs.repeat(3);
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("c", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BooleanArray::from(b)),
            Arc::new(BooleanArray::from(c)),
        ],
    )
    .unwrap();
    ctx.register_table(
        "fixture",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    for expression in [
        "b",
        "TRUE",
        "FALSE",
        "CAST(NULL AS BOOLEAN)",
        "b = TRUE",
        "b = FALSE",
        "NOT b",
        "b AND c",
        "b OR c",
        "NOT (b AND c)",
        "(b = c) = b",
        "b IS NULL",
        "b IS NOT NULL",
        "CASE WHEN b THEN c ELSE b END",
        "CASE b WHEN TRUE THEN c ELSE b END",
        "COALESCE(b = c, b)",
        "b IN (TRUE, FALSE)",
        "b BETWEEN FALSE AND TRUE",
    ] {
        let original = format!("SELECT {expression} FROM fixture");
        let plan = ctx.sql(&original).await.unwrap().to_logical_plan().unwrap();
        let LogicalPlan::Projection(projection) = plan else {
            panic!("{:?}", plan)
        };
        let expr = projection.expr[0].clone();
        let expected = inputs
            .iter()
            .flat_map(|b| {
                inputs.iter().map(move |c| {
                    let equal = b.zip(*c).map(|(b, c)| b == c);
                    let and = match (b, c) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    };
                    match expression {
                        "TRUE" => Some(true),
                        "FALSE" => Some(false),
                        "CAST(NULL AS BOOLEAN)" => None,
                        "b" | "b = TRUE" => *b,
                        "NOT b" | "b = FALSE" => b.map(|v| !v),
                        "b AND c" => and,
                        "NOT (b AND c)" => and.map(|v| !v),
                        "b OR c" => match (b, c) {
                            (Some(true), _) | (_, Some(true)) => Some(true),
                            (Some(false), Some(false)) => Some(false),
                            _ => None,
                        },
                        "(b = c) = b" => equal.zip(*b).map(|(eq, b)| eq == b),
                        "b IS NULL" => Some(b.is_none()),
                        "b IS NOT NULL" => Some(b.is_some()),
                        "COALESCE(b = c, b)" => equal.or(*b),
                        "b IN (TRUE, FALSE)" | "b BETWEEN FALSE AND TRUE" => b.map(|_| true),
                        "CASE WHEN b THEN c ELSE b END" | "CASE b WHEN TRUE THEN c ELSE b END" => {
                            if *b == Some(true) {
                                *c
                            } else {
                                *b
                            }
                        }
                        _ => panic!("Missing independent expectation: {}", expression),
                    }
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(values(&ctx, &original).await, expected, "{}", expression);
        for mssql in [false, true] {
            let scalar = render(expr.clone(), false, mssql);
            let predicate = render(expr.clone(), true, mssql);
            let actual = values(
                &ctx,
                &format!(
                    "SELECT {} FROM fixture",
                    scalar.replace(" AS BIT)", " AS BOOLEAN)")
                ),
            )
            .await;
            assert_eq!(actual, expected, "{expression}: {scalar}");
            let filtered = values(
                &ctx,
                &format!(
                    "SELECT b FROM fixture WHERE {}",
                    predicate.replace(" AS BIT)", " AS BOOLEAN)")
                ),
            )
            .await;
            assert_eq!(
                filtered,
                values(&ctx, &format!("SELECT b FROM fixture WHERE {expression}")).await,
                "{expression}: {predicate}"
            );
            if mssql {
                assert!(
                    !scalar.contains("TRUE") && !scalar.contains("FALSE"),
                    "{}",
                    scalar
                );
                println!(
                    "MSSQL_BOOLEAN_CASE {}",
                    serde_json::json!({"expression": expression, "scalar": scalar, "predicate": predicate, "expected": expected})
                );
            }
        }
    }
}

// The pinned DataFusion SQL planner translates IS [NOT] TRUE/FALSE to ordinary
// equality/inequality, losing their NULL semantics before wrapper rendering.
// Keep this explicit remaining failure separate from the context conversion.
#[tokio::test]
#[ignore = "DataFusion truth-test lowering loses NULL semantics before SQL rendering"]
async fn boolean_context_truth_tests_nulls() {
    let ctx = SessionContext::new();
    let mut actual = vec![];
    for test in ["IS TRUE", "IS FALSE", "IS NOT TRUE", "IS NOT FALSE"] {
        actual.extend(values(&ctx, &format!("SELECT CAST(NULL AS BOOLEAN) {test}")).await);
    }
    assert_eq!(
        actual,
        vec![Some(false), Some(false), Some(true), Some(true)]
    );
}

#[test]
fn boolean_context_aggregate_sql() {
    use datafusion::logical_plan::{col, lit};
    let count = Expr::AggregateFunction {
        fun: AggregateFunction::Count,
        args: vec![col("b")],
        distinct: true,
        within_group: None,
    }
    .eq(lit(2_i64));
    let sum = Expr::AggregateFunction {
        fun: AggregateFunction::Sum,
        args: vec![Expr::Cast {
            expr: Box::new(col("b")),
            data_type: DataType::Int64,
        }],
        distinct: false,
        within_group: None,
    }
    .gt(lit(0_i64));
    for (expr, expected_empty) in [(count, Some(false)), (sum, None)] {
        let scalar = render(expr, false, true);
        assert!(scalar.starts_with("CAST(CASE WHEN"), "{}", scalar);
        assert!(scalar.ends_with("ELSE NULL END AS BIT)"), "{}", scalar);
        println!(
            "MSSQL_BOOLEAN_AGGREGATE {}",
            serde_json::json!({"scalar": scalar, "expected": true, "expected_empty": expected_empty})
        );
    }
}

#[test]
fn boolean_context_sql_boundaries() {
    use datafusion::logical_plan::{col, lit};
    assert_eq!(render(col("b"), true, true), "(\"b\" = CAST(1 AS BIT))");
    assert_eq!(
        render(col("b").not(), true, true),
        "NOT ((\"b\" = CAST(1 AS BIT)))"
    );
    assert_eq!(
        render(col("b").eq(lit(true)), true, true),
        "(\"b\" = CAST(1 AS BIT))"
    );
    assert_eq!(render(col("b").not(), true, false), "NOT (\"b\")");
    assert_eq!(
        render(col("b").eq(lit(true)), false, false),
        "(\"b\" = TRUE)"
    );
    assert_eq!(render(col("b").eq(lit(true)).alias("flag"), false, true),
        "CAST(CASE WHEN (\"b\" = CAST(1 AS BIT)) THEN 1 WHEN NOT ((\"b\" = CAST(1 AS BIT))) THEN 0 ELSE NULL END AS BIT)");
}

#[test]
fn boolean_context_rejects_volatile_scalarization() {
    use datafusion::logical_plan::lit;
    let expr = Expr::ScalarFunction {
        fun: BuiltinScalarFunction::Random,
        args: vec![],
    }
    .gt(lit(0.5));
    let result = WrappedSelectNode::generate_sql_for_expr(
        SqlQuery::new(String::new(), vec![]),
        sql_generator(mssql_boolean_templates()),
        expr,
        None,
        &HashMap::new(),
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("cannot repeat a volatile expression"));
}
