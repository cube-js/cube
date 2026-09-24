use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

use crate::planner::planners::multi_stage::DEFAULT_MAX_MULTI_STAGE_DEPTH as DEFAULT_LIMIT;

const CUBE_HEADER: &str = indoc! {r#"
    cubes:
        - name: orders
          sql: "SELECT * FROM ms_orders"
          dimensions:
              - name: id
                type: number
                sql: id
                primary_key: true
              - name: category
                type: string
                sql: category
          measures:
              - name: amount
                type: sum
                sql: amount
"#};

/// `stages` measures, each computing over the previous one as its own multi-stage stage, so the
/// deepest member carries a chain of `stages` multi-stage members.
fn chained_stages_schema(stages: usize) -> String {
    let mut yaml = String::from(CUBE_HEADER);
    yaml.push_str(concat!(
        "          - name: stage_0\n",
        "            type: number\n",
        "            sql: \"{CUBE.amount}\"\n",
        "            multi_stage: true\n",
        "            add_group_by:\n",
        "                - orders.category\n",
    ));
    for stage in 1..stages {
        yaml.push_str(&format!(
            "          - name: stage_{stage}\n            type: number\n            sql: \"{{CUBE.stage_{previous}}} + 1\"\n            multi_stage: true\n            add_group_by:\n                - orders.category\n",
            stage = stage,
            previous = stage - 1
        ));
    }
    yaml
}

/// The same chain length built from plain calculated measures, which are not stages.
fn chained_plain_schema(members: usize) -> String {
    let mut yaml = String::from(CUBE_HEADER);
    yaml.push_str(concat!(
        "          - name: plain_0\n",
        "            type: number\n",
        "            sql: \"{CUBE.amount}\"\n",
    ));
    for member in 1..members {
        yaml.push_str(&format!(
            "          - name: plain_{member}\n            type: number\n            sql: \"{{CUBE.plain_{previous}}} + 1\"\n",
            member = member,
            previous = member - 1
        ));
    }
    yaml
}

/// The same chain, plus a view re-exporting its deepest measure. A view member is a proxy that
/// inherits `multi_stage` from what it resolves to and adds a dependency level of its own.
fn chained_stages_view_schema(stages: usize) -> String {
    let mut yaml = chained_stages_schema(stages);
    yaml.push_str(&format!(
        "views:\n    - name: orders_view\n      cubes:\n          - join_path: orders\n            includes:\n                - category\n                - stage_{}\n",
        stages - 1
    ));
    yaml
}

fn query_with_limit(measure: &str, limit: usize) -> String {
    format!("{}max_multi_stage_depth: {}\n", query_for(measure), limit)
}

fn query_for(measure: &str) -> String {
    format!(
        indoc! {r#"
            measures:
              - orders.{}
            dimensions:
              - orders.category
        "#},
        measure
    )
}

/// The stack the budget is sized against: what Node gives the thread `buildSqlAndParams` runs
/// on. A debug build spends several times more stack per stage than the release build the
/// budget was measured on, so only a release run holds the budget to its claim.
#[cfg(not(debug_assertions))]
const PLANNING_STACK: usize = 8 * 1024 * 1024;
#[cfg(debug_assertions)]
const PLANNING_STACK: usize = 32 * 1024 * 1024;

fn on_a_planning_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(PLANNING_STACK)
        .spawn(f)
        .unwrap()
        .join()
        .expect("planning must not exhaust the stack")
}

fn build(yaml: &str, measure: &str) -> Result<String, cubenativeutils::CubeError> {
    let schema = MockSchema::from_yaml(yaml).unwrap();
    TestContext::new(schema)
        .unwrap()
        .build_sql(&query_for(measure))
}

fn build_on_view(
    yaml: &str,
    measure: &str,
    limit: usize,
) -> Result<String, cubenativeutils::CubeError> {
    let schema = MockSchema::from_yaml(yaml).unwrap();
    TestContext::new(schema).unwrap().build_sql(&format!(
        "measures:\n  - orders_view.{}\ndimensions:\n  - orders_view.category\nmax_multi_stage_depth: {}\n",
        measure, limit
    ))
}

/// Plans `measure` under an explicit budget, for the cases that are about what counts as a
/// stage rather than about the default.
fn build_with_limit(
    yaml: &str,
    measure: &str,
    limit: usize,
) -> Result<String, cubenativeutils::CubeError> {
    let schema = MockSchema::from_yaml(yaml).unwrap();
    TestContext::new(schema)
        .unwrap()
        .build_sql(&query_with_limit(measure, limit))
}

/// A chain at the default budget still plans, on the stack production gives the planner.
#[test]
fn plans_a_chain_at_the_default_limit() {
    let stages = DEFAULT_LIMIT;
    // `CubeError` carries a neon type that is not `Send`, so only the text crosses back.
    let sql = on_a_planning_stack(move || {
        build(
            &chained_stages_schema(stages),
            &format!("stage_{}", stages - 1),
        )
        .map_err(|e| e.to_string())
    })
    .unwrap();
    assert_eq!(sql.matches(" AS (").count(), stages + 1);
}

/// Past the budget the guard refuses before planning, so this costs no stack at all.
#[test]
fn chain_past_the_default_limit_names_depth() {
    let stages = DEFAULT_LIMIT + 1;
    // Compiling the model recurses per member and runs before the guard, so even the refused
    // case needs a stack that can hold the chain.
    let message = on_a_planning_stack(move || {
        build(
            &chained_stages_schema(stages),
            &format!("stage_{}", stages - 1),
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
        .expect_err("a chain past the limit must be refused, not planned")
    });
    assert!(
        message.contains(&format!(
            "chains {} multi-stage members deep, against a limit of {}",
            stages, DEFAULT_LIMIT
        )),
        "message must name the depth reached and the budget, got: {}",
        message
    );
    assert!(
        message.contains(&format!("orders.stage_{}", stages - 1)),
        "message must name the member the chain hangs from, got: {}",
        message
    );
    assert!(
        message.contains("CUBEJS_MAX_MULTI_STAGE_DEPTH"),
        "message must name the knob that raises the budget, got: {}",
        message
    );
}

/// Only stages count. Reference and calculation chains recurse as well, but cost a fraction of
/// a stage per level, and models built over views reach depths a stage budget would refuse.
#[test]
fn a_plain_member_chain_is_not_a_multi_stage_chain() {
    let members = 40;
    build_with_limit(
        &chained_plain_schema(members),
        &format!("plain_{}", members - 1),
        4,
    )
    .expect("plain members must not be counted as stages");
}

/// A proxy is collapsed before planning and becomes no stage of its own, so the same chain must
/// not be refused merely because a view re-exports it.
#[test]
fn a_view_proxy_is_not_an_extra_stage() {
    let stages = 5;
    build_on_view(
        &chained_stages_view_schema(stages),
        &format!("stage_{}", stages - 1),
        stages,
    )
    .expect("a view re-exporting the chain must not deepen it");
}

/// The limit travels with the query, so a deployment can set it without the planner reading the
/// environment behind the caller's back.
#[test]
fn the_query_carries_the_limit() {
    let yaml = chained_stages_schema(5);
    let schema = MockSchema::from_yaml(&yaml).unwrap();
    let ctx = TestContext::new(schema).unwrap();

    ctx.build_sql(&query_with_limit("stage_4", 5))
        .expect("a chain of 5 must plan under a limit of 5");

    let err = ctx
        .build_sql(&query_with_limit("stage_4", 4))
        .map(|_| ())
        .expect_err("the same chain must be refused under a limit of 4");
    assert!(
        err.to_string()
            .contains("chains 5 multi-stage members deep, against a limit of 4"),
        "message must name the limit the query carried, got: {}",
        err
    );
}
