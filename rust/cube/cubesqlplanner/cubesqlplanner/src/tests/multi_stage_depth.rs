use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

/// The documented default of `CUBEJS_MAX_MULTI_STAGE_DEPTH`, spelled out so that changing the
/// default has to come with a decision about these cases.
const DEFAULT_LIMIT: usize = 32;

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
/// deepest member carries a chain of `stages` + 1 multi-stage members.
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

fn build(yaml: &str, measure: &str) -> Result<String, cubenativeutils::CubeError> {
    let schema = MockSchema::from_yaml(yaml).unwrap();
    TestContext::new(schema)
        .unwrap()
        .build_sql(&query_for(measure))
}

fn build_on_view(yaml: &str, measure: &str) -> Result<String, cubenativeutils::CubeError> {
    let schema = MockSchema::from_yaml(yaml).unwrap();
    TestContext::new(schema).unwrap().build_sql(&format!(
        "measures:\n  - orders_view.{}\ndimensions:\n  - orders_view.category\n",
        measure
    ))
}

#[test]
fn plans_a_chain_up_to_the_depth_limit() {
    let stages = DEFAULT_LIMIT;
    let sql = build(
        &chained_stages_schema(stages),
        &format!("stage_{}", stages - 1),
    )
    .unwrap();
    assert_eq!(sql.matches(" AS (").count(), stages + 1);
}

#[test]
fn chain_past_the_depth_limit_names_depth() {
    let stages = DEFAULT_LIMIT + 8;
    let err = build(
        &chained_stages_schema(stages),
        &format!("stage_{}", stages - 1),
    )
    .map(|_| ())
    .expect_err("a chain past the limit must be refused, not planned");

    let message = err.to_string();
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
    let members = DEFAULT_LIMIT + 8;
    build(
        &chained_plain_schema(members),
        &format!("plain_{}", members - 1),
    )
    .unwrap();
}

/// A proxy is collapsed before planning and becomes no stage of its own, so the same chain must
/// not be refused merely because a view re-exports it.
#[test]
fn a_view_proxy_is_not_an_extra_stage() {
    let stages = DEFAULT_LIMIT;
    build_on_view(
        &chained_stages_view_schema(stages),
        &format!("stage_{}", stages - 1),
    )
    .unwrap();
}
