use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

use crate::planner::planners::multi_stage::DEFAULT_MAX_MULTI_STAGE_STAGES as DEFAULT_LIMIT;

const CUBE_HEADER: &str = indoc! {r#"
    cubes:
        - name: orders
          sql: "SELECT * FROM ms_orders"
          dimensions:
              - name: id
                type: number
                sql: id
                primary_key: true
              - name: created_at
                type: time
                sql: created_at
          measures:
              - name: amount
                type: sum
                sql: amount
              - name: level_0
                type: number
                sql: "{CUBE.amount}"
                multi_stage: true
"#};

/// `levels` levels, each reading the one below it twice: as is, and shifted by a distinct power
/// of two days. Every subset of the shifts is a distinct state, so the bottom level is planned
/// 2^levels times although the model is only `levels` stages deep.
fn doubling_schema(levels: usize) -> String {
    let mut yaml = String::from(CUBE_HEADER);
    for level in 1..=levels {
        yaml.push_str(&format!(
            concat!(
                "          - name: shifted_{level}\n",
                "            type: number\n",
                "            sql: \"{{CUBE.level_{previous}}}\"\n",
                "            multi_stage: true\n",
                "            time_shift:\n",
                "                - interval: \"{days} day\"\n",
                "                  type: prior\n",
                "          - name: level_{level}\n",
                "            type: number\n",
                "            sql: \"{{CUBE.level_{previous}}} + {{CUBE.shifted_{level}}}\"\n",
                "            multi_stage: true\n",
            ),
            level = level,
            previous = level - 1,
            days = 1usize << (level - 1),
        ));
    }
    yaml
}

fn query(measure: &str, limit: Option<usize>) -> String {
    let mut query = format!(
        indoc! {r#"
            measures:
              - orders.{}
            time_dimensions:
              - dimension: orders.created_at
                granularity: day
        "#},
        measure
    );
    if let Some(limit) = limit {
        query.push_str(&format!("max_multi_stage_stages: {}\n", limit));
    }
    query
}

fn build(levels: usize, limit: Option<usize>) -> Result<String, cubenativeutils::CubeError> {
    let schema = MockSchema::from_yaml(&doubling_schema(levels)).unwrap();
    TestContext::new(schema)
        .unwrap()
        .build_sql(&query(&format!("level_{}", levels), limit))
}

/// A shallow model whose stages double per level plans while it fits the budget.
#[test]
fn plans_doubling_stages_within_the_budget() {
    build(4, None).expect("16 states of the bottom level fit the default budget");
}

/// Past the budget planning refuses, naming the knob, instead of planning every state.
#[test]
fn doubling_stages_past_the_default_budget_are_refused() {
    let levels = 12;
    assert!(1usize << levels > DEFAULT_LIMIT);
    let message = build(levels, None)
        .map(|_| ())
        .map_err(|e| e.to_string())
        .expect_err("2^12 states must be refused, not planned");
    assert!(
        message.contains(&format!(
            "needs more than {} multi-stage stages",
            DEFAULT_LIMIT
        )),
        "message must name the budget, got: {}",
        message
    );
    assert!(
        message.contains("CUBEJS_MAX_MULTI_STAGE_STAGES"),
        "message must name the knob that raises the budget, got: {}",
        message
    );
}

/// The budget travels with the query instead of being read from the environment.
#[test]
fn the_query_carries_the_budget() {
    build(3, Some(DEFAULT_LIMIT)).expect("a 3-level model must plan under the default budget");
    let err = build(3, Some(4))
        .map(|_| ())
        .expect_err("the same model must be refused under a budget of 4");
    assert!(
        err.to_string()
            .contains("needs more than 4 multi-stage stages"),
        "message must name the budget the query carried, got: {}",
        err
    );
}
