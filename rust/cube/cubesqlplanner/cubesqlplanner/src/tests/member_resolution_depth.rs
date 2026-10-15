use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

use crate::planner::compiler::DEFAULT_MAX_MEMBER_RESOLUTION_DEPTH as DEFAULT_LIMIT;

const CUBE_HEADER: &str = indoc! {r#"
    cubes:
        - name: orders
          sql: "SELECT * FROM rd_orders"
          dimensions:
              - name: id
                type: number
                sql: id
                primary_key: true
              - name: category
                type: string
                sql: category
              - name: level_0
                type: number
                sql: amount
          measures:
              - name: amount
                type: sum
                sql: amount
"#};

/// `members` plain calculated measures, each over the previous one. Resolving the deepest one
/// nests `members + 1` levels: the chain plus the `amount` it bottoms out in.
fn measure_chain(members: usize) -> String {
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

/// `levels` dimensions, `level_1` .. `level_{levels}`, each over the previous one, on top of
/// `level_0`. Resolving the deepest one nests `levels + 1` levels.
fn dimension_chain(levels: usize) -> String {
    let mut yaml = String::from(CUBE_HEADER);
    let dimensions_end = yaml.find("      measures:").unwrap();
    let mut dimensions = String::new();
    for level in 1..=levels {
        dimensions.push_str(&format!(
            "          - name: level_{level}\n            type: number\n            sql: \"{{CUBE.level_{previous}}} + 1\"\n",
            level = level,
            previous = level - 1
        ));
    }
    yaml.insert_str(dimensions_end, &dimensions);
    yaml
}

fn measure_query(measure: &str, limit: Option<usize>) -> String {
    let mut query = format!(
        "measures:\n  - orders.{}\ndimensions:\n  - orders.category\n",
        measure
    );
    if let Some(limit) = limit {
        query.push_str(&format!("max_member_resolution_depth: {}\n", limit));
    }
    query
}

/// Resolution recurses in Rust here as it does under Node, where the JS calls at every level
/// are what run out first; give the thread the stack a debug build needs to hold the chain.
fn on_a_big_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .expect("resolution must not exhaust the stack")
}

fn build(yaml: String, query: String) -> Result<String, String> {
    on_a_big_stack(move || {
        let schema = MockSchema::from_yaml(&yaml).unwrap();
        // `CubeError` carries a neon type that is not `Send`, so only the text crosses back.
        TestContext::new(schema)
            .unwrap()
            .build_sql(&query)
            .map_err(|e| e.to_string())
    })
}

#[test]
fn resolves_a_chain_at_the_default_limit() {
    let members = DEFAULT_LIMIT - 1;
    build(
        measure_chain(members),
        measure_query(&format!("plain_{}", members - 1), None),
    )
    .expect("a chain nesting exactly the limit must resolve");
}

/// Past the budget resolution stops before going a level deeper, naming the queried member, the
/// member it stopped at, the budget and the knob.
#[test]
fn chain_past_the_default_limit_names_depth() {
    let members = DEFAULT_LIMIT;
    let message = build(
        measure_chain(members),
        measure_query(&format!("plain_{}", members - 1), None),
    )
    .map(|_| ())
    .expect_err("a chain past the limit must be refused, not resolved");
    assert!(
        message.contains(&format!(
            "Member 'orders.plain_{}' references members more than {} levels deep (through 'orders.amount'), against a limit of {}",
            members - 1,
            DEFAULT_LIMIT,
            DEFAULT_LIMIT
        )),
        "message must name the queried member, where it stopped and the budget, got: {}",
        message
    );
    assert!(
        message.contains("CUBEJS_MAX_MEMBER_RESOLUTION_DEPTH"),
        "message must name the knob that raises the budget, got: {}",
        message
    );
}

/// Dimensions resolve through the same path as measures, and are refused the same way.
#[test]
fn a_dimension_chain_is_refused_too() {
    let levels = DEFAULT_LIMIT;
    let message = build(
        dimension_chain(levels),
        format!(
            "measures:\n  - orders.amount\ndimensions:\n  - orders.level_{}\n",
            levels
        ),
    )
    .map(|_| ())
    .expect_err("a dimension chain past the limit must be refused");
    assert!(
        message.contains(&format!(
            "Member 'orders.level_{}' references members more than {} levels deep",
            levels, DEFAULT_LIMIT
        )),
        "got: {}",
        message
    );
}

/// A member referenced from many places is resolved once and cached, so breadth costs no depth.
#[test]
fn a_shared_member_is_not_counted_twice() {
    let mut yaml = measure_chain(3);
    for member in 0..50 {
        yaml.push_str(&format!(
            "          - name: wide_{member}\n            type: number\n            sql: \"{{CUBE.plain_2}} + {member}\"\n",
            member = member
        ));
    }
    let sum = (0..50)
        .map(|member| format!("{{CUBE.wide_{}}}", member))
        .collect::<Vec<_>>()
        .join(" + ");
    yaml.push_str(&format!(
        "          - name: wide_total\n            type: number\n            sql: \"{}\"\n",
        sum
    ));
    // wide_total -> wide_N -> plain_2 -> plain_1 -> plain_0 -> amount: six levels.
    build(yaml.clone(), measure_query("wide_total", Some(6)))
        .expect("six levels must resolve under a limit of six");
    build(yaml, measure_query("wide_total", Some(5)))
        .map(|_| ())
        .expect_err("the same graph must be refused under a limit of five");
}

/// The limit travels with the query, like the multi-stage one.
#[test]
fn the_query_carries_the_limit() {
    // plain_4 .. plain_0, then amount: six levels.
    build(measure_chain(5), measure_query("plain_4", Some(6)))
        .expect("six levels must resolve under a limit of six");
    let message = build(measure_chain(5), measure_query("plain_4", Some(5)))
        .map(|_| ())
        .expect_err("the same chain must be refused under a limit of five");
    assert!(
        message.contains("references members more than 5 levels deep"),
        "message must name the limit the query carried, got: {}",
        message
    );
}

/// Masked-member filters are compiled while the query's state is built, before its members are, so
/// they must already run under the query's limit rather than the default.
#[test]
fn a_mask_filter_runs_under_the_query_limit() {
    let levels = DEFAULT_LIMIT + 5;
    let query = |limit: usize| {
        format!(
            indoc! {r#"
                measures:
                  - orders.amount
                dimensions:
                  - orders.category
                max_member_resolution_depth: {limit}
                maskedMembers:
                  - member: orders.amount
                    filter:
                      member: orders.level_{levels}
                      operator: equals
                      values:
                        - "1"
            "#},
            limit = limit,
            levels = levels
        )
    };
    build(dimension_chain(levels), query(levels + 1))
        .expect("a mask filter as deep as the query's limit must resolve");
    let message = build(dimension_chain(levels), query(levels))
        .map(|_| ())
        .expect_err("the same filter must be refused one level under it");
    assert!(
        message.contains(&format!(
            "references members more than {} levels deep",
            levels
        )),
        "got: {}",
        message
    );
}
