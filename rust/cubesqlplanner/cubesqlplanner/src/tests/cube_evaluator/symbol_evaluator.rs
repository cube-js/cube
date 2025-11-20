//! Tests for SQL generation for individual symbols

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_count_schema_no_pk() -> MockSchema {
    let yaml = indoc! {r#"
        cubes:
          - name: users
            sql: "SELECT 1"
            dimensions:
              - name: id
                type: number
                sql: id
              - name: userName
                type: string
                sql: user_name
            measures:
              - name: count
                type: count
    "#};
    MockSchema::from_yaml(yaml).unwrap()
}

fn create_count_schema_one_pk() -> MockSchema {
    let yaml = indoc! {r#"
        cubes:
          - name: users
            sql: "SELECT 1"
            dimensions:
              - name: id
                type: number
                sql: id
                primary_key: true
              - name: userName
                type: string
                sql: user_name
            measures:
              - name: count
                type: count
    "#};
    MockSchema::from_yaml(yaml).unwrap()
}

fn create_count_schema_two_pk() -> MockSchema {
    let yaml = indoc! {r#"
        cubes:
          - name: users
            sql: "SELECT 1"
            dimensions:
              - name: id
                type: number
                sql: id
                primary_key: true
              - name: userName
                type: string
                sql: user_name
                primary_key: true
            measures:
              - name: count
                type: count
    "#};
    MockSchema::from_yaml(yaml).unwrap()
}

fn create_test_schema() -> MockSchema {
    let yaml = indoc! {r#"
        cubes:
          - name: test_cube
            sql: "SELECT 1"
            dimensions:
              - name: id
                type: number
                sql: id
                primary_key: true
              - name: source
                type: string
                sql: "{CUBE}.source"
              - name: source_extended
                type: string
                sql: "CONCAT({CUBE.source}, '_source')"
              - name: created_at
                type: time
                sql: created_at
              - name: location
                type: geo
                latitude: latitude
                longitude: longitude
            measures:
              - name: sum_revenue
                type: sum
                sql: revenue
              - name: min_revenue
                type: min
                sql: revenue
              - name: max_revenue
                type: max
                sql: revenue
              - name: avg_revenue
                type: avg
                sql: revenue
              - name: complex_measure
                type: number
                sql: "{sum_revenue} + {CUBE.avg_revenue}/{test_cube.min_revenue} - {test_cube.min_revenue}"
              - name: count_distinct_id
                type: countDistinct
                sql: id
              - name: count_distinct_approx_id
                type: countDistinctApprox
                sql: id
    "#};
    MockSchema::from_yaml(yaml).unwrap()
}

#[test]
fn simple_dimension_sql_evaluation() {
    let schema = create_test_schema();
    let context = TestContext::new(schema).unwrap();

    let id_symbol = context.create_dimension("test_cube.id").unwrap();
    let id_sql = context.evaluate_symbol(&id_symbol).unwrap();
    assert_eq!(id_sql, r#""test_cube".id"#);

    let source_symbol = context.create_dimension("test_cube.source").unwrap();
    let source_sql = context.evaluate_symbol(&source_symbol).unwrap();
    assert_eq!(source_sql, r#""test_cube".source"#);

    let created_at_symbol = context.create_dimension("test_cube.created_at").unwrap();
    let created_at_sql = context.evaluate_symbol(&created_at_symbol).unwrap();
    assert_eq!(created_at_sql, r#""test_cube".created_at"#);

    let location_symbol = context.create_dimension("test_cube.location").unwrap();
    let location_sql = context.evaluate_symbol(&location_symbol).unwrap();
    assert_eq!(location_sql, "latitude || ',' || longitude");

    let created_at_day_symbol = context
        .create_dimension("test_cube.created_at.day")
        .unwrap();
    let created_at_day_sql = context.evaluate_symbol(&created_at_day_symbol).unwrap();
    assert_eq!(
        created_at_day_sql,
        "date_trunc('day', (\"test_cube\".created_at::timestamptz AT TIME ZONE 'UTC'))"
    );
}

#[test]
fn simple_aggregate_measures() {
    let schema = create_test_schema();
    let context = TestContext::new(schema).unwrap();

    let sum_symbol = context.create_measure("test_cube.sum_revenue").unwrap();
    let sum_sql = context.evaluate_symbol(&sum_symbol).unwrap();
    assert_eq!(sum_sql, r#"sum("test_cube".revenue)"#);

    let min_symbol = context.create_measure("test_cube.min_revenue").unwrap();
    let min_sql = context.evaluate_symbol(&min_symbol).unwrap();
    assert_eq!(min_sql, r#"min("test_cube".revenue)"#);

    let max_symbol = context.create_measure("test_cube.max_revenue").unwrap();
    let max_sql = context.evaluate_symbol(&max_symbol).unwrap();
    assert_eq!(max_sql, r#"max("test_cube".revenue)"#);

    let avg_symbol = context.create_measure("test_cube.avg_revenue").unwrap();
    let avg_sql = context.evaluate_symbol(&avg_symbol).unwrap();
    assert_eq!(avg_sql, r#"avg("test_cube".revenue)"#);

    let count_distinct_symbol = context
        .create_measure("test_cube.count_distinct_id")
        .unwrap();
    let count_distinct_sql = context.evaluate_symbol(&count_distinct_symbol).unwrap();
    assert_eq!(count_distinct_sql, r#"COUNT(DISTINCT "test_cube".id)"#);

    let count_distinct_approx_symbol = context
        .create_measure("test_cube.count_distinct_approx_id")
        .unwrap();
    let count_distinct_approx_sql = context
        .evaluate_symbol(&count_distinct_approx_symbol)
        .unwrap();
    assert_eq!(
        count_distinct_approx_sql,
        r#"round(hll_cardinality(hll_add_agg(hll_hash_any("test_cube".id))))"#
    );
}

#[test]
fn count_measure_variants() {
    let schema_no_pk = create_count_schema_no_pk();
    let context_no_pk = TestContext::new(schema_no_pk).unwrap();
    let count_no_pk_symbol = context_no_pk.create_measure("users.count").unwrap();
    let count_no_pk_sql = context_no_pk.evaluate_symbol(&count_no_pk_symbol).unwrap();
    assert_eq!(count_no_pk_sql, "count(*)");

    let schema_one_pk = create_count_schema_one_pk();
    let context_one_pk = TestContext::new(schema_one_pk).unwrap();
    let count_one_pk_symbol = context_one_pk.create_measure("users.count").unwrap();
    let count_one_pk_sql = context_one_pk
        .evaluate_symbol(&count_one_pk_symbol)
        .unwrap();
    assert_eq!(count_one_pk_sql, r#"count("users".id)"#);

    let schema_two_pk = create_count_schema_two_pk();
    let context_two_pk = TestContext::new(schema_two_pk).unwrap();
    let count_two_pk_symbol = context_two_pk.create_measure("users.count").unwrap();
    let count_two_pk_sql = context_two_pk
        .evaluate_symbol(&count_two_pk_symbol)
        .unwrap();
    assert_eq!(
        count_two_pk_sql,
        "count(CAST(id AS STRING) || CAST(user_name AS STRING))"
    );
}

#[test]
fn composite_symbols() {
    let schema = create_test_schema();
    let context = TestContext::new(schema).unwrap();

    // Test dimension with member dependency ({CUBE.source})
    let source_extended_symbol = context
        .create_dimension("test_cube.source_extended")
        .unwrap();
    let source_extended_sql = context.evaluate_symbol(&source_extended_symbol).unwrap();
    assert_eq!(
        source_extended_sql,
        r#"CONCAT("test_cube".source, '_source')"#
    );

    let complex_measure_symbol = context.create_measure("test_cube.complex_measure").unwrap();
    let complex_measure_sql = context.evaluate_symbol(&complex_measure_symbol).unwrap();
    assert_eq!(
        complex_measure_sql,
        r#"sum("test_cube".revenue) + avg("test_cube".revenue)/min("test_cube".revenue) - min("test_cube".revenue)"#
    );
}
