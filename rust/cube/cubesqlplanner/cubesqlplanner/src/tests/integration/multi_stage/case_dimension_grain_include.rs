// Reproduction for GitHub issue #11367:
// "Tesseract: Invalid SQL generated when a CASE dimension references a
// multi-stage measure with grain.include"
//
// Schema mirrors the bug report: a `login_report` cube with a `case`-type
// dimension (`percent_bucket`) whose `when` SQL references a `multi_stage`
// measure (`login_percentage`) that itself carries `grain: { include: [...] }`.
// The query requests a dimension that is NOT part of the grain
// (`territory_name`) alongside the case dimension, plus a measure unrelated to
// the multi-stage chain (`total_school`) — the exact combination from the
// report.
//
// This schema is self-contained (built via `MockSchema::from_yaml`, not the
// shared `common/integration_multi_stage.yaml` fixture) so this test does not
// perturb any existing passing test.
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml(indoc! {r#"
        cubes:
          - name: login_report
            sql: "SELECT * FROM ms_login_report"
            dimensions:
              - name: territory_name
                type: string
                sql: territory_name

              - name: school_code
                type: string
                sql: school_code
                primary_key: true

              - name: date_time
                type: time
                sql: date_time

              - name: onboarding_uuid
                type: string
                sql: onboarding_uuid

              - name: login_uuid
                type: string
                sql: login_uuid

              - name: percent_bucket
                type: string
                case:
                    when:
                        - sql: "{CUBE.login_percentage} >= 80"
                          label: High
                    else:
                        label: Low

            measures:
              - name: total_login_count
                type: count_distinct
                sql: login_uuid

              - name: total_onboarded_count
                type: count_distinct
                sql: onboarding_uuid

              - name: login_percentage
                type: number
                sql: "ROUND(({CUBE.total_login_count} / NULLIF({CUBE.total_onboarded_count}, 0)) * 100, 2)"
                multi_stage: true
                grain:
                    include:
                        - login_report.school_code
                        - login_report.date_time.day

              - name: total_school
                type: count_distinct
                sql: school_code
    "#})
    .expect("Failed to parse inline schema");
    TestContext::new(schema).unwrap()
}

// The claimed bug: a later multi-stage CTE re-references the original base
// table alias (`login_report`) for columns that should instead come from the
// previous aggregation CTE, because those columns are only reachable through
// the `percent_bucket` CASE dimension's `login_percentage` dependency chain.
// Expect either an SQL-generation error, or (if it "succeeds") an alias that
// is out of scope for the CTE that uses it.
#[tokio::test(flavor = "multi_thread")]
async fn test_case_dimension_referencing_grain_include_measure() {
    let ctx = create_context();

    let query = indoc! {r#"
        measures:
          - login_report.total_school
          - login_report.total_onboarded_count
        dimensions:
          - login_report.territory_name
          - login_report.percent_bucket
        order:
          - id: login_report.territory_name
    "#};

    let sql = ctx.build_sql(query).unwrap();
    println!("GENERATED SQL:\n{}", sql);

    // The alias that owns the raw base-table columns.
    assert!(
        sql.contains("\"login_report\""),
        "expected the base cube alias to appear at least once (e.g. in the \
         leaf CTE), got:\n{}",
        sql
    );
}
