//! Integration tests for automatic parenthesization of SqlCall arg substitutions.
//!
//! Each test renders a dimension or measure via the full processor chain and
//! inspects the produced SQL for the presence or absence of parentheses
//! around the substituted expression.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;

fn ctx() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/auto_parentheses_tests.yaml");
    TestContext::new(schema).unwrap()
}

fn dimension_sql(ctx: &TestContext, path: &str) -> String {
    let sym = ctx.create_dimension(path).unwrap();
    ctx.evaluate_symbol(&sym).unwrap()
}

fn measure_sql(ctx: &TestContext, path: &str) -> String {
    let sym = ctx.create_measure(path).unwrap();
    ctx.evaluate_symbol(&sym).unwrap()
}

#[test]
fn atomic_dimension_is_never_wrapped() {
    let ctx = ctx();
    let sql = dimension_sql(&ctx, "expr_cube.atomic");
    assert_eq!(sql, "\"expr_cube\".col");
}

#[test]
fn compound_dimension_rendered_standalone_is_not_wrapped() {
    let ctx = ctx();
    // Top-level `evaluate_symbol` call sets no paren-safe expectation, so the
    // outermost compound expression is rendered as-is.
    let sql = dimension_sql(&ctx, "expr_cube.compound_arith");
    assert_eq!(sql, "\"expr_cube\".a + \"expr_cube\".b");
}

#[test]
fn compound_in_arithmetic_context_is_wrapped() {
    let ctx = ctx();
    let sql = dimension_sql(&ctx, "expr_cube.arith_over_compound");
    assert_eq!(sql, "(\"expr_cube\".a + \"expr_cube\".b) * 2");
}

#[test]
fn compound_in_logical_context_is_wrapped() {
    let ctx = ctx();
    let sql = dimension_sql(&ctx, "expr_cube.and_over_compound");
    assert_eq!(
        sql,
        "(\"expr_cube\".a > 0 OR \"expr_cube\".b > 0) AND \"expr_cube\".a > 10"
    );
}

#[test]
fn compound_in_function_arg_is_not_wrapped() {
    let ctx = ctx();
    let sql = dimension_sql(&ctx, "expr_cube.abs_of_compound");
    assert_eq!(sql, "ABS(\"expr_cube\".a + \"expr_cube\".b)");
}

#[test]
fn compound_in_cast_is_not_wrapped() {
    let ctx = ctx();
    let sql = dimension_sql(&ctx, "expr_cube.cast_of_compound");
    assert_eq!(sql, "CAST(\"expr_cube\".a + \"expr_cube\".b AS INT)");
}

#[test]
fn compound_in_case_branch_is_not_wrapped() {
    let ctx = ctx();
    let sql = dimension_sql(&ctx, "expr_cube.case_with_compound");
    assert_eq!(
        sql,
        "CASE WHEN \"expr_cube\".a > 0 THEN \"expr_cube\".a + \"expr_cube\".b ELSE 0 END"
    );
}

#[test]
fn direct_reference_does_not_add_parens() {
    let ctx = ctx();
    // Template is a single `{arg:0}` placeholder — no surrounding operators.
    let sql = dimension_sql(&ctx, "expr_cube.direct_compound");
    assert_eq!(sql, "\"expr_cube\".a + \"expr_cube\".b");
}

#[test]
fn atomic_dep_in_arithmetic_stays_unwrapped() {
    let ctx = ctx();
    let sql = dimension_sql(&ctx, "expr_cube.arith_over_atomic");
    assert_eq!(sql, "\"expr_cube\".col * 2");
}

#[test]
fn aggregate_wrap_resets_flag_for_inner_compound() {
    let ctx = ctx();
    // `SUM(…)` already provides a safe context, so the compound expression
    // inside is not additionally parenthesized.
    let sql = measure_sql(&ctx, "expr_cube.sum_compound");
    assert_eq!(sql, "sum(\"expr_cube\".a + \"expr_cube\".b)");
}

#[test]
fn sum_with_compound_template_no_inner_wrap() {
    let ctx = ctx();
    // The template itself is compound; the SUM wrap resets the child flag
    // so the arithmetic inside stays flat.
    let sql = measure_sql(&ctx, "expr_cube.sum_of_sum_template");
    assert_eq!(sql, "sum(\"expr_cube\".a + \"expr_cube\".b)");
}

#[test]
fn calculated_number_compound_top_level_is_not_wrapped() {
    let ctx = ctx();
    // `type: number` is a passthrough aggregate wrap. Rendered at top level
    // (no caller context), the compound expression comes out as-is.
    let sql = measure_sql(&ctx, "expr_cube.calc_number_compound");
    assert_eq!(sql, "sum(\"expr_cube\".a) + sum(\"expr_cube\".b)");
}

#[test]
fn calculated_number_over_compound_calc_wraps_dep() {
    let ctx = ctx();
    // `{calc_number_compound} * 100` — the dep renders to a compound
    // expression and must be wrapped before being embedded next to `*`.
    let sql = measure_sql(&ctx, "expr_cube.calc_number_over_compound");
    assert_eq!(sql, "(sum(\"expr_cube\".a) + sum(\"expr_cube\".b)) * 100");
}

#[test]
fn calculated_boolean_compound_top_level_is_not_wrapped() {
    let ctx = ctx();
    let sql = measure_sql(&ctx, "expr_cube.calc_boolean_compound");
    assert_eq!(sql, "sum(\"expr_cube\".a) > 0 OR sum(\"expr_cube\".b) > 0");
}

#[test]
fn calculated_boolean_combined_wraps_compound_dep() {
    let ctx = ctx();
    let sql = measure_sql(&ctx, "expr_cube.calc_boolean_combined");
    assert_eq!(
        sql,
        "(sum(\"expr_cube\".a) > 0 OR sum(\"expr_cube\".b) > 0) AND sum(\"expr_cube\".a) > 100"
    );
}

#[test]
fn measure_with_case_definition_renders_safely() {
    let ctx = ctx();
    // `CaseSqlNode` wraps the whole result in `CASE … END`. No substituted
    // deps here, but we verify the node's reset path doesn't break anything.
    let sql = measure_sql(&ctx, "expr_cube.measure_case");
    assert_eq!(
        sql,
        "CASE WHEN \"expr_cube\".a > 0 THEN 'positive' ELSE 'other' END"
    );
}

#[test]
fn sum_over_compound_plus_one_inner_stays_wrapped_outer_not() {
    let ctx = ctx();
    // Two effects stack:
    // - The inner SqlCall template `{calc_number_compound} + 1` marks the
    //   placeholder as unsafe (adjacent to `+`), so the compound dep is
    //   wrapped: `(sum(a) + sum(b)) + 1`.
    // - The outer `SUM(…)` is then applied by FinalMeasure and provides its
    //   own safe wrap, so no further parens are added around the `+ 1`.
    // (The inner `(…)` is over-parenthesized for `+` due to conservative
    // handling — acceptable trade-off, see scanner design notes.)
    let sql = measure_sql(&ctx, "expr_cube.sum_over_calc_number_plus_one");
    assert_eq!(
        sql,
        "sum((sum(\"expr_cube\".a) + sum(\"expr_cube\".b)) + 1)"
    );
}
