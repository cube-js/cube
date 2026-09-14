use crate::logical_plan::*;
use crate::planner::filter::tree_ops;
use crate::planner::filter::FilterItem;
use crate::planner::planners::multi_stage::EvaluationContext;
use crate::planner::query_properties::member_chain_eq;
use crate::planner::MemberSymbol;
use crate::planner::SqlCall;
use std::rc::Rc;

/// Whether two leaves read the same rows — everything about them but the
/// measures they aggregate.
///
/// Conservative by construction: a shape this cannot compare answers `false`.
/// That costs a shared scan the query could have had; the other direction
/// would merge two scans that read differently.
pub fn reads_same_rows(a: &MultiStageLeafMeasure, b: &MultiStageLeafMeasure) -> bool {
    same_evaluation_context(&a.evaluation_context, &b.evaluation_context)
        && same_query_rows(&a.query, &b.query)
}

// The rows a leaf reads are not all in its query: a time shift moves the
// window its time dimension covers, and a row-grain evaluation stops the
// query aggregating at all. Both live here, and neither is visible in the
// query the leaf holds.
fn same_evaluation_context(a: &EvaluationContext, b: &EvaluationContext) -> bool {
    // Destructured rather than read through fields: this comparison is the
    // pass's whole safety argument, and a field added to one of these types
    // must break the build rather than silently widen "reads the same rows".
    let EvaluationContext {
        measure_for_ungrouped,
        time_shifts,
    } = a;
    *measure_for_ungrouped == b.measure_for_ungrouped && *time_shifts == b.time_shifts
}

// `Query` and `LogicalJoin` keep their fields private, so these two cannot be
// destructured the way the rest are. A field added to either has to be added
// here by hand, or two scans that read differently start comparing equal.
fn same_query_rows(a: &Query, b: &Query) -> bool {
    same_grain(a.schema(), b.schema())
        && same_filter(a.filter(), b.filter())
        && same_modifiers(a.modifers(), b.modifers())
        && same_source(a.source(), b.source())
}

pub fn same_members(a: &[Rc<MemberSymbol>], b: &[Rc<MemberSymbol>]) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| member_chain_eq(x, y))
}

fn same_grain(a: &LogicalSchema, b: &LogicalSchema) -> bool {
    let LogicalSchema {
        time_dimensions,
        dimensions,
        // The measures are what the merge widens, so they are deliberately
        // not part of "reads the same rows".
        measures: _,
    } = a;
    same_members(dimensions, &b.dimensions) && same_members(time_dimensions, &b.time_dimensions)
}

fn same_filter(a: &LogicalFilter, b: &LogicalFilter) -> bool {
    let LogicalFilter {
        dimensions_filters,
        time_dimensions_filters,
        measures_filter,
        segments,
    } = a;
    same_filter_items(dimensions_filters, &b.dimensions_filters)
        && same_filter_items(time_dimensions_filters, &b.time_dimensions_filters)
        && same_filter_items(measures_filter, &b.measures_filter)
        && same_filter_items(segments, &b.segments)
}

fn same_filter_items(a: &[FilterItem], b: &[FilterItem]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| tree_ops::eq_with_member(x, y))
}

fn same_modifiers(a: &LogicalQueryModifiers, b: &LogicalQueryModifiers) -> bool {
    let LogicalQueryModifiers {
        offset,
        limit,
        ungrouped,
        order_by,
    } = a;
    *offset == b.offset && *limit == b.limit && *ungrouped == b.ungrouped && *order_by == b.order_by
}

fn same_source(a: &QuerySource, b: &QuerySource) -> bool {
    match (a, b) {
        (QuerySource::LogicalJoin(a), QuerySource::LogicalJoin(b)) => same_join(a, b),
        // A pre-aggregation source is already the cheap read a shared scan
        // aims at, and a full-key aggregate is not a leaf's source at all.
        _ => false,
    }
}

// See the note on [`same_query_rows`]: private fields, compared by hand.
fn same_join(a: &LogicalJoin, b: &LogicalJoin) -> bool {
    same_root(a.root(), b.root())
        && a.joins().len() == b.joins().len()
        && a.joins()
            .iter()
            .zip(b.joins().iter())
            .all(|(x, y)| same_join_item(x, y))
        && a.dimension_subqueries().len() == b.dimension_subqueries().len()
        && a.dimension_subqueries()
            .iter()
            .zip(b.dimension_subqueries().iter())
            .all(|(x, y)| same_dimension_subquery(x, y))
        && a.subquery_joins().len() == b.subquery_joins().len()
        && a.subquery_joins()
            .iter()
            .zip(b.subquery_joins().iter())
            .all(|(x, y)| same_subquery_join(x, y))
}

fn same_root(a: &Option<Rc<Cube>>, b: &Option<Rc<Cube>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => same_cube(a, b),
        (None, None) => true,
        _ => false,
    }
}

fn same_cube(a: &Cube, b: &Cube) -> bool {
    a.name() == b.name()
        && a.original_sql_pre_aggregation().as_ref().map(|p| p.name())
            == b.original_sql_pre_aggregation().as_ref().map(|p| p.name())
}

fn same_join_item(a: &LogicalJoinItem, b: &LogicalJoinItem) -> bool {
    same_cube(a.cube(), b.cube())
        && a.splits_rows() == b.splits_rows()
        && same_join_condition(a.on_sql(), b.on_sql())
}

// A join condition is compiled once per edge and shared by every query built
// over that edge, so identity settles it. Two conditions that are equal
// without being the same object decline the merge.
fn same_join_condition(a: &Rc<SqlCall>, b: &Rc<SqlCall>) -> bool {
    Rc::ptr_eq(a, b)
}

fn same_subquery_join(a: &LogicalSubqueryJoinItem, b: &LogicalSubqueryJoinItem) -> bool {
    let LogicalSubqueryJoinItem {
        sql,
        alias,
        join_type,
        on_sql,
    } = a;
    *sql == b.sql
        && *alias == b.alias
        && *join_type == b.join_type
        && same_join_condition(on_sql, &b.on_sql)
}

// A sub-query dimension contributes a joined-in CTE of its own, so the two
// hosts read the same rows only if those CTEs do — measures included, since
// the dimension's value is one of them.
fn same_dimension_subquery(a: &DimensionSubQuery, b: &DimensionSubQuery) -> bool {
    let DimensionSubQuery {
        query,
        primary_keys_dimensions,
        subquery_dimension,
        measure_for_subquery_dimension,
    } = a;
    member_chain_eq(subquery_dimension, &b.subquery_dimension)
        && member_chain_eq(
            measure_for_subquery_dimension,
            &b.measure_for_subquery_dimension,
        )
        && same_members(primary_keys_dimensions, &b.primary_keys_dimensions)
        && same_members(&query.schema().measures, &b.query.schema().measures)
        && same_query_rows(query, &b.query)
}
