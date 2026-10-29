use crate::logical_plan::*;
use crate::planner::filter::tree_ops;
use crate::planner::filter::FilterItem;
use crate::planner::planners::multi_stage::EvaluationContext;
use crate::planner::query_properties::member_chain_eq;
use crate::planner::{InputId, JoinTree, JoinTreeItem, MemberSymbol, SqlCall};
use std::collections::HashMap;
use std::rc::Rc;

/// The bodies of the plan's inline subqueries, which their readers render.
pub type InlineBodies = HashMap<InputId, Rc<Query>>;

// A time shift moves the window a leaf's time dimension covers, and a
// row-grain evaluation stops the query aggregating at all; neither shows in
// the rest of the query.
fn same_evaluation_context(a: &Option<EvaluationContext>, b: &Option<EvaluationContext>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => same_evaluation_context_fields(a, b),
        _ => false,
    }
}

fn same_evaluation_context_fields(a: &EvaluationContext, b: &EvaluationContext) -> bool {
    // Destructured rather than read through fields: this comparison is the
    // pass's whole safety argument, and a field added to one of these types
    // must break the build rather than silently widen "reads the same rows".
    let EvaluationContext {
        measure_for_ungrouped,
        time_shifts,
    } = a;
    *measure_for_ungrouped == b.measure_for_ungrouped && *time_shifts == b.time_shifts
}

/// Whether two leaves read the same rows — everything about them but the
/// measures they aggregate.
///
/// Conservative by construction: a shape this cannot compare answers `false`.
/// That costs a shared scan the query could have had; the other direction
/// would merge two scans that read differently.
pub fn reads_same_rows(a: &Query, b: &Query, inline_bodies: &InlineBodies) -> bool {
    // `Query` and `QueryInputs` keep their fields private, so these two cannot
    // be destructured the way the rest are. A field added to either has to be
    // added here by hand, or two scans that read differently start comparing
    // equal.
    //
    // A conditional measure aggregates only some of the rows the scan reads,
    // so two scans carrying any are never taken for the same.
    a.conditional_measures().is_empty()
        && b.conditional_measures().is_empty()
        && same_evaluation_context(a.evaluation_context(), b.evaluation_context())
        && a.filter_params_filters().is_none()
        && b.filter_params_filters().is_none()
        && same_grain(a.schema(), b.schema())
        && same_filter(a.filter(), b.filter())
        && same_modifiers(a.modifers(), b.modifers())
        && same_source(a.source().base(), b.source().base())
        && same_dimension_inputs(
            a.source().dimension_inputs(),
            b.source().dimension_inputs(),
            inline_bodies,
        )
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

pub fn same_filter_items(a: &[FilterItem], b: &[FilterItem]) -> bool {
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
        distinct,
        order_by,
    } = a;
    *offset == b.offset
        && *limit == b.limit
        && *ungrouped == b.ungrouped
        && *distinct == b.distinct
        && *order_by == b.order_by
}

fn same_source(a: &InputBase, b: &InputBase) -> bool {
    match (a, b) {
        (InputBase::QueryInputs(a), InputBase::QueryInputs(b)) => same_inputs(a, b),
        // A pre-aggregation source is already the cheap read a shared scan
        // aims at, and a merge is not a leaf's source at all.
        _ => false,
    }
}

// See the note on [`reads_same_rows`]: private fields, compared by hand.
fn same_inputs(a: &QueryInputs, b: &QueryInputs) -> bool {
    a.alias_prefix() == b.alias_prefix()
        && same_join(a.join(), b.join())
        && a.subquery_joins().len() == b.subquery_joins().len()
        && a.subquery_joins()
            .iter()
            .zip(b.subquery_joins().iter())
            .all(|(x, y)| same_subquery_join(x, y))
}

fn same_dimension_inputs(
    a: &[DimensionInput],
    b: &[DimensionInput],
    inline_bodies: &InlineBodies,
) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| same_dimension_input(x, y, inline_bodies))
}

// A shared subquery is the same one exactly when it has the same name. An inline
// one is planned per reader, so its body is compared, measures included.
fn same_dimension_input(
    a: &DimensionInput,
    b: &DimensionInput,
    inline_bodies: &InlineBodies,
) -> bool {
    let DimensionInput { subquery, keys } = a;
    if !same_members(keys, &b.keys) {
        return false;
    }
    match (
        inline_bodies.get(&subquery.id()),
        inline_bodies.get(&b.subquery.id()),
    ) {
        (None, None) => subquery.name() == b.subquery.name(),
        (Some(a_body), Some(b_body)) => {
            same_members(
                &subquery.schema().dimensions,
                &b.subquery.schema().dimensions,
            ) && same_members(&a_body.schema().measures, &b_body.schema().measures)
                && reads_same_rows(a_body, b_body, inline_bodies)
        }
        _ => false,
    }
}

// Only what the join renders is compared. An edge's parent and relationship
// follow from its cubes and condition; the multiplication factors depend on
// the members each query asks for, not on the rows the join reads.
fn same_join(a: &Option<Rc<JoinTree>>, b: &Option<Rc<JoinTree>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => {
            a.root().cube_id() == b.root().cube_id()
                && a.joins().len() == b.joins().len()
                && a.joins()
                    .iter()
                    .zip(b.joins().iter())
                    .all(|(x, y)| same_join_item(x, y))
        }
        (None, None) => true,
        _ => false,
    }
}

fn same_join_item(a: &JoinTreeItem, b: &JoinTreeItem) -> bool {
    a.cube().cube_id() == b.cube().cube_id()
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
