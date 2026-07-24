//! Physical-plan optimizer collapsing trivial pass-through subqueries.
//!
//! Multi-stage planning wraps every CTE reference into a derived table of
//! the form `(SELECT * FROM cte_n AS cte_n) AS alias`. Each such wrapper is
//! semantically a no-op, but engines that inline CTE bodies at every
//! reference (e.g. Cube Store / DataFusion) pay for it with two extra plan
//! nodes (projection + subquery alias) per usage. Deep multi-stage queries
//! can overflow Cube Store's serialized-plan decode recursion limit purely
//! because of these wrappers.
//!
//! This pass rewrites `(SELECT * FROM <table or cte> AS x) AS alias` into a
//! direct `<table or cte> AS alias` reference everywhere in the plan: in the
//! top-level select, inside CTE bodies, join sides, unions and nested
//! subqueries. Only selects that are pure pass-throughs are collapsed: a
//! `SELECT *` projection with no filter, grouping, having, ordering,
//! distinct, limit, offset or own CTEs, reading from a single table
//! reference.

use super::super::{Cte, From, FromSource, Join, JoinItem, QueryPlan, Select, SingleAliasedSource, SingleSource};
use crate::physical_plan::CalcGroupsJoin;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Entry point: returns a plan equivalent to `select` with all trivial
/// pass-through subqueries collapsed into direct table references.
pub fn collapse_trivial_subqueries(select: &Rc<Select>) -> Result<Rc<Select>, CubeError> {
    optimize_select(select)
}

fn optimize_select(select: &Rc<Select>) -> Result<Rc<Select>, CubeError> {
    let ctes = select
        .ctes
        .iter()
        .map(|cte| -> Result<_, CubeError> {
            Ok(Rc::new(Cte::new(
                optimize_plan(cte.query())?,
                cte.name().clone(),
                cte.is_recursive(),
            )))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let from = optimize_from(&select.from)?;

    Ok(Rc::new(Select {
        from,
        ctes,
        ..select.as_ref().clone()
    }))
}

fn optimize_plan(plan: &Rc<QueryPlan>) -> Result<Rc<QueryPlan>, CubeError> {
    let result = match plan.as_ref() {
        QueryPlan::Select(select) => QueryPlan::Select(optimize_select(select)?),
        QueryPlan::Union(union) => {
            let items = union
                .union
                .iter()
                .map(|item| -> Result<_, CubeError> {
                    Ok(match item {
                        QueryPlan::Select(select) => QueryPlan::Select(optimize_select(select)?),
                        other => other.clone(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            QueryPlan::Union(Rc::new(super::super::Union::new(items)))
        }
        QueryPlan::TimeSeries(_) => return Ok(plan.clone()),
    };
    Ok(Rc::new(result))
}

fn optimize_from(from: &Rc<From>) -> Result<Rc<From>, CubeError> {
    let source = match &from.source {
        FromSource::Empty => FromSource::Empty,
        FromSource::Single(source) => FromSource::Single(optimize_single_source(source)?),
        FromSource::Join(join) => {
            let root = optimize_single_source(&join.root)?;
            let joins = join
                .joins
                .iter()
                .map(|item| -> Result<_, CubeError> {
                    Ok(JoinItem {
                        from: optimize_single_source(&item.from)?,
                        on: item.on.clone(),
                        join_type: item.join_type.clone(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            FromSource::Join(Rc::new(Join { root, joins }))
        }
        FromSource::CalcGroupsJoin(join) => {
            let inner = optimize_from(join.from())?;
            FromSource::CalcGroupsJoin(CalcGroupsJoin::try_new(inner, join.calc_groups().clone())?)
        }
    };
    Ok(From::new(source))
}

fn optimize_single_source(
    source: &SingleAliasedSource,
) -> Result<SingleAliasedSource, CubeError> {
    match &source.source {
        SingleSource::Subquery(plan) => {
            let optimized = optimize_plan(plan)?;
            if let Some(collapsed) = try_collapse(&optimized, &source.alias) {
                return Ok(collapsed);
            }
            Ok(SingleAliasedSource::new_from_source(
                SingleSource::Subquery(optimized),
                source.alias.clone(),
            ))
        }
        _ => Ok(source.clone()),
    }
}

/// If `plan` is a trivial pass-through select over a single table
/// reference, return that table reference re-aliased with the outer alias.
fn try_collapse(plan: &Rc<QueryPlan>, alias: &String) -> Option<SingleAliasedSource> {
    let QueryPlan::Select(select) = plan.as_ref() else {
        return None;
    };
    if !is_trivial_passthrough(select) {
        return None;
    }
    let FromSource::Single(inner_source) = &select.from.source else {
        return None;
    };
    let SingleSource::TableReference(reference, _) = &inner_source.source else {
        return None;
    };
    Some(SingleAliasedSource::new_from_table_reference(
        reference.clone(),
        select.schema(),
        Some(alias.clone()),
    ))
}

/// A select that renders as `SELECT * FROM <single source>` with no other
/// clauses: removing it does not change the produced rows or column names.
fn is_trivial_passthrough(select: &Select) -> bool {
    select.projection_columns.is_empty()
        && select.filter.is_none()
        && select.group_by.is_empty()
        && select.having.is_none()
        && select.order_by.is_empty()
        && select.ctes.is_empty()
        && !select.is_distinct
        && select.limit.is_none()
        && select.offset.is_none()
}
