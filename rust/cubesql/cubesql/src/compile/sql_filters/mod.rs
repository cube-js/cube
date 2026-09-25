//! The `/v1/sql-filters` API: reads the filters of a SQL API query from
//! its plan and rewrites the outermost SELECT with a batch of filter
//! additions, removals and replacements.
//!
//! A filter is rendered as an expression by `render`, its member is traced
//! to a relation of the outermost FROM by `resolve`, the clause it lands in
//! is keyed and edited by `matching`, and what a plan reports is read by
//! `report`.

mod matching;
mod render;
mod report;
mod resolve;
#[cfg(test)]
mod test_suite;
#[cfg(test)]
mod tests;

pub use report::extract_filters_from_plan;

use self::{
    matching::{
        and_chain, and_conjuncts_with, append_expr_to_clause, clause_mut, into_and_conjuncts_with,
        keys_of, remove_expr_from_clause, replace_expr_in_clause, with_keys, ClauseBudget,
        ClauseKeys, ClauseKind, MatchContext,
    },
    render::{require_filter_expr, resolve_filter_expr, ModifyAction},
    report::{
        bound_folded_into_range, dedupe_filters, filter_description, filter_key,
        filters_only_time_members, verification_keys, ReportedFilters,
    },
};
use crate::{
    compile::{convert_sql_to_cube_query, CompilationError},
    sql::Session,
    transport::MetaContext,
    CubeError,
};
use cubeclient::models::V1LoadRequestQueryFilterItem;
use datafusion::error::{DataFusionError, Result as DFResult};
use sqlparser::{ast, dialect::PostgreSqlDialect, parser::Parser};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

fn parse_single_query(sql: &str) -> DFResult<Box<ast::Query>> {
    let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| DataFusionError::SQL(e))?;
    let mut ast_iter = ast.into_iter();
    let Some(statement) = ast_iter.next() else {
        return Err(DataFusionError::NotImplemented(
            "No SQL statement found".to_string(),
        ));
    };
    if ast_iter.next().is_some() {
        return Err(DataFusionError::NotImplemented(
            "Only one statement per input is supported".to_string(),
        ));
    }
    let ast::Statement::Query(query) = statement else {
        return Err(DataFusionError::NotImplemented(
            "Only SELECT statements are supported".to_string(),
        ));
    };
    Ok(query)
}

/// The rewrite the endpoints use: a rewrite that fails is the caller's.
fn rewrite_sql(
    sql: &str,
    actions: &[ModifyAction],
    meta: &MetaContext,
    reported: &ReportedFilters,
) -> Result<(String, Vec<bool>), CubeError> {
    modify_parsed_query(sql, actions, meta, reported).map_err(|e| CubeError::user(e.to_string()))
}

fn modify_parsed_query(
    sql: &str,
    actions: &[ModifyAction],
    ctx: &MetaContext,
    reported: &ReportedFilters,
) -> DFResult<(String, Vec<bool>)> {
    let mut query = parse_single_query(sql)?;

    let mut applied = Vec::with_capacity(actions.len());
    let mut clause_keys = ClauseKeys::default();
    let mut clause_budget = ClauseBudget::of(outermost_select(&query)?)?;
    let mut next = 0;
    while next < actions.len() {
        // A run of removals is applied together, keying each clause a fixed
        // number of times rather than once per filter removed
        let removals = actions[next..]
            .iter()
            .map_while(|action| match action {
                ModifyAction::Remove(filter) => Some(filter),
                _ => None,
            })
            .collect::<Vec<_>>();
        if removals.is_empty() {
            applied.push(apply_action_to_outermost_select(
                query.as_mut(),
                &actions[next],
                ctx,
                reported,
                &mut clause_keys,
                &mut clause_budget,
            )?);
            next += 1;
        } else {
            next += removals.len();
            applied.extend(remove_filters_from_outermost_select(
                query.as_mut(),
                &removals,
                ctx,
                reported,
                &mut clause_keys,
            )?);
            clause_budget.recount(outermost_select(&query)?)?;
        }
    }

    let modified_sql = query.to_string();
    Ok((modified_sql, applied))
}

/// Applies the action to the outermost SELECT only: the member must resolve
/// there, as a column of a cube in its FROM or as an output column a derived
/// table or CTE exposes directly. Computed columns do not qualify.
fn apply_action_to_outermost_select(
    query: &mut ast::Query,
    action: &ModifyAction,
    ctx: &MetaContext,
    reported: &ReportedFilters,
    clause_keys: &mut ClauseKeys,
    clause_budget: &mut ClauseBudget,
) -> DFResult<bool> {
    // `with` is only read, and it is a field of its own, so a split borrow
    // keeps the whole CTE list out of the per-action work
    let ast::Query { with, body, .. } = query;
    let with = with.as_ref();
    let ast::SetExpr::Select(select) = body.as_mut() else {
        return Err(DataFusionError::NotImplemented(
            "Only plain SELECT statements are supported at the outermost level".to_string(),
        ));
    };

    // A bare column can only be one relation's when the FROM holds one
    let single_relation = select.from.len() == 1 && select.from[0].joins.is_empty();
    let match_context = |filter: &V1LoadRequestQueryFilterItem, sole_reported| MatchContext {
        sole_reported,
        ignore_qualifier: single_relation,
        normalize_dates: filters_only_time_members(filter, ctx),
    };

    match action {
        ModifyAction::Add(filter) => {
            let (expr, kind) = require_filter_expr(filter, select, with, ctx)?;
            let match_ctx = match_context(filter, false);
            if clause_keys.holds(kind, match_ctx, clause_mut(select, kind), &expr) {
                return Ok(false);
            }
            clause_budget.charge(kind, &expr)?;
            clause_keys.note_appended(kind, &expr, single_relation);
            append_expr_to_clause(clause_mut(select, kind), expr);
            Ok(true)
        }
        ModifyAction::Remove(_) => {
            unreachable!("removals are applied as a batch by modify_parsed_query")
        }
        ModifyAction::Replace { old, new } => {
            let Some((old_expr, old_kind)) = resolve_filter_expr(old, select, with, ctx)? else {
                return Ok(false);
            };
            let (new_expr, new_kind) = require_filter_expr(new, select, with, ctx)?;
            let match_ctx = match_context(old, reported.is_sole_on_member(old));
            // The new filter is nowhere in the query yet, so nothing about it
            // is reported; it is matched only against what the clause already
            // holds, under the member it stands on itself
            let new_match_ctx = match_context(new, false);
            clause_keys.forget(old_kind);
            if old_kind == new_kind {
                // Replace in place, preserving positions within the clause
                let replaced = replace_expr_in_clause(
                    clause_mut(select, old_kind),
                    &old_expr,
                    new_expr,
                    match_ctx,
                    new_match_ctx,
                );
                // Every occurrence is replaced, so the clause can grow or
                // shrink by more than one expression
                clause_budget.recount(select)?;
                Ok(replaced > 0)
            } else {
                // The filters belong to different clauses: remove, then add
                if remove_expr_from_clause(clause_mut(select, old_kind), &old_expr, match_ctx) == 0
                {
                    return Ok(false);
                }
                if !clause_keys.holds(
                    new_kind,
                    new_match_ctx,
                    clause_mut(select, new_kind),
                    &new_expr,
                ) {
                    clause_budget.charge(new_kind, &new_expr)?;
                    clause_keys.note_appended(new_kind, &new_expr, single_relation);
                    append_expr_to_clause(clause_mut(select, new_kind), new_expr);
                }
                clause_budget.recount(select)?;
                Ok(true)
            }
        }
    }
}

/// Removes a batch of filters from the outermost SELECT, returning whether each
/// was found. See [`remove_filters_from_select`].
fn remove_filters_from_outermost_select(
    query: &mut ast::Query,
    filters: &[&V1LoadRequestQueryFilterItem],
    ctx: &MetaContext,
    reported: &ReportedFilters,
    clause_keys: &mut ClauseKeys,
) -> DFResult<Vec<bool>> {
    let ast::Query { with, body, .. } = query;
    let ast::SetExpr::Select(select) = body.as_mut() else {
        return Err(DataFusionError::NotImplemented(
            "Only plain SELECT statements are supported at the outermost level".to_string(),
        ));
    };
    remove_filters_from_select(select, with.as_ref(), filters, ctx, reported, clause_keys)
}

/// Removes a batch of filters, keying each clause once per split rather than
/// once per filter; only what neither split found goes the long way, one by one.
fn remove_filters_from_select(
    select: &mut ast::Select,
    with: Option<&ast::With>,
    filters: &[&V1LoadRequestQueryFilterItem],
    ctx: &MetaContext,
    reported: &ReportedFilters,
    clause_keys: &mut ClauseKeys,
) -> DFResult<Vec<bool>> {
    let single_relation = select.from.len() == 1 && select.from[0].joins.is_empty();
    let mut applied = vec![false; filters.len()];

    let mut needles = Vec::with_capacity(filters.len());
    for filter in filters {
        needles.push(
            resolve_filter_expr(filter, select, with, ctx)?.map(|(expr, kind)| {
                (
                    expr,
                    kind,
                    MatchContext {
                        sole_reported: reported.is_sole_on_member(filter),
                        ignore_qualifier: single_relation,
                        normalize_dates: filters_only_time_members(filter, ctx),
                    },
                )
            }),
        );
    }

    for kind in [ClauseKind::Where, ClauseKind::Having] {
        let in_clause = (0..needles.len())
            .filter(|&i| needles[i].as_ref().is_some_and(|(_, k, _)| *k == kind))
            .collect::<Vec<_>>();
        if in_clause.is_empty() {
            continue;
        }
        // Forgotten once for the batch, not once per removal
        clause_keys.forget(kind);
        let slot = clause_mut(select, kind);

        for normalize_dates in [false, true] {
            let ctx = MatchContext {
                sole_reported: false,
                ignore_qualifier: single_relation,
                normalize_dates,
            };
            let mut by_key: HashMap<FilterKey, Vec<usize>> = HashMap::new();
            for &i in &in_clause {
                let (expr, _, needle_ctx) = needles[i].as_ref().unwrap();
                if needle_ctx.normalize_dates == normalize_dates {
                    by_key.entry(ctx.key(expr)).or_default().push(i);
                }
            }
            if by_key.is_empty() {
                continue;
            }
            for transparent in [false, true] {
                // Whether the split matches is decided before the clause is
                // taken apart, so one holding no needle stays as it was
                let Some(clause) = slot.take() else {
                    break;
                };
                let keys = keys_of(and_conjuncts_with(&clause, transparent), ctx);
                if !keys.iter().any(|key| by_key.contains_key(key)) {
                    *slot = Some(clause);
                    continue;
                }
                let conjuncts = into_and_conjuncts_with(clause, transparent);
                let mut kept = Vec::with_capacity(conjuncts.len());
                for (key, conjunct) in with_keys(keys, conjuncts, ctx) {
                    match by_key.get(&key) {
                        Some(found) => found.iter().for_each(|&i| applied[i] = true),
                        None => kept.push(conjunct),
                    }
                }
                *slot = and_chain(kept);
            }
        }

        for &i in &in_clause {
            if !applied[i] {
                let (expr, _, needle_ctx) = needles[i].as_ref().unwrap();
                applied[i] = remove_expr_from_clause(slot, expr, *needle_ctx) > 0;
            }
        }
    }

    Ok(applied)
}

/// The SELECT the actions apply to.
fn outermost_select(query: &ast::Query) -> DFResult<&ast::Select> {
    match query.body.as_ref() {
        ast::SetExpr::Select(select) => Ok(select),
        _ => Err(DataFusionError::NotImplemented(
            "Only plain SELECT statements are supported at the outermost level".to_string(),
        )),
    }
}

type FilterKey = String;

/// Upper bound on the filters a request may carry, counted in the leaves of
/// filter groups: a batch is one plan, so what it bounds is the predicate the
/// rewrite engine saturates over.
const MAX_FILTERS: usize = 500;

/// Upper bound on the nodes of one normalized filter tree: at most
/// [`MAX_FILTERS`] leaves, and a normalized group holds two children or more,
/// so fewer groups than leaves.
const MAX_FILTER_NODES: usize = 2 * MAX_FILTERS;

/// Refuses a request carrying more than [`MAX_FILTERS`] filters, counted in
/// the leaves of groups and across every array the request holds.
fn assert_filter_count<'a>(
    filters: impl IntoIterator<Item = &'a V1LoadRequestQueryFilterItem>,
) -> Result<(), CubeError> {
    let count = filters.into_iter().map(count_filter_leaves).sum::<usize>();
    if count > MAX_FILTERS {
        return Err(CubeError::user(format!(
            "At most {} filters are supported per request, got {}",
            MAX_FILTERS, count
        )));
    }
    Ok(())
}

fn count_filter_leaves(filter: &V1LoadRequestQueryFilterItem) -> usize {
    match filter.and.as_ref().or(filter.or.as_ref()) {
        Some(items) => items
            .iter()
            .map(count_filter_json_leaves)
            .sum::<usize>()
            .max(1),
        None => 1,
    }
}

fn count_filter_json_leaves(item: &serde_json::Value) -> usize {
    // A group is whichever of the two fields holds an array: `and` being
    // present but null is how a plain `or` group deserializes
    let items = item
        .get("and")
        .and_then(|items| items.as_array())
        .or_else(|| item.get("or").and_then(|items| items.as_array()));
    match items {
        Some(items) => items
            .iter()
            .map(count_filter_json_leaves)
            .sum::<usize>()
            .max(1),
        None => 1,
    }
}

/// The rewritten query and the filters its plan reports.
#[derive(Debug)]
pub struct SqlFiltersUpdate {
    pub sql: String,
    pub filters: Vec<V1LoadRequestQueryFilterItem>,
}

/// Extracts the Cube filters of a SQL query from its logical plan.
/// Time dimension date ranges are represented as `inDateRange` filter items.
pub async fn get_sql_filters(
    sql: &str,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<Vec<V1LoadRequestQueryFilterItem>, CubeError> {
    plan_and_extract_filters(sql, meta, session, "Failed to plan the query").await
}

async fn plan_and_extract_filters(
    sql: &str,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
    error_prefix: &str,
) -> Result<Vec<V1LoadRequestQueryFilterItem>, CubeError> {
    // A planner fault of its own stays internal, so the gateway answers it as
    // one; a query that does not plan is the caller's
    let query_plan = convert_sql_to_cube_query(sql, meta.clone(), session)
        .await
        .map_err(|e| {
            let message = format!("{}: {}", error_prefix, e);
            match e {
                CompilationError::Internal(..) => CubeError::internal(message),
                _ => CubeError::user(message),
            }
        })?;
    // A statement that compiles but has no plan - SET, SHOW, BEGIN - is the
    // caller's to fix, like the parse error `add` gives the same input
    let logical_plan = query_plan
        .try_as_logical_plan()
        .map_err(|_| CubeError::user("Only SELECT queries are supported".to_string()))?;
    extract_filters_from_plan(logical_plan, &meta)
}

/// Adds the filters to the SQL query and verifies each of them is picked up
/// by the logical plan of the rewritten query.
async fn add_filters_and_verify(
    sql: &str,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    let actions = filters
        .iter()
        .map(|filter| ModifyAction::Add(filter.clone()))
        .collect::<Vec<_>>();
    // Only additions, which match nothing that is already there
    let (new_sql, _) = rewrite_sql(sql, &actions, &meta, &ReportedFilters::none())?;

    verify_additions(Some(sql), new_sql, filters, meta, session).await
}

/// Verifies each added filter against the plan of the rewritten query. On
/// failure the original, when not yet known to plan, is planned to tell a
/// query that never planned from a rewrite that broke it.
async fn verify_additions(
    unplanned_original: Option<&str>,
    new_sql: String,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    let new_filters = match plan_and_extract_filters(
        &new_sql,
        meta.clone(),
        session.clone(),
        "Failed to plan the rewritten query",
    )
    .await
    {
        Ok(filters) => filters,
        Err(rewritten_error) => {
            // Told apart from a query that never planned, which is reported
            // as such. A rewrite that stops planning is still the caller's:
            // a measure filter added to a query with no GROUP BY does that
            if let Some(original_sql) = unplanned_original {
                plan_and_extract_filters(original_sql, meta, session, "Failed to plan the query")
                    .await?;
            }
            return Err(rewritten_error);
        }
    };
    assert_filters_applied(filters, &new_filters, &meta, "Filter")?;

    Ok(SqlFiltersUpdate {
        sql: new_sql,
        filters: new_filters,
    })
}

/// Whether each filter written is in the plan of the rewritten query: by its
/// keys, or as an end of a range the engine merged it into.
fn assert_filters_applied(
    filters: &[V1LoadRequestQueryFilterItem],
    extracted: &[V1LoadRequestQueryFilterItem],
    ctx: &MetaContext,
    what: &str,
) -> Result<(), CubeError> {
    let keys = extracted
        .iter()
        .map(|filter| filter_key(filter, ctx))
        .collect::<HashSet<_>>();
    for filter in filters {
        let applied = verification_keys(filter, ctx)
            .iter()
            .all(|key| keys.contains(key))
            || bound_folded_into_range(filter, extracted);
        if !applied {
            return Err(CubeError::user(format!(
                "{} {} was not applied to the query",
                what,
                filter_description(filter)
            )));
        }
    }
    Ok(())
}

/// Adds the filters to the outermost SELECT; ones already present are left as
/// is. Verified against the plan of the rewritten query, which is plan-wide,
/// so an equal filter in a CTE or a subquery satisfies it too.
pub async fn add_sql_filters(
    sql: &str,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    assert_filter_count(filters)?;

    add_filters_and_verify(sql, filters, meta, session).await
}

/// Removes every filter the plan reports from the outermost WHERE and HAVING,
/// then adds the given ones; a reported filter the outermost SELECT does not
/// hold, a CTE's, stays.
pub async fn set_sql_filters(
    sql: &str,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    assert_filter_count(filters)?;

    let reported_filters = plan_and_extract_filters(
        sql,
        meta.clone(),
        session.clone(),
        "Failed to plan the query",
    )
    .await?;
    // The removals are as many as the plan reports, and each walks the clause,
    // so they are bounded the way a request's own filters are
    assert_filter_count(&reported_filters).map_err(|_| {
        CubeError::user(format!(
            "The query reports more than {} filters, which is more than set can replace",
            MAX_FILTERS
        ))
    })?;
    let reported = ReportedFilters::of(&reported_filters);
    let actions = set_actions(&reported_filters, filters);
    let (new_sql, _) = rewrite_sql(sql, &actions, &meta, &reported)?;

    // The original planned above
    verify_additions(None, new_sql, filters, meta, session).await
}

/// The actions a `set` is made of: the removal of every reported filter,
/// then the additions.
fn set_actions(
    reported: &[V1LoadRequestQueryFilterItem],
    filters: &[V1LoadRequestQueryFilterItem],
) -> Vec<ModifyAction> {
    reported
        .iter()
        .map(|filter| ModifyAction::Remove(filter.clone()))
        .chain(
            filters
                .iter()
                .map(|filter| ModifyAction::Add(filter.clone())),
        )
        .collect()
}

/// Deletes the filters from the outermost SELECT: every equal filter goes, one
/// that is not there is a no-op. Decided on the AST, since plan-wide extraction
/// would false-alarm on a CTE's copy; the rewritten query is still planned.
pub async fn delete_sql_filters(
    sql: &str,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    assert_filter_count(filters)?;

    // One not spelled out as this API writes it is matched by its column
    let reported = ReportedFilters::of(
        &plan_and_extract_filters(
            sql,
            meta.clone(),
            session.clone(),
            "Failed to plan the query",
        )
        .await?,
    );

    let actions = filters
        .iter()
        .map(|filter| ModifyAction::Remove(filter.clone()))
        .collect::<Vec<_>>();
    let (new_sql, _) = rewrite_sql(sql, &actions, &meta, &reported)?;

    let new_filters = plan_and_extract_filters(
        &new_sql,
        meta,
        session,
        "Failed to plan the rewritten query",
    )
    .await?;

    Ok(SqlFiltersUpdate {
        sql: new_sql,
        filters: new_filters,
    })
}

/// Replaces one exact set of filters with another in the outermost SELECT:
/// every old filter must be present, all equal occurrences are replaced, and a
/// one-for-one replacement in one clause keeps its position. Verified as
/// [`add_sql_filters`] is; removal decided as [`delete_sql_filters`] does.
pub async fn replace_sql_filters(
    sql: &str,
    old_filters: &[V1LoadRequestQueryFilterItem],
    new_filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    if old_filters.is_empty() {
        return Err(CubeError::user(
            "At least one filter to replace is required".to_string(),
        ));
    }

    // One request, one bound: the old and the new filters together
    assert_filter_count(old_filters.iter().chain(new_filters))?;

    // A filter listed twice is one filter to find: the first removal takes
    // every occurrence, and the second would find nothing left
    let old_filters = &dedupe_filters(old_filters, &meta)[..];

    // As for a deletion, the filters the query reports decide which of them
    // are matched by the column they stand on rather than by their values
    let reported = ReportedFilters::of(
        &plan_and_extract_filters(
            sql,
            meta.clone(),
            session.clone(),
            "Failed to plan the query",
        )
        .await?,
    );

    let actions = if let ([old_filter], [new_filter]) = (old_filters, new_filters) {
        // One-to-one replacement is done in place to preserve positions
        vec![ModifyAction::Replace {
            old: old_filter.clone(),
            new: new_filter.clone(),
        }]
    } else {
        old_filters
            .iter()
            .map(|filter| ModifyAction::Remove(filter.clone()))
            .chain(
                new_filters
                    .iter()
                    .map(|filter| ModifyAction::Add(filter.clone())),
            )
            .collect()
    };
    let (new_sql, applied) = rewrite_sql(sql, &actions, &meta, &reported)?;

    // Every filter to replace has to be found in the outermost SELECT: the
    // removal (or in-place replacement) actions come first, one per old filter
    for (filter, applied) in old_filters.iter().zip(&applied) {
        if !applied {
            return Err(CubeError::user(format!(
                "Filter to replace {} was not found in the outermost SELECT",
                filter_description(filter)
            )));
        }
    }

    let extracted_filters = plan_and_extract_filters(
        &new_sql,
        meta.clone(),
        session,
        "Failed to plan the rewritten query",
    )
    .await?;
    assert_filters_applied(new_filters, &extracted_filters, &meta, "Replacement filter")?;

    Ok(SqlFiltersUpdate {
        sql: new_sql,
        filters: extracted_filters,
    })
}
