//! The `/v1/sql-filters` API: reads the filters of a SQL API query from
//! its plan and rewrites the outermost SELECT with a batch of filter
//! additions, removals and replacements.

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
        append_expr_to_clause, clause_contains, clause_mut, remove_expr_from_clause,
        replace_expr_in_clause, MatchContext,
    },
    render::{require_filter_expr, resolve_filter_expr, ModifyAction, Purpose},
    report::{
        bound_folded_into_range, dedupe_filters, filter_description, filter_key, filter_members,
        filters_only_time_members, verification_keys, ReportedFilters, LIKE_FAMILY_OPERATORS,
    },
    resolve::MemberSources,
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
use std::{collections::HashSet, sync::Arc};

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

/// The rewrite the endpoints use. Its failures are the caller's errors, except
/// `Internal` ones, which are faults of the rewrite itself.
fn rewrite_sql(
    sql: &str,
    actions: &[ModifyAction],
    meta: &MetaContext,
    reported: &ReportedFilters,
) -> Result<(String, Vec<bool>), CubeError> {
    modify_parsed_query(sql, actions, meta, reported).map_err(|e| match e {
        DataFusionError::Internal(_) => CubeError::internal(e.to_string()),
        _ => CubeError::user(e.to_string()),
    })
}

fn modify_parsed_query(
    sql: &str,
    actions: &[ModifyAction],
    ctx: &MetaContext,
    reported: &ReportedFilters,
) -> DFResult<(String, Vec<bool>)> {
    let mut query = parse_single_query(sql)?;
    // sqlparser prints a doubled quote as it stands, so a query holding one
    // would come back as another
    let original_reads_back =
        parse_single_query(&query.to_string()).is_ok_and(|reparsed| reparsed == query);
    let sources = MemberSources::default();
    let applied = actions
        .iter()
        .map(|action| {
            apply_action_to_outermost_select(query.as_mut(), action, ctx, reported, &sources)
        })
        .collect::<DFResult<Vec<_>>>()?;

    let modified_sql = query.to_string();
    // The rewrite can deepen a clause past the parser's nesting limit, so the
    // printed query no longer reads back
    let reads_back = parse_single_query(&modified_sql)
        .is_ok_and(|reparsed| reparsed.to_string() == modified_sql);
    if !original_reads_back || !reads_back {
        return Err(DataFusionError::NotImplemented(
            "The rewritten query does not read back as written".to_string(),
        ));
    }
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
    sources: &MemberSources,
) -> DFResult<bool> {
    let ast::Query { with, body, .. } = query;
    let with = with.as_ref();
    let ast::SetExpr::Select(select) = body.as_mut() else {
        return Err(DataFusionError::NotImplemented(
            "Only plain SELECT statements are supported at the outermost level".to_string(),
        ));
    };

    // A bare column can only be one relation's when the FROM holds one
    let single_relation = select.from.len() == 1 && select.from[0].joins.is_empty();
    let match_context = |filter: &V1LoadRequestQueryFilterItem| MatchContext {
        sole_reported: reported.is_sole_on_member(filter, ctx),
        ignore_qualifier: single_relation,
        normalize_dates: filters_only_time_members(filter, ctx),
    };

    match action {
        ModifyAction::Add(filter) => {
            let (expr, kind) = require_filter_expr(filter, select, with, ctx, sources)?;
            let clause = clause_mut(select, kind);
            if clause_contains(clause, &expr, match_context(filter)) {
                return Ok(false);
            }
            append_expr_to_clause(clause, expr);
            Ok(true)
        }
        ModifyAction::Remove(filter) | ModifyAction::RemoveToReplace(filter) => {
            let held = reported.holds(filter, ctx);
            let purpose = match action {
                ModifyAction::Remove(_) => Purpose::Remove { reported: held },
                _ => Purpose::Replace { reported: held },
            };
            let Some((expr, kind)) =
                resolve_filter_expr(filter, select, with, ctx, sources, purpose)?
            else {
                return Ok(false);
            };
            let clause = clause_mut(select, kind);
            Ok(remove_expr_from_clause(clause, &expr, match_context(filter)) > 0)
        }
        ModifyAction::Replace { old, new } => {
            let Some((old_expr, old_kind)) = resolve_filter_expr(
                old,
                select,
                with,
                ctx,
                sources,
                Purpose::Replace {
                    reported: reported.holds(old, ctx),
                },
            )?
            else {
                return Ok(false);
            };
            let (new_expr, new_kind) = require_filter_expr(new, select, with, ctx, sources)?;
            let match_ctx = match_context(old);
            // The new filter is nowhere in the query yet, so nothing about it
            // is reported; it is matched only against what the clause already
            // holds, under the member it stands on itself
            let new_match_ctx = MatchContext {
                sole_reported: false,
                ..match_context(new)
            };
            if old_kind == new_kind {
                // Replace in place, preserving positions within the clause
                let replaced = replace_expr_in_clause(
                    clause_mut(select, old_kind),
                    &old_expr,
                    new_expr,
                    match_ctx,
                    new_match_ctx,
                );
                return Ok(replaced > 0);
            }
            if remove_expr_from_clause(clause_mut(select, old_kind), &old_expr, match_ctx) == 0 {
                return Ok(false);
            }
            let new_clause = clause_mut(select, new_kind);
            if !clause_contains(new_clause, &new_expr, new_match_ctx) {
                append_expr_to_clause(new_clause, new_expr);
            }
            Ok(true)
        }
    }
}

type FilterKey = String;

/// Upper bound on the filters a request may carry, counted in the leaves of
/// filter groups: a batch is one plan, so what it bounds is the predicate the
/// rewrite engine saturates over.
const MAX_FILTERS: usize = 500;

/// Upper bound on the values the filters of a request carry together, which
/// a single `IN` list would otherwise grow past the filter count.
const MAX_FILTER_VALUES: usize = 10_000;

/// Refuses a request carrying more than [`MAX_FILTERS`] filters, counted in
/// the leaves of groups, or more than [`MAX_FILTER_VALUES`] values.
fn assert_filter_count<'a>(
    filters: impl IntoIterator<Item = &'a V1LoadRequestQueryFilterItem>,
) -> Result<(), CubeError> {
    let (mut count, mut values) = (0, 0);
    let mut pending = filters
        .into_iter()
        .map(|filter| serde_json::to_value(filter).unwrap_or_default())
        .collect::<Vec<_>>();
    while let Some(item) = pending.pop() {
        let items = ["and", "or"]
            .iter()
            .filter_map(|group| item.get(group).and_then(|items| items.as_array()))
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        let item_values = item
            .get("values")
            .and_then(|values| values.as_array())
            .map_or(0, |values| values.len());
        if items.is_empty() {
            // A LIKE-family filter is written as one predicate per value
            count += match item.get("operator").and_then(|operator| operator.as_str()) {
                Some(operator) if LIKE_FAMILY_OPERATORS.contains(&operator) => item_values.max(1),
                _ => 1,
            };
        }
        values += item_values;
        pending.extend(items);
    }
    if count > MAX_FILTERS {
        return Err(CubeError::user(format!(
            "At most {} filters are supported per request, got {}",
            MAX_FILTERS, count
        )));
    }
    if values > MAX_FILTER_VALUES {
        return Err(CubeError::user(format!(
            "At most {} filter values are supported per request, got {}",
            MAX_FILTER_VALUES, values
        )));
    }
    Ok(())
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
    // Other statements act on the session as they plan, as `SET ROLE` does;
    // one that does not parse is left to the planner's own error
    let is_query = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map(|statements| matches!(&statements[..], [ast::Statement::Query(_)]));
    if matches!(is_query, Ok(false)) {
        return Err(CubeError::user(
            "Only SELECT queries are supported".to_string(),
        ));
    }

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
    // A query the compiler answers without a logical plan is the caller's to
    // fix, like the parse error `add` gives the same input
    let logical_plan = query_plan
        .try_as_logical_plan()
        .map_err(|_| CubeError::user("Only SELECT queries are supported".to_string()))?;
    extract_filters_from_plan(logical_plan, &meta)
}

/// Adds the filters to the SQL query and verifies each of them is picked up
/// by the logical plan of the rewritten query.
async fn add_filters_and_verify(
    sql: &str,
    actions: &[ModifyAction],
    filters: &[V1LoadRequestQueryFilterItem],
    reported: &ReportedFilters,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<(SqlFiltersUpdate, Vec<bool>), CubeError> {
    let (new_sql, applied) = rewrite_sql(sql, actions, &meta, reported)?;
    let new_filters = plan_and_extract_filters(
        &new_sql,
        meta.clone(),
        session,
        "Failed to plan the rewritten query",
    )
    .await?;
    assert_filters_applied(filters, &new_filters, &meta)?;
    let update = SqlFiltersUpdate {
        sql: new_sql,
        filters: new_filters,
    };
    Ok((update, applied))
}

/// Whether each filter written is in the plan of the rewritten query: by its
/// keys, or as an end of a range the engine merged it into.
fn assert_filters_applied(
    filters: &[V1LoadRequestQueryFilterItem],
    extracted: &[V1LoadRequestQueryFilterItem],
    ctx: &MetaContext,
) -> Result<(), CubeError> {
    let keys = extracted
        .iter()
        .map(|filter| filter_key(filter, ctx))
        .collect::<HashSet<_>>();
    for filter in filters {
        // The members of a top-level `and` may be bounds the engine merged
        let member_applied = |key: &FilterKey| {
            keys.contains(key)
                || serde_json::from_str::<V1LoadRequestQueryFilterItem>(key)
                    .is_ok_and(|member| bound_folded_into_range(&member, extracted))
        };
        let applied = verification_keys(filter, ctx).iter().all(member_applied)
            || bound_folded_into_range(filter, extracted);
        if !applied {
            return Err(CubeError::user(format!(
                "Filter {} is not among the filters the rewritten query's plan reports",
                filter_description(filter)
            )));
        }
    }
    Ok(())
}

/// Adds the filters to the outermost SELECT, skipping ones already written there.
/// Verified against the plan of the rewritten query, so an equal filter in a CTE
/// or a subquery satisfies it too.
pub async fn add_sql_filters(
    sql: &str,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> Result<SqlFiltersUpdate, CubeError> {
    assert_filter_count(filters)?;
    let actions = filters
        .iter()
        .map(|filter| ModifyAction::Add(filter.clone()))
        .collect::<Vec<_>>();
    let reported = ReportedFilters::none();
    let (update, _) =
        add_filters_and_verify(sql, &actions, filters, &reported, meta, session).await?;
    Ok(update)
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
    let reported = ReportedFilters::of(&reported_filters, &meta);
    let actions = set_actions(&reported_filters, filters);
    let (update, applied) =
        add_filters_and_verify(sql, &actions, filters, &reported, meta.clone(), session).await?;
    let removals = &actions[..reported_filters.len()];
    assert_removed_by_column(
        sql,
        removals,
        &applied,
        filters,
        &reported,
        &update.filters,
        &meta,
    )?;
    Ok(update)
}

/// A filter the query reports alone on its member is removed by its column
/// when the query spells it otherwise. If the plan still reports it, it lives
/// elsewhere, as in a CTE under a `LIMIT`, and another predicate went instead.
fn assert_removed_by_column(
    sql: &str,
    actions: &[ModifyAction],
    applied: &[bool],
    added: &[V1LoadRequestQueryFilterItem],
    reported: &ReportedFilters,
    extracted: &[V1LoadRequestQueryFilterItem],
    ctx: &MetaContext,
) -> Result<(), CubeError> {
    let still_reported = ReportedFilters::of(extracted, ctx);
    // A member filtered anew may rightly be reported again, in a shape of the
    // engine's; a new filter lands in the outermost SELECT and is verified
    let added = added
        .iter()
        .flat_map(filter_members)
        .collect::<HashSet<_>>();
    let suspect = |i: usize| match &actions[i] {
        ModifyAction::Remove(filter)
        | ModifyAction::RemoveToReplace(filter)
        | ModifyAction::Replace { old: filter, .. } => (applied[i]
            && filter_members(filter).is_disjoint(&added)
            && reported.is_sole_on_member(filter, ctx)
            && still_reported.holds(filter, ctx))
        .then_some(filter),
        _ => None,
    };
    if !(0..actions.len()).any(|i| suspect(i).is_some()) {
        return Ok(());
    }
    // Rewritten again with no filter alone on its member, so none is matched
    // by its column: what this still removes is spelled as the API writes it
    let (_, as_written) = rewrite_sql(sql, actions, ctx, &reported.none_alone())?;
    let by_column = (0..actions.len()).find_map(|i| suspect(i).filter(|_| !as_written[i]));
    match by_column {
        Some(filter) => Err(CubeError::user(format!(
            "Filter {} is not the outermost SELECT's own to remove",
            filter_description(filter)
        ))),
        None => Ok(()),
    }
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
    let reported_filters = plan_and_extract_filters(
        sql,
        meta.clone(),
        session.clone(),
        "Failed to plan the query",
    )
    .await?;
    let reported = ReportedFilters::of(&reported_filters, &meta);

    let actions = filters
        .iter()
        .map(|filter| ModifyAction::Remove(filter.clone()))
        .collect::<Vec<_>>();
    let (new_sql, applied) = rewrite_sql(sql, &actions, &meta, &reported)?;

    let new_filters = plan_and_extract_filters(
        &new_sql,
        meta.clone(),
        session,
        "Failed to plan the rewritten query",
    )
    .await?;
    assert_removed_by_column(sql, &actions, &applied, &[], &reported, &new_filters, &meta)?;

    Ok(SqlFiltersUpdate {
        sql: new_sql,
        filters: new_filters,
    })
}

/// Replaces one exact set of filters with another in the outermost SELECT:
/// every old filter must be present, and a one-for-one replacement in one
/// clause keeps its position.
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
    let reported_filters = plan_and_extract_filters(
        sql,
        meta.clone(),
        session.clone(),
        "Failed to plan the query",
    )
    .await?;
    let reported = ReportedFilters::of(&reported_filters, &meta);

    let actions = if let ([old_filter], [new_filter]) = (old_filters, new_filters) {
        vec![ModifyAction::Replace {
            old: old_filter.clone(),
            new: new_filter.clone(),
        }]
    } else {
        old_filters
            .iter()
            .map(|filter| ModifyAction::RemoveToReplace(filter.clone()))
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
    assert_filters_applied(new_filters, &extracted_filters, &meta)?;
    let removals = &actions[..old_filters.len()];
    assert_removed_by_column(
        sql,
        removals,
        &applied,
        new_filters,
        &reported,
        &extracted_filters,
        &meta,
    )?;

    Ok(SqlFiltersUpdate {
        sql: new_sql,
        filters: extracted_filters,
    })
}
