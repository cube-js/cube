//! Tracing a member to a relation of the outermost FROM: the cube itself,
//! or a derived table or CTE exposing it as an output column.

use crate::transport::MetaContext;
use cubeclient::models::{V1CubeMetaDimension, V1CubeMetaMeasure};
use datafusion::error::{DataFusionError, Result as DFResult};
use sqlparser::ast;
use std::{cell::Cell, collections::HashSet, iter};

/// Aggregations a measure may be projected or filtered with.
pub(super) const AGG_FUNCTIONS: [&str; 7] =
    ["COUNT", "SUM", "AVG", "MIN", "MAX", "MEASURE", "AGGREGATE"];

#[derive(Debug)]
pub(super) enum MetaMember {
    Dimension(V1CubeMetaDimension),
    Measure(V1CubeMetaMeasure),
}

impl MetaMember {
    pub(super) fn get_from_ctx(
        ctx: &MetaContext,
        cube_name: &str,
        member_name: &str,
    ) -> DFResult<Self> {
        let full_member_name = format!("{}.{}", cube_name, member_name);
        if let Some(dimension) = ctx.find_dimension_with_name(&full_member_name) {
            return Ok(MetaMember::Dimension(dimension.clone()));
        }
        if let Some(measure) = ctx.find_measure_with_name(&full_member_name) {
            return Ok(MetaMember::Measure(measure.clone()));
        }
        Err(DataFusionError::Plan(format!(
            "Member \"{}\" not found in data model",
            full_member_name
        )))
    }

    /// Whether the member holds a time, which is where the planner
    /// canonicalizes the value a filter was written with.
    pub(super) fn is_time(&self) -> bool {
        let kind = match self {
            MetaMember::Dimension(dimension) => &dimension.r#type,
            MetaMember::Measure(measure) => &measure.r#type,
        };
        kind == "time"
    }

    pub(super) fn short_name(&self) -> String {
        let full_name = match self {
            MetaMember::Dimension(dimension) => &dimension.name,
            MetaMember::Measure(measure) => &measure.name,
        };
        full_name
            .split('.')
            .next_back()
            .unwrap_or(full_name)
            .to_string()
    }
}

/// Where the member column comes from in the outermost SELECT.
#[derive(Debug)]
pub(super) enum MemberSource {
    /// The cube is referenced directly in the outermost FROM: dimensions are
    /// filtered in WHERE, measures are aggregated and filtered in HAVING.
    CubeTable { relation_alias: ast::Ident },
    /// The member is exposed as an output column of a derived table or a CTE
    /// reference in the outermost FROM: filtered as a plain column in WHERE.
    /// The column is carried as written, since quoting decides whether the
    /// identifier folds.
    DerivedColumn {
        relation_alias: ast::Ident,
        column: ast::Ident,
    },
}

/// How many relations deep a chain of derived tables and CTE references is
/// followed when resolving a member; past it the member is unavailable. It
/// also refuses a self-referencing CTE.
const MAX_RELATION_DEPTH: usize = 25;

/// Resolves the member to a column available in the outermost SELECT.
/// Returns `None` if the member is not available there.
pub(super) fn resolve_member_source(
    select: &ast::Select,
    with: Option<&ast::With>,
    cube_name: &str,
    member_name: &str,
    meta_member: &MetaMember,
) -> Option<MemberSource> {
    let withs = with.into_iter().collect::<Vec<_>>();

    // Cube referenced directly in the outermost FROM
    if let Some(relation_alias) =
        alias_for_relation_in_from(cube_name, &select.from, &cte_names(&withs))
    {
        return Some(MemberSource::CubeTable { relation_alias });
    }

    // Derived tables and CTE references in the outermost FROM exposing the
    // member, directly or through relations of their own
    let budget = Cell::new(MAX_RELATION_EXPANSIONS);
    for table_with_joins in &select.from {
        let factors = iter::once(&table_with_joins.relation)
            .chain(table_with_joins.joins.iter().map(|join| &join.relation));
        for factor in factors {
            if let Some((relation_alias, column)) = member_column_in_table_factor(
                factor,
                &withs,
                cube_name,
                member_name,
                meta_member,
                0,
                &budget,
            ) {
                return Some(MemberSource::DerivedColumn {
                    relation_alias,
                    column,
                });
            }
        }
    }

    None
}

/// Names of the CTEs visible at a point. A relation with such a name refers
/// to the CTE, not to a cube of the same name.
fn cte_names(withs: &[&ast::With]) -> HashSet<String> {
    withs
        .iter()
        .flat_map(|with| with.cte_tables.iter())
        .map(|cte| cte.alias.name.value.to_ascii_lowercase())
        .collect()
}

/// How many relations are followed in total while resolving a member or
/// deciding whether a column can be one. Depth alone doesn't bound the work:
/// a query whose relations each reference several others branches out.
const MAX_RELATION_EXPANSIONS: usize = 1000;

/// Consumes one relation expansion, returning whether there was budget left.
fn spend_expansion(budget: &Cell<usize>) -> bool {
    let left = budget.get();
    if left == 0 {
        return false;
    }
    budget.set(left - 1);
    true
}

/// If the table factor is a derived table or a CTE reference which exposes the
/// member as an output column, returns the relation alias to qualify the
/// column with and the output column name.
fn member_column_in_table_factor<'a>(
    factor: &'a ast::TableFactor,
    withs: &[&'a ast::With],
    cube_name: &str,
    member_name: &str,
    meta_member: &MetaMember,
    depth: usize,
    budget: &Cell<usize>,
) -> Option<(ast::Ident, ast::Ident)> {
    match factor {
        ast::TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } if alias.columns.is_empty() => {
            let column = member_output_column_in_query(
                subquery,
                withs,
                cube_name,
                member_name,
                meta_member,
                depth,
                budget,
            )?;
            Some((alias.name.clone(), column))
        }
        ast::TableFactor::Table { name, alias, .. } => {
            // The table may be a reference to a CTE
            let ast::ObjectName(parts) = name;
            let [part] = &parts[..] else {
                return None;
            };
            let table_ident = part.as_ident()?;
            // Unquoted identifiers are case-insensitive; the same
            // approximation is made for cube names when matching relations
            let cte = withs.iter().find_map(|with| {
                with.cte_tables.iter().find(|cte| {
                    cte.alias
                        .name
                        .value
                        .eq_ignore_ascii_case(&table_ident.value)
                })
            })?;
            if !cte.alias.columns.is_empty() {
                return None;
            }
            let column = member_output_column_in_query(
                &cte.query,
                withs,
                cube_name,
                member_name,
                meta_member,
                depth,
                budget,
            )?;
            let relation_alias = match alias {
                Some(alias) if alias.columns.is_empty() => alias.name.clone(),
                Some(_) => return None,
                None => table_ident.clone(),
            };
            Some((relation_alias, column))
        }
        _ => None,
    }
}

/// The output column of a query that exposes the member: a direct column or
/// wildcard of a cube in its FROM, an aliased aggregation for a measure, or a
/// column forwarded unchanged from a relation that exposes it.
fn member_output_column_in_query<'a>(
    query: &'a ast::Query,
    withs: &[&'a ast::With],
    cube_name: &str,
    member_name: &str,
    meta_member: &MetaMember,
    depth: usize,
    budget: &Cell<usize>,
) -> Option<ast::Ident> {
    if depth >= MAX_RELATION_DEPTH || !spend_expansion(budget) {
        return None;
    }

    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    // A query declares CTEs of its own on top of the ones already visible
    let withs = query
        .with
        .as_ref()
        .into_iter()
        .chain(withs.iter().copied())
        .collect::<Vec<_>>();

    // A bare column reference can't be attributed to a specific relation, so
    // it is only accepted when there is a single relation in FROM
    let allow_unqualified = relation_count(&select.from) == 1;

    // The cube itself in this query's FROM
    if let Some(cube_alias) =
        alias_for_relation_in_from(cube_name, &select.from, &cte_names(&withs))
    {
        for item in &select.projection {
            match item {
                ast::SelectItem::UnnamedExpr(expr) => {
                    if matches!(meta_member, MetaMember::Dimension(_))
                        && is_direct_member_ref(expr, &cube_alias, member_name, allow_unqualified)
                    {
                        // Output column name is the column name itself
                        return Some(ast::Ident::with_quote('"', member_name));
                    }
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    if is_member_expr(
                        expr,
                        &cube_alias,
                        member_name,
                        meta_member,
                        allow_unqualified,
                    ) {
                        return Some(alias.clone());
                    }
                }
                ast::SelectItem::Wildcard(_) => {
                    // `SELECT *` exposes raw cube columns: dimensions keep their name,
                    // measures stay unaggregated and cannot be filtered. Over a join a bare
                    // name may belong to any relation, so it is restricted as elsewhere.
                    if allow_unqualified && matches!(meta_member, MetaMember::Dimension(_)) {
                        return Some(ast::Ident::with_quote('"', member_name));
                    }
                }
                ast::SelectItem::QualifiedWildcard(
                    ast::SelectItemQualifiedWildcardKind::ObjectName(name),
                    _,
                ) => {
                    let ast::ObjectName(parts) = name;
                    if let [qualifier] = &parts[..] {
                        if qualifier
                            .as_ident()
                            .is_some_and(|i| i.value.eq_ignore_ascii_case(&cube_alias.value))
                            && matches!(meta_member, MetaMember::Dimension(_))
                        {
                            return Some(ast::Ident::with_quote('"', member_name));
                        }
                    }
                }
                _ => {}
            }
        }

        // The cube being in this FROM doesn't mean the projection reads the
        // member from it - another relation may expose it too, so the search
        // carries on below rather than ending here
    }

    // A relation of this query may expose it, in which case the projection
    // has to forward that column unchanged
    for table_with_joins in &select.from {
        let factors = iter::once(&table_with_joins.relation)
            .chain(table_with_joins.joins.iter().map(|join| &join.relation));
        for factor in factors {
            let Some((relation_alias, column)) = member_column_in_table_factor(
                factor,
                &withs,
                cube_name,
                member_name,
                meta_member,
                depth + 1,
                budget,
            ) else {
                continue;
            };
            if let Some(forwarded) =
                forwarded_output_column(select, &relation_alias, &column, allow_unqualified)
            {
                return Some(forwarded);
            }
        }
    }

    None
}

/// Finds the output column of a SELECT that forwards a column of one of its
/// relations unchanged, as opposed to computing something from it.
fn forwarded_output_column(
    select: &ast::Select,
    relation: &ast::Ident,
    column: &ast::Ident,
    allow_unqualified: bool,
) -> Option<ast::Ident> {
    let mut wildcard = None;
    for item in &select.projection {
        match item {
            ast::SelectItem::UnnamedExpr(expr) => {
                // The output name of an unaliased item is the column name, as
                // the reference wrote it
                if is_direct_member_ref(expr, relation, &column.value, allow_unqualified) {
                    return Some(match expr {
                        ast::Expr::CompoundIdentifier(idents) => idents.last()?.clone(),
                        _ => column.clone(),
                    });
                }
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                if is_direct_member_ref(expr, relation, &column.value, allow_unqualified) {
                    return Some(alias.clone());
                }
            }
            ast::SelectItem::Wildcard(_) => {
                if allow_unqualified {
                    wildcard = Some(column.clone());
                }
            }
            ast::SelectItem::QualifiedWildcard(
                ast::SelectItemQualifiedWildcardKind::ObjectName(name),
                _,
            ) => {
                let ast::ObjectName(parts) = name;
                if let [qualifier] = &parts[..] {
                    if qualifier
                        .as_ident()
                        .is_some_and(|i| i.value.eq_ignore_ascii_case(&relation.value))
                    {
                        wildcard = Some(column.clone());
                    }
                }
            }
            _ => {}
        }
    }

    wildcard
}

/// Whether the expression directly exposes the member: a direct column
/// reference for dimensions, an aggregation of the raw column for measures.
fn is_member_expr(
    expr: &ast::Expr,
    cube_alias: &ast::Ident,
    member_name: &str,
    meta_member: &MetaMember,
    allow_unqualified: bool,
) -> bool {
    match meta_member {
        MetaMember::Dimension(_) => {
            is_direct_member_ref(expr, cube_alias, member_name, allow_unqualified)
        }
        MetaMember::Measure(_) => match expr {
            ast::Expr::Function(func) => {
                let ast::ObjectName(name_parts) = &func.name;
                let Some(func_name) = name_parts.last().and_then(|part| part.as_ident()) else {
                    return false;
                };
                if !AGG_FUNCTIONS
                    .iter()
                    .any(|agg| func_name.value.eq_ignore_ascii_case(agg))
                {
                    return false;
                }
                let ast::FunctionArguments::List(arg_list) = &func.args else {
                    return false;
                };
                let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg))] =
                    &arg_list.args[..]
                else {
                    return false;
                };
                is_direct_member_ref(arg, cube_alias, member_name, allow_unqualified)
            }
            _ => false,
        },
    }
}

/// Whether the expression is a reference to the member column of the cube.
/// An unqualified reference can only be attributed to the cube when it is the
/// sole relation of the FROM clause, hence `allow_unqualified`.
fn is_direct_member_ref(
    expr: &ast::Expr,
    cube_alias: &ast::Ident,
    member_name: &str,
    allow_unqualified: bool,
) -> bool {
    match expr {
        ast::Expr::Identifier(ident) => {
            allow_unqualified && ident.value.eq_ignore_ascii_case(member_name)
        }
        ast::Expr::CompoundIdentifier(idents) => {
            let [qualifier, column] = &idents[..] else {
                return false;
            };
            // The relation itself was matched case-insensitively, so the
            // qualifier written on the column may differ in case from it
            qualifier.value.eq_ignore_ascii_case(&cube_alias.value)
                && column.value.eq_ignore_ascii_case(member_name)
        }
        _ => false,
    }
}

/// Number of relations in the FROM clause, joins included.
fn relation_count(from: &[ast::TableWithJoins]) -> usize {
    from.iter()
        .map(|table_with_joins| 1 + table_with_joins.joins.len())
        .sum()
}

fn alias_for_relation_in_from(
    relation_name: &str,
    from: &[ast::TableWithJoins],
    cte_names: &HashSet<String>,
) -> Option<ast::Ident> {
    for table_with_joins in from {
        if let Some(alias) =
            alias_for_relation_in_table_factor(relation_name, &table_with_joins.relation, cte_names)
        {
            return Some(alias);
        }
        for join in &table_with_joins.joins {
            if let Some(alias) =
                alias_for_relation_in_table_factor(relation_name, &join.relation, cte_names)
            {
                return Some(alias);
            }
        }
    }
    None
}

fn alias_for_relation_in_table_factor(
    relation_name: &str,
    table_factor: &ast::TableFactor,
    cte_names: &HashSet<String>,
) -> Option<ast::Ident> {
    match table_factor {
        ast::TableFactor::Table { name, alias, .. } => {
            let ast::ObjectName(parts) = name;
            let last_ident = parts.last()?.as_ident()?;
            let table_name = &last_ident.value;
            if !table_name.eq_ignore_ascii_case(relation_name) {
                return None;
            }
            // A relation named after a cube may in fact be a CTE reference,
            // whose output columns have nothing to do with the cube's ones
            if parts.len() == 1 && cte_names.contains(&table_name.to_ascii_lowercase()) {
                return None;
            }
            let Some(alias) = alias else {
                return Some(last_ident.clone());
            };
            if !alias.columns.is_empty() {
                return None;
            }
            Some(alias.name.clone())
        }
        _ => None,
    }
}
