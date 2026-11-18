//! Tracing a member to a relation of the outermost FROM: the cube itself,
//! or a derived table or CTE exposing it as an output column.

use crate::transport::MetaContext;
use cubeclient::models::{V1CubeMetaDimension, V1CubeMetaMeasure};
use datafusion::error::{DataFusionError, Result as DFResult};
use sqlparser::ast;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    iter,
};

/// Aggregations that stand for any measure, whatever its type.
const MEASURE_FUNCTIONS: [&str; 2] = ["MEASURE", "AGGREGATE"];

/// The SQL aggregate a measure of this type is also read from, besides
/// `MEASURE`; `None` for a type with no such aggregate. Distinct types come
/// camelCase from the meta API and snake_case elsewhere in the transport.
fn measure_sql_aggregate(measure: &V1CubeMetaMeasure) -> Option<&'static str> {
    match measure.agg_type.as_deref() {
        Some(
            "count"
            | "count_distinct"
            | "countDistinct"
            | "count_distinct_approx"
            | "countDistinctApprox",
        ) => Some("COUNT"),
        Some("sum") => Some("SUM"),
        Some("avg") => Some("AVG"),
        Some("min") => Some("MIN"),
        Some("max") => Some("MAX"),
        _ => None,
    }
}

/// Whether the measure's own aggregate takes `DISTINCT`: always on a
/// countDistinct, never on a count, as the planner reads either the other way
/// as another measure or refuses it; `None` where both read as the measure.
fn measure_sql_distinct(measure: &V1CubeMetaMeasure) -> Option<bool> {
    match measure.agg_type.as_deref() {
        Some(
            "count_distinct" | "countDistinct" | "count_distinct_approx" | "countDistinctApprox",
        ) => Some(true),
        Some("count") => Some(false),
        _ => None,
    }
}

/// Whether the function stands for any measure, whatever its type.
fn stands_for_any_measure(name: &str) -> bool {
    MEASURE_FUNCTIONS
        .iter()
        .any(|function| name.eq_ignore_ascii_case(function))
}

/// Whether the function stands for the measure: `MEASURE`, or its own aggregate.
fn is_measure_function(name: &str, measure: &V1CubeMetaMeasure) -> bool {
    stands_for_any_measure(name)
        || measure_sql_aggregate(measure)
            .is_some_and(|aggregate| name.eq_ignore_ascii_case(aggregate))
}

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

    /// How a filter value on the member is written, per its type.
    pub(super) fn value_kind(&self) -> ValueKind {
        match self {
            MetaMember::Dimension(dimension) => match dimension.r#type.as_str() {
                "number" => ValueKind::Numeric,
                "boolean" => ValueKind::Boolean,
                _ => ValueKind::String,
            },
            MetaMember::Measure(measure) => match measure.r#type.as_str() {
                "string" | "time" => ValueKind::String,
                "boolean" => ValueKind::Boolean,
                _ => ValueKind::Numeric,
            },
        }
    }

    pub(super) fn is_time_dimension(&self) -> bool {
        matches!(self, MetaMember::Dimension(dimension) if dimension.r#type == "time")
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

/// How a filter value is written, per the type of the member it filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ValueKind {
    Numeric,
    Boolean,
    String,
}

/// Where the member column comes from in the outermost SELECT.
#[derive(Debug, Clone)]
pub(super) enum MemberSource {
    /// The cube is referenced directly in the outermost FROM: dimensions are
    /// filtered in WHERE, measures are aggregated and filtered in HAVING.
    CubeTable { relation_alias: ast::Ident },
    /// An output column of a derived table or CTE in the outermost FROM,
    /// filtered in WHERE; carried as written, as quoting decides folding.
    DerivedColumn {
        relation_alias: ast::Ident,
        column: ast::Ident,
    },
}

/// How many relations deep a chain of derived tables and CTE references is
/// followed when resolving a member; past it the member is unavailable.
const MAX_RELATION_DEPTH: usize = 25;

/// A member more than one relation in the outermost FROM exposes, which
/// leaves no side to filter.
#[derive(Debug, Clone)]
pub(super) struct Ambiguous(String);

impl From<Ambiguous> for DataFusionError {
    fn from(Ambiguous(message): Ambiguous) -> Self {
        DataFusionError::Plan(message)
    }
}

/// Where each member of a request resolves, by cube and member name. Every
/// filter on a member resolves the same, as the actions edit the outermost
/// WHERE and HAVING only, and a resolution can follow many relations.
#[derive(Default)]
pub(super) struct MemberSources(
    RefCell<HashMap<(String, String), Result<Option<MemberSource>, Ambiguous>>>,
);

impl MemberSources {
    /// See [`resolve_member_source`].
    pub(super) fn resolve(
        &self,
        select: &ast::Select,
        with: Option<&ast::With>,
        cube_name: &str,
        member_name: &str,
        meta_member: &MetaMember,
    ) -> Result<Option<MemberSource>, Ambiguous> {
        let key = (cube_name.to_string(), member_name.to_string());
        if let Some(source) = self.0.borrow().get(&key) {
            return source.clone();
        }
        let source = resolve_member_source(select, with, cube_name, member_name, meta_member);
        self.0.borrow_mut().insert(key, source.clone());
        source
    }
}

/// Resolves the member to a column available in the outermost SELECT.
/// Returns `None` if the member is not available there, and an error if more
/// than one relation there exposes it, as in a self-join, with no side to pick.
fn resolve_member_source(
    select: &ast::Select,
    with: Option<&ast::With>,
    cube_name: &str,
    member_name: &str,
    meta_member: &MetaMember,
) -> Result<Option<MemberSource>, Ambiguous> {
    let scopes = with
        .map(|with| &with.cte_tables[..])
        .into_iter()
        .collect::<Vec<_>>();
    let aliases = select
        .from
        .iter()
        .flat_map(|table_with_joins| {
            iter::once(&table_with_joins.relation)
                .chain(table_with_joins.joins.iter().map(|join| &join.relation))
        })
        .filter_map(|factor| alias_for_relation_in_table_factor(cube_name, factor, &scopes))
        .collect::<Vec<_>>();
    match &aliases[..] {
        [relation_alias] => {
            return Ok(Some(MemberSource::CubeTable {
                relation_alias: relation_alias.clone(),
            }));
        }
        [_, _, ..] => {
            return Err(Ambiguous(format!(
                "Member \"{}.{}\" is ambiguous: \"{}\" appears more than once in the outermost FROM",
                cube_name, member_name, cube_name
            )));
        }
        [] => {}
    }

    let budget = Expansions::new();
    let mut sources = select.from.iter().flat_map(|table_with_joins| {
        iter::once(&table_with_joins.relation)
            .chain(table_with_joins.joins.iter().map(|join| &join.relation))
            .filter_map(|factor| {
                member_column_in_table_factor(
                    factor,
                    &scopes,
                    cube_name,
                    member_name,
                    meta_member,
                    0,
                    &budget,
                )
            })
    });
    let first = sources.next();
    // Two derived relations exposing the member leave no side to filter either
    let second = first.as_ref().and_then(|_| sources.next());
    if second.is_some() {
        return Err(Ambiguous(format!(
            "Member \"{}.{}\" is ambiguous: more than one relation in the outermost FROM exposes it",
            cube_name, member_name
        )));
    }
    let Some((relation_alias, column)) = first else {
        return Ok(None);
    };
    Ok(Some(MemberSource::DerivedColumn {
        relation_alias,
        column,
    }))
}

/// Whether a CTE of this name is visible at a point. A relation with such a
/// name refers to the CTE, not to a cube of the same name.
fn is_visible_cte(scopes: &[&[ast::Cte]], name: &ast::Ident) -> bool {
    scopes
        .iter()
        .flat_map(|scope| scope.iter())
        .any(|cte| cte.alias.name.value.eq_ignore_ascii_case(&name.value))
}

/// How many relations are followed in total while resolving a member. Depth
/// alone doesn't bound the work: a query whose relations each reference
/// several others branches out.
const MAX_RELATION_EXPANSIONS: usize = 1000;

/// The relations left to follow while resolving a member.
struct Expansions(Cell<usize>);

impl Expansions {
    fn new() -> Self {
        Self(Cell::new(MAX_RELATION_EXPANSIONS))
    }

    /// Consumes one relation expansion at this depth, returning whether the
    /// relation may be followed.
    fn spend(&self, depth: usize) -> bool {
        let left = self.0.get();
        if depth >= MAX_RELATION_DEPTH || left == 0 {
            return false;
        }
        self.0.set(left - 1);
        true
    }
}

/// If the table factor is a derived table or a CTE reference which exposes the
/// member as an output column, returns the relation alias to qualify the
/// column with and the output column name.
fn member_column_in_table_factor<'a>(
    factor: &'a ast::TableFactor,
    scopes: &[&'a [ast::Cte]],
    cube_name: &str,
    member_name: &str,
    meta_member: &MetaMember,
    depth: usize,
    budget: &Expansions,
) -> Option<(ast::Ident, ast::Ident)> {
    match factor {
        ast::TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } if alias.columns.is_empty() => {
            let column = member_output_column_in_query(
                subquery,
                scopes,
                cube_name,
                member_name,
                meta_member,
                depth,
                budget,
            )?;
            Some((alias.name.clone(), column))
        }
        ast::TableFactor::Table { name, alias, .. } => {
            let ast::ObjectName(parts) = name;
            let [part] = &parts[..] else {
                return None;
            };
            let table_ident = part.as_ident()?;
            // Unquoted identifiers are case-insensitive; the same
            // approximation is made for cube names when matching relations
            let (scope, index) = scopes.iter().enumerate().find_map(|(scope, ctes)| {
                ctes.iter()
                    .position(|cte| {
                        cte.alias
                            .name
                            .value
                            .eq_ignore_ascii_case(&table_ident.value)
                    })
                    .map(|index| (scope, index))
            })?;
            let cte = &scopes[scope][index];
            if !cte.alias.columns.is_empty() {
                return None;
            }
            // A CTE's body sees the ones declared before it, not itself: a
            // name it shares with a cube is the cube there
            let visible = iter::once(&scopes[scope][..index])
                .chain(scopes[scope + 1..].iter().copied())
                .collect::<Vec<_>>();
            let column = member_output_column_in_query(
                &cte.query,
                &visible,
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
    scopes: &[&'a [ast::Cte]],
    cube_name: &str,
    member_name: &str,
    meta_member: &MetaMember,
    depth: usize,
    budget: &Expansions,
) -> Option<ast::Ident> {
    if !budget.spend(depth) {
        return None;
    }

    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    // A query declares CTEs of its own on top of the ones already visible
    let scopes = query
        .with
        .as_ref()
        .map(|with| &with.cte_tables[..])
        .into_iter()
        .chain(scopes.iter().copied())
        .collect::<Vec<_>>();

    // A bare column reference can't be attributed to a specific relation, so
    // it is only accepted when there is a single relation in FROM
    let allow_unqualified = relation_count(&select.from) == 1;

    if let Some(cube_alias) = alias_for_relation_in_from(cube_name, &select.from, &scopes) {
        for item in &select.projection {
            match item {
                ast::SelectItem::UnnamedExpr(expr) => {
                    if matches!(meta_member, MetaMember::Dimension(_))
                        && is_direct_member_ref(expr, &cube_alias, member_name, allow_unqualified)
                    {
                        return Some(ast::Ident::with_quote('"', meta_member.short_name()));
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
                        return Some(ast::Ident::with_quote('"', meta_member.short_name()));
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
                            return Some(ast::Ident::with_quote('"', meta_member.short_name()));
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
                &scopes,
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
                        ast::Expr::Identifier(ident) => ident.clone(),
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
        MetaMember::Measure(measure) => match expr {
            ast::Expr::Function(func) => {
                let ast::ObjectName(name_parts) = &func.name;
                let Some(func_name) = name_parts.last().and_then(|part| part.as_ident()) else {
                    return false;
                };
                // A windowed aggregate is computed over other rows than the measure
                if func.over.is_some() || !is_measure_function(&func_name.value, measure) {
                    return false;
                }
                let ast::FunctionArguments::List(arg_list) = &func.args else {
                    return false;
                };
                let distinct = matches!(
                    arg_list.duplicate_treatment,
                    Some(ast::DuplicateTreatment::Distinct)
                );
                if !stands_for_any_measure(&func_name.value)
                    && measure_sql_distinct(measure).is_some_and(|takes| takes != distinct)
                {
                    return false;
                }
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
    scopes: &[&[ast::Cte]],
) -> Option<ast::Ident> {
    for table_with_joins in from {
        if let Some(alias) =
            alias_for_relation_in_table_factor(relation_name, &table_with_joins.relation, scopes)
        {
            return Some(alias);
        }
        for join in &table_with_joins.joins {
            if let Some(alias) =
                alias_for_relation_in_table_factor(relation_name, &join.relation, scopes)
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
    scopes: &[&[ast::Cte]],
) -> Option<ast::Ident> {
    // Only a relation named after the cube is looked up among the CTEs, as
    // there can be thousands of them and this runs once per relation followed
    let is_cte = |table: &ast::Ident| {
        table.value.eq_ignore_ascii_case(relation_name) && is_visible_cte(scopes, table)
    };
    let (table, alias) = table_relation(table_factor, is_cte)?;
    table
        .value
        .eq_ignore_ascii_case(relation_name)
        .then(|| alias.clone())
}

/// The table a factor reads, possibly schema-qualified, and the name it goes
/// by in the query; `None` for anything but a table, or a CTE reference.
fn table_relation<'a>(
    table_factor: &'a ast::TableFactor,
    is_cte: impl Fn(&ast::Ident) -> bool,
) -> Option<(&'a ast::Ident, &'a ast::Ident)> {
    let ast::TableFactor::Table { name, alias, .. } = table_factor else {
        return None;
    };
    let ast::ObjectName(parts) = name;
    let table = parts.last()?.as_ident()?;
    // A relation named after a cube may in fact be a CTE reference, whose
    // output columns have nothing to do with the cube's ones
    if parts.len() == 1 && is_cte(table) {
        return None;
    }
    match alias {
        None => Some((table, table)),
        Some(alias) if alias.columns.is_empty() => Some((table, &alias.name)),
        Some(_) => None,
    }
}
