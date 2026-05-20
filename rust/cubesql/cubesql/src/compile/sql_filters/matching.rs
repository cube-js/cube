//! Keying the predicates of a WHERE or HAVING so a filter can be found by
//! its shape, and the splits and edits of a clause that rest on it.

use super::{
    report::normalize_filter_value,
    resolve::{measure_sql_aggregate, stands_for_any_measure, table_relation},
    FilterKey,
};
use crate::{
    compile::rewrite::rules::members::MemberRules,
    transport::{MetaContext, V1CubeMetaMeasureExt},
};
use datafusion::error::{DataFusionError, Result as DFResult};
use sqlparser::ast::{self, VisitMut, VisitorMut};
use std::{
    collections::{HashMap, HashSet},
    iter::{self, Peekable},
    ops::ControlFlow,
    str::Chars,
};

/// How deep a single expression is unwrapped: a group's parentheses, and the
/// truncations or extractions around the column a predicate filters.
const MAX_EXPR_NESTING: usize = 25;

/// The functions the engine evaluates when it plans, which a filter can be
/// written against: `CURRENT_DATE - INTERVAL '7 days'` is reported as the
/// date it came out to, so the SQL and the filter read back differ.
const PLANNING_TIME_FUNCTIONS: [&str; 7] = [
    "current_date",
    "current_timestamp",
    "localtimestamp",
    "now",
    "date_trunc",
    "to_date",
    "to_timestamp",
];

/// Whether the expression is a date the engine computes when it plans: only
/// literals, intervals and the functions above, no column reference anywhere.
/// Walked iteratively, with a node budget.
fn is_planning_time_date(expr: &ast::Expr) -> bool {
    let mut is_date = false;
    let mut pending = vec![expr];
    let mut visited = 0;

    while let Some(expr) = pending.pop() {
        visited += 1;
        if visited > MAX_EXPR_NODES {
            return false;
        }

        match expr {
            ast::Expr::Value(_) => {}
            ast::Expr::TypedString { .. } => is_date = true,
            ast::Expr::Interval(interval) => {
                is_date = true;
                pending.push(&interval.value);
            }
            ast::Expr::Nested(inner) | ast::Expr::Cast { expr: inner, .. } => pending.push(inner),
            ast::Expr::UnaryOp { op, expr: inner } => {
                if !matches!(op, ast::UnaryOperator::Minus | ast::UnaryOperator::Plus) {
                    return false;
                }
                pending.push(inner);
            }
            ast::Expr::BinaryOp { left, op, right } => {
                if !matches!(
                    op,
                    ast::BinaryOperator::Plus
                        | ast::BinaryOperator::Minus
                        | ast::BinaryOperator::Multiply
                        | ast::BinaryOperator::Divide
                ) {
                    return false;
                }
                pending.push(left);
                pending.push(right);
            }
            ast::Expr::Function(func) => {
                let ast::ObjectName(name_parts) = &func.name;
                let Some(name) = name_parts.last().and_then(|part| part.as_ident()) else {
                    return false;
                };
                if !PLANNING_TIME_FUNCTIONS
                    .iter()
                    .any(|known| name.value.eq_ignore_ascii_case(known))
                {
                    return false;
                }
                is_date = true;
                match &func.args {
                    ast::FunctionArguments::None => {}
                    ast::FunctionArguments::List(arg_list) => {
                        for arg in &arg_list.args {
                            let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg)) = arg
                            else {
                                return false;
                            };
                            pending.push(arg);
                        }
                    }
                    _ => return false,
                }
            }
            _ => return false,
        }
    }

    is_date
}

/// The column a `DATE_TRUNC(unit, column)` truncates. The engine turns a
/// comparison against one into a range over the column itself, so it is that
/// column the predicate filters.
fn date_trunc_arg(func: &ast::Function) -> Option<&ast::Expr> {
    let ast::ObjectName(name_parts) = &func.name;
    let name = name_parts.last().and_then(|part| part.as_ident())?;
    if !name.value.eq_ignore_ascii_case("date_trunc") {
        return None;
    }
    let ast::FunctionArguments::List(arg_list) = &func.args else {
        return None;
    };
    let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(unit)), ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(column))] =
        &arg_list.args[..]
    else {
        return None;
    };
    is_literal(unit).then_some(column)
}

/// The column side of a predicate with the truncation or the extraction, if
/// any, peeled off. The engine turns a comparison against either into a range
/// over the column itself, so it is that column the predicate filters.
fn filter_column_expr(expr: &ast::Expr) -> &ast::Expr {
    let mut expr = expr;
    // A truncation of a truncation is still a filter on the innermost column,
    // and the loop keeps a nested one from costing a frame
    for _ in 0..MAX_EXPR_NESTING {
        match expr {
            ast::Expr::Function(func) => match date_trunc_arg(func) {
                Some(arg) => expr = arg,
                None => return expr,
            },
            ast::Expr::Extract { expr: inner, .. } => expr = inner,
            expr => return expr,
        }
    }
    expr
}

/// The value side of a filter predicate: a literal, or a date the engine
/// computes when it plans.
fn is_filter_value(expr: &ast::Expr) -> bool {
    is_literal(expr) || is_planning_time_date(expr)
}

fn is_literal(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Value(_) => true,
        // Negative numbers are parsed as a unary operation over a literal
        ast::Expr::UnaryOp { op, expr } => {
            matches!(op, ast::UnaryOperator::Minus | ast::UnaryOperator::Plus)
                && matches!(expr.as_ref(), ast::Expr::Value(_))
        }
        // `'2024-01-01'::date` and `CAST('2024-01-01' AS DATE)`, as BI tools
        // spell a typed value
        ast::Expr::Cast { expr, .. } => is_literal(expr),
        _ => false,
    }
}

/// Which clause of the outermost SELECT the filter belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum ClauseKind {
    Where,
    Having,
}

pub(super) fn clause_mut(select: &mut ast::Select, kind: ClauseKind) -> &mut Option<ast::Expr> {
    match kind {
        ClauseKind::Where => &mut select.selection,
        ClauseKind::Having => &mut select.having,
    }
}

/// Bounds the walks over a clause of the outermost SELECT. An order of magnitude
/// above what [`super::MAX_FILTERS`] additions can build, so this API never refuses a
/// query it produced.
pub(super) const MAX_CLAUSE_PREDICATES: usize = 10_000;

/// Upper bound on the nodes one expression walk visits: a group needle holds
/// up to [`super::MAX_FILTERS`] leaves, so an expression can be as large as a clause
/// and shares its bound, named apart so such a walk reads as one.
const MAX_EXPR_NODES: usize = MAX_CLAUSE_PREDICATES;

/// Counts the predicates of a clause: its leaves under AND, OR and
/// parentheses. Iterative, as a clause may be as long as the bound allows.
fn clause_predicate_count(expr: &ast::Expr) -> usize {
    let mut count = 0;
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::And | ast::BinaryOperator::Or,
                right,
            } => {
                pending.push(left);
                pending.push(right);
            }
            ast::Expr::Nested(inner) => pending.push(inner),
            _ => count += 1,
        }
    }
    count
}

/// The predicate counts of the outermost clauses across a batch, recounted
/// after an edit that can shrink one. A clause over the bound as parsed is
/// refused outright: every walk over it is what the bound limits.
pub(super) struct ClauseBudget {
    counts: HashMap<ClauseKind, usize>,
}

impl ClauseBudget {
    /// Counts both clauses, refusing a SELECT already over the bound.
    pub(super) fn of(select: &ast::Select) -> DFResult<Self> {
        let mut budget = Self {
            counts: HashMap::new(),
        };
        for (kind, clause) in [
            (ClauseKind::Where, select.selection.as_ref()),
            (ClauseKind::Having, select.having.as_ref()),
        ] {
            if let Some(clause) = clause {
                budget.counts.insert(kind, clause_predicate_count(clause));
                budget.assert_bounded(kind)?;
            }
        }
        Ok(budget)
    }

    /// Accounts for an expression about to be appended to the clause.
    pub(super) fn charge(&mut self, kind: ClauseKind, expr: &ast::Expr) -> DFResult<()> {
        *self.counts.entry(kind).or_insert(0) += clause_predicate_count(expr);
        self.assert_bounded(kind)
    }

    /// Takes the clauses at the size an edit left them, so that one which
    /// shrank a clause is not held to the size it had.
    pub(super) fn recount(&mut self, select: &ast::Select) -> DFResult<()> {
        for (kind, clause) in [
            (ClauseKind::Where, select.selection.as_ref()),
            (ClauseKind::Having, select.having.as_ref()),
        ] {
            self.counts
                .insert(kind, clause.map_or(0, clause_predicate_count));
            self.assert_bounded(kind)?;
        }
        Ok(())
    }

    fn assert_bounded(&self, kind: ClauseKind) -> DFResult<()> {
        let count = self.counts.get(&kind).copied().unwrap_or(0);
        if count > MAX_CLAUSE_PREDICATES {
            return Err(DataFusionError::Plan(format!(
                "A clause of the outermost SELECT has {} predicates, more than the {} supported",
                count, MAX_CLAUSE_PREDICATES
            )));
        }
        Ok(())
    }
}

/// Whether the expression is an AND chain, and so a grouping the conjunct
/// split may look through.
fn is_and_chain(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::BinaryOp {
            op: ast::BinaryOperator::And,
            ..
        }
    )
}

/// Splits a clause into the conjuncts of its top-level AND chain,
/// iteratively, as a clause holds a conjunct per filter. Parentheses around
/// the whole clause are kept, since a filter group is what they may be.
fn into_and_conjuncts_exact(expr: ast::Expr) -> Vec<ast::Expr> {
    into_and_conjuncts_with(expr, false)
}

/// The consuming split, with parenthesized AND chains looked through or not.
/// [`and_conjuncts_with`] is its borrowing twin and walks in the same order,
/// which [`with_keys`] relies on.
pub(super) fn into_and_conjuncts_with(expr: ast::Expr, transparent: bool) -> Vec<ast::Expr> {
    let mut conjuncts = Vec::new();
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::And,
                right,
            } => {
                pending.push(*right);
                pending.push(*left);
            }
            ast::Expr::Nested(inner) if transparent && is_and_chain(&inner) => pending.push(*inner),
            expr => conjuncts.push(expr),
        }
    }
    conjuncts
}

/// As [`into_and_conjuncts_exact`], with parenthesized AND chains looked
/// through: the rewrite engine flattens a top-level `and` group into sibling
/// filters, so its members have to be reachable as filters of their own.
fn into_and_conjuncts(expr: ast::Expr) -> Vec<ast::Expr> {
    into_and_conjuncts_with(expr, true)
}

/// The borrowing counterpart of [`into_and_conjuncts_exact`].
fn and_conjuncts_exact(expr: &ast::Expr) -> Vec<&ast::Expr> {
    and_conjuncts_with(expr, false)
}

/// The borrowing split; see [`into_and_conjuncts_with`].
pub(super) fn and_conjuncts_with(expr: &ast::Expr, transparent: bool) -> Vec<&ast::Expr> {
    let mut conjuncts = Vec::new();
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::And,
                right,
            } => {
                pending.push(right);
                pending.push(left);
            }
            ast::Expr::Nested(inner) if transparent && is_and_chain(inner) => pending.push(inner),
            expr => conjuncts.push(expr),
        }
    }
    conjuncts
}

/// The borrowing counterpart of [`into_and_conjuncts`].
fn and_conjuncts(expr: &ast::Expr) -> Vec<&ast::Expr> {
    and_conjuncts_with(expr, true)
}

/// Joins conjuncts back into a left-deep AND chain.
pub(super) fn and_chain(conjuncts: Vec<ast::Expr>) -> Option<ast::Expr> {
    conjuncts
        .into_iter()
        .reduce(|left, right| ast::Expr::BinaryOp {
            left: Box::new(left),
            op: ast::BinaryOperator::And,
            right: Box::new(right),
        })
}

/// Whether the clause, keyed as `keys` by [`clause_key_set`], already holds
/// the expression: as itself, or as a group each of whose members it holds.
fn clause_holds(keys: &HashSet<FilterKey>, expr: &ast::Expr, ctx: MatchContext) -> bool {
    if keys.contains(&ctx.key(expr)) {
        return true;
    }
    let members = group_members(expr);
    !members.is_empty() && members.iter().all(|member| keys.contains(&ctx.key(member)))
}

/// The keys an appended expression adds to a clause's key set: itself under
/// both splits, and its members under the looking-through one.
fn appended_keys(expr: &ast::Expr, ctx: MatchContext) -> Vec<FilterKey> {
    iter::once(ctx.key(expr))
        .chain(group_members(expr).into_iter().map(|m| ctx.key(m)))
        .collect()
}

/// Appends the expression to the clause with AND. Whether it is already there
/// is decided by the caller against the clause's keys.
pub(super) fn append_expr_to_clause(option_clause: &mut Option<ast::Expr>, expr: ast::Expr) {
    // The clause is taken rather than borrowed: it grows by a predicate per
    // addition, and cloning it each time would make a batch quadratic
    let Some(clause) = option_clause.take() else {
        *option_clause = Some(expr);
        return;
    };
    // The existing clause may be a top-level OR chain, and sqlparser rendering
    // is not precedence-aware: parentheses only survive as `Expr::Nested`.
    // Operators binding looser than AND have to be parenthesized before AND-ing.
    let existing = match &clause {
        ast::Expr::BinaryOp {
            op: ast::BinaryOperator::Or | ast::BinaryOperator::Xor,
            ..
        } => ast::Expr::Nested(Box::new(clause)),
        _ => clause,
    };
    *option_clause = Some(ast::Expr::BinaryOp {
        left: Box::new(existing),
        op: ast::BinaryOperator::And,
        right: Box::new(expr),
    });
}

/// How a filter this API writes is matched against the predicates a query
/// wrote itself.
#[derive(Debug, Clone, Copy)]
pub(super) struct MatchContext<'a> {
    /// The plan reports the filter and nothing else on its member, so it may
    /// be matched by its column alone.
    pub(super) sole_reported: bool,
    /// The outermost FROM holds one relation, so a bare column is the same
    /// column as a qualified one.
    pub(super) ignore_qualifier: bool,
    /// The filter stands on time members alone, whose dates the planner
    /// canonicalizes; on any other member `'2024-01-01'` and
    /// `'2024-01-01T00:00:00.000Z'` are two different strings.
    pub(super) normalize_dates: bool,
    pub(super) aggregates: &'a MeasureAggregates,
}

impl MatchContext<'_> {
    pub(super) fn key(&self, expr: &ast::Expr) -> FilterKey {
        expr_key(
            expr,
            self.ignore_qualifier,
            self.normalize_dates,
            self.aggregates,
        )
    }
}

/// The aggregates the measures of the outermost FROM's cubes are written with,
/// by relation: an aggregate keys as a measure only for one of its own, so
/// `AVG(x)` is not a sum `x`, whichever other cube has one of that name.
#[derive(Debug, Default)]
pub(super) struct MeasureAggregates {
    by_relation: HashMap<String, HashMap<String, &'static str>>,
    /// Whether the outermost FROM holds a single cube with a `count` measure
    /// the planner reads `COUNT(*)` as; under a loose type match it may read
    /// it as more, which this misses rather than mistakes.
    counts_rows: bool,
}

impl MeasureAggregates {
    pub(super) fn of(ctx: &MetaContext, query: &ast::Query) -> Self {
        let mut by_relation = HashMap::new();
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            return Self::default();
        };
        let mut counted = Vec::new();
        let ctes = query
            .with
            .iter()
            .flat_map(|with| with.cte_tables.iter())
            .map(|cte| cte.alias.name.value.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        let factors = select.from.iter().flat_map(|table_with_joins| {
            iter::once(&table_with_joins.relation)
                .chain(table_with_joins.joins.iter().map(|join| &join.relation))
        });
        for factor in factors {
            let Some((table, relation)) = table_relation(factor, &ctes) else {
                continue;
            };
            let Some(cube) = ctx
                .cubes
                .iter()
                .find(|cube| cube.name.eq_ignore_ascii_case(&table.value))
            else {
                continue;
            };
            let count = MemberRules::default_count_measure_name();
            counted.push(cube.measures.iter().any(|measure| {
                let name = measure.name.rsplit('.').next().unwrap_or(&measure.name);
                name.eq_ignore_ascii_case(&count) && measure.is_same_agg_type("count", false)
            }));
            let aggregates = cube
                .measures
                .iter()
                .filter_map(|measure| {
                    let aggregate = measure_sql_aggregate(measure)?;
                    let name = measure.name.rsplit('.').next().unwrap_or(&measure.name);
                    Some((name.to_ascii_lowercase(), aggregate))
                })
                .collect();
            by_relation.insert(relation.value.to_ascii_lowercase(), aggregates);
        }
        Self {
            by_relation,
            counts_rows: counted == [true],
        }
    }

    fn stands_for_measure(&self, function: &str, column: Option<(Option<String>, String)>) -> bool {
        if stands_for_any_measure(function) {
            return true;
        }
        let Some((relation, name)) = column else {
            return false;
        };
        // A bare column can only be told apart when one relation is there
        let aggregates = match relation {
            Some(relation) => self.by_relation.get(&relation),
            None if self.by_relation.len() == 1 => self.by_relation.values().next(),
            None => None,
        };
        aggregates
            .and_then(|aggregates| aggregates.get(&name))
            .is_some_and(|aggregate| function.eq_ignore_ascii_case(aggregate))
    }
}

/// The expression with each `COUNT(*)` and `COUNT(1)` spelled as the
/// `MEASURE(count)` the plan reads it as; `None` when it holds none.
fn count_rows_as_measure(expr: &ast::Expr) -> Option<ast::Expr> {
    struct CountRows(bool);
    impl VisitorMut for CountRows {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<()> {
            let ast::Expr::Function(function) = expr else {
                return ControlFlow::Continue(());
            };
            let ast::FunctionArguments::List(list) = &mut function.args else {
                return ControlFlow::Continue(());
            };
            // A windowed or filtered count is no row count of the query
            let counts_rows = function.name.to_string().eq_ignore_ascii_case("count")
                && function.over.is_none()
                && function.filter.is_none()
                && list.duplicate_treatment.is_none()
                && matches!(
                    &list.args[..],
                    [ast::FunctionArg::Unnamed(
                        ast::FunctionArgExpr::Wildcard
                            | ast::FunctionArgExpr::Expr(ast::Expr::Value(_))
                    )]
                );
            if counts_rows {
                function.name = ast::ObjectName::from(vec![ast::Ident::new("MEASURE")]);
                list.args = vec![ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                    ast::Expr::Identifier(ast::Ident::new(
                        MemberRules::default_count_measure_name(),
                    )),
                ))];
                self.0 = true;
            }
            ControlFlow::Continue(())
        }
    }

    let mut counted = expr.clone();
    let mut visitor = CountRows(false);
    let _ = counted.visit(&mut visitor);
    visitor.0.then_some(counted)
}

/// The column an aggregate call is over, read ahead of its `(`: its relation
/// and name, unquoted and in lower case; `None` for anything but a column.
fn call_column(chars: &Peekable<Chars>) -> Option<(Option<String>, String)> {
    let mut argument = String::new();
    for c in chars.clone() {
        match c {
            ')' => break,
            '(' => return None,
            c => argument.push(c),
        }
    }
    let argument = argument.trim();
    let argument = argument.strip_prefix("DISTINCT ").unwrap_or(argument);
    let unquoted = |part: &str| {
        let part = part.trim();
        let part = part
            .strip_prefix('"')
            .and_then(|part| part.strip_suffix('"'))
            .unwrap_or(part);
        let bare = !part.is_empty() && part.chars().all(|c| c.is_alphanumeric() || c == '_');
        bare.then(|| part.to_ascii_lowercase())
    };
    match argument.rsplit_once('.') {
        Some((relation, name)) => Some((Some(unquoted(relation)?), unquoted(name)?)),
        None => Some((None, unquoted(argument)?)),
    }
}

/// A comparison key for a filter expression: its SQL rendering with the
/// identifier quoting reduced, as this API quotes what a query may write bare.
/// `drop_qualifiers` drops relation names; `normalize_dates` reduces dates.
fn expr_key(
    expr: &ast::Expr,
    drop_qualifiers: bool,
    normalize_dates: bool,
    aggregates: &MeasureAggregates,
) -> String {
    // A group this API writes carries its parentheses, while a query holding
    // one predicate writes the same group without them
    let mut expr = expr;
    for _ in 0..MAX_EXPR_NESTING {
        let ast::Expr::Nested(inner) = expr else {
            break;
        };
        expr = inner;
    }

    // On a time dimension the plan reads `x IN ('d')` and `x = 'd'` apart
    let canonical = canonical_predicate(expr, !normalize_dates);
    let expr = canonical.as_ref().unwrap_or(expr);
    let counted = aggregates
        .counts_rows
        .then(|| count_rows_as_measure(expr))
        .flatten();
    let expr = counted.as_ref().unwrap_or(expr);

    let rendered = expr.to_string();
    let mut key = String::with_capacity(rendered.len());
    let mut word = String::new();
    let mut chars = rendered.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                // `E'...'` escapes with backslashes; keyed as the plain
                // literal it stands for
                let escaped = word.eq_ignore_ascii_case("e");
                if escaped {
                    word.clear();
                }
                push_word(&mut key, &mut word);
                let literal = if escaped {
                    read_escaped(&mut chars)
                } else {
                    read_quoted(&mut chars, '\'')
                };
                let literal = if normalize_dates {
                    normalize_filter_value(&literal)
                } else {
                    literal
                };
                key.push('\'');
                key.push_str(&literal.replace('\'', "''"));
                key.push('\'');
            }
            '"' => {
                push_word(&mut key, &mut word);
                let ident = read_quoted(&mut chars, '"');
                if drop_qualifiers && chars.peek() == Some(&'.') {
                    chars.next();
                    continue;
                }
                // cubesql resolves an identifier case-insensitively however
                // it is quoted, so the key spells it lower case, and bare
                // when it can be written bare
                let ident = ident.to_ascii_lowercase();
                if is_bare_identifier(&ident) {
                    key.push_str(&ident);
                } else {
                    key.push('"');
                    key.push_str(&ident.replace('"', "\"\""));
                    key.push('"');
                }
            }
            c if c.is_ascii_alphanumeric() || c == '_' => word.push(c),
            c => {
                let is_qualifier = c == '.' && starts_identifier(&word);
                if drop_qualifiers && is_qualifier {
                    word.clear();
                    continue;
                }
                // A Cube filter carries no aggregation, but only the measure's
                // own stands for it: `AVG(x) > 1` is no filter on a sum `x`
                if c == '(' && aggregates.stands_for_measure(&word, call_column(&chars)) {
                    word = "measure".to_string();
                }
                push_word(&mut key, &mut word);
                key.push(c);
            }
        }
    }
    push_word(&mut key, &mut word);

    key
}

/// The predicate spelled as the plan reads it, or `None` when it already is,
/// through every `AND`, `OR` and `NOT`: `x IN ('v')` as `x = 'v'` when
/// `single_in` holds, a bare `flag` as `flag = true`, `NOT` folded in.
fn canonical_predicate(expr: &ast::Expr, single_in: bool) -> Option<ast::Expr> {
    let is_column = |expr: &ast::Expr| {
        matches!(
            expr,
            ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_)
        )
    };
    let compare = |column: &ast::Expr, op, value| ast::Expr::BinaryOp {
        left: Box::new(column.clone()),
        op,
        right: Box::new(ast::Expr::Value(ast::Value::Boolean(value).into())),
    };
    let operand = |expr: &ast::Expr| canonical_predicate(expr, single_in).map(Box::new);
    let eq = ast::BinaryOperator::Eq;
    let not_eq = ast::BinaryOperator::NotEq;
    match expr {
        ast::Expr::Nested(inner) => operand(inner).map(ast::Expr::Nested),
        column if is_column(column) => Some(compare(column, eq, true)),
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Not,
            expr: negated,
        } => {
            let mut inner = negated.as_ref();
            while let ast::Expr::Nested(nested) = inner {
                inner = nested;
            }
            if is_column(inner) {
                return Some(compare(inner, eq, false));
            }
            if let Some(flipped) = negation(inner) {
                return Some(canonical_predicate(&flipped, single_in).unwrap_or(flipped));
            }
            operand(negated).map(|expr| ast::Expr::UnaryOp {
                op: ast::UnaryOperator::Not,
                expr,
            })
        }
        ast::Expr::IsTrue(column) if is_column(column) => Some(compare(column, eq, true)),
        ast::Expr::IsFalse(column) if is_column(column) => Some(compare(column, eq, false)),
        ast::Expr::IsNotTrue(column) if is_column(column) => Some(compare(column, not_eq, true)),
        ast::Expr::IsNotFalse(column) if is_column(column) => Some(compare(column, not_eq, false)),
        ast::Expr::BinaryOp {
            left,
            op: op @ (ast::BinaryOperator::And | ast::BinaryOperator::Or),
            right,
        } => {
            // A query may leave an `AND` under an `OR` bare, as precedence
            // allows, while this API writes every subgroup parenthesized
            let is_or = matches!(op, ast::BinaryOperator::Or);
            let side = |side: &ast::Expr| {
                let canonical = canonical_predicate(side, single_in);
                let spelled = canonical.as_ref().unwrap_or(side);
                let bare_and = matches!(
                    spelled,
                    ast::Expr::BinaryOp {
                        op: ast::BinaryOperator::And,
                        ..
                    }
                );
                if is_or && bare_and {
                    Some(Box::new(ast::Expr::Nested(Box::new(spelled.clone()))))
                } else {
                    canonical.map(Box::new)
                }
            };
            let (new_left, new_right) = (side(left), side(right));
            if new_left.is_none() && new_right.is_none() {
                return None;
            }
            Some(ast::Expr::BinaryOp {
                left: new_left.unwrap_or_else(|| left.clone()),
                op: op.clone(),
                right: new_right.unwrap_or_else(|| right.clone()),
            })
        }
        ast::Expr::InList {
            expr: tested,
            list,
            negated,
        } if single_in && list.len() == 1 => Some(ast::Expr::BinaryOp {
            left: tested.clone(),
            op: if *negated { not_eq } else { eq },
            right: Box::new(list[0].clone()),
        }),
        _ => None,
    }
}

/// The predicate `NOT` over it stands for, spelled without the `NOT`, as the
/// plan reports it; `None` for one with no such spelling.
fn negation(expr: &ast::Expr) -> Option<ast::Expr> {
    let mut flipped = expr.clone();
    match &mut flipped {
        ast::Expr::BinaryOp { op, .. } => {
            *op = match op {
                ast::BinaryOperator::Eq => ast::BinaryOperator::NotEq,
                ast::BinaryOperator::NotEq => ast::BinaryOperator::Eq,
                _ => return None,
            }
        }
        ast::Expr::InList { negated, .. }
        | ast::Expr::Between { negated, .. }
        | ast::Expr::Like { negated, .. }
        | ast::Expr::ILike { negated, .. } => *negated = !*negated,
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Not,
            expr,
        } => {
            let mut expr = expr.as_ref();
            while let ast::Expr::Nested(inner) = expr {
                expr = inner;
            }
            return Some(expr.clone());
        }
        ast::Expr::IsNull(expr) => return Some(ast::Expr::IsNotNull(expr.clone())),
        ast::Expr::IsNotNull(expr) => return Some(ast::Expr::IsNull(expr.clone())),
        ast::Expr::IsTrue(expr) => return Some(ast::Expr::IsNotTrue(expr.clone())),
        ast::Expr::IsNotTrue(expr) => return Some(ast::Expr::IsTrue(expr.clone())),
        ast::Expr::IsFalse(expr) => return Some(ast::Expr::IsNotFalse(expr.clone())),
        ast::Expr::IsNotFalse(expr) => return Some(ast::Expr::IsFalse(expr.clone())),
        _ => return None,
    }
    Some(flipped)
}

/// Flushes unquoted characters into the key in lower case, `ILIKE` as `LIKE`:
/// a Cube filter carries no case sensitivity.
fn push_word(key: &mut String, word: &mut String) {
    if word.is_empty() {
        return;
    }
    let lowered = word.to_ascii_lowercase();
    key.push_str(if lowered == "ilike" { "like" } else { &lowered });
    word.clear();
}

/// Reads the rest of a quoted run, in which the quote character is written
/// twice to stand for itself.
fn read_quoted(chars: &mut Peekable<Chars>, quote: char) -> String {
    let mut quoted = String::new();
    loop {
        match chars.next() {
            Some(c) if c == quote => {
                if chars.peek() == Some(&quote) {
                    chars.next();
                    quoted.push(quote);
                } else {
                    break;
                }
            }
            Some(other) => quoted.push(other),
            None => break,
        }
    }
    quoted
}

/// Reads the rest of an `E'...'` literal, decoding its backslash escapes.
fn read_escaped(chars: &mut Peekable<Chars>) -> String {
    let mut decoded = String::new();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some('n') => decoded.push('\n'),
                Some('t') => decoded.push('\t'),
                Some('r') => decoded.push('\r'),
                Some('b') => decoded.push('\u{8}'),
                Some('f') => decoded.push('\u{c}'),
                Some(other) => decoded.push(other),
                None => break,
            },
            '\'' if chars.peek() == Some(&'\'') => {
                chars.next();
                decoded.push('\'');
            }
            '\'' => break,
            other => decoded.push(other),
        }
    }
    decoded
}

/// Whether a run of unquoted characters is a name rather than a number, and
/// so can be the relation of a qualified column.
fn starts_identifier(word: &str) -> bool {
    word.chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
}

/// Whether an identifier can be written without quotes: nothing in it would
/// end a bare one. Case does not matter, as cubesql resolves an identifier
/// case-insensitively whichever way it is written.
fn is_bare_identifier(ident: &str) -> bool {
    let mut chars = ident.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The members of an `and` group, which the engine reports as sibling
/// filters, so the group is present when each member is; anything else,
/// an `or` group included, stands only for itself.
fn group_members(needle: &ast::Expr) -> Vec<&ast::Expr> {
    match needle {
        ast::Expr::Nested(inner) if is_and_chain(inner) => and_conjuncts(inner),
        _ => Vec::new(),
    }
}

/// The conjuncts of a clause under both splits: as the clause stands, so that
/// a filter group is seen as the group it is, and with nested AND chains
/// looked through, so that a member of one is seen on its own.
fn all_conjuncts<'a>(clause: &'a ast::Expr) -> Vec<&'a ast::Expr> {
    and_conjuncts_exact(clause)
        .into_iter()
        .chain(and_conjuncts(clause))
        .collect()
}

/// The keys of every conjunct of the clause, under both splits, which is
/// what an addition checks its filter against.
fn clause_key_set(clause: &Option<ast::Expr>, ctx: MatchContext) -> HashSet<FilterKey> {
    clause
        .iter()
        .flat_map(all_conjuncts)
        .map(|expr| ctx.key(expr))
        .collect()
}

/// The conjunct keys of the outermost clauses, kept across a batch so that
/// additions key a clause once. One set per clause and per way of keying;
/// every set of a clause is extended on append, forgotten on any other change.
#[derive(Default)]
pub(super) struct ClauseKeys {
    sets: HashMap<(ClauseKind, bool), HashSet<FilterKey>>,
}

impl ClauseKeys {
    /// Whether the clause already holds the expression.
    pub(super) fn holds(
        &mut self,
        kind: ClauseKind,
        ctx: MatchContext,
        clause: &Option<ast::Expr>,
        expr: &ast::Expr,
    ) -> bool {
        let keys = self
            .sets
            .entry((kind, ctx.normalize_dates))
            .or_insert_with(|| clause_key_set(clause, ctx));
        clause_holds(keys, expr, ctx)
    }

    /// Records an append in every set of the clause, each under its own way
    /// of keying, so that none of them goes stale.
    pub(super) fn note_appended(&mut self, kind: ClauseKind, expr: &ast::Expr, ctx: MatchContext) {
        for ((known, normalize_dates), keys) in self.sets.iter_mut() {
            if *known != kind {
                continue;
            }
            let ctx = MatchContext {
                sole_reported: false,
                normalize_dates: *normalize_dates,
                ..ctx
            };
            keys.extend(appended_keys(expr, ctx));
        }
    }

    pub(super) fn forget(&mut self, kind: ClauseKind) {
        self.sets.retain(|(known, _), _| *known != kind);
    }
}

/// Splits the clause the way that matches the needle: as it stands, so that a
/// group matches as a group, else with nested AND chains looked through.
/// `Err` hands the clause back untouched when the needle is in neither.
fn matching_conjuncts(
    clause: ast::Expr,
    needle_key: &FilterKey,
    ctx: MatchContext,
) -> Result<Vec<(FilterKey, ast::Expr)>, ast::Expr> {
    let exact_keys = keys_of(and_conjuncts_exact(&clause), ctx);
    if exact_keys.contains(needle_key) {
        return Ok(with_keys(exact_keys, into_and_conjuncts_exact(clause), ctx));
    }

    let keys = keys_of(and_conjuncts(&clause), ctx);
    if keys.contains(needle_key) {
        return Ok(with_keys(keys, into_and_conjuncts(clause), ctx));
    }

    Err(clause)
}

pub(super) fn keys_of(conjuncts: Vec<&ast::Expr>, ctx: MatchContext) -> Vec<FilterKey> {
    conjuncts
        .into_iter()
        .map(|conjunct| ctx.key(conjunct))
        .collect()
}

/// Pairs each conjunct with the key worked out while the clause was borrowed,
/// so a removal keys a conjunct once. The two splits walk in the same order;
/// should they ever not, the keys are worked out again rather than mispaired.
pub(super) fn with_keys(
    keys: Vec<FilterKey>,
    conjuncts: Vec<ast::Expr>,
    ctx: MatchContext,
) -> Vec<(FilterKey, ast::Expr)> {
    if keys.len() != conjuncts.len() {
        return conjuncts
            .into_iter()
            .map(|conjunct| (ctx.key(&conjunct), conjunct))
            .collect();
    }

    keys.into_iter().zip(conjuncts).collect()
}

/// Removes all identical expressions from the top-level AND chain.
/// Returns the number of expressions removed.
pub(super) fn remove_expr_from_clause(
    option_clause: &mut Option<ast::Expr>,
    needle: &ast::Expr,
    ctx: MatchContext,
) -> usize {
    spell_between_apart(option_clause, needle, ctx);
    let Some(clause) = option_clause.take() else {
        return 0;
    };

    // Which split matches is decided before the clause is taken apart, so a
    // needle that isn't there leaves it exactly as it was
    let needle_key = ctx.key(needle);
    let clause = match matching_conjuncts(clause, &needle_key, ctx) {
        Ok(conjuncts) => {
            let before = conjuncts.len();
            let kept = conjuncts
                .into_iter()
                .filter(|(key, _)| key != &needle_key)
                .map(|(_, conjunct)| conjunct)
                .collect::<Vec<_>>();
            let removed = before - kept.len();
            *option_clause = and_chain(kept);
            return removed;
        }
        Err(clause) => clause,
    };

    // A group the clause doesn't hold as one expression is still there when
    // each of its members is, and then it is those that are removed
    let members = group_members(needle);
    let member_keys = members.iter().map(|m| ctx.key(m)).collect::<Vec<_>>();
    let holds_every_member = !members.is_empty() && {
        let keys = all_conjuncts(&clause)
            .into_iter()
            .map(|expr| ctx.key(expr))
            .collect::<Vec<_>>();
        member_keys.iter().all(|member| keys.contains(member))
    };
    if holds_every_member {
        let conjuncts = into_and_conjuncts(clause);
        let before = conjuncts.len();
        let kept = conjuncts
            .into_iter()
            .filter(|conjunct| !member_keys.contains(&ctx.key(conjunct)))
            .collect::<Vec<_>>();
        let removed = before - kept.len();
        *option_clause = and_chain(kept);
        return removed;
    }

    remove_reported_column_predicates(option_clause, clause, needle, ctx)
}

/// The expression inside its parentheses, looked through to a bounded depth.
fn unparenthesized(mut expr: &ast::Expr) -> &ast::Expr {
    for _ in 0..MAX_EXPR_NESTING {
        let ast::Expr::Nested(inner) = expr else {
            break;
        };
        expr = inner;
    }
    expr
}

/// The plan reports a `BETWEEN` as two filters; when the needle is one of
/// them, the clause spells the two apart so that matching takes that one.
/// A clause holding no such half stays as it was.
fn spell_between_apart(
    option_clause: &mut Option<ast::Expr>,
    needle: &ast::Expr,
    ctx: MatchContext,
) {
    let Some(clause) = option_clause.as_ref() else {
        return;
    };
    let halves = |conjunct: &ast::Expr| match unparenthesized(conjunct) {
        ast::Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } => Some((
            ast::Expr::BinaryOp {
                left: expr.clone(),
                op: ast::BinaryOperator::GtEq,
                right: low.clone(),
            },
            ast::Expr::BinaryOp {
                left: expr.clone(),
                op: ast::BinaryOperator::LtEq,
                right: high.clone(),
            },
        )),
        _ => None,
    };
    let needle_key = ctx.key(needle);
    let asked_for = |conjunct: &ast::Expr| {
        halves(conjunct)
            .is_some_and(|(low, high)| ctx.key(&low) == needle_key || ctx.key(&high) == needle_key)
    };
    if !and_conjuncts_with(clause, true).into_iter().any(asked_for) {
        return;
    }

    let mut spelled = Vec::new();
    for conjunct in into_and_conjuncts_with(option_clause.take().unwrap(), true) {
        if asked_for(&conjunct) {
            let (low, high) = halves(&conjunct).unwrap();
            spelled.push(low);
            spelled.push(high);
        } else {
            spelled.push(conjunct);
        }
    }
    *option_clause = and_chain(spelled);
}

/// Removes a sole reported filter the query spells otherwise, by its column:
/// one filter-shaped predicate there, or a time member's bounds the plan merges
/// into a range. Anything else may not be a Cube filter at all, so none go.
fn remove_reported_column_predicates(
    option_clause: &mut Option<ast::Expr>,
    clause: ast::Expr,
    needle: &ast::Expr,
    ctx: MatchContext,
) -> usize {
    let needle_column = filter_column_key(needle, ctx).filter(|_| ctx.sole_reported);
    let Some(needle_column) = needle_column else {
        *option_clause = Some(clause);
        return 0;
    };

    let on_needle_column =
        |conjunct: &ast::Expr| filter_column_key(conjunct, ctx).as_ref() == Some(&needle_column);

    // Whether they can go is decided before the clause is taken apart, so a
    // clause holding none, or an unclear many, is left exactly as it was
    let on_column = and_conjuncts(&clause)
        .into_iter()
        .filter(|conjunct| on_needle_column(conjunct))
        .collect::<Vec<_>>();
    let removable = match &on_column[..] {
        [] => false,
        [one] => is_filter_shaped(one),
        many => ctx.normalize_dates && many.iter().all(|conjunct| is_bound(conjunct)),
    };
    if !removable {
        *option_clause = Some(clause);
        return 0;
    }

    let conjuncts = into_and_conjuncts(clause);
    let before = conjuncts.len();
    let kept = conjuncts
        .into_iter()
        .filter(|conjunct| !on_needle_column(conjunct))
        .collect::<Vec<_>>();
    let removed = before - kept.len();
    *option_clause = and_chain(kept);
    removed
}

/// A shape the engine reads as a Cube filter whatever its values; a `LIKE`
/// pattern is one only when this API could have written it, so it is not.
fn is_filter_shaped(expr: &ast::Expr) -> bool {
    is_bound(expr)
        || matches!(
            unparenthesized(expr),
            ast::Expr::InList { .. } | ast::Expr::IsNull(_) | ast::Expr::IsNotNull(_)
        )
}

/// A comparison or a BETWEEN, the shapes a date range's bounds take.
fn is_bound(expr: &ast::Expr) -> bool {
    match unparenthesized(expr) {
        ast::Expr::BinaryOp { op, .. } => is_comparison_op(op),
        ast::Expr::Between { .. } => true,
        _ => false,
    }
}

/// The single column a filter expression stands on, as a comparison key.
/// A filter over more than one column, a group of them included, has none.
fn filter_column_key(expr: &ast::Expr, ctx: MatchContext) -> Option<FilterKey> {
    let mut column: Option<String> = None;
    let mut pending = vec![expr];
    let mut visited = 0;

    while let Some(expr) = pending.pop() {
        visited += 1;
        if visited > MAX_EXPR_NODES {
            return None;
        }

        match expr {
            ast::Expr::Nested(inner) => pending.push(inner),
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::And | ast::BinaryOperator::Or,
                right,
            } => {
                pending.push(left);
                pending.push(right);
            }
            expr => {
                let key = ctx.key(filter_column_expr(predicate_column(expr)?));
                match &column {
                    Some(seen) if seen != &key => return None,
                    Some(_) => {}
                    None => column = Some(key),
                }
            }
        }
    }

    column
}

/// The column side of a single predicate.
fn predicate_column(expr: &ast::Expr) -> Option<&ast::Expr> {
    match expr {
        ast::Expr::BinaryOp { left, op, right } => {
            if !is_comparison_op(op) {
                return None;
            }
            if is_filter_value(right) {
                Some(left)
            } else if is_filter_value(left) {
                Some(right)
            } else {
                None
            }
        }
        ast::Expr::InList { expr, .. }
        | ast::Expr::Between { expr, .. }
        | ast::Expr::Like { expr, .. }
        | ast::Expr::ILike { expr, .. }
        | ast::Expr::IsNull(expr)
        | ast::Expr::IsNotNull(expr) => Some(expr),
        _ => None,
    }
}

fn is_comparison_op(op: &ast::BinaryOperator) -> bool {
    matches!(
        op,
        ast::BinaryOperator::Eq
            | ast::BinaryOperator::NotEq
            | ast::BinaryOperator::Lt
            | ast::BinaryOperator::LtEq
            | ast::BinaryOperator::Gt
            | ast::BinaryOperator::GtEq
    )
}

/// Replaces every occurrence of the expression in place within the top-level
/// AND chain, keeping positions; when the clause already holds the new one,
/// the old is dropped instead. Returns the number of expressions replaced.
pub(super) fn replace_expr_in_clause(
    option_clause: &mut Option<ast::Expr>,
    old: &ast::Expr,
    new: ast::Expr,
    ctx: MatchContext,
    new_ctx: MatchContext,
) -> usize {
    let old_key = ctx.key(old);
    // Replacing a filter with itself leaves it where it is
    let already_held = ctx.key(&new) != old_key
        && clause_holds(&clause_key_set(option_clause, new_ctx), &new, new_ctx);
    let Some(clause) = option_clause.take() else {
        return 0;
    };

    let clause = match matching_conjuncts(clause, &old_key, ctx) {
        Ok(conjuncts) => {
            let mut replaced = 0;
            let conjuncts = conjuncts
                .into_iter()
                .filter_map(|(key, conjunct)| {
                    if key != old_key {
                        return Some(conjunct);
                    }
                    replaced += 1;
                    (!already_held).then(|| new.clone())
                })
                .collect::<Vec<_>>();
            *option_clause = and_chain(conjuncts);
            return replaced;
        }
        Err(clause) => clause,
    };

    // As for removal, a group the clause holds as separate members is
    // replaced by dropping those and appending the new filter. Its position
    // isn't kept, as the members had no single one to keep.
    *option_clause = Some(clause);
    let removed = remove_expr_from_clause(option_clause, old, ctx);
    if removed == 0 {
        return 0;
    }
    // The filter being added is matched as itself: what counts as the same
    // value is decided by the member it stands on, not by the one it replaces
    if !clause_holds(&clause_key_set(option_clause, new_ctx), &new, new_ctx) {
        append_expr_to_clause(option_clause, new);
    }
    removed
}
