//! Keying the predicates of a WHERE or HAVING so a filter can be found by
//! its shape, and the splits and edits of a clause that rest on it.

use super::{
    report::{canonical_number, normalize_filter_value},
    FilterKey,
};
use crate::compile::rewrite::rules::utils::DeltaTimeUnitToken;
use sqlparser::ast;
use std::{collections::HashSet, iter::Peekable, str::Chars};

/// How deep a single expression is unwrapped: a group's parentheses, and the
/// truncations or extractions around the column a predicate filters.
const MAX_EXPR_NESTING: usize = 25;

/// Functions the planner evaluates once as it plans, so that a call to one over
/// values, as in `CURRENT_DATE - INTERVAL '7 days'`, is a value to compare to.
const PLANNING_TIME_FUNCTIONS: [&str; 7] = [
    "current_date",
    "current_timestamp",
    "localtimestamp",
    "now",
    "date_trunc",
    "to_date",
    "to_timestamp",
];

/// Whether the function is one of `known` by its own name, the last part of a
/// qualified one, in any case: the engine lowercases a quoted name too.
fn is_function_in(function: &ast::Function, known: &[&str]) -> bool {
    let ast::ObjectName(parts) = &function.name;
    let Some(name) = parts.last().and_then(|part| part.as_ident()) else {
        return false;
    };
    known
        .iter()
        .any(|known| name.value.eq_ignore_ascii_case(known))
}

/// The column a `DATE_TRUNC(unit, column)` truncates. The engine turns a
/// comparison against one into a range over the column itself, so it is that
/// column the predicate filters.
fn date_trunc_arg(func: &ast::Function) -> Option<&ast::Expr> {
    if !is_function_in(func, &["date_trunc"]) {
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
        if let Some((_, column)) = date_part(expr) {
            expr = column;
            continue;
        }
        match expr {
            ast::Expr::Function(func) => match date_trunc_arg(func) {
                Some(arg) => expr = arg,
                None => return expr,
            },
            expr => return expr,
        }
    }
    expr
}

/// The column a date part the engine merges into a range is taken from, as
/// `EXTRACT` or `DATE_PART`, and whether it is the year: any other part is a
/// range only beside a year or a range on the column, never on its own.
fn date_part(expr: &ast::Expr) -> Option<(bool, &ast::Expr)> {
    // Read as the engine reads it, so that a unit is merged here when it is there
    let is_year = |unit: &str| match unit.parse::<DeltaTimeUnitToken>().ok()? {
        DeltaTimeUnitToken::Year => Some(true),
        DeltaTimeUnitToken::Quarter | DeltaTimeUnitToken::Month | DeltaTimeUnitToken::Week => {
            Some(false)
        }
        _ => None,
    };
    match expr {
        ast::Expr::Extract { field, expr, .. } => {
            let unit = match field {
                ast::DateTimeField::Custom(ident) => ident.value.clone(),
                ast::DateTimeField::Week(Some(_)) => return None,
                field => field.to_string(),
            };
            Some((is_year(&unit)?, expr))
        }
        ast::Expr::Function(func) if is_function_in(func, &["date_part"]) => {
            let ast::FunctionArguments::List(list) = &func.args else {
                return None;
            };
            let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(unit)), ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(column))] =
                &list.args[..]
            else {
                return None;
            };
            let ast::Expr::Value(unit) = unit else {
                return None;
            };
            let (ast::Value::SingleQuotedString(unit) | ast::Value::EscapedStringLiteral(unit)) =
                &unit.value
            else {
                return None;
            };
            Some((is_year(unit)?, column))
        }
        _ => None,
    }
}

/// Whether a predicate compares a date part other than the year, which the
/// engine reports nothing for unless a year or a range shares its column.
fn is_partial_date_part(expr: &ast::Expr) -> bool {
    predicate_column(unparenthesized(expr))
        .and_then(date_part)
        .is_some_and(|(is_year, _)| !is_year)
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

/// Whether the clause already holds the expression, as itself or as a group
/// each of whose members it holds.
pub(super) fn clause_contains(
    clause: &Option<ast::Expr>,
    expr: &ast::Expr,
    ctx: MatchContext,
) -> bool {
    clause_holds(&clause_key_set(clause, ctx), expr, ctx)
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
pub(super) struct MatchContext {
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
}

impl MatchContext {
    pub(super) fn key(&self, expr: &ast::Expr) -> FilterKey {
        expr_key(expr, self.ignore_qualifier, self.normalize_dates)
    }
}

/// A filter expression's SQL spelled as the plan reads it. `drop_qualifiers`
/// drops relation names;
/// `normalize_dates` reduces dates and keeps a one-value `IN` apart from `=`.
fn expr_key(expr: &ast::Expr, drop_qualifiers: bool, normalize_dates: bool) -> String {
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

    let rendered = expr.to_string();
    let mut key = String::with_capacity(rendered.len());
    let mut word = String::new();
    let mut chars = rendered.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                push_word(&mut key, &mut word);
                let literal = read_quoted(&mut chars, '\'');
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
            '.' if is_number(&word)
                || word.is_empty() && chars.peek().is_some_and(char::is_ascii_digit) =>
            {
                word.push('.')
            }
            c => {
                let is_qualifier = c == '.' && starts_identifier(&word);
                if drop_qualifiers && is_qualifier {
                    word.clear();
                    continue;
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
/// through every `AND` and `OR`: `x IN ('v')` as `x = 'v'` when `single_in`
/// holds, as the console writes an `equals` filter.
fn canonical_predicate(expr: &ast::Expr, single_in: bool) -> Option<ast::Expr> {
    canonical_predicate_at(expr, single_in, 0)
}

/// How deep a predicate is respelled: an OR chain nests a level per term, so
/// past this its tail keys as written, a miss rather than a stack overflow.
const MAX_CANONICAL_DEPTH: usize = 64;

fn canonical_predicate_at(expr: &ast::Expr, single_in: bool, depth: usize) -> Option<ast::Expr> {
    if depth > MAX_CANONICAL_DEPTH {
        return None;
    }
    let operand =
        |expr: &ast::Expr| canonical_predicate_at(expr, single_in, depth + 1).map(Box::new);
    match expr {
        ast::Expr::Nested(inner) => operand(inner).map(ast::Expr::Nested),
        ast::Expr::BinaryOp {
            left,
            op: op @ (ast::BinaryOperator::And | ast::BinaryOperator::Or),
            right,
        } => {
            let (new_left, new_right) = (operand(left), operand(right));
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
            op: if *negated {
                ast::BinaryOperator::NotEq
            } else {
                ast::BinaryOperator::Eq
            },
            right: Box::new(list[0].clone()),
        }),
        _ => None,
    }
}

fn is_number(word: &str) -> bool {
    word.starts_with(|c: char| c.is_ascii_digit() || c == '.')
}

/// Flushes unquoted characters into the key in lower case, `ILIKE` as `LIKE`:
/// a Cube filter carries no case sensitivity.
fn push_word(key: &mut String, word: &mut String) {
    if word.is_empty() {
        return;
    }
    // A number is keyed in the form the plan reports it, `10.50` as `10.5`
    let lowered = if is_number(word) {
        canonical_number(word)
    } else {
        word.to_ascii_lowercase()
    };
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

/// Splits the clause the way that matches the needle: as it stands, so that a
/// group matches as a group, else with nested AND chains looked through.
/// `Err` hands the clause back untouched when the needle is in neither.
fn matching_conjuncts(
    clause: ast::Expr,
    needle: &ast::Expr,
    needle_key: &FilterKey,
    ctx: MatchContext,
) -> Result<Vec<(FilterKey, ast::Expr)>, ast::Expr> {
    let count = |keys: &[FilterKey], key: &FilterKey| keys.iter().filter(|k| *k == key).count();
    let exact_keys = keys_of(and_conjuncts_exact(&clause), ctx);
    let keys = keys_of(and_conjuncts(&clause), ctx);
    // The exact split keeps groups whole, unless a full copy sits elsewhere: a
    // leaf inside a group, or every member of a group again, spelled apart or
    // nested deeper; a member another conjunct shares is no copy
    let copies = count(&exact_keys, needle_key);
    let in_place = copies > 0
        && match &group_members(needle)[..] {
            [] => copies == count(&keys, needle_key),
            members => {
                let member_keys = keys_of(members.to_vec(), ctx);
                member_keys.iter().any(|key| {
                    let each = count(&member_keys, key);
                    count(&keys, key).saturating_sub(copies * each) < each
                })
            }
        };
    if in_place {
        return Ok(with_keys(exact_keys, into_and_conjuncts_exact(clause), ctx));
    }
    if count(&keys, needle_key) > 0 {
        return Ok(with_keys(keys, into_and_conjuncts(clause), ctx));
    }

    Err(clause)
}

fn keys_of(conjuncts: Vec<&ast::Expr>, ctx: MatchContext) -> Vec<FilterKey> {
    conjuncts
        .into_iter()
        .map(|conjunct| ctx.key(conjunct))
        .collect()
}

/// Pairs each conjunct with the key worked out while the clause was borrowed,
/// so a removal keys a conjunct once. The two splits walk in the same order;
/// should they ever not, the keys are worked out again rather than mispaired.
fn with_keys(
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

/// Removes every conjunct keyed as the needle, or else its group's members or a
/// sole reported filter's predicates on its column. Returns how many went.
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
    let clause = match matching_conjuncts(clause, needle, &needle_key, ctx) {
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
        [one] => is_filter_shaped(one) && !is_partial_date_part(one),
        many => {
            ctx.normalize_dates
                && many.iter().all(|conjunct| is_bound(conjunct))
                && !many.iter().all(|conjunct| is_partial_date_part(conjunct))
        }
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
    while let Some(expr) = pending.pop() {
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

/// The value side of a filter predicate: one the planner folds to a value
/// however it is spelled, as `CURRENT_DATE - INTERVAL '7 days'` or `1 + 1`,
/// from literals other than `NULL`, arithmetic and planning-time functions.
fn is_filter_value(expr: &ast::Expr) -> bool {
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            // A `NULL` mostly makes the comparison hold for no row, so any one is
            // refused; `COALESCE(NULL, 'a')`, which folds to a value, is a miss
            ast::Expr::Value(value) if !matches!(value.value, ast::Value::Null) => {}
            ast::Expr::TypedString { .. } => {}
            ast::Expr::Interval(interval) => pending.push(&interval.value),
            ast::Expr::Nested(inner)
            | ast::Expr::Cast { expr: inner, .. }
            | ast::Expr::UnaryOp { expr: inner, .. } => pending.push(inner),
            ast::Expr::BinaryOp { left, op, right } => {
                if !matches!(
                    op,
                    ast::BinaryOperator::Plus
                        | ast::BinaryOperator::Minus
                        | ast::BinaryOperator::Multiply
                        | ast::BinaryOperator::Divide
                        | ast::BinaryOperator::Modulo
                        | ast::BinaryOperator::StringConcat
                ) {
                    return false;
                }
                pending.push(left);
                pending.push(right);
            }
            // `RANDOM()` or `CURRENT_USER` is no value the plan reads as one
            ast::Expr::Function(function) => {
                if !is_function_in(function, &PLANNING_TIME_FUNCTIONS) || function.over.is_some() {
                    return false;
                }
                if let ast::FunctionArguments::List(list) = &function.args {
                    for arg in &list.args {
                        let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg)) = arg else {
                            return false;
                        };
                        pending.push(arg);
                    }
                }
            }
            _ => return false,
        }
    }

    true
}

/// The column side of a single predicate.
fn predicate_column(expr: &ast::Expr) -> Option<&ast::Expr> {
    match expr {
        ast::Expr::BinaryOp { left, op, right } => {
            if !is_comparison_op(op) {
                return None;
            }
            // A computed value is one here as in an `IN` list, and a `BETWEEN`
            // split apart compares against its bounds
            if is_filter_value(right) {
                Some(left)
            } else if is_filter_value(left) {
                Some(right)
            } else {
                None
            }
        }
        // `x IN (other_col)` compares two columns, which is no Cube filter
        ast::Expr::InList { expr, list, .. } => list.iter().all(is_filter_value).then_some(expr),
        ast::Expr::Between {
            expr, low, high, ..
        } => (is_filter_value(low) && is_filter_value(high)).then_some(expr),
        ast::Expr::Like { expr, .. }
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
    let Some(clause) = option_clause.take() else {
        return 0;
    };

    let clause = match matching_conjuncts(clause, old, &old_key, ctx) {
        Ok(conjuncts) => {
            // Held by what stays, not by the old filter a group may contain;
            // replacing a filter with itself leaves it where it is
            let kept_keys = conjuncts
                .iter()
                .filter(|(key, _)| key != &old_key)
                .flat_map(|(_, conjunct)| all_conjuncts(conjunct))
                .map(|conjunct| new_ctx.key(conjunct))
                .collect::<HashSet<_>>();
            let already_held = ctx.key(&new) != old_key && clause_holds(&kept_keys, &new, new_ctx);
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
