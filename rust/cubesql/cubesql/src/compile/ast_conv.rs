use std::{cell::Cell, collections::HashSet, sync::Arc};

use cubeclient::models::{V1CubeMetaDimension, V1CubeMetaMeasure, V1LoadRequestQueryFilterItem};
use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{LogicalPlan, PlanVisitor},
};
use sqlparser::{ast, dialect::PostgreSqlDialect, parser::Parser};

use crate::{
    compile::{
        convert_sql_to_cube_query,
        engine::df::{
            scan::CubeScanNode,
            wrapper::{CubeScanWrappedSqlNode, CubeScanWrapperNode},
        },
    },
    sql::Session,
    transport::MetaContext,
    CubeError,
};

#[derive(Debug)]
enum ModifyAction {
    Add(V1LoadRequestQueryFilterItem),
    Remove(V1LoadRequestQueryFilterItem),
    Replace {
        old: V1LoadRequestQueryFilterItem,
        new: V1LoadRequestQueryFilterItem,
    },
}

impl ModifyAction {
    fn get_cube_and_member_name(filter: &V1LoadRequestQueryFilterItem) -> Result<(String, String)> {
        let member_name = filter
            .member
            .as_ref()
            .ok_or_else(|| DataFusionError::Plan("Filter must have a member".to_string()))?;
        let (cube, member) = member_name
            .split_once('.')
            .ok_or_else(|| DataFusionError::NotImplemented("Invalid member format".to_string()))?;
        Ok((cube.to_string(), member.to_string()))
    }

    /// Builds the comparison expression for a plain (non-group) filter.
    fn leaf_expr(
        filter: &V1LoadRequestQueryFilterItem,
        source: &MemberSource,
        meta_member: &MetaMember,
    ) -> Result<ast::Expr> {
        let expr = match source {
            MemberSource::CubeTable { relation_alias } => {
                let column_ident = ast::Ident::with_quote('"', meta_member.short_name());
                let column_expr =
                    ast::Expr::CompoundIdentifier(vec![relation_alias.clone(), column_ident]);
                match meta_member {
                    MetaMember::Dimension(_) => column_expr,
                    MetaMember::Measure(measure) => {
                        // The meta API spells the distinct aggregations in
                        // camelCase; the snake_case forms are accepted too,
                        // as they are elsewhere in the transport layer.
                        // `countDistinctApprox` has no exact SQL equivalent,
                        // so it stays on the MEASURE path.
                        let func_name = match measure.agg_type.as_deref() {
                            Some("count") => "COUNT",
                            Some("count_distinct" | "countDistinct") => "COUNT",
                            Some("sum") => "SUM",
                            Some("avg") => "AVG",
                            Some("min") => "MIN",
                            Some("max") => "MAX",
                            _ => "MEASURE",
                        };
                        let distinct = matches!(
                            measure.agg_type.as_deref(),
                            Some("count_distinct" | "countDistinct")
                        );
                        ast::Expr::Function(ast::Function {
                            name: ast::ObjectName::from(vec![ast::Ident::new(func_name)]),
                            uses_odbc_syntax: false,
                            parameters: ast::FunctionArguments::None,
                            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                                duplicate_treatment: distinct
                                    .then_some(ast::DuplicateTreatment::Distinct),
                                args: vec![ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                    column_expr,
                                ))],
                                clauses: vec![],
                            }),
                            filter: None,
                            null_treatment: None,
                            over: None,
                            within_group: vec![],
                            approximate: false,
                        })
                    }
                }
            }
            // The member is already computed as an output column of a derived
            // table / CTE reference, so it is referenced as a plain column
            MemberSource::DerivedColumn {
                relation_alias,
                column,
            } => ast::Expr::CompoundIdentifier(vec![relation_alias.clone(), column.clone()]),
        };

        match filter.operator.as_deref() {
            Some("equals") => {
                let Some(values) = &filter.values else {
                    return Err(DataFusionError::Plan(
                        "Filter values are required for \"equals\" operator".to_string(),
                    ));
                };
                match values.len() {
                    0 => {
                        return Err(DataFusionError::Plan(
                            "At least one filter value is required for \"equals\" operator"
                                .to_string(),
                        ))
                    }
                    1 => {
                        let value_expr =
                            Self::value_to_expr_by_member_type(&values[0], meta_member)?;
                        Ok(ast::Expr::BinaryOp {
                            left: Box::new(expr),
                            op: ast::BinaryOperator::Eq,
                            right: Box::new(value_expr),
                        })
                    }
                    _ => {
                        let values_exprs = values
                            .iter()
                            .map(|v| Self::value_to_expr_by_member_type(v, meta_member))
                            .collect::<Result<Vec<_>>>()?;
                        Ok(ast::Expr::InList {
                            expr: Box::new(expr),
                            list: values_exprs,
                            negated: false,
                        })
                    }
                }
            }
            Some("notEquals") => {
                let Some(values) = &filter.values else {
                    return Err(DataFusionError::Plan(
                        "Filter values are required for \"notEquals\" operator".to_string(),
                    ));
                };
                match values.len() {
                    0 => {
                        return Err(DataFusionError::Plan(
                            "At least one filter value is required for \"notEquals\" operator"
                                .to_string(),
                        ))
                    }
                    1 => {
                        let value_expr =
                            Self::value_to_expr_by_member_type(&values[0], meta_member)?;
                        Ok(ast::Expr::BinaryOp {
                            left: Box::new(expr),
                            op: ast::BinaryOperator::NotEq,
                            right: Box::new(value_expr),
                        })
                    }
                    _ => {
                        let values_exprs = values
                            .iter()
                            .map(|v| Self::value_to_expr_by_member_type(v, meta_member))
                            .collect::<Result<Vec<_>>>()?;
                        Ok(ast::Expr::InList {
                            expr: Box::new(expr),
                            list: values_exprs,
                            negated: true,
                        })
                    }
                }
            }
            Some("set") => Ok(ast::Expr::IsNotNull(Box::new(expr))),
            Some("notSet") => Ok(ast::Expr::IsNull(Box::new(expr))),
            Some("inDateRange") => Self::date_range_expr(
                expr,
                filter,
                "inDateRange",
                ast::BinaryOperator::GtEq,
                ast::BinaryOperator::LtEq,
                ast::BinaryOperator::And,
            ),
            Some("beforeDate") => Self::single_string_cmp_expr(
                expr,
                filter.values.as_deref(),
                "beforeDate",
                ast::BinaryOperator::Lt,
            ),
            Some("beforeOrOnDate") => Self::single_string_cmp_expr(
                expr,
                filter.values.as_deref(),
                "beforeOrOnDate",
                ast::BinaryOperator::LtEq,
            ),
            Some("afterDate") => Self::single_string_cmp_expr(
                expr,
                filter.values.as_deref(),
                "afterDate",
                ast::BinaryOperator::Gt,
            ),
            Some("afterOrOnDate") => Self::single_string_cmp_expr(
                expr,
                filter.values.as_deref(),
                "afterOrOnDate",
                ast::BinaryOperator::GtEq,
            ),
            // A negated range is only recognized as `notInDateRange` when it
            // is written as NOT BETWEEN; the equivalent disjunction of
            // comparisons comes back as two separate bounds instead
            Some("notInDateRange") => Self::between_expr(expr, filter, "notInDateRange", true),
            Some("gt") => Self::numeric_cmp_expr(
                expr,
                filter.values.as_deref(),
                "gt",
                ast::BinaryOperator::Gt,
            ),
            Some("gte") => Self::numeric_cmp_expr(
                expr,
                filter.values.as_deref(),
                "gte",
                ast::BinaryOperator::GtEq,
            ),
            Some("lt") => Self::numeric_cmp_expr(
                expr,
                filter.values.as_deref(),
                "lt",
                ast::BinaryOperator::Lt,
            ),
            Some("lte") => Self::numeric_cmp_expr(
                expr,
                filter.values.as_deref(),
                "lte",
                ast::BinaryOperator::LtEq,
            ),
            Some("contains") => {
                Self::like_family_expr(&expr, filter, "contains", LikeShape::Contains, false)
            }
            Some("notContains") => {
                Self::like_family_expr(&expr, filter, "notContains", LikeShape::Contains, true)
            }
            Some("startsWith") => {
                Self::like_family_expr(&expr, filter, "startsWith", LikeShape::StartsWith, false)
            }
            Some("notStartsWith") => {
                Self::like_family_expr(&expr, filter, "notStartsWith", LikeShape::StartsWith, true)
            }
            Some("endsWith") => {
                Self::like_family_expr(&expr, filter, "endsWith", LikeShape::EndsWith, false)
            }
            Some("notEndsWith") => {
                Self::like_family_expr(&expr, filter, "notEndsWith", LikeShape::EndsWith, true)
            }
            _ => Err(DataFusionError::Plan(format!(
                "Unsupported filter operator: {:?}",
                filter.operator
            ))),
        }
    }

    fn numeric_cmp_expr(
        column_expr: ast::Expr,
        values: Option<&[String]>,
        op_name: &str,
        op: ast::BinaryOperator,
    ) -> Result<ast::Expr> {
        let value = Self::single_numeric_value(values, op_name)?;
        Ok(ast::Expr::BinaryOp {
            left: Box::new(column_expr),
            op,
            right: Box::new(Self::numeric_value_expr(value)?),
        })
    }

    /// Numeric literals are rendered verbatim, so the value must be validated
    /// to be a plain SQL numeric literal before it is placed into the AST.
    /// `f64` parsing alone would admit `inf`/`NaN` and surrounding whitespace,
    /// hence the character check.
    fn numeric_value_expr(value: &str) -> Result<ast::Expr> {
        if !Self::is_numeric_literal(value) {
            return Err(DataFusionError::Plan(format!(
                "Filter value must be numeric, got {:?}",
                value
            )));
        }
        Ok(ast::Expr::Value(
            ast::Value::Number(value.to_string(), false).into(),
        ))
    }

    fn is_numeric_literal(value: &str) -> bool {
        !value.is_empty()
            && value
                .chars()
                .all(|c| c.is_ascii_digit() || matches!(c, '.' | '-' | '+' | 'e' | 'E'))
            && value.parse::<f64>().is_ok_and(|value| value.is_finite())
    }

    fn single_numeric_value<'a>(values: Option<&'a [String]>, op_name: &str) -> Result<&'a str> {
        let Some(values) = values else {
            return Err(DataFusionError::Plan(format!(
                "Filter values are required for \"{}\" operator",
                op_name
            )));
        };
        if values.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "Exactly one filter value is required for \"{}\" operator",
                op_name
            )));
        }
        let value = values[0].as_str();
        if !Self::is_numeric_literal(value) {
            return Err(DataFusionError::Plan(format!(
                "Filter value for \"{}\" operator must be numeric, got {:?}",
                op_name, value
            )));
        }
        Ok(value)
    }

    fn single_string_cmp_expr(
        column_expr: ast::Expr,
        values: Option<&[String]>,
        op_name: &str,
        op: ast::BinaryOperator,
    ) -> Result<ast::Expr> {
        let Some(values) = values else {
            return Err(DataFusionError::Plan(format!(
                "Filter values are required for \"{}\" operator",
                op_name
            )));
        };
        if values.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "Exactly one filter value is required for \"{}\" operator",
                op_name
            )));
        }
        Ok(ast::Expr::BinaryOp {
            left: Box::new(column_expr),
            op,
            right: Box::new(ast::Expr::Value(
                ast::Value::SingleQuotedString(values[0].clone()).into(),
            )),
        })
    }

    fn between_expr(
        column_expr: ast::Expr,
        filter: &V1LoadRequestQueryFilterItem,
        op_name: &str,
        negated: bool,
    ) -> Result<ast::Expr> {
        let values = Self::two_values(filter, op_name)?;
        Ok(ast::Expr::Between {
            expr: Box::new(column_expr),
            negated,
            low: Box::new(ast::Expr::Value(
                ast::Value::SingleQuotedString(values[0].clone()).into(),
            )),
            high: Box::new(ast::Expr::Value(
                ast::Value::SingleQuotedString(values[1].clone()).into(),
            )),
        })
    }

    fn two_values<'a>(
        filter: &'a V1LoadRequestQueryFilterItem,
        op_name: &str,
    ) -> Result<&'a [String]> {
        let Some(values) = &filter.values else {
            return Err(DataFusionError::Plan(format!(
                "Filter values are required for \"{}\" operator",
                op_name
            )));
        };
        if values.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "Exactly two filter values are required for \"{}\" operator",
                op_name
            )));
        }
        Ok(values)
    }

    fn date_range_expr(
        column_expr: ast::Expr,
        filter: &V1LoadRequestQueryFilterItem,
        op_name: &str,
        lower_op: ast::BinaryOperator,
        upper_op: ast::BinaryOperator,
        join_op: ast::BinaryOperator,
    ) -> Result<ast::Expr> {
        let values = Self::two_values(filter, op_name)?;
        let lower = ast::Expr::BinaryOp {
            left: Box::new(column_expr.clone()),
            op: lower_op,
            right: Box::new(ast::Expr::Value(
                ast::Value::SingleQuotedString(values[0].clone()).into(),
            )),
        };
        let upper = ast::Expr::BinaryOp {
            left: Box::new(column_expr),
            op: upper_op,
            right: Box::new(ast::Expr::Value(
                ast::Value::SingleQuotedString(values[1].clone()).into(),
            )),
        };
        Ok(ast::Expr::Nested(Box::new(ast::Expr::BinaryOp {
            left: Box::new(lower),
            op: join_op,
            right: Box::new(upper),
        })))
    }

    fn multi_value_join<F>(
        values: Option<&[String]>,
        op_name: &str,
        negated: bool,
        mut make_expr: F,
    ) -> Result<ast::Expr>
    where
        F: FnMut(&str) -> ast::Expr,
    {
        let Some(values) = values else {
            return Err(DataFusionError::Plan(format!(
                "Filter values are required for \"{}\" operator",
                op_name
            )));
        };
        if values.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "At least one filter value is required for \"{}\" operator",
                op_name
            )));
        }
        let join_op = if negated {
            ast::BinaryOperator::And
        } else {
            ast::BinaryOperator::Or
        };
        let mut value_exprs = values.iter().map(|v| make_expr(v.as_str()));
        let first = value_exprs.next().unwrap();
        let combined = value_exprs.fold(first, |acc, e| ast::Expr::BinaryOp {
            left: Box::new(acc),
            op: join_op.clone(),
            right: Box::new(e),
        });
        Ok(if values.len() > 1 {
            ast::Expr::Nested(Box::new(combined))
        } else {
            combined
        })
    }

    fn like_family_expr(
        column_expr: &ast::Expr,
        filter: &V1LoadRequestQueryFilterItem,
        op_name: &str,
        shape: LikeShape,
        negated: bool,
    ) -> Result<ast::Expr> {
        Self::multi_value_join(filter.values.as_deref(), op_name, negated, |v| {
            // Backslash is the escape character the LIKE pattern parser of the
            // filter rewrite rules assumes, which is why it is not spelled out
            // with an ESCAPE clause: a pattern with an explicit escape
            // character takes a different rewrite path that does not produce a
            // Cube filter. The doubled backslash below is a single backslash
            // in the resulting string literal under standard_conforming_strings.
            let escaped = v
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_");
            let pattern = match shape {
                LikeShape::Contains => format!("%{}%", escaped),
                LikeShape::StartsWith => format!("{}%", escaped),
                LikeShape::EndsWith => format!("%{}", escaped),
            };
            ast::Expr::ILike {
                negated,
                any: false,
                expr: Box::new(column_expr.clone()),
                pattern: Box::new(ast::Expr::Value(
                    ast::Value::SingleQuotedString(pattern).into(),
                )),
                escape_char: None,
            }
        })
    }

    fn value_to_expr_by_member_type(value: &str, meta_member: &MetaMember) -> Result<ast::Expr> {
        let kind = match meta_member {
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
        };

        match kind {
            ValueKind::Numeric => Self::numeric_value_expr(value),
            // The filter rewrite rules read a boolean literal and render it
            // back as `true`/`false`, which is what the request carries
            ValueKind::Boolean => {
                let value = value.parse::<bool>().map_err(|_| {
                    DataFusionError::Plan(format!(
                        "Filter value must be a boolean, got {:?}",
                        value
                    ))
                })?;
                Ok(ast::Expr::Value(ast::Value::Boolean(value).into()))
            }
            ValueKind::String => Ok(ast::Expr::Value(
                ast::Value::SingleQuotedString(value.to_string()).into(),
            )),
        }
    }
}

/// Aggregations a measure may be projected or filtered with.
const AGG_FUNCTIONS: [&str; 7] = ["COUNT", "SUM", "AVG", "MIN", "MAX", "MEASURE", "AGGREGATE"];

/// How a filter value is rendered, per the type of the member it filters.
#[derive(Debug, Clone, Copy)]
enum ValueKind {
    Numeric,
    Boolean,
    String,
}

#[derive(Debug, Clone, Copy)]
enum LikeShape {
    Contains,
    StartsWith,
    EndsWith,
}

#[derive(Debug)]
enum MetaMember {
    Dimension(V1CubeMetaDimension),
    Measure(V1CubeMetaMeasure),
}

impl MetaMember {
    fn get_from_ctx(ctx: &MetaContext, cube_name: &str, member_name: &str) -> Result<Self> {
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

    fn short_name(&self) -> String {
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
enum MemberSource {
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

fn parse_single_query(sql: &str) -> Result<Box<ast::Query>> {
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

/// Applies a single action; a convenience wrapper over [`modify_sql_ast_many`].
#[cfg(test)]
fn modify_sql_ast(sql: &str, action: &ModifyAction, ctx: &MetaContext) -> Result<(String, bool)> {
    let (sql, applied) = modify_sql_ast_many(sql, std::slice::from_ref(action), ctx)?;
    Ok((sql, applied[0]))
}

/// Applies the actions in order to a single parsed AST, so that a batch of
/// filters costs one parse and one render rather than one of each per filter.
/// Returns the rewritten SQL and whether each action was applied.
fn modify_sql_ast_many(
    sql: &str,
    actions: &[ModifyAction],
    ctx: &MetaContext,
) -> Result<(String, Vec<bool>)> {
    let mut query = parse_single_query(sql)?;

    let mut applied = Vec::with_capacity(actions.len());
    for action in actions {
        applied.push(apply_action_to_outermost_select(
            query.as_mut(),
            action,
            ctx,
        )?);
    }

    let modified_sql = query.to_string();
    Ok((modified_sql, applied))
}

/// Drops the filter predicates of the WHERE and HAVING clauses of the
/// outermost SELECT, keeping every other predicate.
fn clear_outermost_filters(sql: &str) -> Result<String> {
    let mut query = parse_single_query(sql)?;
    let ast::Query { with, body, .. } = query.as_mut();
    let with = with.as_ref();
    let ast::SetExpr::Select(select) = body.as_mut() else {
        return Err(DataFusionError::NotImplemented(
            "Only plain SELECT statements are supported at the outermost level".to_string(),
        ));
    };
    assert_clauses_are_bounded(select)?;

    // The relations are needed to tell a filter on a member column from a
    // predicate on a column that only looks like one. They borrow the SELECT,
    // so the clauses are taken out first and put back once that borrow ends.
    let selection = select.selection.take();
    let having = select.having.take();
    let (selection, having) = {
        let relations = OutermostRelations::of(select, with.into_iter().collect());
        (
            selection.and_then(|expr| drop_filter_predicates(expr, &relations)),
            having.and_then(|expr| drop_filter_predicates(expr, &relations)),
        )
    };
    select.selection = selection;
    select.having = having;

    Ok(query.to_string())
}

/// Where the columns of a relation of the outermost FROM come from.
enum RelationColumns<'a> {
    /// A table, whose columns are the raw columns of the cube behind it.
    Cube,
    /// A derived table or a CTE reference, whose columns are what its query
    /// projects.
    Query(&'a ast::Query),
    /// A relation whose columns can't be attributed to anything - one that
    /// renames them positionally with a column list, so a name means a
    /// different thing outside the relation than inside it. Member
    /// resolution refuses these, and so must the recognizer.
    Opaque,
}

/// The relations of the outermost FROM, used to decide whether a column
/// reference in a predicate can be a Cube member.
struct OutermostRelations<'a> {
    /// Relations by the name they are referenced with.
    relations: Vec<(ast::Ident, RelationColumns<'a>)>,
    /// The CTE lists visible here, innermost first. A query nested in a CTE
    /// body can name its siblings, so the enclosing lists stay in scope.
    withs: Vec<&'a ast::With>,
}

impl<'a> OutermostRelations<'a> {
    fn of(select: &'a ast::Select, withs: Vec<&'a ast::With>) -> Self {
        let mut relations = Vec::new();
        for table_with_joins in &select.from {
            let factors = std::iter::once(&table_with_joins.relation)
                .chain(table_with_joins.joins.iter().map(|join| &join.relation));
            for factor in factors {
                match factor {
                    ast::TableFactor::Derived {
                        subquery,
                        alias: Some(alias),
                        ..
                    } => {
                        let columns = if alias.columns.is_empty() {
                            RelationColumns::Query(subquery.as_ref())
                        } else {
                            RelationColumns::Opaque
                        };
                        relations.push((alias.name.clone(), columns));
                    }
                    ast::TableFactor::Table { name, alias, .. } => {
                        let ast::ObjectName(parts) = name;
                        let Some(table_ident) = parts.last().and_then(|part| part.as_ident())
                        else {
                            continue;
                        };
                        let cte = withs.iter().find_map(|with| {
                            with.cte_tables.iter().find(|cte| {
                                cte.alias
                                    .name
                                    .value
                                    .eq_ignore_ascii_case(&table_ident.value)
                            })
                        });
                        let renames_columns = alias
                            .as_ref()
                            .is_some_and(|alias| !alias.columns.is_empty())
                            || cte.is_some_and(|cte| !cte.alias.columns.is_empty());
                        let columns = if renames_columns {
                            RelationColumns::Opaque
                        } else {
                            match cte {
                                Some(cte) => RelationColumns::Query(cte.query.as_ref()),
                                None => RelationColumns::Cube,
                            }
                        };
                        let relation_alias = match alias {
                            Some(alias) => alias.name.clone(),
                            None => table_ident.clone(),
                        };
                        relations.push((relation_alias, columns));
                    }
                    // Any other shape - a parenthesized join, a table
                    // function, an unaliased derived table - can't be
                    // described, but it is still a relation an unqualified
                    // column may come from, so it has to be counted
                    _ => relations.push((ast::Ident::new(""), RelationColumns::Opaque)),
                }
            }
        }
        Self { relations, withs }
    }

    /// The relations a column reference may belong to. An unqualified column
    /// can only be attributed when there is a single relation to attribute it
    /// to.
    fn candidates<'q>(
        &'q self,
        qualifier: Option<&'q ast::Ident>,
    ) -> impl Iterator<Item = &'q RelationColumns<'a>> {
        self.relations
            .iter()
            .filter(move |(name, _)| match qualifier {
                Some(qualifier) => name.value.eq_ignore_ascii_case(&qualifier.value),
                None => self.relations.len() == 1,
            })
            .map(|(_, columns)| columns)
    }

    /// Whether the expression is a column of a relation that stands for the
    /// cube itself. Member resolution only accepts an aggregation where the
    /// cube is in the same FROM, so an aggregation over a column forwarded by
    /// another relation is not a measure filter this API could restore.
    fn is_cube_column(&self, expr: &ast::Expr) -> bool {
        let (qualifier, _) = match expr {
            ast::Expr::Identifier(ident) => (None, ident),
            ast::Expr::CompoundIdentifier(idents) => match &idents[..] {
                [qualifier, column] => (Some(qualifier), column),
                _ => return false,
            },
            _ => return false,
        };
        self.candidates(qualifier)
            .any(|columns| matches!(columns, RelationColumns::Cube))
    }

    /// Whether the column reference can be a Cube member column. A column of
    /// a derived table or a CTE only can when that query exposes it directly
    /// rather than computing it, which is decided recursively.
    fn is_member_column(
        &self,
        qualifier: Option<&ast::Ident>,
        column: &ast::Ident,
        depth: usize,
        budget: &Cell<usize>,
    ) -> bool {
        self.candidates(qualifier).any(|columns| match columns {
            RelationColumns::Cube => true,
            RelationColumns::Query(query) => {
                query_exposes_column_directly(query, &self.withs, column, depth + 1, budget)
            }
            RelationColumns::Opaque => false,
        })
    }
}

/// Whether the query projects the column as a plain column reference or an
/// aggregation over one, as opposed to computing it. A projection that
/// forwards a column of a relation of its own is only as direct as that
/// column is, so attribution continues there.
fn query_exposes_column_directly<'a>(
    query: &'a ast::Query,
    outer_withs: &[&'a ast::With],
    column: &ast::Ident,
    depth: usize,
    budget: &Cell<usize>,
) -> bool {
    // Past the bound a member can't be resolved either, so the column is not
    // one this API can restore
    if depth >= MAX_RELATION_DEPTH || !spend_expansion(budget) {
        return false;
    }

    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    // A query declares CTEs of its own on top of the ones already visible
    let withs = query
        .with
        .as_ref()
        .into_iter()
        .chain(outer_withs.iter().copied())
        .collect();
    let relations = OutermostRelations::of(select, withs);

    // An output name is unique regardless of where it appears in the
    // projection, so an explicit item of that name shadows a wildcard even
    // when the wildcard comes first
    let mut has_wildcard = false;
    for item in &select.projection {
        match item {
            ast::SelectItem::UnnamedExpr(expr) => {
                // The output name of an unaliased item is the column name
                let name = match expr {
                    ast::Expr::Identifier(ident) => ident,
                    ast::Expr::CompoundIdentifier(idents) => match idents.last() {
                        Some(ident) => ident,
                        None => continue,
                    },
                    _ => continue,
                };
                if name.value.eq_ignore_ascii_case(&column.value) {
                    return is_member_column_ref(expr, &relations, depth, budget);
                }
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                if alias.value.eq_ignore_ascii_case(&column.value) {
                    return is_member_column_ref(expr, &relations, depth, budget);
                }
            }
            // A wildcard passes the underlying columns through unchanged
            ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(..) => {
                has_wildcard = true;
            }
            _ => continue,
        }
    }

    // What a wildcard passes through is only as direct as the relation it
    // passes it through from
    has_wildcard && relations.is_member_column(None, column, depth, budget)
}

/// How many relations deep a chain of derived tables and CTE references is
/// followed, both when resolving a member and when deciding whether a column
/// can be one. The two have to agree, or one side would call a column a
/// filter that the other can't restore afterwards. The bound also stops a
/// self-referencing CTE from recursing forever: such a query exhausts the
/// depth and is refused rather than resolved.
const MAX_RELATION_DEPTH: usize = 25;

/// Drops the conjuncts of a clause that are filter predicates. A join
/// condition, a subquery predicate or a predicate over a computed expression
/// is not a Cube filter, and must not be discarded along with them, as it can
/// neither be reported by filter extraction nor restored through this API.
fn drop_filter_predicates(expr: ast::Expr, relations: &OutermostRelations) -> Option<ast::Expr> {
    let kept = into_and_conjuncts(expr)
        .into_iter()
        .filter(|conjunct| !is_filter_predicate(conjunct, relations))
        .collect::<Vec<_>>();
    and_chain(kept)
}

/// Whether the expression is a predicate comparing a member column against
/// literals - the shape every Cube filter takes. A boolean combination of
/// such predicates is one too, so that filter groups are recognized as well.
/// The combination is walked iteratively: a group carries one predicate per
/// filter, and recursion would cost a frame for each.
fn is_filter_predicate(expr: &ast::Expr, relations: &OutermostRelations) -> bool {
    // One budget for the whole expression, so a large one can't be used to
    // multiply the relation walking either
    let budget = Cell::new(MAX_RELATION_EXPANSIONS);

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
                if !is_leaf_filter_predicate(expr, relations, &budget) {
                    return false;
                }
            }
        }
    }

    true
}

/// Whether a single predicate - no boolean combination - is a filter.
fn is_leaf_filter_predicate(
    expr: &ast::Expr,
    relations: &OutermostRelations,
    budget: &Cell<usize>,
) -> bool {
    let is_member_ref = |expr: &ast::Expr| is_member_column_ref(expr, relations, 0, budget);

    match expr {
        ast::Expr::BinaryOp { left, op, right } => match op {
            ast::BinaryOperator::Eq
            | ast::BinaryOperator::NotEq
            | ast::BinaryOperator::Lt
            | ast::BinaryOperator::LtEq
            | ast::BinaryOperator::Gt
            | ast::BinaryOperator::GtEq => {
                // A column compared against another column is a join
                // condition, not a filter
                (is_member_ref(left) && is_literal(right))
                    || (is_literal(left) && is_member_ref(right))
            }
            _ => false,
        },
        ast::Expr::InList { expr, list, .. } => is_member_ref(expr) && list.iter().all(is_literal),
        ast::Expr::IsNull(expr) | ast::Expr::IsNotNull(expr) => is_member_ref(expr),
        // A pattern with an explicit escape character takes a rewrite path
        // that doesn't produce a Cube filter, so it isn't one to drop either
        ast::Expr::Like {
            expr,
            pattern,
            escape_char,
            ..
        }
        | ast::Expr::ILike {
            expr,
            pattern,
            escape_char,
            ..
        } => escape_char.is_none() && is_member_ref(expr) && is_literal(pattern),
        ast::Expr::Between {
            expr, low, high, ..
        } => is_member_ref(expr) && is_literal(low) && is_literal(high),
        _ => false,
    }
}

/// The argument of an aggregation over a single expression, as measure
/// filters take.
fn aggregate_arg(func: &ast::Function) -> Option<&ast::Expr> {
    let ast::FunctionArguments::List(arg_list) = &func.args else {
        return None;
    };
    let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg))] = &arg_list.args[..] else {
        return None;
    };
    let ast::ObjectName(name_parts) = &func.name;
    let func_name = name_parts.last().and_then(|part| part.as_ident())?;
    AGG_FUNCTIONS
        .iter()
        .any(|agg| func_name.value.eq_ignore_ascii_case(agg))
        .then_some(arg)
}

/// Whether the expression is a reference to a column a Cube member could be
/// behind: a column reference, or an aggregation over one as measure filters
/// take, attributable to a relation of the FROM clause. A column of a derived
/// table or a CTE that is computed rather than passed through is not one,
/// however filter-shaped the predicate over it looks.
fn is_member_column_ref(
    expr: &ast::Expr,
    relations: &OutermostRelations,
    depth: usize,
    budget: &Cell<usize>,
) -> bool {
    let column_expr = match expr {
        ast::Expr::Function(func) => match aggregate_arg(func) {
            // Resolution only reaches a measure where the aggregation sits in
            // the query the cube is in, so an aggregation over a column a
            // relation merely forwards isn't a filter this API can restore
            Some(arg) if relations.is_cube_column(arg) => arg,
            _ => return false,
        },
        expr => expr,
    };
    match column_expr {
        ast::Expr::Identifier(ident) => relations.is_member_column(None, ident, depth, budget),
        ast::Expr::CompoundIdentifier(idents) => match &idents[..] {
            [qualifier, column] => {
                relations.is_member_column(Some(qualifier), column, depth, budget)
            }
            _ => false,
        },
        _ => false,
    }
}

fn is_literal(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Value(_) => true,
        // Negative numbers are parsed as a unary operation over a literal
        ast::Expr::UnaryOp { op, expr } => {
            matches!(op, ast::UnaryOperator::Minus | ast::UnaryOperator::Plus)
                && matches!(expr.as_ref(), ast::Expr::Value(_))
        }
        _ => false,
    }
}

/// Which clause of the outermost SELECT the filter belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClauseKind {
    Where,
    Having,
}

fn clause_mut(select: &mut ast::Select, kind: ClauseKind) -> &mut Option<ast::Expr> {
    match kind {
        ClauseKind::Where => &mut select.selection,
        ClauseKind::Having => &mut select.having,
    }
}

/// Applies the action to the outermost SELECT only. CTEs and subqueries are
/// never modified: the member must be resolvable in the outermost SELECT,
/// either as a column of a cube referenced directly in FROM, or as an output
/// column of a derived table / CTE reference that directly exposes the
/// dimension or measure. Post-processed and generated columns don't qualify.
fn apply_action_to_outermost_select(
    query: &mut ast::Query,
    action: &ModifyAction,
    ctx: &MetaContext,
) -> Result<bool> {
    // `with` is only read, and it is a field of its own, so a split borrow
    // keeps the whole CTE list out of the per-action work
    let ast::Query { with, body, .. } = query;
    let with = with.as_ref();
    let ast::SetExpr::Select(select) = body.as_mut() else {
        return Err(DataFusionError::NotImplemented(
            "Only plain SELECT statements are supported at the outermost level".to_string(),
        ));
    };
    assert_clauses_are_bounded(select)?;

    match action {
        ModifyAction::Add(filter) => {
            let (expr, kind) = require_filter_expr(filter, select, with, ctx)?;
            Ok(add_expr_to_clause(clause_mut(select, kind), expr))
        }
        ModifyAction::Remove(filter) => {
            // A filter whose member is not available in the outermost SELECT
            // can't be present in it either, so removal is a no-op
            let Some((expr, kind)) = resolve_filter_expr(filter, select, with, ctx)? else {
                return Ok(false);
            };
            Ok(remove_expr_from_clause(clause_mut(select, kind), &expr) > 0)
        }
        ModifyAction::Replace { old, new } => {
            let Some((old_expr, old_kind)) = resolve_filter_expr(old, select, with, ctx)? else {
                return Ok(false);
            };
            let (new_expr, new_kind) = require_filter_expr(new, select, with, ctx)?;
            if old_kind == new_kind {
                // Replace in place, preserving positions within the clause
                Ok(replace_expr_in_clause(clause_mut(select, old_kind), &old_expr, new_expr) > 0)
            } else {
                // The filters belong to different clauses: remove, then add
                if remove_expr_from_clause(clause_mut(select, old_kind), &old_expr) == 0 {
                    return Ok(false);
                }
                add_expr_to_clause(clause_mut(select, new_kind), new_expr);
                Ok(true)
            }
        }
    }
}

/// Same as [`resolve_filter_expr`], but a member that is not available in the
/// outermost SELECT is an error rather than `None`.
fn require_filter_expr(
    filter: &V1LoadRequestQueryFilterItem,
    select: &ast::Select,
    with: Option<&ast::With>,
    ctx: &MetaContext,
) -> Result<(ast::Expr, ClauseKind)> {
    resolve_filter_expr(filter, select, with, ctx)?.ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Filter {} is not available in the outermost SELECT",
            filter_description(filter)
        ))
    })
}

/// Resolves a filter or an and/or filter group to the SQL expression to
/// filter with and the clause of the outermost SELECT it belongs to.
/// All filters of a group must belong to the same clause. Returns `None` if
/// a member of the filter is not available in the outermost SELECT.
fn resolve_filter_expr(
    filter: &V1LoadRequestQueryFilterItem,
    select: &ast::Select,
    with: Option<&ast::With>,
    ctx: &MetaContext,
) -> Result<Option<(ast::Expr, ClauseKind)>> {
    if filter.and.is_some() && filter.or.is_some() {
        return Err(DataFusionError::Plan(
            "Filter can't be both \"and\" and \"or\" group at once".to_string(),
        ));
    }

    let group = filter
        .and
        .as_ref()
        .map(|items| (items, ast::BinaryOperator::And))
        .or_else(|| {
            filter
                .or
                .as_ref()
                .map(|items| (items, ast::BinaryOperator::Or))
        });
    let Some((items, join_op)) = group else {
        // A plain filter
        let (cube_name, member_name) = ModifyAction::get_cube_and_member_name(filter)?;
        let meta_member = MetaMember::get_from_ctx(ctx, &cube_name, &member_name)?;
        let Some(source) =
            resolve_member_source(select, with, &cube_name, &member_name, &meta_member)
        else {
            return Ok(None);
        };
        let kind = match (&source, &meta_member) {
            (MemberSource::CubeTable { .. }, MetaMember::Measure(_)) => ClauseKind::Having,
            _ => ClauseKind::Where,
        };
        let expr = ModifyAction::leaf_expr(filter, &source, &meta_member)?;
        return Ok(Some((expr, kind)));
    };

    if filter.member.is_some() || filter.operator.is_some() || filter.values.is_some() {
        return Err(DataFusionError::Plan(
            "Filter group can't have member, operator or values".to_string(),
        ));
    }
    if items.is_empty() {
        return Err(DataFusionError::Plan(
            "Filter group must contain at least one filter".to_string(),
        ));
    }

    let mut exprs = Vec::with_capacity(items.len());
    let mut group_kind = None;
    for item in items {
        let item_filter: V1LoadRequestQueryFilterItem = serde_json::from_value(item.clone())
            .map_err(|e| {
                DataFusionError::Plan(format!("Invalid filter in a filter group: {}", e))
            })?;
        let Some((expr, kind)) = resolve_filter_expr(&item_filter, select, with, ctx)? else {
            return Ok(None);
        };
        match group_kind {
            None => group_kind = Some(kind),
            Some(group_kind) if group_kind != kind => {
                return Err(DataFusionError::Plan(
                    "Filter group can't mix dimension (WHERE) and measure (HAVING) filters"
                        .to_string(),
                ))
            }
            Some(_) => {}
        }
        exprs.push(expr);
    }

    let multiple = exprs.len() > 1;
    let mut exprs_iter = exprs.into_iter();
    let first = exprs_iter.next().unwrap();
    let combined = exprs_iter.fold(first, |acc, expr| ast::Expr::BinaryOp {
        left: Box::new(acc),
        op: join_op.clone(),
        right: Box::new(expr),
    });
    let expr = if multiple {
        ast::Expr::Nested(Box::new(combined))
    } else {
        combined
    };
    Ok(Some((expr, group_kind.unwrap())))
}

/// Resolves the member to a column available in the outermost SELECT.
/// Returns `None` if the member is not available there.
fn resolve_member_source(
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
        let factors = std::iter::once(&table_with_joins.relation)
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

/// Upper bound on the number of predicates a clause of the outermost SELECT
/// may hold. The walks over a clause are iterative, so this is not about
/// their stack depth - it is a backstop against what the parser and the
/// renderer, which do recurse, are asked to carry: past roughly ten times
/// this, printing or dropping such a clause exhausts the stack. It sits an
/// order of magnitude above the clause [`MAX_FILTERS`] additions can build,
/// so a query this API produced is never one it then refuses.
const MAX_CLAUSE_PREDICATES: usize = 10_000;

/// Counts the predicates of a clause, iteratively so that counting can't
/// overflow the stack it is there to protect. Only the shapes the walks
/// recurse through are followed, since those are what cost a frame.
fn clause_predicate_count(expr: &ast::Expr) -> usize {
    let mut count = 0;
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        count += 1;
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
            _ => {}
        }
    }
    count
}

/// Rejects a SELECT whose clauses are too large to walk.
fn assert_clauses_are_bounded(select: &ast::Select) -> Result<()> {
    for clause in
        IntoIterator::into_iter([select.selection.as_ref(), select.having.as_ref()]).flatten()
    {
        let count = clause_predicate_count(clause);
        if count > MAX_CLAUSE_PREDICATES {
            return Err(DataFusionError::Plan(format!(
                "A clause of the outermost SELECT has {} predicates, more than the {} supported",
                count, MAX_CLAUSE_PREDICATES
            )));
        }
    }
    Ok(())
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

/// Finds the output column of a query which exposes the member, either
/// because the cube is in its own FROM - dimensions projected as direct
/// column references or via a wildcard, measures as an aliased aggregation of
/// the raw column - or because one of its relations exposes it and the
/// projection forwards that column unchanged. Anything post-processed or
/// generated doesn't expose the member.
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
                    // `SELECT *` exposes raw cube columns: dimensions keep
                    // their name, measures remain unaggregated and can't be
                    // filtered. Over a join the name may belong to any of the
                    // relations, so bare references are restricted the same way
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
        let factors = std::iter::once(&table_with_joins.relation)
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

/// Splits a clause into the conjuncts of its top-level AND chain, iteratively:
/// a clause holds one conjunct per filter, so walking it recursively would
/// cost a stack frame per filter. Parentheses around the whole clause are
/// kept, since a filter group is exactly what they may be.
fn into_and_conjuncts_exact(expr: ast::Expr) -> Vec<ast::Expr> {
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
            expr => conjuncts.push(expr),
        }
    }
    conjuncts
}

/// Same as [`into_and_conjuncts_exact`], but parenthesized AND chains are
/// transparent wherever they appear: the rewrite engine flattens a top-level
/// `and` group into sibling filters, so its members are filters in their own
/// right and have to be reachable as such.
fn into_and_conjuncts(expr: ast::Expr) -> Vec<ast::Expr> {
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
            ast::Expr::Nested(inner) if is_and_chain(&inner) => pending.push(*inner),
            expr => conjuncts.push(expr),
        }
    }
    conjuncts
}

/// The borrowing counterpart of [`into_and_conjuncts_exact`].
fn and_conjuncts_exact(expr: &ast::Expr) -> Vec<&ast::Expr> {
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
            expr => conjuncts.push(expr),
        }
    }
    conjuncts
}

/// The borrowing counterpart of [`into_and_conjuncts`].
fn and_conjuncts(expr: &ast::Expr) -> Vec<&ast::Expr> {
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
            ast::Expr::Nested(inner) if is_and_chain(inner) => pending.push(inner),
            expr => conjuncts.push(expr),
        }
    }
    conjuncts
}

/// Joins conjuncts back into a left-deep AND chain.
fn and_chain(conjuncts: Vec<ast::Expr>) -> Option<ast::Expr> {
    conjuncts
        .into_iter()
        .reduce(|left, right| ast::Expr::BinaryOp {
            left: Box::new(left),
            op: ast::BinaryOperator::And,
            right: Box::new(right),
        })
}

/// Appends the expression to the clause with AND unless an identical
/// expression is already present.
fn add_expr_to_clause(option_clause: &mut Option<ast::Expr>, expr: ast::Expr) -> bool {
    // The clause is taken rather than borrowed: it grows by a predicate per
    // addition, and cloning it each time would make a batch quadratic
    let Some(clause) = option_clause.take() else {
        *option_clause = Some(expr);
        return true;
    };
    if clause_contains_expr(&clause, &expr) {
        *option_clause = Some(clause);
        return false;
    }
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
    true
}

/// The filters a needle stands for besides itself. The rewrite engine reports
/// a top-level `and` group as sibling filters, so the group is present when
/// each of its members is, and removing it removes each of them. Anything
/// else, an `or` group included, stands only for itself.
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

/// Whether an identical expression is present in the top-level AND chain.
fn clause_contains_expr(clause: &ast::Expr, needle: &ast::Expr) -> bool {
    let conjuncts = all_conjuncts(clause);
    if conjuncts.contains(&needle) {
        return true;
    }

    let members = group_members(needle);
    !members.is_empty() && members.iter().all(|member| conjuncts.contains(member))
}

/// Splits the clause the way that matches the needle: as it stands, so that a
/// filter group is matched as the group it is, and failing that with nested
/// AND chains looked through, so that a member of one is reachable on its
/// own. Returns `None` when the needle is in neither, leaving the clause to
/// be put back untouched.
fn matching_conjuncts(
    clause: ast::Expr,
    needle: &ast::Expr,
) -> std::result::Result<Vec<ast::Expr>, ast::Expr> {
    if and_conjuncts_exact(&clause)
        .into_iter()
        .any(|conjunct| conjunct == needle)
    {
        return Ok(into_and_conjuncts_exact(clause));
    }
    if and_conjuncts(&clause)
        .into_iter()
        .any(|conjunct| conjunct == needle)
    {
        return Ok(into_and_conjuncts(clause));
    }
    Err(clause)
}

/// Removes all identical expressions from the top-level AND chain.
/// Returns the number of expressions removed.
fn remove_expr_from_clause(option_clause: &mut Option<ast::Expr>, needle: &ast::Expr) -> usize {
    let Some(clause) = option_clause.take() else {
        return 0;
    };

    // Which split matches is decided before the clause is taken apart, so a
    // needle that isn't there leaves it exactly as it was
    let clause = match matching_conjuncts(clause, needle) {
        Ok(conjuncts) => {
            let before = conjuncts.len();
            let kept = conjuncts
                .into_iter()
                .filter(|conjunct| conjunct != needle)
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
    let holds_every_member = !members.is_empty() && {
        let conjuncts = all_conjuncts(&clause);
        members.iter().all(|member| conjuncts.contains(member))
    };
    if !holds_every_member {
        *option_clause = Some(clause);
        return 0;
    }

    let members = members.into_iter().cloned().collect::<Vec<_>>();
    let conjuncts = into_and_conjuncts(clause);
    let before = conjuncts.len();
    let kept = conjuncts
        .into_iter()
        .filter(|conjunct| !members.contains(conjunct))
        .collect::<Vec<_>>();
    let removed = before - kept.len();
    *option_clause = and_chain(kept);
    removed
}

/// Replaces all identical expressions in place within the top-level AND
/// chain, preserving their positions. Returns the number of replacements.
fn replace_expr_in_clause(
    option_clause: &mut Option<ast::Expr>,
    old: &ast::Expr,
    new: ast::Expr,
) -> usize {
    let Some(clause) = option_clause.take() else {
        return 0;
    };

    let clause = match matching_conjuncts(clause, old) {
        Ok(conjuncts) => {
            let mut replaced = 0;
            let conjuncts = conjuncts
                .into_iter()
                .map(|conjunct| {
                    if &conjunct == old {
                        replaced += 1;
                        new.clone()
                    } else {
                        conjunct
                    }
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
    let removed = remove_expr_from_clause(option_clause, old);
    if removed == 0 {
        return 0;
    }
    add_expr_to_clause(option_clause, new);
    removed
}

/// Extracts Cube filters from all CubeScan nodes of a logical plan, including
/// those of CTEs and subqueries. Time dimension date ranges are represented as
/// `inDateRange` filter items.
pub fn extract_filters_from_plan(
    plan: &LogicalPlan,
) -> std::result::Result<Vec<V1LoadRequestQueryFilterItem>, CubeError> {
    struct CollectFiltersVisitor(Vec<V1LoadRequestQueryFilterItem>);

    impl PlanVisitor for CollectFiltersVisitor {
        type Error = CubeError;

        fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, Self::Error> {
            if let LogicalPlan::Extension(ext) = plan {
                if let Some(scan_node) = ext.node.as_any().downcast_ref::<CubeScanNode>() {
                    if let Some(filters) = &scan_node.request.filters {
                        self.0.extend(filters.iter().cloned());
                    }
                    for time_dimension in scan_node.request.time_dimensions.iter().flatten() {
                        let Some(date_range) = &time_dimension.date_range else {
                            continue;
                        };
                        let values = match date_range {
                            serde_json::Value::Array(values) => values
                                .iter()
                                .filter_map(|value| value.as_str().map(|s| s.to_string()))
                                .collect(),
                            serde_json::Value::String(value) => vec![value.clone()],
                            _ => continue,
                        };
                        self.0.push(V1LoadRequestQueryFilterItem {
                            member: Some(time_dimension.dimension.clone()),
                            operator: Some("inDateRange".to_string()),
                            values: Some(values),
                            ..Default::default()
                        });
                    }
                } else if let Some(wrapper_node) =
                    ext.node.as_any().downcast_ref::<CubeScanWrapperNode>()
                {
                    wrapper_node.wrapped_plan.accept(self)?;
                } else if let Some(wrapper_node) =
                    ext.node.as_any().downcast_ref::<CubeScanWrappedSqlNode>()
                {
                    wrapper_node.wrapped_plan.accept(self)?;
                }
            }
            Ok(true)
        }
    }

    let mut visitor = CollectFiltersVisitor(Vec::new());
    // The extracted set is used as the verification oracle, so a partial
    // collection must not be mistaken for a complete one
    plan.accept(&mut visitor)?;

    // Dedup while preserving order
    let mut seen = HashSet::new();
    Ok(visitor
        .0
        .into_iter()
        .filter(|filter| seen.insert(filter_key(filter)))
        .collect())
}

type FilterKey = String;

/// A canonical representation of a filter (or an and/or filter group)
/// used for perfect-match comparison.
fn filter_key(filter: &V1LoadRequestQueryFilterItem) -> FilterKey {
    normalized_filter_json(filter).to_string()
}

/// Keys that have to be present in the filters of a plan for the filter to
/// count as applied. A top-level `and` group is flattened by the rewrite
/// engine into sibling filters, so it is verified through its members.
fn verification_keys(filter: &V1LoadRequestQueryFilterItem) -> Vec<FilterKey> {
    let json = normalized_filter_json(filter);
    match json.get("and").and_then(|items| items.as_array()) {
        Some(items) => items.iter().map(|item| item.to_string()).collect(),
        None => vec![json.to_string()],
    }
}

/// LIKE-family operators, which take any number of values.
const LIKE_FAMILY_OPERATORS: [&str; 6] = [
    "contains",
    "notContains",
    "startsWith",
    "notStartsWith",
    "endsWith",
    "notEndsWith",
];

/// The canonical JSON of a filter after the normalizations that this module
/// and the rewrite engine perform on the way to a plan, applied at every
/// depth of a filter tree:
///
/// - a group of a single member is emitted as that member alone;
/// - a LIKE-family filter of several values is emitted as a chain of
///   single-value predicates, joined by OR for the plain operators and by AND
///   for the negated ones;
/// - a group member of the same kind as its enclosing group is flattened into
///   it, as the engine collapses same-operator nesting.
///
/// The planner's own output is already in this form, so normalizing both
/// sides of a comparison leaves it untouched.
fn normalized_filter_json(filter: &V1LoadRequestQueryFilterItem) -> serde_json::Value {
    let group = filter
        .and
        .as_ref()
        .map(|items| ("and", items))
        .or_else(|| filter.or.as_ref().map(|items| ("or", items)));

    if let Some((op, items)) = group {
        let items = items
            .iter()
            .map(|item| {
                match serde_json::from_value::<V1LoadRequestQueryFilterItem>(item.clone()) {
                    Ok(item_filter) => normalized_filter_json(&item_filter),
                    Err(_) => item.clone(),
                }
            })
            .collect::<Vec<_>>();

        if let [single] = &items[..] {
            return single.clone();
        }

        let mut flattened = Vec::with_capacity(items.len());
        for item in items {
            match item.get(op).and_then(|nested| nested.as_array()) {
                Some(nested) => flattened.extend(nested.iter().cloned()),
                None => flattened.push(item),
            }
        }
        return serde_json::json!({ op: flattened });
    }

    if let Some(expanded) = like_family_expansion(filter) {
        return expanded;
    }

    leaf_filter_json(filter)
}

/// Expands a LIKE-family filter of several values into the group of
/// single-value filters it is emitted as. Returns `None` for anything else.
fn like_family_expansion(filter: &V1LoadRequestQueryFilterItem) -> Option<serde_json::Value> {
    let operator = filter.operator.as_deref()?;
    let values = filter.values.as_ref()?;
    if values.len() < 2 || !LIKE_FAMILY_OPERATORS.contains(&operator) {
        return None;
    }

    let items = values
        .iter()
        .map(|value| {
            leaf_filter_json(&V1LoadRequestQueryFilterItem {
                member: filter.member.clone(),
                operator: Some(operator.to_string()),
                values: Some(vec![value.clone()]),
                ..Default::default()
            })
        })
        .collect::<Vec<_>>();

    // The negated operators chain with AND, the plain ones with OR
    let op = if operator.starts_with("not") {
        "and"
    } else {
        "or"
    };
    Some(serde_json::json!({ op: items }))
}

fn leaf_filter_json(filter: &V1LoadRequestQueryFilterItem) -> serde_json::Value {
    let values = filter
        .values
        .iter()
        .flatten()
        .map(|value| normalize_filter_value(value))
        .collect::<Vec<_>>();
    serde_json::json!({
        "member": filter.member.as_deref().unwrap_or_default(),
        "operator": filter.operator.as_deref().unwrap_or_default(),
        "values": values,
    })
}

/// The planner canonicalizes date literals to ISO-8601 in UTC, so a filter
/// requested as `2024-01-01` comes back as `2024-01-01T00:00:00.000Z`. Such
/// values are reduced to a common form so that they compare equal. Values
/// that don't look like a date are left alone, so that a plain string value
/// is never altered.
fn normalize_filter_value(value: &str) -> String {
    if !is_iso_date_like(value) {
        return value.to_string();
    }

    let value = strip_any_suffix(value, &["Z", "+00:00", "+0000"]);
    let value = strip_any_suffix(value, &[".000"]);
    let value = strip_any_suffix(value, &["T00:00:00", " 00:00:00"]);
    value.to_string()
}

fn strip_any_suffix<'a>(value: &'a str, suffixes: &[&str]) -> &'a str {
    suffixes
        .iter()
        .find_map(|suffix| value.strip_suffix(suffix))
        .unwrap_or(value)
}

/// Whether the value starts with an ISO-8601 date, optionally followed by a
/// time part.
fn is_iso_date_like(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 10
        && bytes[..10].iter().enumerate().all(|(i, b)| match i {
            4 | 7 => *b == b'-',
            _ => b.is_ascii_digit(),
        })
        && (bytes.len() == 10 || bytes[10] == b'T' || bytes[10] == b' ')
}

/// Upper bound on the number of filters a single request may carry. A whole
/// batch costs one parse and one plan, so the bound is about the size of the
/// predicate the rewrite engine ends up saturating over rather than about the
/// number of requests. A filter group nests an arbitrary number of leaves
/// inside one array entry, so the leaves are what is counted.
const MAX_FILTERS: usize = 500;

fn assert_filter_count(
    filters: &[V1LoadRequestQueryFilterItem],
) -> std::result::Result<(), CubeError> {
    let count = filters.iter().map(count_filter_leaves).sum::<usize>();
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

/// Result of updating filters of a SQL query.
#[derive(Debug)]
pub struct SqlFiltersUpdate {
    /// The rewritten SQL query.
    pub sql: String,
    /// Filters extracted from the logical plan of the rewritten query.
    pub filters: Vec<V1LoadRequestQueryFilterItem>,
}

/// Extracts the Cube filters of a SQL query from its logical plan.
/// Time dimension date ranges are represented as `inDateRange` filter items.
pub async fn get_sql_filters(
    sql: &str,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> std::result::Result<Vec<V1LoadRequestQueryFilterItem>, CubeError> {
    let query_plan = convert_sql_to_cube_query(sql, meta, session)
        .await
        .map_err(|e| CubeError::user(format!("Failed to plan the query: {}", e)))?;
    let logical_plan = query_plan.try_as_logical_plan()?;
    extract_filters_from_plan(logical_plan)
}

/// A short human-readable filter description for error messages.
fn filter_description(filter: &V1LoadRequestQueryFilterItem) -> String {
    if let Some(member) = &filter.member {
        format!("on \"{}\"", member)
    } else if filter.and.is_some() {
        "group \"and\"".to_string()
    } else if filter.or.is_some() {
        "group \"or\"".to_string()
    } else {
        "".to_string()
    }
}

async fn plan_and_extract_filters(
    sql: &str,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
    error_prefix: &str,
) -> std::result::Result<Vec<V1LoadRequestQueryFilterItem>, CubeError> {
    let query_plan = convert_sql_to_cube_query(sql, meta, session)
        .await
        .map_err(|e| CubeError::user(format!("{}: {}", error_prefix, e)))?;
    let logical_plan = query_plan.try_as_logical_plan()?;
    extract_filters_from_plan(logical_plan)
}

/// Adds the filters to the SQL query and verifies each of them is picked up
/// by the logical plan of the rewritten query.
async fn add_filters_and_verify(
    sql: String,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> std::result::Result<SqlFiltersUpdate, CubeError> {
    let actions = filters
        .iter()
        .map(|filter| ModifyAction::Add(filter.clone()))
        .collect::<Vec<_>>();
    let (new_sql, _) =
        modify_sql_ast_many(&sql, &actions, &meta).map_err(|e| CubeError::user(e.to_string()))?;

    // Extract filters from the new logical plan and verify the changes
    let new_filters = plan_and_extract_filters(
        &new_sql,
        meta,
        session,
        "Failed to plan the rewritten query",
    )
    .await?;
    let new_keys = new_filters.iter().map(filter_key).collect::<HashSet<_>>();

    for filter in filters {
        if !verification_keys(filter)
            .iter()
            .all(|key| new_keys.contains(key))
        {
            return Err(CubeError::user(format!(
                "Filter {} was not applied to the query",
                filter_description(filter)
            )));
        }
    }

    Ok(SqlFiltersUpdate {
        sql: new_sql,
        filters: new_filters,
    })
}

/// Adds the filters to the SQL query, modifying the outermost SELECT only.
/// Filters already present in the query are left as is. The changes are
/// verified against the logical plan of the rewritten query; note that
/// verification is plan-wide, so it can also be satisfied by an equal filter
/// living in a CTE or a subquery.
pub async fn add_sql_filters(
    sql: &str,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> std::result::Result<SqlFiltersUpdate, CubeError> {
    assert_filter_count(filters)?;

    // Make sure the original query plans before rewriting
    plan_and_extract_filters(
        sql,
        meta.clone(),
        session.clone(),
        "Failed to plan the query",
    )
    .await?;

    add_filters_and_verify(sql.to_string(), filters, meta, session).await
}

/// Replaces all filters of the outermost SELECT with the specified set: the
/// filter predicates of the outermost WHERE and HAVING clauses are dropped,
/// and the new filters are added. Anything in a CTE or a subquery is left
/// alone, as is every predicate of the outermost clauses that is not
/// recognized as a filter - a join condition, a subquery predicate, or a
/// predicate over a column that no Cube member can be behind. Recognition is
/// a syntactic approximation of what filter extraction reports, so a
/// predicate this API can't restore is only preserved as far as that
/// approximation holds. The added filters are verified against the logical
/// plan of the rewritten query.
pub async fn set_sql_filters(
    sql: &str,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> std::result::Result<SqlFiltersUpdate, CubeError> {
    assert_filter_count(filters)?;

    // Make sure the original query plans before rewriting
    plan_and_extract_filters(
        sql,
        meta.clone(),
        session.clone(),
        "Failed to plan the query",
    )
    .await?;

    let new_sql = clear_outermost_filters(sql).map_err(|e| CubeError::user(e.to_string()))?;

    add_filters_and_verify(new_sql, filters, meta, session).await
}

/// Attempts to delete the filters from the SQL query, modifying the
/// outermost SELECT only. All occurrences of equal filters are deleted;
/// filters not present in the outermost SELECT are ignored, including those
/// only present in a CTE or a subquery.
///
/// Unlike additions, deletions are not checked against the filters of the
/// rewritten plan: filter extraction is plan-wide, so a filter that also
/// lives in a CTE or a subquery stays in the extracted set no matter what
/// was deleted from the outermost SELECT, and such a check could only ever
/// produce false alarms. Removal from the outermost clause is decided on the
/// AST itself, which is exact. The rewritten query is still planned, both to
/// reject a rewrite that doesn't compile and to report the resulting filters.
pub async fn delete_sql_filters(
    sql: &str,
    filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> std::result::Result<SqlFiltersUpdate, CubeError> {
    assert_filter_count(filters)?;

    // Make sure the original query plans before rewriting
    plan_and_extract_filters(
        sql,
        meta.clone(),
        session.clone(),
        "Failed to plan the query",
    )
    .await?;

    let actions = filters
        .iter()
        .map(|filter| ModifyAction::Remove(filter.clone()))
        .collect::<Vec<_>>();
    let (new_sql, _) =
        modify_sql_ast_many(sql, &actions, &meta).map_err(|e| CubeError::user(e.to_string()))?;

    // Make sure the rewritten query still plans, and report its filters
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

/// Replaces one exact set of Cube filters of a SQL query with another,
/// modifying the outermost SELECT only. Every old filter must perfectly
/// match a filter present in the outermost SELECT; all occurrences of equal
/// filters are replaced. When replacing one filter with another within the
/// same clause, positions within the clause are preserved.
///
/// The replacement filters are verified against the logical plan of the
/// rewritten query. Removal of the replaced filters is decided on the AST,
/// for the reason given on [`delete_sql_filters`].
pub async fn replace_sql_filters(
    sql: &str,
    old_filters: &[V1LoadRequestQueryFilterItem],
    new_filters: &[V1LoadRequestQueryFilterItem],
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> std::result::Result<SqlFiltersUpdate, CubeError> {
    if old_filters.is_empty() {
        return Err(CubeError::user(
            "At least one filter to replace is required".to_string(),
        ));
    }

    assert_filter_count(old_filters)?;
    assert_filter_count(new_filters)?;

    // Make sure the original query plans before rewriting
    plan_and_extract_filters(
        sql,
        meta.clone(),
        session.clone(),
        "Failed to plan the query",
    )
    .await?;

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
    let (new_sql, applied) =
        modify_sql_ast_many(sql, &actions, &meta).map_err(|e| CubeError::user(e.to_string()))?;

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

    // Extract filters from the new logical plan and verify the changes
    let extracted_filters = plan_and_extract_filters(
        &new_sql,
        meta,
        session,
        "Failed to plan the rewritten query",
    )
    .await?;
    let extracted_keys = extracted_filters
        .iter()
        .map(filter_key)
        .collect::<HashSet<_>>();

    for filter in new_filters {
        if !verification_keys(filter)
            .iter()
            .all(|key| extracted_keys.contains(key))
        {
            return Err(CubeError::user(format!(
                "Replacement filter {} was not applied to the query",
                filter_description(filter)
            )));
        }
    }

    Ok(SqlFiltersUpdate {
        sql: new_sql,
        filters: extracted_filters,
    })
}

#[cfg(test)]
mod tests {
    use std::slice;

    use super::*;
    use crate::compile::test::get_test_tenant_ctx;

    #[test]
    fn test_modify_sql_ast() -> Result<()> {
        let sql = r#"
            SELECT
                KibanaSampleDataEcommerce.customer_gender,
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price,
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
            ORDER BY 1
        "#;

        // Test adding "equals" filter
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notEquals" filter with multiple values
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEquals".to_string()),
            values: Some(vec![
                "test1".to_string(),
                "test2".to_string(),
                "test3".to_string(),
            ]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT IN ('test1', 'test2', 'test3') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "notEquals" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEquals".to_string()),
            values: Some(vec![
                "test1".to_string(),
                "test2".to_string(),
                "test3".to_string(),
            ]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing non-existing filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEquals".to_string()),
            values: Some(vec![
                "test1".to_string(),
                "test2".to_string(),
                "test3".to_string(),
            ]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        // Make sure no modifications were made
        assert!(!applied);

        // Test adding "contains" filter with a single value
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("contains".to_string()),
            values: Some(vec!["abc".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%abc%' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "contains" filter with multiple values (OR-combined, escaped)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("contains".to_string()),
            values: Some(vec!["x".to_string(), "y%z_w\\v".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%abc%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%x%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%y\\%z\\_w\\\\v%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notContains" filter with a single value
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notContains".to_string()),
            values: Some(vec!["foo".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%abc%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%x%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%y\\%z\\_w\\\\v%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%foo%' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notContains" filter with multiple values (AND-combined, escaped)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notContains".to_string()),
            values: Some(vec!["bar".to_string(), "baz%_\\qux".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%abc%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%x%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%y\\%z\\_w\\\\v%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%bar%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%baz\\%\\_\\\\qux%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing single-value "contains" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("contains".to_string()),
            values: Some(vec!["abc".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%x%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%y\\%z\\_w\\\\v%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%bar%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%baz\\%\\_\\\\qux%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing multi-value "contains" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("contains".to_string()),
            values: Some(vec!["x".to_string(), "y%z_w\\v".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%bar%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%baz\\%\\_\\\\qux%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing single-value "notContains" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notContains".to_string()),
            values: Some(vec!["foo".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%bar%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%baz\\%\\_\\\\qux%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing multi-value "notContains" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notContains".to_string()),
            values: Some(vec!["bar".to_string(), "baz%_\\qux".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "startsWith" filter with a single value
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("startsWith".to_string()),
            values: Some(vec!["pre".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "startsWith" filter with multiple values (OR-combined, escaped)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("startsWith".to_string()),
            values: Some(vec!["a".to_string(), "b%".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notStartsWith" filter with a single value
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notStartsWith".to_string()),
            values: Some(vec!["foo".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notStartsWith" filter with multiple values (AND-combined, escaped)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notStartsWith".to_string()),
            values: Some(vec!["x".to_string(), "_y".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "endsWith" filter with a single value
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("endsWith".to_string()),
            values: Some(vec!["end".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%end' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "endsWith" filter with multiple values (OR-combined, escaped)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("endsWith".to_string()),
            values: Some(vec!["m".to_string(), "n\\o".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%end' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%m' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%n\\\\o') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notEndsWith" filter with a single value
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEndsWith".to_string()),
            values: Some(vec!["tail".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%end' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%m' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%n\\\\o') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%tail' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notEndsWith" filter with multiple values (AND-combined, escaped)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEndsWith".to_string()),
            values: Some(vec!["p".to_string(), "q_r".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'pre%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'a%' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE 'b\\%%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'foo%' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE 'x%' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '\\_y%') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%end' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%m' \
                    OR KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%n\\\\o') \
                AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%tail' \
                AND (KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%p' \
                    AND KibanaSampleDataEcommerce.\"customer_gender\" NOT ILIKE '%q\\_r') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing all four newly-added prefix/suffix filters in reverse insertion order
        let remove_ops: Vec<(&str, Vec<&str>)> = vec![
            ("notEndsWith", vec!["p", "q_r"]),
            ("notEndsWith", vec!["tail"]),
            ("endsWith", vec!["m", "n\\o"]),
            ("endsWith", vec!["end"]),
            ("notStartsWith", vec!["x", "_y"]),
            ("notStartsWith", vec!["foo"]),
            ("startsWith", vec!["a", "b%"]),
            ("startsWith", vec!["pre"]),
        ];
        let mut sql = modified_sql;
        for (op, values) in remove_ops {
            let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some(op.to_string()),
                values: Some(values.into_iter().map(|s| s.to_string()).collect()),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
            assert!(applied, "remove {} should be applied", op);
            sql = next_sql;
        }
        assert_eq!(
            sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );

        // Test adding "gt" filter (integer value) on a numeric dimension
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["42".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "gt" filter (decimal value) on same member, AND-combined
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["3.14".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 3.14 \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing the integer "gt" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["42".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 3.14 \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test "gt" with non-numeric value rejected
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["not_a_number".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("must be numeric"),
            "unexpected error: {}",
            err
        );

        // Test "gt" with wrong number of values rejected
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["1".to_string(), "2".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("Exactly one filter value"),
            "unexpected error: {}",
            err
        );

        // Test adding "gte", "lt", "lte" filters chained on the numeric dimension
        let add_ops: Vec<(&str, &str)> = vec![("gte", "5"), ("lt", "100"), ("lte", "10.5")];
        let mut sql = modified_sql;
        for (op, value) in add_ops {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some(op.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
            assert!(applied, "add {} should be applied", op);
            sql = next_sql;
        }
        assert_eq!(
            sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 3.14 \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" >= 5 \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" < 100 \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" <= 10.5 \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );

        // Test removing "gte", "lt", "lte", and remaining "gt" filters
        let remove_ops: Vec<(&str, &str)> =
            vec![("lte", "10.5"), ("lt", "100"), ("gte", "5"), ("gt", "3.14")];
        for (op, value) in remove_ops {
            let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some(op.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
            assert!(applied, "remove {} should be applied", op);
            sql = next_sql;
        }
        assert_eq!(
            sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );

        // Test non-numeric value rejected for each of gte/lt/lte
        for op in ["gte", "lt", "lte"] {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some(op.to_string()),
                values: Some(vec!["not_a_number".to_string()]),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let err = modify_sql_ast(&sql, &action, &ctx).unwrap_err();
            assert!(
                err.to_string().contains("must be numeric"),
                "unexpected error for {}: {}",
                op,
                err
            );
        }

        // Test adding "set" filter (no values)
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("set".to_string()),
            values: None,
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" IS NOT NULL \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "set" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("set".to_string()),
            values: None,
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "notSet" filter (values ignored)
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notSet".to_string()),
            values: Some(vec!["ignored".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" IS NULL \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "notSet" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notSet".to_string()),
            values: None,
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "inDateRange" filter
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("inDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND (KibanaSampleDataEcommerce.\"order_date\" >= '2024-01-01' \
                    AND KibanaSampleDataEcommerce.\"order_date\" <= '2024-12-31') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "inDateRange" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("inDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test "inDateRange" with wrong number of values rejected
        for values in [
            vec![],
            vec!["2024-01-01".to_string()],
            vec![
                "2024-01-01".to_string(),
                "2024-06-01".to_string(),
                "2024-12-31".to_string(),
            ],
        ] {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                operator: Some("inDateRange".to_string()),
                values: Some(values),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
            assert!(
                err.to_string().contains("Exactly two filter values"),
                "unexpected error: {}",
                err
            );
        }

        // Test adding "notInDateRange" filter
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("notInDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"order_date\" NOT BETWEEN '2024-01-01' AND '2024-12-31' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "notInDateRange" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("notInDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test "notInDateRange" with wrong number of values rejected
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("notInDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("Exactly two filter values"),
            "unexpected error: {}",
            err
        );

        // Test adding "beforeDate" filter
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("beforeDate".to_string()),
            values: Some(vec!["2024-06-01".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"order_date\" < '2024-06-01' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "beforeDate" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("beforeDate".to_string()),
            values: Some(vec!["2024-06-01".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test "beforeDate" with wrong number of values rejected
        for values in [vec![], vec!["a".to_string(), "b".to_string()]] {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                operator: Some("beforeDate".to_string()),
                values: Some(values),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
            assert!(
                err.to_string().contains("Exactly one filter value"),
                "unexpected error: {}",
                err
            );
        }

        // Test adding "beforeOrOnDate" filter
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("beforeOrOnDate".to_string()),
            values: Some(vec!["2024-06-01".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"order_date\" <= '2024-06-01' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "beforeOrOnDate" filter
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("beforeOrOnDate".to_string()),
            values: Some(vec!["2024-06-01".to_string()]),
            ..Default::default()
        });
        let ctx = get_test_tenant_ctx();
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test adding "afterDate" then "afterOrOnDate" filters; verify SQL operators
        let add_ops: Vec<(&str, &str, &str)> = vec![
            ("afterDate", "2024-06-01", ">"),
            ("afterOrOnDate", "2024-07-01", ">="),
        ];
        let mut sql = modified_sql;
        for (op, value, _) in &add_ops {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                operator: Some(op.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
            assert!(applied, "add {} should be applied", op);
            sql = next_sql;
        }
        assert_eq!(
            sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"order_date\" > '2024-06-01' \
                AND KibanaSampleDataEcommerce.\"order_date\" >= '2024-07-01' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );

        // Test removing both filters
        for (op, value, _) in add_ops.iter().rev() {
            let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                operator: Some(op.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            });
            let ctx = get_test_tenant_ctx();
            let (next_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
            assert!(applied, "remove {} should be applied", op);
            sql = next_sql;
        }
        assert_eq!(
            sql,
            "\
            SELECT \
                KibanaSampleDataEcommerce.customer_gender, \
                SUM(KibanaSampleDataEcommerce.taxful_total_price) AS taxful_total_price, \
                MEASURE(KibanaSampleDataEcommerce.custom_measure) AS custom_measure \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_cte_outermost_only() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        let sql = "\
            WITH gendered AS (\
                SELECT \
                    KibanaSampleDataEcommerce.customer_gender AS gender, \
                    MAX(KibanaSampleDataEcommerce.maxPrice) AS max_price \
                FROM KibanaSampleDataEcommerce \
                GROUP BY 1\
            ) \
            SELECT gender, max_price FROM gendered\
        ";

        // Dimension exposed by the CTE: filtered in the outermost WHERE,
        // the CTE itself is left untouched
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            WITH gendered AS (\
                SELECT \
                    KibanaSampleDataEcommerce.customer_gender AS gender, \
                    MAX(KibanaSampleDataEcommerce.maxPrice) AS max_price \
                FROM KibanaSampleDataEcommerce \
                GROUP BY 1\
            ) \
            SELECT gender, max_price FROM gendered \
            WHERE gendered.gender = 'test'\
            "
        );
        assert!(applied);

        // Measure exposed by the CTE as an aggregation: filtered as a plain
        // column in the outermost WHERE
        let sql = modified_sql;
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["42".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            WITH gendered AS (\
                SELECT \
                    KibanaSampleDataEcommerce.customer_gender AS gender, \
                    MAX(KibanaSampleDataEcommerce.maxPrice) AS max_price \
                FROM KibanaSampleDataEcommerce \
                GROUP BY 1\
            ) \
            SELECT gender, max_price FROM gendered \
            WHERE gendered.gender = 'test' AND gendered.max_price > 42\
            "
        );
        assert!(applied);

        // Removing both filters restores the original query
        let sql = modified_sql;
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["42".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert!(applied);
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&modified_sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            WITH gendered AS (\
                SELECT \
                    KibanaSampleDataEcommerce.customer_gender AS gender, \
                    MAX(KibanaSampleDataEcommerce.maxPrice) AS max_price \
                FROM KibanaSampleDataEcommerce \
                GROUP BY 1\
            ) \
            SELECT gender, max_price FROM gendered\
            "
        );
        assert!(applied);

        // Member not exposed by the CTE is rejected
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("beforeDate".to_string()),
            values: Some(vec!["2024-06-01".to_string()]),
            ..Default::default()
        });
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("is not available in the outermost SELECT"),
            "unexpected error: {}",
            err
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_cte_filters_not_removed_from_cte() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        // The filter lives inside the CTE; removal only looks at the outermost
        // SELECT, so nothing is modified
        let sql = "\
            WITH gendered AS (\
                SELECT KibanaSampleDataEcommerce.customer_gender AS gender \
                FROM KibanaSampleDataEcommerce \
                WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test'\
            ) \
            SELECT * FROM gendered\
        ";
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(modified_sql, sql);
        assert!(!applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_derived_table() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        let sql = "\
            SELECT * FROM (\
                SELECT customer_gender FROM KibanaSampleDataEcommerce\
            ) AS t\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT * FROM (\
                SELECT customer_gender FROM KibanaSampleDataEcommerce\
            ) AS t \
            WHERE t.\"customer_gender\" = 'test'\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_outermost_set_operation_rejected() {
        let ctx = get_test_tenant_ctx();
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            UNION ALL \
            SELECT customer_gender FROM KibanaSampleDataEcommerce\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let err = modify_sql_ast(&sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("Only plain SELECT statements are supported at the outermost level"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_add_delete_sql_filters() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "\
            SELECT customer_gender, MAX(maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
        ";

        // The original query has no filters
        let filters = get_sql_filters(sql, meta.clone(), session.clone()).await?;
        assert!(filters.is_empty());

        // Add a filter
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender, MAX(maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1\
            "
        );
        assert!(result
            .filters
            .iter()
            .any(|filter| filter_key(filter) == filter_key(&filters[0])));

        // The added filter is extracted from the logical plan
        let extracted = get_sql_filters(&result.sql, meta.clone(), session.clone()).await?;
        assert!(extracted
            .iter()
            .any(|filter| filter_key(filter) == filter_key(&filters[0])));

        // Adding the same filter again is a no-op
        let noop_result =
            add_sql_filters(&result.sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(noop_result.sql, result.sql);

        // Delete the filter
        let result =
            delete_sql_filters(&result.sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender, MAX(maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
            "
        );
        assert!(result.filters.is_empty());

        // Deleting a filter that is not present is a no-op
        let result = delete_sql_filters(&result.sql, &filters, meta, session).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender, MAX(maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
            "
        );

        Ok(())
    }

    fn or_group(items: Vec<serde_json::Value>) -> V1LoadRequestQueryFilterItem {
        V1LoadRequestQueryFilterItem {
            or: Some(items),
            ..Default::default()
        }
    }

    fn and_group(items: Vec<serde_json::Value>) -> V1LoadRequestQueryFilterItem {
        V1LoadRequestQueryFilterItem {
            and: Some(items),
            ..Default::default()
        }
    }

    #[test]
    fn test_modify_sql_ast_filter_groups() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Add an "or" filter group
        let group = or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["male"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "contains",
                "values": ["vip"],
            }),
        ]);
        let action = ModifyAction::Add(group.clone());
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'male' \
                OR KibanaSampleDataEcommerce.\"notes\" ILIKE '%vip%') \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // Removing a group with a different order of filters is not
        // a perfect match, so nothing is removed
        let sql = modified_sql;
        let reversed_group = or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "contains",
                "values": ["vip"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["male"],
            }),
        ]);
        let action = ModifyAction::Remove(reversed_group);
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(modified_sql, sql);
        assert!(!applied);

        // Removing a perfectly matching group works
        let action = ModifyAction::Remove(group);
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
        );
        assert!(applied);

        // Add a nested filter group: "and" group inside an "or" group
        let sql = modified_sql;
        let nested_group = or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "and": [
                    {
                        "member": "KibanaSampleDataEcommerce.taxful_total_price",
                        "operator": "gt",
                        "values": ["1"],
                    },
                    {
                        "member": "KibanaSampleDataEcommerce.taxful_total_price",
                        "operator": "lt",
                        "values": ["5"],
                    },
                ],
            }),
        ]);
        let action = ModifyAction::Add(nested_group.clone());
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
                OR (KibanaSampleDataEcommerce.\"taxful_total_price\" > 1 \
                    AND KibanaSampleDataEcommerce.\"taxful_total_price\" < 5)) \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // Removing the perfectly matching nested group works
        let sql = modified_sql;
        let action = ModifyAction::Remove(nested_group);
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
        );
        assert!(applied);

        // A group mixing dimension (WHERE) and measure (HAVING) filters is rejected
        let mixed_group = or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.maxPrice",
                "operator": "gt",
                "values": ["10"],
            }),
        ]);
        let action = ModifyAction::Add(mixed_group);
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("can't mix"),
            "unexpected error: {}",
            err
        );

        // A filter that is both an "and" and an "or" group is rejected
        let mut invalid_group = and_group(vec![serde_json::json!({
            "member": "KibanaSampleDataEcommerce.customer_gender",
            "operator": "equals",
            "values": ["x"],
        })]);
        invalid_group.or = Some(vec![]);
        let action = ModifyAction::Add(invalid_group);
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("can't be both \"and\" and \"or\""),
            "unexpected error: {}",
            err
        );

        // An empty group is rejected
        let action = ModifyAction::Add(or_group(vec![]));
        let err = modify_sql_ast(&modified_sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("at least one filter"),
            "unexpected error: {}",
            err
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_replace_filter() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            GROUP BY 1\
        ";

        // Replace a filter in place: the position within the clause is preserved
        let action = ModifyAction::Replace {
            old: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["test".to_string()]),
                ..Default::default()
            },
            new: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("notEquals".to_string()),
                values: Some(vec!["a".to_string(), "b".to_string()]),
                ..Default::default()
            },
        };
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" NOT IN ('a', 'b') \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // Replacing a filter that is not present is not applied
        let sql = modified_sql;
        let action = ModifyAction::Replace {
            old: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["missing".to_string()]),
                ..Default::default()
            },
            new: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("set".to_string()),
                ..Default::default()
            },
        };
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(modified_sql, sql);
        assert!(!applied);

        // Replace a plain filter with an "or" filter group in place
        let action = ModifyAction::Replace {
            old: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some("gt".to_string()),
                values: Some(vec!["42".to_string()]),
                ..Default::default()
            },
            new: or_group(vec![
                serde_json::json!({
                    "member": "KibanaSampleDataEcommerce.taxful_total_price",
                    "operator": "lt",
                    "values": ["10"],
                }),
                serde_json::json!({
                    "member": "KibanaSampleDataEcommerce.taxful_total_price",
                    "operator": "gt",
                    "values": ["100"],
                }),
            ]),
        };
        let (modified_sql, applied) = modify_sql_ast(&modified_sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" NOT IN ('a', 'b') \
                AND (KibanaSampleDataEcommerce.\"taxful_total_price\" < 10 \
                    OR KibanaSampleDataEcommerce.\"taxful_total_price\" > 100) \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // Replace a dimension (WHERE) filter with a measure (HAVING) filter
        let sql = modified_sql;
        let action = ModifyAction::Replace {
            old: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("notEquals".to_string()),
                values: Some(vec!["a".to_string(), "b".to_string()]),
                ..Default::default()
            },
            new: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
                operator: Some("gt".to_string()),
                values: Some(vec!["10".to_string()]),
                ..Default::default()
            },
        };
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"taxful_total_price\" < 10 \
                OR KibanaSampleDataEcommerce.\"taxful_total_price\" > 100) \
            GROUP BY 1 \
            HAVING MAX(KibanaSampleDataEcommerce.\"maxPrice\") > 10\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_delete_sql_filters_group() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Add an "or" filter group
        let filters = vec![or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "equals",
                "values": ["y"],
            }),
        ])];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
                OR KibanaSampleDataEcommerce.\"notes\" = 'y') \
            GROUP BY 1\
            "
        );
        assert!(result
            .filters
            .iter()
            .any(|filter| filter_key(filter) == filter_key(&filters[0])));

        // Delete the group
        let result = delete_sql_filters(&result.sql, &filters, meta, session).await?;
        assert_eq!(
            result.sql,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
        );
        assert!(result.filters.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_replace_sql_filters() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1\
        ";

        // Replace a plain filter with another plain filter
        let old_filter = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        };
        let new_filter = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("notEquals".to_string()),
            values: Some(vec!["other".to_string()]),
            ..Default::default()
        };
        let result = replace_sql_filters(
            sql,
            slice::from_ref(&old_filter),
            slice::from_ref(&new_filter),
            meta.clone(),
            session.clone(),
        )
        .await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" <> 'other' \
            GROUP BY 1\
            "
        );
        assert!(result
            .filters
            .iter()
            .any(|filter| filter_key(filter) == filter_key(&new_filter)));
        assert!(!result
            .filters
            .iter()
            .any(|filter| filter_key(filter) == filter_key(&old_filter)));

        // Replacing a filter that is not present fails
        let err = replace_sql_filters(
            &result.sql,
            slice::from_ref(&old_filter),
            slice::from_ref(&new_filter),
            meta.clone(),
            session.clone(),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("was not found in the outermost SELECT"),
            "unexpected error: {}",
            err
        );

        // Replace one set of filters with another
        let old_set = vec![new_filter];
        let new_set = vec![
            V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["a".to_string()]),
                ..Default::default()
            },
            V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some("gt".to_string()),
                values: Some(vec!["10".to_string()]),
                ..Default::default()
            },
        ];
        let result = replace_sql_filters(
            &result.sql,
            &old_set,
            &new_set,
            meta.clone(),
            session.clone(),
        )
        .await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 10 \
            GROUP BY 1\
            "
        );
        for filter in &new_set {
            assert!(result
                .filters
                .iter()
                .any(|extracted| filter_key(extracted) == filter_key(filter)));
        }
        assert!(!result
            .filters
            .iter()
            .any(|extracted| filter_key(extracted) == filter_key(&old_set[0])));

        // An empty set of filters to replace is rejected
        let err = replace_sql_filters(&result.sql, &[], &new_set, meta, session)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("At least one filter"),
            "unexpected error: {}",
            err
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_duplicate_filters() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        // The same filter expression appears twice in the WHERE clause
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
                AND KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1\
        ";

        // Deleting removes all equal filters
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // Replacing replaces all equal filters, preserving positions
        let action = ModifyAction::Replace {
            old: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["test".to_string()]),
                ..Default::default()
            },
            new: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("notEquals".to_string()),
                values: Some(vec!["other".to_string()]),
                ..Default::default()
            },
        };
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" <> 'other' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
                AND KibanaSampleDataEcommerce.\"customer_gender\" <> 'other' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_sql_filters() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
                AND KibanaSampleDataEcommerce.\"taxful_total_price\" > 42 \
            GROUP BY 1\
        ";

        // Set replaces all outermost filters with the new set
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["y".to_string()]),
            ..Default::default()
        }];
        let result = set_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'y' \
            GROUP BY 1\
            "
        );
        assert!(result
            .filters
            .iter()
            .any(|filter| filter_key(filter) == filter_key(&filters[0])));
        assert!(!result
            .filters
            .iter()
            .any(|filter| filter.member.as_deref()
                == Some("KibanaSampleDataEcommerce.customer_gender")));

        // Setting an empty set clears all outermost filters
        let result = set_sql_filters(&result.sql, &[], meta, session).await?;
        assert_eq!(
            result.sql,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
        );
        assert!(result.filters.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_add_sql_filters_like_family_round_trip() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Several values are emitted as a boolean chain of one predicate per
        // value, which the planner reports as several single-value filters
        for operator in [
            "contains",
            "notContains",
            "startsWith",
            "notStartsWith",
            "endsWith",
            "notEndsWith",
        ] {
            let filters = vec![V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some(operator.to_string()),
                values: Some(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                ..Default::default()
            }];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            let extracted = result
                .filters
                .iter()
                .map(filter_key)
                .collect::<HashSet<_>>();
            let expected = verification_keys(&filters[0]);
            assert!(
                expected.iter().all(|key| extracted.contains(key)),
                "{} filter did not round trip, wanted {:?}, got {:?}",
                operator,
                expected,
                result.filters
            );
            // The plain operators chain with OR and stay one group, the
            // negated ones chain with AND and are flattened into siblings
            assert_eq!(
                expected.len(),
                if operator.starts_with("not") { 3 } else { 1 },
                "unexpected verification keys for {}: {:?}",
                operator,
                expected
            );
        }

        // LIKE-family filters must survive the planner round trip, including
        // values with characters that have to be escaped in the pattern
        for (operator, value) in [
            ("contains", "abc"),
            ("startsWith", "pre"),
            ("endsWith", "end"),
            ("notContains", "x"),
            ("contains", "50%_off\\now"),
        ] {
            let filters = vec![V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some(operator.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            }];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            assert!(
                result
                    .filters
                    .iter()
                    .any(|filter| filter_key(filter) == filter_key(&filters[0])),
                "{} filter on {:?} did not round trip, got {:?}",
                operator,
                value,
                result.filters
            );
        }

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_numeric_value_validation() {
        let ctx = get_test_tenant_ctx();
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Numeric literals are rendered verbatim, so anything that is not a
        // plain number must be rejected instead of reaching the SQL text
        for value in ["0 OR 1=1", "abc", "1; DROP TABLE x", "inf", "NaN", " 1", ""] {
            for operator in ["equals", "notEquals"] {
                let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                    member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                    operator: Some(operator.to_string()),
                    values: Some(vec![value.to_string()]),
                    ..Default::default()
                });
                let err = modify_sql_ast(sql, &action, &ctx).unwrap_err();
                assert!(
                    err.to_string().contains("must be numeric"),
                    "unexpected error for {} {:?}: {}",
                    operator,
                    value,
                    err
                );
            }
        }

        // Multi-value filters validate every value
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["1".to_string(), "2 OR 1=1".to_string()]),
            ..Default::default()
        });
        let err = modify_sql_ast(sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("must be numeric"),
            "unexpected error: {}",
            err
        );

        // Well-formed numbers are still accepted
        for value in ["42", "-1", "3.14", "1e3"] {
            let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.taxful_total_price".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            });
            let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx).unwrap();
            assert!(applied);
            assert!(
                modified_sql.ends_with(&format!(
                    "WHERE KibanaSampleDataEcommerce.\"taxful_total_price\" = {} GROUP BY 1",
                    value
                )),
                "unexpected SQL for {:?}: {}",
                value,
                modified_sql
            );
        }
    }

    #[test]
    fn test_modify_sql_ast_or_clause_is_parenthesized() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        // AND-ing onto a top-level OR chain must not rebind the disjunction
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" = 'b' \
            GROUP BY 1\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["x".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
                OR KibanaSampleDataEcommerce.\"customer_gender\" = 'b') \
                AND KibanaSampleDataEcommerce.\"notes\" = 'x' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_parenthesized_clause() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        // A redundantly parenthesized clause must not defeat lookup or removal
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'a' \
                AND KibanaSampleDataEcommerce.\"notes\" = 'x') \
            GROUP BY 1\
        ";
        let filter = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["a".to_string()]),
            ..Default::default()
        };

        // Already present: adding is a no-op
        let action = ModifyAction::Add(filter.clone());
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(modified_sql, sql.trim_end());
        assert!(!applied);

        // Removal descends into the parentheses. Those around the clause as a
        // whole say nothing about its structure, so they don't survive it
        let action = ModifyAction::Remove(filter);
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'x' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_cte_shadowing_cube_name() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        // The CTE is named after the cube, but exposes its own columns only
        let sql = "\
            WITH KibanaSampleDataEcommerce AS (\
                SELECT customer_gender AS gender FROM KibanaSampleDataEcommerce\
            ) \
            SELECT gender FROM KibanaSampleDataEcommerce\
        ";

        // The cube's own column is not exposed by the CTE
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let err = modify_sql_ast(&sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("is not available in the outermost SELECT"),
            "unexpected error: {}",
            err
        );

        // Deleting a filter whose member is not available is a no-op
        let action = ModifyAction::Remove(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(modified_sql, sql.trim_end());
        assert!(!applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_unqualified_ref_in_join() {
        let ctx = get_test_tenant_ctx();
        // A bare column reference in a multi-relation FROM can't be attributed
        // to a specific cube, so it doesn't expose the member
        let sql = "\
            SELECT sub.customer_gender FROM (\
                SELECT customer_gender FROM KibanaSampleDataEcommerce \
                CROSS JOIN Logs\
            ) AS sub\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let err = modify_sql_ast(sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("is not available in the outermost SELECT"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_delete_sql_filters_unresolvable_member() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        // The member is not selected by the query, so there is nothing to
        // delete - that is a no-op rather than an error
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["x".to_string()]),
            ..Default::default()
        }];
        let result = delete_sql_filters(sql, &filters, meta, session).await?;
        assert_eq!(result.sql, sql);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_sql_filters_measure_round_trip() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "\
            SELECT customer_gender, MAX(maxPrice) AS max_price \
            FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
        ";

        // Measure filters land in HAVING as a synthesized aggregation, which
        // has to be recognized by the filter rewrite rules
        for (member, operator, value) in [
            ("KibanaSampleDataEcommerce.maxPrice", "gt", "10"),
            ("KibanaSampleDataEcommerce.maxPrice", "equals", "42"),
            ("KibanaSampleDataEcommerce.count", "gte", "1"),
        ] {
            let filters = vec![V1LoadRequestQueryFilterItem {
                member: Some(member.to_string()),
                operator: Some(operator.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            }];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            assert!(
                result.sql.contains("HAVING"),
                "{} {} filter did not produce a HAVING clause: {}",
                member,
                operator,
                result.sql
            );
            assert!(
                result
                    .filters
                    .iter()
                    .any(|filter| filter_key(filter) == filter_key(&filters[0])),
                "{} {} filter did not round trip, got {:?}",
                member,
                operator,
                result.filters
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_add_sql_filters_date_round_trip() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT order_date FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Date range filters are reconstructed from the time dimension of the
        // plan rather than from its filters, and their values are reshaped by
        // the planner
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("inDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter) == filter_key(&filters[0])),
            "inDateRange filter did not round trip, got {:?}",
            result.filters
        );

        // A negated range has to be emitted as NOT BETWEEN to be recognized
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
            operator: Some("notInDateRange".to_string()),
            values: Some(vec!["2024-01-01".to_string(), "2024-12-31".to_string()]),
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT order_date FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"order_date\" \
                NOT BETWEEN '2024-01-01' AND '2024-12-31' \
            GROUP BY 1\
            "
        );
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter) == filter_key(&filters[0])),
            "notInDateRange filter did not round trip, got {:?}",
            result.filters
        );

        // Single-bound date operators stay plain filters
        for (operator, value) in [
            ("beforeDate", "2024-06-01"),
            ("afterOrOnDate", "2024-07-01"),
        ] {
            let filters = vec![V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.order_date".to_string()),
                operator: Some(operator.to_string()),
                values: Some(vec![value.to_string()]),
                ..Default::default()
            }];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            assert!(
                result
                    .filters
                    .iter()
                    .any(|filter| filter_key(filter) == filter_key(&filters[0])),
                "{} filter did not round trip, got {:?}",
                operator,
                result.filters
            );
        }

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_string_value_quoting() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Single quotes in string values must be doubled, so that the value
        // can't terminate the literal and inject SQL
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["O'Brien' OR 1=1 --".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'O''Brien'' OR 1=1 --' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // The rewritten SQL still parses as a single predicate
        let query = parse_single_query(&modified_sql)?;
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected a plain SELECT");
        };
        assert!(matches!(
            select.selection,
            Some(ast::Expr::BinaryOp {
                op: ast::BinaryOperator::Eq,
                ..
            })
        ));

        // LIKE patterns quote the same way
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("contains".to_string()),
            values: Some(vec!["O'Brien".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"customer_gender\" ILIKE '%O''Brien%' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_sql_filters_also_present_in_cte() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        // The same filter is present both in the CTE and in the outermost
        // SELECT: deleting the outermost one is correct even though the plan
        // still carries the CTE's copy
        let sql = "\
            WITH recent AS (\
                SELECT customer_gender \
                FROM KibanaSampleDataEcommerce \
                WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test'\
            ) \
            SELECT customer_gender FROM recent \
            WHERE recent.\"customer_gender\" = 'test' \
            GROUP BY 1\
        ";
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        }];
        let result = delete_sql_filters(sql, &filters, meta, session).await?;
        assert_eq!(
            result.sql,
            "\
            WITH recent AS (\
                SELECT customer_gender \
                FROM KibanaSampleDataEcommerce \
                WHERE KibanaSampleDataEcommerce.\"customer_gender\" = 'test'\
            ) \
            SELECT customer_gender FROM recent \
            GROUP BY 1\
            "
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_cte_name_case_mismatch() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        // Unquoted identifiers are case-insensitive: the CTE reference and its
        // declaration may differ in case
        let sql = "\
            WITH Gendered AS (\
                SELECT customer_gender AS gender FROM KibanaSampleDataEcommerce\
            ) \
            SELECT gender FROM gendered\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            WITH Gendered AS (\
                SELECT customer_gender AS gender FROM KibanaSampleDataEcommerce\
            ) \
            SELECT gender FROM gendered \
            WHERE gendered.gender = 'test'\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[test]
    fn test_clear_outermost_filters_keeps_non_filter_predicates() -> Result<()> {
        // An old-style join condition is not a filter and must survive `set`,
        // or the query silently becomes a cross join
        let sql = "\
            SELECT a.customer_gender FROM KibanaSampleDataEcommerce AS a, \
                KibanaSampleDataEcommerce AS b \
            WHERE a.\"customer_gender\" = b.\"customer_gender\" \
                AND a.\"customer_gender\" = 'test' \
            GROUP BY 1\
        ";
        assert_eq!(
            clear_outermost_filters(sql)?,
            "\
            SELECT a.customer_gender FROM KibanaSampleDataEcommerce AS a, \
                KibanaSampleDataEcommerce AS b \
            WHERE a.\"customer_gender\" = b.\"customer_gender\" \
            GROUP BY 1\
            "
        );

        // Subquery predicates and predicates over computed expressions are
        // not filters either
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE LOWER(customer_gender) = 'test' \
                AND EXISTS (SELECT 1 FROM Logs) \
                AND customer_gender = 'test' \
            GROUP BY 1\
        ";
        assert_eq!(
            clear_outermost_filters(sql)?,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE LOWER(customer_gender) = 'test' \
                AND EXISTS (SELECT 1 FROM Logs) \
            GROUP BY 1\
            "
        );

        // Filters of every shape are dropped, HAVING included
        let sql = "\
            SELECT customer_gender, MAX(maxPrice) FROM KibanaSampleDataEcommerce \
            WHERE customer_gender IN ('a', 'b') \
                AND (notes ILIKE '%x%' OR notes IS NULL) \
                AND taxful_total_price BETWEEN 1 AND 2 \
                AND taxful_total_price > -5 \
            GROUP BY 1 \
            HAVING MAX(maxPrice) > 10\
        ";
        assert_eq!(
            clear_outermost_filters(sql)?,
            "\
            SELECT customer_gender, MAX(maxPrice) FROM KibanaSampleDataEcommerce \
            GROUP BY 1\
            "
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_set_sql_filters_keeps_non_filter_predicate() -> std::result::Result<(), CubeError>
    {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        // A predicate over a computed expression is not a Cube filter, so it
        // survives `set` while the filter next to it is dropped
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE LOWER(customer_gender) = 'test' \
                AND KibanaSampleDataEcommerce.\"customer_gender\" = 'test' \
            GROUP BY 1\
        ";
        let result = set_sql_filters(sql, &[], meta, session).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE LOWER(customer_gender) = 'test' \
            GROUP BY 1\
            "
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_identifier_case_mismatch() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        // The relation is matched case-insensitively, so the qualifier and the
        // column of the projection may be written in a different case
        let sql = "\
            SELECT customer_gender FROM (\
                SELECT KibanaSampleDataEcommerce.CUSTOMER_GENDER AS customer_gender \
                FROM kibanasampledataecommerce\
            ) AS t\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(&sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT customer_gender FROM (\
                SELECT KibanaSampleDataEcommerce.CUSTOMER_GENDER AS customer_gender \
                FROM kibanasampledataecommerce\
            ) AS t \
            WHERE t.customer_gender = 'test'\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_wildcard_over_join() {
        let ctx = get_test_tenant_ctx();
        // A wildcard over a join exposes columns of every relation, so a bare
        // dimension name can't be attributed to the cube
        let sql = "\
            SELECT customer_gender FROM (\
                SELECT * FROM KibanaSampleDataEcommerce CROSS JOIN Logs\
            ) AS sub \
            GROUP BY 1\
        ";
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        });
        let err = modify_sql_ast(sql, &action, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("is not available in the outermost SELECT"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_modify_sql_ast_count_distinct_measure() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        let sql = "SELECT content, COUNT(DISTINCT agentCount) FROM Logs GROUP BY 1";

        // `countDistinct` is how the meta API spells the aggregation
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("Logs.agentCount".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["10".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT content, COUNT(DISTINCT agentCount) FROM Logs \
            GROUP BY 1 \
            HAVING COUNT(DISTINCT Logs.\"agentCount\") > 10\
            "
        );
        assert!(applied);

        // `countDistinctApprox` has no exact SQL equivalent and stays on the
        // MEASURE path
        let action = ModifyAction::Add(V1LoadRequestQueryFilterItem {
            member: Some("Logs.agentCountApprox".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["10".to_string()]),
            ..Default::default()
        });
        let (modified_sql, applied) = modify_sql_ast(sql, &action, &ctx)?;
        assert!(
            modified_sql.contains("HAVING MEASURE(Logs.\"agentCountApprox\") > 10"),
            "unexpected SQL: {}",
            modified_sql
        );
        assert!(applied);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_delete_sql_filters_and_group() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // A top-level "and" group is flattened by the planner into sibling
        // filters, so it is verified through its members
        let filters = vec![and_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "equals",
                "values": ["y"],
            }),
        ])];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
                AND KibanaSampleDataEcommerce.\"notes\" = 'y') \
            GROUP BY 1\
            "
        );

        // A single-member "and" group is emitted as a plain filter
        let single = vec![and_group(vec![serde_json::json!({
            "member": "KibanaSampleDataEcommerce.notes",
            "operator": "equals",
            "values": ["z"],
        })])];
        let result = add_sql_filters(sql, &single, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'z' \
            GROUP BY 1\
            "
        );

        // A single-member "or" group is emitted as a plain filter too
        let single = vec![or_group(vec![serde_json::json!({
            "member": "KibanaSampleDataEcommerce.notes",
            "operator": "equals",
            "values": ["z"],
        })])];
        let result = add_sql_filters(sql, &single, meta.clone(), session.clone()).await?;
        assert_eq!(
            result.sql,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'z' \
            GROUP BY 1\
            "
        );

        // An "and" group nested inside an "or" survives as a group
        let nested = vec![or_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "and": [
                    {
                        "member": "KibanaSampleDataEcommerce.notes",
                        "operator": "equals",
                        "values": ["y"],
                    },
                    {
                        "member": "KibanaSampleDataEcommerce.taxful_total_price",
                        "operator": "gt",
                        "values": ["1"],
                    },
                ],
            }),
        ])];
        let result = add_sql_filters(sql, &nested, meta, session).await?;
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter) == filter_key(&nested[0])),
            "nested group did not round trip, got {:?}",
            result.filters
        );

        Ok(())
    }

    #[test]
    fn test_clear_outermost_filters_keeps_computed_derived_column() -> Result<()> {
        // The predicate is filter-shaped, but the column it references is
        // computed by the derived table, so no Cube filter can be behind it
        let sql = "\
            SELECT t.lc FROM (\
                SELECT LOWER(customer_gender) AS lc, customer_gender AS gender \
                FROM KibanaSampleDataEcommerce\
            ) AS t \
            WHERE t.lc = 'test' AND t.gender = 'test' \
            GROUP BY 1\
        ";
        assert_eq!(
            clear_outermost_filters(sql)?,
            "\
            SELECT t.lc FROM (\
                SELECT LOWER(customer_gender) AS lc, customer_gender AS gender \
                FROM KibanaSampleDataEcommerce\
            ) AS t \
            WHERE t.lc = 'test' \
            GROUP BY 1\
            "
        );

        // The wildcard may come first: an explicit projection of that name
        // shadows it regardless of the order
        let sql = "\
            SELECT t.lc FROM (\
                SELECT *, LOWER(customer_gender) AS lc FROM KibanaSampleDataEcommerce\
            ) AS t \
            WHERE t.lc = 'test' AND t.customer_gender = 'test' \
            GROUP BY 1\
        ";
        assert_eq!(
            clear_outermost_filters(sql)?,
            "\
            SELECT t.lc FROM (\
                SELECT *, LOWER(customer_gender) AS lc FROM KibanaSampleDataEcommerce\
            ) AS t \
            WHERE t.lc = 'test' \
            GROUP BY 1\
            "
        );

        // A column of a CTE is treated the same way
        let sql = "\
            WITH t AS (\
                SELECT LOWER(customer_gender) AS lc FROM KibanaSampleDataEcommerce\
            ) \
            SELECT lc FROM t WHERE t.lc = 'test' GROUP BY 1\
        ";
        assert_eq!(clear_outermost_filters(sql)?, sql.trim_end());

        // A relation that renames its columns positionally means something
        // different outside than inside, and member resolution refuses it, so
        // its predicates are not filters either - for a derived table,
        let sql = "\
            SELECT t.customer_gender FROM (\
                SELECT LOWER(notes) AS lc, customer_gender FROM KibanaSampleDataEcommerce\
            ) AS t (customer_gender, other) \
            WHERE t.customer_gender = 'test' \
            GROUP BY 1\
        ";
        assert_eq!(clear_outermost_filters(sql)?, sql.trim_end());

        // a CTE,
        let sql = "\
            WITH t (customer_gender, other) AS (\
                SELECT LOWER(notes) AS lc, customer_gender FROM KibanaSampleDataEcommerce\
            ) \
            SELECT customer_gender FROM t \
            WHERE t.customer_gender = 'test' \
            GROUP BY 1\
        ";
        assert_eq!(clear_outermost_filters(sql)?, sql.trim_end());

        // and a cube referenced directly
        let sql = "\
            SELECT t.customer_gender \
            FROM KibanaSampleDataEcommerce AS t (customer_gender, other) \
            WHERE t.customer_gender = 'test' \
            GROUP BY 1\
        ";
        assert_eq!(clear_outermost_filters(sql)?, sql.trim_end());

        // A relation shape the recognizer can't describe still counts as a
        // relation, so an unqualified column can't be attributed to the one
        // relation next to it
        let sql = "\
            SELECT customer_gender \
            FROM Logs, (KibanaSampleDataEcommerce CROSS JOIN Logs) \
            WHERE customer_gender = 'test' \
            GROUP BY 1\
        ";
        assert_eq!(clear_outermost_filters(sql)?, sql.trim_end());

        // A column forwarded from a nested relation is only as direct as it is
        // there, whether it is forwarded by name
        let sql = "\
            SELECT t.g FROM (\
                SELECT u.g FROM (\
                    SELECT LOWER(customer_gender) AS g FROM KibanaSampleDataEcommerce\
                ) AS u\
            ) AS t \
            WHERE t.g = 'test' \
            GROUP BY 1\
        ";
        assert_eq!(clear_outermost_filters(sql)?, sql.trim_end());

        // or by a wildcard
        let sql = "\
            SELECT t.g FROM (\
                SELECT * FROM (\
                    SELECT LOWER(customer_gender) AS g FROM KibanaSampleDataEcommerce\
                ) AS u\
            ) AS t \
            WHERE t.g = 'test' \
            GROUP BY 1\
        ";
        assert_eq!(clear_outermost_filters(sql)?, sql.trim_end());

        // A CTE body sees its siblings, so a column forwarded from one is
        // attributed there rather than taken for a raw cube column
        let sql = "\
            WITH t0 AS (\
                SELECT LOWER(customer_gender) AS g FROM KibanaSampleDataEcommerce\
            ), t1 AS (SELECT g FROM t0) \
            SELECT g FROM t1 WHERE t1.g = 'test' GROUP BY 1\
        ";
        assert_eq!(clear_outermost_filters(sql)?, sql.trim_end());

        // An aggregation is only a measure filter where the cube is in the
        // same FROM: over a column a relation merely forwards, resolution
        // can't reach the member, so the predicate isn't one to drop
        let sql = "\
            SELECT t.g, MAX(t.mp) FROM (\
                SELECT customer_gender AS g, maxPrice AS mp FROM KibanaSampleDataEcommerce\
            ) AS t \
            GROUP BY 1 \
            HAVING MAX(t.mp) > 10\
        ";
        assert_eq!(clear_outermost_filters(sql)?, sql.trim_end());

        // A pattern with an explicit escape character is not a Cube filter,
        // since one can't be emitted for it either
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE notes LIKE 'a!%b' ESCAPE '!' \
            GROUP BY 1\
        ";
        assert_eq!(clear_outermost_filters(sql)?, sql.trim_end());

        // A column that no relation of the outermost FROM exposes is left
        // alone as well
        let sql = "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE other.\"column\" = 'test' \
            GROUP BY 1\
        ";
        assert_eq!(clear_outermost_filters(sql)?, sql.trim_end());

        Ok(())
    }

    #[tokio::test]
    async fn test_add_sql_filters_multi_value_and_null_round_trip(
    ) -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // Multi-value equality becomes IN / NOT IN, and the null checks
        // become IS [NOT] NULL
        for (operator, values) in [
            ("equals", Some(vec!["a".to_string(), "b".to_string()])),
            ("notEquals", Some(vec!["a".to_string(), "b".to_string()])),
            ("set", None),
            ("notSet", None),
        ] {
            let filters = vec![V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some(operator.to_string()),
                values,
                ..Default::default()
            }];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            assert!(
                result
                    .filters
                    .iter()
                    .any(|filter| filter_key(filter) == filter_key(&filters[0])),
                "{} filter did not round trip, got {:?}",
                operator,
                result.filters
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_add_sql_filters_nested_group_normalization() -> std::result::Result<(), CubeError>
    {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // The normalizations that AST generation and the planner perform reach
        // every depth of a filter tree, not just its root
        let cases = vec![
            // A multi-value LIKE-family filter chains with OR, which the
            // engine collapses into the enclosing group
            (
                "multi-value contains inside an or",
                or_group(vec![
                    serde_json::json!({
                        "member": "KibanaSampleDataEcommerce.customer_gender",
                        "operator": "contains",
                        "values": ["a", "b"],
                    }),
                    serde_json::json!({
                        "member": "KibanaSampleDataEcommerce.notes",
                        "operator": "equals",
                        "values": ["y"],
                    }),
                ]),
            ),
            // The negated operators chain with AND, which survives as a group
            // of its own inside the enclosing or
            (
                "multi-value notContains inside an or",
                or_group(vec![
                    serde_json::json!({
                        "member": "KibanaSampleDataEcommerce.customer_gender",
                        "operator": "notContains",
                        "values": ["a", "b"],
                    }),
                    serde_json::json!({
                        "member": "KibanaSampleDataEcommerce.notes",
                        "operator": "equals",
                        "values": ["y"],
                    }),
                ]),
            ),
            // A single-member group is emitted as its member alone
            (
                "single-member and inside an or",
                or_group(vec![
                    serde_json::json!({
                        "and": [{
                            "member": "KibanaSampleDataEcommerce.notes",
                            "operator": "equals",
                            "values": ["y"],
                        }],
                    }),
                    serde_json::json!({
                        "member": "KibanaSampleDataEcommerce.customer_gender",
                        "operator": "equals",
                        "values": ["x"],
                    }),
                ]),
            ),
        ];

        for (label, filter) in cases {
            let filters = vec![filter];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            let extracted = result
                .filters
                .iter()
                .map(filter_key)
                .collect::<HashSet<_>>();
            let expected = verification_keys(&filters[0]);
            assert!(
                expected.iter().all(|key| extracted.contains(key)),
                "{} did not round trip, wanted {:?}, got {:?}",
                label,
                expected,
                result.filters
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_filters_count_is_bounded() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
        let leaf = |i: usize| {
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": [format!("v{}", i)],
            })
        };

        // A group nests any number of leaves inside a single array entry, so
        // counting entries alone would let the bound be walked around
        let nested = vec![or_group((0..MAX_FILTERS + 1).map(leaf).collect())];
        let err = add_sql_filters(sql, &nested, meta.clone(), session.clone())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("At most"),
            "unexpected error: {}",
            err
        );

        // A group whose `and` is present but null is a plain `or` group, and
        // its members still count
        let null_and = vec![or_group(vec![serde_json::json!({
            "and": null,
            "or": (0..MAX_FILTERS + 1).map(leaf).collect::<Vec<_>>(),
        })])];
        let err = add_sql_filters(sql, &null_and, meta.clone(), session.clone())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("At most"),
            "unexpected error: {}",
            err
        );

        // Nesting a level deeper doesn't help either
        let deeply_nested = vec![or_group(vec![serde_json::json!({
            "and": (0..MAX_FILTERS + 1).map(leaf).collect::<Vec<_>>(),
        })])];
        let err = add_sql_filters(sql, &deeply_nested, meta.clone(), session.clone())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("At most"),
            "unexpected error: {}",
            err
        );

        // The bound applies to every operation that takes filters
        let flat = (0..MAX_FILTERS + 1)
            .map(|i| V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec![format!("v{}", i)]),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        for result in [
            set_sql_filters(sql, &flat, meta.clone(), session.clone()).await,
            delete_sql_filters(sql, &flat, meta.clone(), session.clone()).await,
            replace_sql_filters(sql, &flat, &[], meta.clone(), session.clone()).await,
            replace_sql_filters(sql, &flat[..1], &flat, meta.clone(), session.clone()).await,
        ] {
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("At most"),
                "unexpected error: {}",
                err
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_filters_boolean_member() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT has_subscription FROM KibanaSampleDataEcommerce GROUP BY 1";
        let filter = |value: &str| V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.has_subscription".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        };

        // A boolean member is filtered with a boolean literal, which is what
        // the filter rewrite rules read
        for value in ["true", "false"] {
            let filters = vec![filter(value)];
            let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
            assert_eq!(
                result.sql,
                format!(
                    "SELECT has_subscription FROM KibanaSampleDataEcommerce \
                     WHERE KibanaSampleDataEcommerce.\"has_subscription\" = {} \
                     GROUP BY 1",
                    value
                )
            );
            assert!(
                result
                    .filters
                    .iter()
                    .any(|extracted| filter_key(extracted) == filter_key(&filters[0])),
                "{} filter did not round trip, got {:?}",
                value,
                result.filters
            );
        }

        // A value that is not a boolean is rejected rather than passed through
        let ctx = get_test_tenant_ctx();
        let err = modify_sql_ast(sql, &ModifyAction::Add(filter("notabool")), &ctx).unwrap_err();
        assert!(
            err.to_string().contains("must be a boolean"),
            "unexpected error: {}",
            err
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_alias_quoting_is_preserved() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        let action = || {
            ModifyAction::Add(V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["x".to_string()]),
                ..Default::default()
            })
        };

        // Quoting decides whether an identifier folds, so the alias is emitted
        // exactly as the projection wrote it - unquoted here,
        let sql = "\
            SELECT t.Gender FROM (\
                SELECT customer_gender AS Gender FROM KibanaSampleDataEcommerce\
            ) AS t \
            GROUP BY 1\
        ";
        let (modified_sql, applied) = modify_sql_ast(sql, &action(), &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT t.Gender FROM (\
                SELECT customer_gender AS Gender FROM KibanaSampleDataEcommerce\
            ) AS t \
            WHERE t.Gender = 'x' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // and quoted there
        let sql = "\
            SELECT t.\"Gender\" FROM (\
                SELECT customer_gender AS \"Gender\" FROM KibanaSampleDataEcommerce\
            ) AS t \
            GROUP BY 1\
        ";
        let (modified_sql, applied) = modify_sql_ast(sql, &action(), &ctx)?;
        assert_eq!(
            modified_sql,
            "\
            SELECT t.\"Gender\" FROM (\
                SELECT customer_gender AS \"Gender\" FROM KibanaSampleDataEcommerce\
            ) AS t \
            WHERE t.\"Gender\" = 'x' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_filters_cte_chain() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let gender = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        };

        // A member is followed through a chain of relations, each of which may
        // rename it, and the predicate names it as the outermost one does
        let cases = vec![
            (
                "WITH t0 AS (SELECT customer_gender FROM KibanaSampleDataEcommerce), \
                 t1 AS (SELECT customer_gender FROM t0) \
                 SELECT customer_gender FROM t1 GROUP BY 1",
                "t1.\"customer_gender\" = 'test'",
            ),
            (
                "WITH t0 AS (SELECT customer_gender FROM KibanaSampleDataEcommerce), \
                 t1 AS (SELECT customer_gender FROM t0), \
                 t2 AS (SELECT customer_gender FROM t1) \
                 SELECT customer_gender FROM t2 GROUP BY 1",
                "t2.\"customer_gender\" = 'test'",
            ),
            (
                "WITH t0 AS (SELECT customer_gender AS g FROM KibanaSampleDataEcommerce), \
                 t1 AS (SELECT g AS gg FROM t0) \
                 SELECT gg FROM t1 GROUP BY 1",
                "t1.gg = 'test'",
            ),
            (
                "SELECT t.customer_gender FROM (\
                     SELECT u.customer_gender FROM (\
                         SELECT customer_gender FROM KibanaSampleDataEcommerce\
                     ) AS u\
                 ) AS t GROUP BY 1",
                "t.customer_gender = 'test'",
            ),
        ];

        for (sql, predicate) in cases {
            let result = add_sql_filters(
                sql,
                std::slice::from_ref(&gender),
                meta.clone(),
                session.clone(),
            )
            .await?;
            assert!(
                result.sql.contains(predicate),
                "expected {} in {}",
                predicate,
                result.sql
            );
            assert!(
                result
                    .filters
                    .iter()
                    .any(|filter| filter_key(filter) == filter_key(&gender)),
                "filter did not round trip through {}, got {:?}",
                sql,
                result.filters
            );

            // and deleting it puts the query back as it was
            let deleted = delete_sql_filters(
                &result.sql,
                std::slice::from_ref(&gender),
                meta.clone(),
                session.clone(),
            )
            .await?;
            assert!(
                !deleted.sql.contains(predicate),
                "predicate survived deletion in {}",
                deleted.sql
            );
        }

        // A measure aggregated at the bottom of a chain is forwarded the same way
        let max_price = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.maxPrice".to_string()),
            operator: Some("gt".to_string()),
            values: Some(vec!["10".to_string()]),
            ..Default::default()
        };
        let sql = "WITH t0 AS (\
                       SELECT customer_gender, MAX(maxPrice) AS mp \
                       FROM KibanaSampleDataEcommerce GROUP BY 1\
                   ), t1 AS (SELECT customer_gender, mp FROM t0) \
                   SELECT customer_gender, mp FROM t1";
        let result = add_sql_filters(
            sql,
            std::slice::from_ref(&max_price),
            meta.clone(),
            session.clone(),
        )
        .await?;
        assert!(
            result.sql.contains("WHERE t1.mp > 10"),
            "unexpected SQL: {}",
            result.sql
        );

        // A column computed anywhere along the chain is still not a member
        let sql = "WITH t0 AS (\
                       SELECT LOWER(customer_gender) AS customer_gender \
                       FROM KibanaSampleDataEcommerce\
                   ), t1 AS (SELECT customer_gender FROM t0) \
                   SELECT customer_gender FROM t1 GROUP BY 1";
        let err = add_sql_filters(sql, std::slice::from_ref(&gender), meta, session)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("is not available in the outermost SELECT"),
            "unexpected error: {}",
            err
        );

        Ok(())
    }

    /// The native layer runs these on a multi-threaded runtime, so their
    /// futures have to stay `Send`. Nothing in the rewriting path may be held
    /// across an await - a `!Sync` value such as the relation-expansion
    /// budget would take `Send` away and this would stop compiling.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sql_filters_futures_are_send() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1".to_string();
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        }];

        let spawned = tokio::spawn({
            let (meta, session, sql, filters) =
                (meta.clone(), session.clone(), sql.clone(), filters.clone());
            async move {
                let added = add_sql_filters(&sql, &filters, meta.clone(), session.clone()).await?;
                let filters = get_sql_filters(&added.sql, meta.clone(), session.clone()).await?;
                let set =
                    set_sql_filters(&added.sql, &filters, meta.clone(), session.clone()).await?;
                let replaced = replace_sql_filters(
                    &set.sql,
                    &filters,
                    &filters,
                    meta.clone(),
                    session.clone(),
                )
                .await?;
                delete_sql_filters(&replaced.sql, &filters, meta, session).await
            }
        });

        let result = spawned
            .await
            .map_err(|e| CubeError::internal(format!("join error: {}", e)))??;
        assert_eq!(result.sql, sql);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_large_clause() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        let filter = |i: usize| V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec![format!("v{}", i)]),
            ..Default::default()
        };

        // A clause holds a predicate per filter, so a batch the size of the
        // filter limit builds one longer than a recursive walk could carry
        let sql = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
        let actions = (0..MAX_FILTERS)
            .map(|i| ModifyAction::Add(filter(i)))
            .collect::<Vec<_>>();
        let (built, applied) = modify_sql_ast_many(sql, &actions, &ctx)?;
        assert!(applied.iter().all(|applied| *applied));
        assert_eq!(built.matches(" AND ").count(), MAX_FILTERS - 1);

        // and that clause can be walked again, to find, remove and clear
        let (removed, applied) = modify_sql_ast(&built, &ModifyAction::Remove(filter(0)), &ctx)?;
        assert!(applied);
        assert_eq!(removed.matches(" AND ").count(), MAX_FILTERS - 2);
        assert_eq!(
            clear_outermost_filters(&built)?,
            "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1"
        );

        // Past the clause bound the request is refused rather than attempted
        let mut clause = String::from("1 = 1");
        for i in 0..MAX_CLAUSE_PREDICATES {
            clause.push_str(&format!(" AND customer_gender = 'v{}'", i));
        }
        let sql = format!(
            "SELECT customer_gender FROM KibanaSampleDataEcommerce WHERE {} GROUP BY 1",
            clause
        );
        for result in [
            clear_outermost_filters(&sql),
            modify_sql_ast(&sql, &ModifyAction::Add(filter(0)), &ctx).map(|(sql, _)| sql),
        ] {
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("more than the"),
                "unexpected error: {}",
                err
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_filters_sibling_relation() -> std::result::Result<(), CubeError> {
        use crate::compile::{test::get_test_session, DatabaseProtocol};

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;

        // The cube is in the FROM, but the projection reads the member from a
        // sibling relation, so finding the cube can't end the search
        let sql = "\
            SELECT x.g FROM (\
                SELECT t.g FROM KibanaSampleDataEcommerce k \
                JOIN (SELECT customer_gender AS g FROM KibanaSampleDataEcommerce) t ON true\
            ) x \
            GROUP BY 1\
        ";
        let filters = vec![V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["test".to_string()]),
            ..Default::default()
        }];
        let result = add_sql_filters(sql, &filters, meta.clone(), session.clone()).await?;
        assert!(
            result.sql.contains("WHERE x.g = 'test'"),
            "unexpected SQL: {}",
            result.sql
        );
        assert!(
            result
                .filters
                .iter()
                .any(|filter| filter_key(filter) == filter_key(&filters[0])),
            "filter did not round trip, got {:?}",
            result.filters
        );

        // and what `add` can write, `set` may drop
        let cleared = set_sql_filters(&result.sql, &[], meta, session).await?;
        assert!(
            !cleared.sql.contains("WHERE"),
            "predicate survived set: {}",
            cleared.sql
        );

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_group_as_whole_clause() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        let group = and_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "equals",
                "values": ["y"],
            }),
        ]);
        let base = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";

        // A group added to a query with no WHERE becomes the whole clause,
        // parentheses and all
        let (with_group, applied) = modify_sql_ast(base, &ModifyAction::Add(group.clone()), &ctx)?;
        assert_eq!(
            with_group,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE (KibanaSampleDataEcommerce.\"customer_gender\" = 'x' \
                AND KibanaSampleDataEcommerce.\"notes\" = 'y') \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // and is still matched as that group: adding it again is a no-op,
        let (again, applied) =
            modify_sql_ast(&with_group, &ModifyAction::Add(group.clone()), &ctx)?;
        assert_eq!(again, with_group);
        assert!(!applied);

        // deleting it takes the whole clause,
        let (deleted, applied) =
            modify_sql_ast(&with_group, &ModifyAction::Remove(group.clone()), &ctx)?;
        assert_eq!(deleted, base);
        assert!(applied);

        // and replacing it swaps the group rather than its members
        let action = ModifyAction::Replace {
            old: group,
            new: V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.notes".to_string()),
                operator: Some("equals".to_string()),
                values: Some(vec!["z".to_string()]),
                ..Default::default()
            },
        };
        let (replaced, applied) = modify_sql_ast(&with_group, &action, &ctx)?;
        assert_eq!(
            replaced,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'z' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // A filter inside those parentheses is still reachable on its own
        let leaf = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["x".to_string()]),
            ..Default::default()
        };
        let (leaf_deleted, applied) =
            modify_sql_ast(&with_group, &ModifyAction::Remove(leaf), &ctx)?;
        assert_eq!(
            leaf_deleted,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'y' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_group_member_reachability() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        let group = and_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "equals",
                "values": ["y"],
            }),
        ]);
        let member = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["x".to_string()]),
            ..Default::default()
        };
        let absent = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["absent".to_string()]),
            ..Default::default()
        };
        let other = V1LoadRequestQueryFilterItem {
            member: Some("KibanaSampleDataEcommerce.notes".to_string()),
            operator: Some("equals".to_string()),
            values: Some(vec!["c".to_string()]),
            ..Default::default()
        };

        let base = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
        let (alone, _) = modify_sql_ast(base, &ModifyAction::Add(group.clone()), &ctx)?;
        let (mixed, _) = modify_sql_ast(&alone, &ModifyAction::Add(other), &ctx)?;

        // The rewrite engine flattens a top-level "and" group into sibling
        // filters, so its members are filters of their own whether the group
        // stands alone in the clause or sits next to something else
        for clause in [&alone, &mixed] {
            // A member is removable on its own, which leaves its siblings
            let (removed, applied) =
                modify_sql_ast(clause, &ModifyAction::Remove(member.clone()), &ctx)?;
            assert!(applied);
            assert!(
                !removed.contains("\"customer_gender\" = 'x'"),
                "member survived removal: {}",
                removed
            );
            assert!(
                removed.contains("\"notes\" = 'y'"),
                "sibling was removed too: {}",
                removed
            );

            // and adding it back is a no-op rather than a duplicate
            let (added, applied) =
                modify_sql_ast(clause, &ModifyAction::Add(member.clone()), &ctx)?;
            assert_eq!(&added, clause);
            assert!(!applied);

            // The group is still removable as a whole
            let (removed, applied) =
                modify_sql_ast(clause, &ModifyAction::Remove(group.clone()), &ctx)?;
            assert!(applied);
            assert!(
                !removed.contains("\"customer_gender\" = 'x'")
                    && !removed.contains("\"notes\" = 'y'"),
                "group survived removal: {}",
                removed
            );

            // and a filter that isn't there leaves the clause as it was,
            // parentheses included
            let (untouched, applied) =
                modify_sql_ast(clause, &ModifyAction::Remove(absent.clone()), &ctx)?;
            assert_eq!(&untouched, clause);
            assert!(!applied);
        }

        Ok(())
    }

    #[test]
    fn test_modify_sql_ast_group_added_after_its_members() -> Result<()> {
        let ctx = get_test_tenant_ctx();
        let filter = |member: &str, value: &str| V1LoadRequestQueryFilterItem {
            member: Some(format!("KibanaSampleDataEcommerce.{}", member)),
            operator: Some("equals".to_string()),
            values: Some(vec![value.to_string()]),
            ..Default::default()
        };
        let group = and_group(vec![
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.customer_gender",
                "operator": "equals",
                "values": ["x"],
            }),
            serde_json::json!({
                "member": "KibanaSampleDataEcommerce.notes",
                "operator": "equals",
                "values": ["y"],
            }),
        ]);

        // The rewrite engine reports a top-level "and" group as sibling
        // filters, so a clause that holds its members separately holds the
        // group itself, however the two got there
        let base = "SELECT customer_gender FROM KibanaSampleDataEcommerce GROUP BY 1";
        let (with_a, _) = modify_sql_ast(
            base,
            &ModifyAction::Add(filter("customer_gender", "x")),
            &ctx,
        )?;
        let (members, _) = modify_sql_ast(&with_a, &ModifyAction::Add(filter("notes", "y")), &ctx)?;

        // Adding the group they make up is a no-op, not a duplicate
        let (added, applied) = modify_sql_ast(&members, &ModifyAction::Add(group.clone()), &ctx)?;
        assert_eq!(added, members);
        assert!(!applied);

        // Deleting it takes both members
        let (deleted, applied) =
            modify_sql_ast(&members, &ModifyAction::Remove(group.clone()), &ctx)?;
        assert_eq!(deleted, base);
        assert!(applied);

        // and replacing it drops them for the new filter
        let action = ModifyAction::Replace {
            old: group.clone(),
            new: filter("notes", "z"),
        };
        let (replaced, applied) = modify_sql_ast(&members, &action, &ctx)?;
        assert_eq!(
            replaced,
            "\
            SELECT customer_gender FROM KibanaSampleDataEcommerce \
            WHERE KibanaSampleDataEcommerce.\"notes\" = 'z' \
            GROUP BY 1\
            "
        );
        assert!(applied);

        // With only part of the group present it is not present at all: the
        // filter is neither half-removed nor taken for a duplicate
        let (deleted, applied) =
            modify_sql_ast(&with_a, &ModifyAction::Remove(group.clone()), &ctx)?;
        assert_eq!(deleted, with_a);
        assert!(!applied);

        let (added, applied) = modify_sql_ast(&with_a, &ModifyAction::Add(group), &ctx)?;
        assert!(added.contains("AND (KibanaSampleDataEcommerce.\"customer_gender\" = 'x'"));
        assert!(applied);

        Ok(())
    }
}
