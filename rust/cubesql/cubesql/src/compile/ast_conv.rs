use cubeclient::models::{V1CubeMetaDimension, V1CubeMetaMeasure, V1LoadRequestQueryFilterItem};
use datafusion::error::{DataFusionError, Result};
use sqlparser::{ast, dialect::PostgreSqlDialect, parser::Parser};

use crate::transport::MetaContext;

#[derive(Debug)]
enum ModifyAction {
    AddFilter(V1LoadRequestQueryFilterItem),
    RemoveFilter(V1LoadRequestQueryFilterItem),
}

impl ModifyAction {
    fn get_cube_and_member_name(&self) -> Result<(String, String)> {
        match self {
            ModifyAction::AddFilter(filter) | ModifyAction::RemoveFilter(filter) => {
                let member_name = filter.member.as_ref().ok_or_else(|| {
                    DataFusionError::NotImplemented(
                        "\"and\" and \"or\" filters are not yet supported".to_string(),
                    )
                })?;
                let (cube, member) = member_name.split_once('.').ok_or_else(|| {
                    DataFusionError::NotImplemented("Invalid member format".to_string())
                })?;
                Ok((cube.to_string(), member.to_string()))
            }
        }
    }

    fn issue_expr(
        &self,
        relation_alias: &ast::Ident,
        meta_member: &MetaMember,
    ) -> Result<ast::Expr> {
        let column_ident = ast::Ident::with_quote('"', meta_member.short_name());
        let column_expr = ast::Expr::CompoundIdentifier(vec![relation_alias.clone(), column_ident]);
        let expr = match meta_member {
            MetaMember::Dimension(_) => column_expr,
            MetaMember::Measure(measure) => {
                let func_name = match measure.agg_type.as_deref() {
                    Some("count") => "COUNT",
                    Some("count_distinct") => "COUNT",
                    Some("sum") => "SUM",
                    Some("avg") => "AVG",
                    Some("min") => "MIN",
                    Some("max") => "MAX",
                    _ => "MEASURE",
                };
                let distinct = matches!(measure.agg_type.as_deref(), Some("count_distinct"));
                ast::Expr::Function(ast::Function {
                    name: ast::ObjectName(vec![ast::Ident::new(func_name)]),
                    args: vec![ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                        column_expr,
                    ))],
                    over: None,
                    distinct,
                    special: false,
                    approximate: false,
                })
            }
        };

        match self {
            ModifyAction::AddFilter(filter) | ModifyAction::RemoveFilter(filter) => {
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
                                    Self::value_to_expr_by_member_type(&values[0], meta_member);
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
                                    .collect::<Vec<_>>();
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
                            0 => return Err(DataFusionError::Plan(
                                "At least one filter value is required for \"notEquals\" operator"
                                    .to_string(),
                            )),
                            1 => {
                                let value_expr =
                                    Self::value_to_expr_by_member_type(&values[0], meta_member);
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
                                    .collect::<Vec<_>>();
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
                    Some("notInDateRange") => Self::date_range_expr(
                        expr,
                        filter,
                        "notInDateRange",
                        ast::BinaryOperator::Lt,
                        ast::BinaryOperator::Gt,
                        ast::BinaryOperator::Or,
                    ),
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
                    Some("contains") => Self::like_family_expr(
                        &expr,
                        filter,
                        "contains",
                        LikeShape::Contains,
                        false,
                    ),
                    Some("notContains") => Self::like_family_expr(
                        &expr,
                        filter,
                        "notContains",
                        LikeShape::Contains,
                        true,
                    ),
                    Some("startsWith") => Self::like_family_expr(
                        &expr,
                        filter,
                        "startsWith",
                        LikeShape::StartsWith,
                        false,
                    ),
                    Some("notStartsWith") => Self::like_family_expr(
                        &expr,
                        filter,
                        "notStartsWith",
                        LikeShape::StartsWith,
                        true,
                    ),
                    Some("endsWith") => Self::like_family_expr(
                        &expr,
                        filter,
                        "endsWith",
                        LikeShape::EndsWith,
                        false,
                    ),
                    Some("notEndsWith") => Self::like_family_expr(
                        &expr,
                        filter,
                        "notEndsWith",
                        LikeShape::EndsWith,
                        true,
                    ),
                    _ => Err(DataFusionError::Plan(format!(
                        "Unsupported filter operator: {:?}",
                        filter.operator
                    ))),
                }
            }
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
            right: Box::new(ast::Expr::Value(ast::Value::Number(
                value.to_string(),
                false,
            ))),
        })
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
        if value.parse::<f64>().is_err() {
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
            right: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(
                values[0].clone(),
            ))),
        })
    }

    fn date_range_expr(
        column_expr: ast::Expr,
        filter: &V1LoadRequestQueryFilterItem,
        op_name: &str,
        lower_op: ast::BinaryOperator,
        upper_op: ast::BinaryOperator,
        join_op: ast::BinaryOperator,
    ) -> Result<ast::Expr> {
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
        let lower = ast::Expr::BinaryOp {
            left: Box::new(column_expr.clone()),
            op: lower_op,
            right: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(
                values[0].clone(),
            ))),
        };
        let upper = ast::Expr::BinaryOp {
            left: Box::new(column_expr),
            op: upper_op,
            right: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(
                values[1].clone(),
            ))),
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
                expr: Box::new(column_expr.clone()),
                pattern: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(pattern))),
                escape_char: None,
            }
        })
    }

    fn value_to_expr_by_member_type(value: &str, meta_member: &MetaMember) -> ast::Expr {
        match meta_member {
            MetaMember::Dimension(dimension) => {
                if dimension.r#type == "number" {
                    ast::Expr::Value(ast::Value::Number(value.to_string(), false))
                } else {
                    ast::Expr::Value(ast::Value::SingleQuotedString(value.to_string()))
                }
            }
            MetaMember::Measure(measure) => match measure.r#type.as_str() {
                "string" | "time" | "boolean" => {
                    ast::Expr::Value(ast::Value::SingleQuotedString(value.to_string()))
                }
                _ => ast::Expr::Value(ast::Value::Number(value.to_string(), false)),
            },
        }
    }
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
        full_name.split('.').last().unwrap_or(full_name).to_string()
    }
}

#[derive(Debug)]
enum PullUpAction {
    Continue,
    Stop(bool), // whether modification was applied
    RemoveAndStop,
}

fn modify_sql_ast(sql: &str, action: &ModifyAction, ctx: &MetaContext) -> Result<(String, bool)> {
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
    let ast::Statement::Query(mut query) = statement else {
        return Err(DataFusionError::NotImplemented(
            "Only SELECT statements are supported".to_string(),
        ));
    };

    let applied = apply_action_to_query(query.as_mut(), action, ctx)?;

    let modified_sql = query.to_string();
    Ok((modified_sql, applied))
}

fn apply_action_to_query(
    query: &mut ast::Query,
    action: &ModifyAction,
    ctx: &MetaContext,
) -> Result<bool> {
    let mut applied = false;

    if let Some(with) = query.with.as_mut() {
        for cte in &mut with.cte_tables {
            if apply_action_to_query(&mut cte.query, action, ctx)? {
                applied = true;
            }
        }
    }

    if apply_action_to_set_expr(&mut query.body, action, ctx)? {
        applied = true;
    }

    Ok(applied)
}

fn apply_action_to_set_expr(
    set_expr: &mut ast::SetExpr,
    action: &ModifyAction,
    ctx: &MetaContext,
) -> Result<bool> {
    match set_expr {
        ast::SetExpr::Select(select) => apply_action_to_select(select.as_mut(), action, ctx),
        ast::SetExpr::Query(query) => apply_action_to_query(query.as_mut(), action, ctx),
        ast::SetExpr::SetOperation { left, right, .. } => {
            let mut applied = false;
            if apply_action_to_set_expr(left.as_mut(), action, ctx)? {
                applied = true;
            }
            if apply_action_to_set_expr(right.as_mut(), action, ctx)? {
                applied = true;
            }
            Ok(applied)
        }
        ast::SetExpr::Values(_) => Ok(false),
        ast::SetExpr::Insert(_) => Err(DataFusionError::NotImplemented(
            "INSERT statements are not supported".to_string(),
        )),
    }
}

fn apply_action_to_select(
    select: &mut ast::Select,
    action: &ModifyAction,
    ctx: &MetaContext,
) -> Result<bool> {
    let mut applied = false;

    for table_with_joins in &mut select.from {
        if apply_action_to_table_with_joins(table_with_joins, action, ctx)? {
            applied = true;
        }
    }

    // Find out if relation of the action exists in FROM clause
    let (cube_name, member_name) = action.get_cube_and_member_name()?;
    let Some(relation_alias) = alias_for_relation_in_from(&cube_name, &select.from) else {
        return Ok(applied);
    };

    // Find member and apply action to the respective clause
    let meta_member = MetaMember::get_from_ctx(ctx, &cube_name, &member_name)?;
    let clause = if matches!(meta_member, MetaMember::Dimension(_)) {
        &mut select.selection
    } else {
        &mut select.having
    };

    if apply_action_to_option_clause(clause, &relation_alias, &meta_member, action)? {
        applied = true;
    }
    Ok(applied)
}

fn apply_action_to_table_with_joins(
    table_with_joins: &mut ast::TableWithJoins,
    action: &ModifyAction,
    ctx: &MetaContext,
) -> Result<bool> {
    let mut applied = false;

    if apply_action_to_table_factor(&mut table_with_joins.relation, action, ctx)? {
        applied = true;
    }

    for join in &mut table_with_joins.joins {
        if apply_action_to_table_factor(&mut join.relation, action, ctx)? {
            applied = true;
        }
    }

    Ok(applied)
}

fn apply_action_to_table_factor(
    table_factor: &mut ast::TableFactor,
    action: &ModifyAction,
    ctx: &MetaContext,
) -> Result<bool> {
    match table_factor {
        ast::TableFactor::Table { .. } | ast::TableFactor::TableFunction { .. } => Ok(false),
        ast::TableFactor::Derived { subquery, .. } => apply_action_to_query(subquery, action, ctx),
        ast::TableFactor::NestedJoin(table_with_joins) => {
            apply_action_to_table_with_joins(table_with_joins, action, ctx)
        }
    }
}

fn alias_for_relation_in_from(
    relation_name: &str,
    from: &[ast::TableWithJoins],
) -> Option<ast::Ident> {
    for table_with_joins in from {
        if let Some(alias) =
            alias_for_relation_in_table_factor(relation_name, &table_with_joins.relation)
        {
            return Some(alias);
        }
        for join in &table_with_joins.joins {
            if let Some(alias) = alias_for_relation_in_table_factor(relation_name, &join.relation) {
                return Some(alias);
            }
        }
    }
    None
}

fn alias_for_relation_in_table_factor(
    relation_name: &str,
    table_factor: &ast::TableFactor,
) -> Option<ast::Ident> {
    match table_factor {
        ast::TableFactor::Table { name, alias, .. } => {
            let ast::ObjectName(idents) = name;
            let last_ident = idents.last()?;
            let table_name = &last_ident.value;
            if !table_name.eq_ignore_ascii_case(relation_name) {
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

fn apply_action_to_option_clause(
    option_clause: &mut Option<ast::Expr>,
    relation_alias: &ast::Ident,
    meta_member: &MetaMember,
    action: &ModifyAction,
) -> Result<bool> {
    let Some(clause) = option_clause.as_mut() else {
        match action {
            ModifyAction::AddFilter(_) => {
                let expr = action.issue_expr(relation_alias, meta_member)?;
                *option_clause = Some(expr);
                return Ok(true);
            }
            ModifyAction::RemoveFilter(_) => {
                return Ok(false);
            }
        }
    };

    match apply_action_to_clause(clause, relation_alias, meta_member, action)? {
        PullUpAction::Continue => match action {
            ModifyAction::AddFilter(_) => {
                let expr = action.issue_expr(relation_alias, meta_member)?;
                *clause = ast::Expr::BinaryOp {
                    left: Box::new(clause.clone()),
                    op: ast::BinaryOperator::And,
                    right: Box::new(expr),
                };
                Ok(true)
            }
            ModifyAction::RemoveFilter(_) => Ok(false),
        },
        PullUpAction::Stop(modified) => Ok(modified),
        PullUpAction::RemoveAndStop => {
            *option_clause = None;
            Ok(true)
        }
    }
}

fn apply_action_to_clause(
    expr: &mut ast::Expr,
    relation_alias: &ast::Ident,
    meta_member: &MetaMember,
    action: &ModifyAction,
) -> Result<PullUpAction> {
    let new_expr = action.issue_expr(relation_alias, meta_member)?;
    if expr == &new_expr {
        match action {
            ModifyAction::AddFilter(_) => return Ok(PullUpAction::Stop(false)),
            ModifyAction::RemoveFilter(_) => return Ok(PullUpAction::RemoveAndStop),
        }
    }

    match expr {
        ast::Expr::BinaryOp { left, op, right } => match op {
            ast::BinaryOperator::And => {
                match apply_action_to_clause(left.as_mut(), relation_alias, meta_member, action)? {
                    PullUpAction::Continue => {}
                    PullUpAction::Stop(modified) => {
                        return Ok(PullUpAction::Stop(modified));
                    }
                    PullUpAction::RemoveAndStop => {
                        *expr = right.as_ref().clone();
                        return Ok(PullUpAction::Stop(true));
                    }
                }

                match apply_action_to_clause(right.as_mut(), relation_alias, meta_member, action)? {
                    PullUpAction::Continue => Ok(PullUpAction::Continue),
                    PullUpAction::Stop(modified) => Ok(PullUpAction::Stop(modified)),
                    PullUpAction::RemoveAndStop => {
                        *expr = left.as_ref().clone();
                        Ok(PullUpAction::Stop(true))
                    }
                }
            }
            _ => Ok(PullUpAction::Continue),
        },
        _ => Ok(PullUpAction::Continue),
    }
}

#[cfg(test)]
mod tests {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
            let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
            let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
            let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
            let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
            let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
                AND (KibanaSampleDataEcommerce.\"order_date\" < '2024-01-01' \
                    OR KibanaSampleDataEcommerce.\"order_date\" > '2024-12-31') \
            GROUP BY 1 \
            ORDER BY 1\
            "
        );
        assert!(applied);

        // Test removing existing "notInDateRange" filter
        let sql = modified_sql;
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
            let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
        let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
            let action = ModifyAction::AddFilter(V1LoadRequestQueryFilterItem {
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
            let action = ModifyAction::RemoveFilter(V1LoadRequestQueryFilterItem {
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
}
