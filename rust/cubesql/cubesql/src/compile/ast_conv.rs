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
                    _ => Err(DataFusionError::Plan(format!(
                        "Unsupported filter operator: {:?}",
                        filter.operator
                    ))),
                }
            }
        }
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

// fn relation_matches_table_factor(
//     table_factor: &mut ast::TableFactor,
//     relation_name: &str,
//     action: &ModifyAction,
//     ctx: &MetaContext,
// ) -> Result<

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

        Ok(())
    }
}
