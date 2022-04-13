use msql_srv::{Column, ColumnFlags, ColumnType};
use sqlparser::ast;

#[derive(Debug)]
pub enum BindValue {
    String(String),
    Int64(i64),
    #[allow(unused)]
    UInt64(u64),
    Float64(f64),
    Bool(bool),
}

trait Visitor<'ast> {
    fn visit_value(&mut self, _val: &mut ast::Value) {}

    fn visit_identifier(&mut self, _identifier: &mut ast::Ident) {}

    fn visit_expr(&mut self, expr: &mut ast::Expr) {
        match expr {
            ast::Expr::Value(value) => self.visit_value(value),
            ast::Expr::Identifier(identifier) => self.visit_identifier(identifier),
            ast::Expr::Nested(v) => self.visit_expr(&mut *v),
            ast::Expr::Between {
                expr,
                negated: _,
                low,
                high,
            } => {
                self.visit_expr(&mut *expr);
                self.visit_expr(&mut *low);
                self.visit_expr(&mut *high);
            }
            ast::Expr::BinaryOp { left, op: _, right } => {
                self.visit_expr(&mut *left);
                self.visit_expr(&mut *right);
            }
            ast::Expr::InList { expr, list, .. } => {
                self.visit_expr(&mut *expr);

                for v in list.iter_mut() {
                    self.visit_expr(v);
                }
            }
            _ => {}
        }
    }

    fn visit_table_factor(&mut self, factor: &mut ast::TableFactor) {
        match factor {
            ast::TableFactor::Derived { subquery, .. } => {
                self.visit_query(subquery);
            }
            _ => {}
        }
    }

    fn visit_join(&mut self, join: &mut ast::Join) {
        self.visit_table_factor(&mut join.relation);
    }

    fn visit_table_with_joins(&mut self, twj: &mut ast::TableWithJoins) {
        self.visit_table_factor(&mut twj.relation);

        for join in twj.joins.iter_mut() {
            self.visit_join(join);
        }
    }

    fn visit_select_item(&mut self, select: &mut ast::SelectItem) {
        match select {
            ast::SelectItem::ExprWithAlias { expr, .. } => self.visit_expr(expr),
            ast::SelectItem::UnnamedExpr(expr) => self.visit_expr(expr),
            _ => {}
        }
    }

    fn visit_select(&mut self, select: &mut Box<ast::Select>) {
        if let Some(selection) = &mut select.selection {
            self.visit_expr(selection);
        };

        for projection in &mut select.projection {
            self.visit_select_item(projection);
        }

        for from in &mut select.from {
            self.visit_table_with_joins(from);
        }
    }

    fn visit_set_expr(&mut self, body: &mut ast::SetExpr) {
        match body {
            ast::SetExpr::Select(select) => self.visit_select(select),
            ast::SetExpr::Query(query) => self.visit_query(query),
            ast::SetExpr::SetOperation { left, right, .. } => {
                self.visit_set_expr(&mut *left);
                self.visit_set_expr(&mut *right);
            }
            _ => {}
        }
    }

    fn visit_query(&mut self, query: &mut Box<ast::Query>) {
        self.visit_set_expr(&mut query.body);
    }

    fn visit_statement(&mut self, statement: &mut ast::Statement) {
        match statement {
            ast::Statement::Query(query) => self.visit_query(query),
            _ => {}
        }
    }
}

#[derive(Debug)]
pub struct FoundParameter {}

impl Into<Column> for FoundParameter {
    fn into(self) -> Column {
        Column {
            table: String::new(),
            column: "not implemented".to_owned(),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::empty(),
        }
    }
}

#[derive(Debug)]
pub struct StatementParamsFinder {
    parameters: Vec<FoundParameter>,
}

impl StatementParamsFinder {
    pub fn new() -> Self {
        Self { parameters: vec![] }
    }

    pub fn prepare(mut self, stmt: &mut ast::Statement) -> Vec<FoundParameter> {
        self.visit_statement(stmt);

        self.parameters
    }
}

impl<'ast> Visitor<'ast> for StatementParamsFinder {
    fn visit_value(&mut self, _: &mut ast::Value) {
        self.parameters.push(FoundParameter {})
    }
}

#[derive(Debug)]
pub struct StatementParamsBinder {
    position: usize,
    values: Vec<BindValue>,
}

impl StatementParamsBinder {
    pub fn new(values: Vec<BindValue>) -> Self {
        Self {
            position: 0,
            values,
        }
    }

    pub fn bind(mut self, stmt: &mut ast::Statement) {
        self.visit_statement(stmt);
    }
}

impl<'ast> Visitor<'ast> for StatementParamsBinder {
    fn visit_value(&mut self, value: &mut ast::Value) {
        match &value {
            ast::Value::Placeholder(_) => {
                let to_replace = self.values.get(self.position).expect(
                    format!(
                        "Unable to find value for placeholder at position: {}",
                        self.position
                    )
                    .as_str(),
                );
                self.position += 1;

                match to_replace {
                    BindValue::String(v) => {
                        *value = ast::Value::SingleQuotedString(v.clone());
                    }
                    BindValue::Bool(v) => {
                        *value = ast::Value::Boolean(*v);
                    }
                    BindValue::UInt64(v) => {
                        *value = ast::Value::Number(v.to_string(), false);
                    }
                    BindValue::Int64(v) => {
                        *value = ast::Value::Number(v.to_string(), *v < 0_i64);
                    }
                    BindValue::Float64(v) => {
                        *value = ast::Value::Number(v.to_string(), *v < 0_f64);
                    }
                }
            }
            _ => {}
        }
    }
}

#[derive(Debug)]
pub struct StatementPlaceholderReplacer {}

impl StatementPlaceholderReplacer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn replace(mut self, stmt: &mut ast::Statement) -> &mut ast::Statement {
        self.visit_statement(stmt);

        stmt
    }
}

impl<'ast> Visitor<'ast> for StatementPlaceholderReplacer {
    fn visit_value(&mut self, value: &mut ast::Value) {
        match &value {
            ast::Value::Placeholder(_) => {
                *value = ast::Value::SingleQuotedString("replaced_placeholder".to_string());
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CubeError;
    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    fn test_binder(input: &str, output: &str, values: Vec<BindValue>) -> Result<(), CubeError> {
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, &input).unwrap();

        let binder = StatementParamsBinder::new(values);
        let mut input = stmts[0].clone();
        binder.bind(&mut input);

        assert_eq!(input.to_string(), output);

        Ok(())
    }

    #[test]
    fn test_binder_named() -> Result<(), CubeError> {
        test_binder(
            "SELECT ?",
            "SELECT 'test'",
            vec![BindValue::String("test".to_string())],
        )?;

        test_binder(
            "SELECT ? AS t1, ? AS t2",
            "SELECT 'test1' AS t1, 'test2' AS t2",
            vec![
                BindValue::String("test1".to_string()),
                BindValue::String("test2".to_string()),
            ],
        )?;

        // binary op
        test_binder(
            r#"
                SELECT *
                FROM testdata
                WHERE fieldA = $1 AND fieldB = $2 OR (fieldC = $3 AND fieldD = $4)
            "#,
            "SELECT * FROM testdata WHERE fieldA = 'test' AND fieldB = 1 OR (fieldC = 2 AND fieldD = 2)",
            vec![
                BindValue::String("test".to_string()),
                BindValue::Int64(1),
                BindValue::UInt64(2),
                BindValue::Float64(2.0),
                BindValue::Bool(true),
            ],
        )?;

        // IN
        test_binder(
            r#"
                SELECT *
                FROM testdata
                WHERE fieldA IN ($1, $2)
            "#,
            "SELECT * FROM testdata WHERE fieldA IN ('test1', 'test2')",
            vec![
                BindValue::String("test1".to_string()),
                BindValue::String("test2".to_string()),
            ],
        )?;

        // BETWEEN
        test_binder(
            r#"
                SELECT *
                FROM testdata
                WHERE fieldA BETWEEN $1 AND $2
            "#,
            "SELECT * FROM testdata WHERE fieldA BETWEEN 'test1' AND 'test2'",
            vec![
                BindValue::String("test1".to_string()),
                BindValue::String("test2".to_string()),
            ],
        )?;

        test_binder(
            r#"
                SELECT *
                FROM testdata
                WHERE fieldA = $1
                UNION ALL
                SELECT *
                FROM testdata
                WHERE fieldA = $2
            "#,
            "SELECT * FROM testdata WHERE fieldA = 'test1' UNION ALL SELECT * FROM testdata WHERE fieldA = 'test2'",
            vec![
                BindValue::String(
                    "test1".to_string(),
                ),
                BindValue::String(
                    "test2".to_string(),
                ),
            ]
        )?;

        test_binder(
            r#"
                SELECT * FROM (
                    SELECT *
                    FROM testdata
                    WHERE fieldA = $1
                )
            "#,
            "SELECT * FROM (SELECT * FROM testdata WHERE fieldA = 'test1')",
            vec![BindValue::String("test1".to_string())],
        )?;

        Ok(())
    }

    fn assert_placeholder_replacer(input: &str, output: &str) -> Result<(), CubeError> {
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, &input).unwrap();

        let binder = StatementPlaceholderReplacer::new();
        let mut input = stmts[0].clone();
        binder.replace(&mut input);

        assert_eq!(input.to_string(), output);

        Ok(())
    }

    #[test]
    fn test_placeholder_replacer() -> Result<(), CubeError> {
        assert_placeholder_replacer("SELECT ?", "SELECT 'replaced_placeholder'")?;

        Ok(())
    }
}
