use itertools::Itertools;
use log::trace;
use pg_srv::{
    protocol::{ErrorCode, ErrorResponse},
    BindValue, PgType, PgTypeId,
};
use sqlparser::ast::{
    self, ArrayAgg, Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName, Value,
    WithinGroup,
};
use std::{collections::HashMap, error::Error};

use super::types::ColumnType;
use crate::sql::shim::ConnectionError;

#[derive(Debug)]
enum PlaceholderType {
    String,
    Number,
}

impl PlaceholderType {
    pub fn to_coltype(self) -> ColumnType {
        match self {
            Self::String => ColumnType::String,
            Self::Number => ColumnType::Int64,
        }
    }
}

trait Visitor<'ast, E: Error> {
    fn visit_value(
        &mut self,
        _val: &mut ast::Value,
        _placeholder_type: PlaceholderType,
    ) -> Result<(), E> {
        Ok(())
    }

    fn visit_identifier(&mut self, _identifier: &mut ast::Ident) -> Result<(), E> {
        Ok(())
    }

    fn visit_expr(&mut self, expr: &mut Expr) -> Result<(), E> {
        self.visit_expr_with_placeholder_type(expr, PlaceholderType::String)
    }

    fn visit_expr_with_placeholder_type(
        &mut self,
        expr: &mut Expr,
        placeholder_type: PlaceholderType,
    ) -> Result<(), E> {
        match expr {
            Expr::Value(value) => self.visit_value(value, placeholder_type)?,
            Expr::Identifier(identifier) => self.visit_identifier(identifier)?,
            Expr::CompoundIdentifier(identifiers) => {
                for ident in identifiers.iter_mut() {
                    self.visit_identifier(ident)?;
                }
            }
            Expr::Nested(v) => self.visit_expr(&mut *v)?,
            Expr::Cast { .. } => self.visit_cast(expr)?,
            Expr::Between {
                expr,
                negated: _,
                low,
                high,
            } => {
                self.visit_expr(&mut *expr)?;
                self.visit_expr(&mut *low)?;
                self.visit_expr(&mut *high)?;
            }
            Expr::AnyOp(expr) => {
                self.visit_expr(expr)?;
            }
            Expr::AllOp(expr) => {
                self.visit_expr(&mut *expr)?;
            }
            Expr::BinaryOp { left, op: _, right } => {
                self.visit_expr(&mut *left)?;
                self.visit_expr(&mut *right)?;
            }
            Expr::Like { expr, pattern, .. }
            | Expr::ILike { expr, pattern, .. }
            | Expr::SimilarTo { expr, pattern, .. } => {
                self.visit_expr(&mut *expr)?;
                self.visit_expr(&mut *pattern)?;
            }
            Expr::InList { expr, list, .. } => {
                self.visit_expr(&mut *expr)?;

                for v in list.iter_mut() {
                    self.visit_expr(v)?;
                }
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(op) = operand {
                    self.visit_expr(&mut *op)?;
                }
                for con in conditions.iter_mut() {
                    self.visit_expr(&mut *con)?;
                }
                for res in results.iter_mut() {
                    self.visit_expr(&mut *res)?;
                }
                if let Some(res) = else_result {
                    self.visit_expr(&mut *res)?;
                }
            }
            Expr::IsNull(expr) | Expr::IsNotNull(expr) => self.visit_expr(expr)?,
            Expr::IsDistinctFrom(expr_1, expr_2) | Expr::IsNotDistinctFrom(expr_1, expr_2) => {
                self.visit_expr(expr_1)?;
                self.visit_expr(expr_2)?;
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.visit_expr(expr)?;
                self.visit_query(subquery)?;
            }
            Expr::AnyAllSubquery(query) => {
                self.visit_query(query)?;
            }
            Expr::InUnnest {
                expr, array_expr, ..
            } => {
                self.visit_expr(expr)?;
                self.visit_expr(array_expr)?;
            }
            Expr::UnaryOp { expr, .. } => {
                self.visit_expr(expr)?;
            }
            Expr::TryCast { expr, .. } | Expr::Extract { expr, .. } => self.visit_expr(expr)?,
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                self.visit_expr(expr)?;
                if let Some(res) = substring_from {
                    self.visit_expr(res)?;
                }
                if let Some(res) = substring_for {
                    self.visit_expr(res)?;
                }
            }
            Expr::Trim { expr, trim_where } => {
                self.visit_expr(expr)?;
                if let Some((_, res)) = trim_where {
                    self.visit_expr(res)?;
                }
            }
            Expr::Collate { expr, collation } => {
                self.visit_expr(expr)?;
                for res in collation.0.iter_mut() {
                    self.visit_identifier(res)?;
                }
            }
            Expr::MapAccess { column, keys } => {
                self.visit_expr(column)?;
                for res in keys.iter_mut() {
                    self.visit_expr(res)?;
                }
            }
            Expr::Function(fun) => self.visit_function(fun)?,
            Expr::Exists(query) | Expr::Subquery(query) => self.visit_query(query)?,
            Expr::ListAgg(list_agg) => {
                self.visit_expr(&mut list_agg.expr)?;
                if let Some(separator) = &mut list_agg.separator {
                    self.visit_expr(separator)?;
                }
                if let Some(on_overflow) = &mut list_agg.on_overflow {
                    if let ast::ListAggOnOverflow::Truncate { filler, .. } = on_overflow {
                        if let Some(expr) = filler {
                            self.visit_expr(expr)?;
                        }
                    }
                }
            }
            Expr::GroupingSets(vec) | Expr::Cube(vec) | Expr::Rollup(vec) => {
                for v in vec.iter_mut() {
                    for expr in v.iter_mut() {
                        self.visit_expr(expr)?;
                    }
                }
            }
            Expr::Tuple(vec) => {
                for expr in vec.iter_mut() {
                    self.visit_expr(expr)?;
                }
            }
            Expr::ArrayIndex { obj, indexs } => {
                self.visit_expr(obj)?;
                for expr in indexs.iter_mut() {
                    self.visit_expr(expr)?;
                }
            }
            Expr::Array(arr) => {
                for expr in arr.elem.iter_mut() {
                    self.visit_expr(expr)?;
                }
            }
            Expr::ArraySubquery(query) => self.visit_query(query)?,
            Expr::DotExpr { expr, field } => {
                self.visit_expr(expr)?;
                self.visit_identifier(field)?;
            }
            Expr::TypedString { .. } => (),
            Expr::AtTimeZone { timestamp, .. } => self.visit_expr(timestamp)?,
            Expr::Position { expr, r#in } => {
                self.visit_expr(expr)?;
                self.visit_expr(r#in)?;
            }
            Expr::ArrayAgg(ArrayAgg {
                expr,
                order_by,
                limit,
                ..
            }) => {
                self.visit_expr(expr)?;
                if let Some(order_by) = order_by {
                    self.visit_expr(&mut order_by.expr)?;
                }
                if let Some(limit) = limit {
                    self.visit_expr(limit)?;
                }
            }
            Expr::WithinGroup(WithinGroup { expr, order_by }) => {
                self.visit_expr(expr)?;
                for order_by_expr in order_by {
                    self.visit_expr(&mut order_by_expr.expr)?;
                }
            }
        };

        Ok(())
    }

    fn visit_table_factor(&mut self, factor: &mut ast::TableFactor) -> Result<(), E> {
        match factor {
            ast::TableFactor::Derived {
                subquery, alias, ..
            } => {
                self.visit_query(subquery)?;
                self.visit_table_alias(alias)?;
            }
            ast::TableFactor::TableFunction { expr, alias } => {
                self.visit_expr(expr)?;
                self.visit_table_alias(alias)?;
            }
            ast::TableFactor::NestedJoin(table_with_joins) => {
                self.visit_table_with_joins(&mut *table_with_joins)?;
            }
            ast::TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            } => {
                for ident in name.0.iter_mut() {
                    self.visit_identifier(ident)?;
                }
                self.visit_table_alias(alias)?;
                self.visit_function_args(args)?;
                for hint in with_hints.iter_mut() {
                    self.visit_expr(hint)?;
                }
            }
        };

        Ok(())
    }

    fn visit_join(&mut self, join: &mut ast::Join) -> Result<(), E> {
        self.visit_table_factor(&mut join.relation)?;

        match &mut join.join_operator {
            ast::JoinOperator::Inner(constr)
            | ast::JoinOperator::LeftOuter(constr)
            | ast::JoinOperator::RightOuter(constr)
            | ast::JoinOperator::FullOuter(constr) => match constr {
                ast::JoinConstraint::On(expr) => {
                    self.visit_expr(expr)?;
                }
                ast::JoinConstraint::Using(idents) => {
                    for ident in idents.iter_mut() {
                        self.visit_identifier(ident)?;
                    }
                }
                ast::JoinConstraint::Natural | ast::JoinConstraint::None => (),
            },
            ast::JoinOperator::CrossJoin
            | ast::JoinOperator::CrossApply
            | ast::JoinOperator::OuterApply => (),
        };

        Ok(())
    }

    fn visit_table_with_joins(&mut self, twj: &mut ast::TableWithJoins) -> Result<(), E> {
        self.visit_table_factor(&mut twj.relation)?;

        for join in twj.joins.iter_mut() {
            self.visit_join(join)?;
        }

        Ok(())
    }

    fn visit_select_item(&mut self, select: &mut ast::SelectItem) -> Result<(), E> {
        match select {
            ast::SelectItem::ExprWithAlias { expr, .. } => self.visit_expr(expr)?,
            ast::SelectItem::UnnamedExpr(expr) => self.visit_expr(expr)?,
            ast::SelectItem::QualifiedWildcard(name) => {
                for ident in name.0.iter_mut() {
                    self.visit_identifier(ident)?;
                }
            }
            ast::SelectItem::Wildcard => (),
        };

        Ok(())
    }

    fn visit_select(&mut self, select: &mut Box<ast::Select>) -> Result<(), E> {
        if let Some(selection) = &mut select.selection {
            self.visit_expr(selection)?;
        };

        for projection in &mut select.projection {
            self.visit_select_item(projection)?;
        }

        for from in &mut select.from {
            self.visit_table_with_joins(from)?;
        }

        if let Some(having) = &mut select.having {
            self.visit_expr(having)?;
        }

        for group_by in &mut select.group_by {
            self.visit_expr(group_by)?;
        }

        Ok(())
    }

    fn visit_set_expr(&mut self, body: &mut ast::SetExpr) -> Result<(), E> {
        match body {
            ast::SetExpr::Select(select) => self.visit_select(select)?,
            ast::SetExpr::Query(query) => self.visit_query(query)?,
            ast::SetExpr::SetOperation { left, right, .. } => {
                self.visit_set_expr(&mut *left)?;
                self.visit_set_expr(&mut *right)?;
            }
            ast::SetExpr::Values(vals) => {
                for v in vals.0.iter_mut() {
                    for expr in v.iter_mut() {
                        self.visit_expr(expr)?;
                    }
                }
            }
            ast::SetExpr::Insert(_) => (),
        };

        Ok(())
    }

    fn visit_query(&mut self, query: &mut Box<ast::Query>) -> Result<(), E> {
        self.visit_set_expr(&mut query.body)?;
        if let Some(with) = query.with.as_mut() {
            self.visit_with(with)?;
        }
        if let Some(limit) = query.limit.as_mut() {
            self.visit_expr_with_placeholder_type(limit, PlaceholderType::Number)?;
        }
        if let Some(offset) = query.offset.as_mut() {
            self.visit_expr_with_placeholder_type(&mut offset.value, PlaceholderType::Number)?;
        }
        if let Some(fetch) = query.fetch.as_mut() {
            if let Some(quantity) = fetch.quantity.as_mut() {
                self.visit_expr_with_placeholder_type(quantity, PlaceholderType::Number)?;
            }
        }

        Ok(())
    }

    fn visit_with(&mut self, with: &mut ast::With) -> Result<(), E> {
        for cte in &mut with.cte_tables {
            self.visit_query(&mut cte.query)?;
        }

        Ok(())
    }

    fn visit_statement(&mut self, statement: &mut ast::Statement) -> Result<(), E> {
        match statement {
            ast::Statement::Query(query) => self.visit_query(query)?,
            ast::Statement::Explain { statement, .. } => self.visit_statement(statement)?,
            ast::Statement::Declare { query, .. } => self.visit_query(query)?,
            // TODO:
            _ => {}
        };

        Ok(())
    }

    fn visit_cast(&mut self, expr: &mut Expr) -> Result<(), E> {
        if let Expr::Cast { expr, .. } = expr {
            self.visit_expr(expr)?;
        } else {
            unreachable!(
                "visit_expr requires Cast expression as an argument, actual: {}",
                expr
            )
        };

        Ok(())
    }

    fn visit_function(&mut self, fun: &mut ast::Function) -> Result<(), E> {
        for res in fun.name.0.iter_mut() {
            self.visit_identifier(res)?;
        }
        self.visit_function_args(&mut fun.args)?;
        if let Some(over) = &mut fun.over {
            for res in over.partition_by.iter_mut() {
                self.visit_expr(res)?;
            }
            for order_expr in over.order_by.iter_mut() {
                self.visit_expr(&mut order_expr.expr)?;
            }
        }

        Ok(())
    }

    fn visit_function_args(&mut self, args: &mut Vec<ast::FunctionArg>) -> Result<(), E> {
        for a in args.iter_mut() {
            match a {
                ast::FunctionArg::Named { name, arg } => {
                    self.visit_identifier(name)?;
                    self.visit_function_arg_expr(arg)?;
                }
                ast::FunctionArg::Unnamed(arg) => self.visit_function_arg_expr(arg)?,
            }
        }

        Ok(())
    }

    fn visit_function_arg_expr(&mut self, arg: &mut ast::FunctionArgExpr) -> Result<(), E> {
        match arg {
            ast::FunctionArgExpr::Expr(expr) => self.visit_expr(expr)?,
            ast::FunctionArgExpr::QualifiedWildcard(name) => {
                for ident in name.0.iter_mut() {
                    self.visit_identifier(ident)?;
                }
            }
            ast::FunctionArgExpr::Wildcard => (),
        };

        Ok(())
    }

    fn visit_table_alias(&mut self, alias: &mut Option<ast::TableAlias>) -> Result<(), E> {
        if let Some(a) = alias {
            self.visit_identifier(&mut a.name)?;
            for ident in a.columns.iter_mut() {
                self.visit_identifier(ident)?;
            }
        }

        Ok(())
    }

    fn extract_placeholder_index(&self, name: &str) -> Result<usize, ConnectionError> {
        if name.len() > 1 && name[0..1] == *"$" {
            let n = name[1..].to_string().parse::<i32>().map_err(|err| {
                ConnectionError::from(ErrorResponse::error(
                    ErrorCode::SyntaxError,
                    format!(
                        "Unable to extract position for placeholder, actual: {name}, err: {err}"
                    ),
                ))
            })?;

            if n < 1 {
                return Err(ConnectionError::from(ErrorResponse::error(
                    ErrorCode::SyntaxError,
                    format!("Placeholder index must be >= 1, actual: {}", n),
                )));
            }

            Ok(n as usize - 1)
        } else {
            Err(ConnectionError::from(ErrorResponse::error(
                ErrorCode::SyntaxError,
                format!(
                    "Unable to extract index for placeholder, It must starts with $, actual: {}",
                    name
                ),
            )))
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct FoundParameter {
    pub coltype: ColumnType,
}

impl FoundParameter {
    fn new(coltype: ColumnType) -> Self {
        Self { coltype }
    }
}

#[derive(Debug)]
pub struct PostgresStatementParamsFinder<'t> {
    parameters: HashMap<usize, FoundParameter>,
    types: &'t [u32],
}

impl<'t> PostgresStatementParamsFinder<'t> {
    pub fn new(types: &'t [u32]) -> Self {
        Self {
            parameters: HashMap::new(),
            types,
        }
    }

    pub fn find(mut self, stmt: &ast::Statement) -> Result<Vec<FoundParameter>, ConnectionError> {
        self.visit_statement(&mut stmt.clone())?;

        Ok(self
            .parameters
            .into_iter()
            .sorted_by(|(l, _), (r, _)| Ord::cmp(l, r))
            .map(|(_, v)| v)
            .collect())
    }
}

impl<'ast, 't> Visitor<'ast, ConnectionError> for PostgresStatementParamsFinder<'t> {
    fn visit_value(
        &mut self,
        v: &mut ast::Value,
        pt: PlaceholderType,
    ) -> Result<(), ConnectionError> {
        match v {
            Value::Placeholder(name) => {
                let position = self.extract_placeholder_index(&name)?;

                let coltype = self
                    .types
                    .get(position)
                    .and_then(|pg_type_oid| PgTypeId::from_oid(*pg_type_oid))
                    .and_then(|pg_type| ColumnType::from_pg_tid(pg_type).ok())
                    .unwrap_or_else(|| pt.to_coltype());

                self.parameters
                    .insert(position, FoundParameter::new(coltype));
            }
            _ => {}
        };

        Ok(())
    }
}

#[derive(Debug)]
pub struct PostgresStatementParamsBinder {
    values: Vec<BindValue>,
}

impl PostgresStatementParamsBinder {
    pub fn new(values: Vec<BindValue>) -> Self {
        Self { values }
    }

    pub fn bind(mut self, stmt: &mut ast::Statement) -> Result<(), ConnectionError> {
        self.visit_statement(stmt)
    }
}

impl<'ast> Visitor<'ast, ConnectionError> for PostgresStatementParamsBinder {
    fn visit_value(
        &mut self,
        value: &mut ast::Value,
        placeholder_type: PlaceholderType,
    ) -> Result<(), ConnectionError> {
        match &value {
            ast::Value::Placeholder(name) => {
                let position = self.extract_placeholder_index(&name)?;
                let to_replace = self.values.get(position).ok_or({
                    ConnectionError::from(ErrorResponse::error(
                        ErrorCode::InternalError,
                        format!(
                            "Unable to find value for placeholder at position: {}",
                            position
                        ),
                    ))
                })?;
                match to_replace {
                    BindValue::String(v) => {
                        // FIXME: this workaround is needed as we don't know types on Bind
                        *value = match placeholder_type {
                            PlaceholderType::String => ast::Value::SingleQuotedString(v.clone()),
                            PlaceholderType::Number => ast::Value::Number(v.clone(), false),
                        };
                    }
                    BindValue::Bool(v) => {
                        *value = ast::Value::Boolean(*v);
                    }
                    BindValue::Int64(v) => {
                        *value = ast::Value::Number(v.to_string(), *v < 0_i64);
                    }
                    BindValue::Float64(v) => {
                        *value = ast::Value::Number(v.to_string(), *v < 0_f64);
                    }
                    BindValue::Timestamp(v) => {
                        *value = ast::Value::SingleQuotedString(v.to_string());
                    }
                    BindValue::Date(v) => {
                        *value = ast::Value::SingleQuotedString(v.to_string());
                    }
                    BindValue::Null => {
                        *value = ast::Value::Null;
                    }
                }
            }
            _ => {}
        };

        Ok(())
    }
}

#[derive(Debug)]
pub struct StatementPlaceholderReplacer {}

impl StatementPlaceholderReplacer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn replace(mut self, stmt: ast::Statement) -> Result<ast::Statement, ConnectionError> {
        let mut result = stmt;

        self.visit_statement(&mut result)?;

        Ok(result)
    }
}

impl<'ast> Visitor<'ast, ConnectionError> for StatementPlaceholderReplacer {
    fn visit_value(
        &mut self,
        value: &mut ast::Value,
        placeholder_type: PlaceholderType,
    ) -> Result<(), ConnectionError> {
        match &value {
            // NOTE: it does not do any harm if a numeric placeholder is replaced with a string,
            // this will be handled with Bind anyway
            ast::Value::Placeholder(_) => {
                *value = match placeholder_type {
                    PlaceholderType::String => {
                        ast::Value::SingleQuotedString("replaced_placeholder".to_string())
                    }
                    PlaceholderType::Number => ast::Value::Number("1".to_string(), false),
                };
            }
            _ => {}
        };

        Ok(())
    }
}

#[derive(Debug)]
pub struct CastReplacer {}

impl CastReplacer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn replace(mut self, stmt: ast::Statement) -> ast::Statement {
        let mut result = stmt;

        self.visit_statement(&mut result).unwrap();

        result
    }

    fn parse_value_to_str<'a>(&self, expr: &'a Value) -> Option<&'a str> {
        match expr {
            Value::SingleQuotedString(str) | Value::DoubleQuotedString(str) => Some(&str),
            _ => None,
        }
    }
}

impl<'ast> Visitor<'ast, ConnectionError> for CastReplacer {
    fn visit_cast(&mut self, expr: &mut Expr) -> Result<(), ConnectionError> {
        if let Expr::Cast {
            expr: cast_expr,
            data_type,
        } = expr
        {
            match data_type {
                ast::DataType::Custom(name) => match name.to_string().to_lowercase().as_str() {
                    "name" | "oid" | "information_schema.cardinal_number" | "regproc" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *expr = *cast_expr.clone();
                    }
                    "xid" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *data_type = ast::DataType::UnsignedInt(None);
                    }
                    "int2" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *data_type = ast::DataType::SmallInt(None)
                    }
                    "int4" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *data_type = ast::DataType::Int(None)
                    }
                    "int8" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *data_type = ast::DataType::BigInt(None)
                    }
                    "float8" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *data_type = ast::DataType::Double;
                    }
                    "bool" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *data_type = ast::DataType::Boolean;
                    }
                    "timestamptz" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *data_type = ast::DataType::Timestamp;
                    }
                    "regtype" => {
                        self.visit_expr(&mut *cast_expr)?;

                        if let Expr::Identifier(_) = &**cast_expr {
                            *expr = Expr::Function(Function {
                                name: ObjectName(vec![Ident {
                                    value: "format_type".to_string(),
                                    quote_style: None,
                                }]),
                                args: vec![
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(*cast_expr.clone())),
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                        Value::Null,
                                    ))),
                                ],
                                over: None,
                                distinct: false,
                                special: false,
                                approximate: false,
                            })
                        }
                    }
                    "\"char\"" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *data_type = ast::DataType::Text;
                    }
                    // TODO:
                    _ => (),
                },
                ast::DataType::Regclass => match &**cast_expr {
                    Expr::Value(val) => {
                        let str_val = self.parse_value_to_str(&val);
                        let Some(str_val) = str_val else {
                            return Ok(());
                        };
                        let str_val = str_val.strip_prefix("pg_catalog.").unwrap_or(&str_val);

                        for typ in PgType::get_all() {
                            if typ.typname == str_val {
                                *expr = Expr::Value(Value::Number(typ.typrelid.to_string(), false));
                                return Ok(());
                            }
                        }

                        trace!(
                            r#"Unable to cast string to RegClass via CastReplacer, type "{}" is not defined"#,
                            str_val
                        );
                    }
                    _ => {
                        self.visit_expr(&mut *cast_expr)?;

                        *expr = ast::Expr::Function(ast::Function {
                            name: ast::ObjectName(vec![ast::Ident {
                                value: "__cube_regclass_cast".to_string(),
                                quote_style: None,
                            }]),
                            args: vec![ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                *cast_expr.clone(),
                            ))],
                            over: None,
                            distinct: false,
                            special: false,
                            approximate: false,
                        })
                    }
                },
                _ => self.visit_expr(&mut *cast_expr)?,
            }
        };

        Ok(())
    }
}

#[derive(Debug)]
pub struct RedshiftDatePartReplacer {}

impl RedshiftDatePartReplacer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn replace(mut self, stmt: ast::Statement) -> ast::Statement {
        let mut result = stmt;

        self.visit_statement(&mut result).unwrap();

        result
    }
}

impl<'ast> Visitor<'ast, ConnectionError> for RedshiftDatePartReplacer {
    fn visit_function(&mut self, fun: &mut Function) -> Result<(), ConnectionError> {
        for res in fun.name.0.iter_mut() {
            self.visit_identifier(res)?;
        }
        let fn_name = fun.name.to_string().to_lowercase();
        if (fn_name == "datediff" || fn_name == "dateadd") && fun.args.len() == 3 {
            if let ast::FunctionArg::Unnamed(arg) = &mut fun.args[0] {
                if let FunctionArgExpr::Expr(arg) = arg {
                    if let Expr::Identifier(ident) = arg {
                        let granularity_in_identifier = ident.value.to_lowercase();
                        match granularity_in_identifier.as_str() {
                            "second" | "minute" | "hour" | "day" | "qtr" | "week" | "month"
                            | "year" => {
                                *arg = Expr::Value(Value::SingleQuotedString(
                                    granularity_in_identifier,
                                ));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        self.visit_function_args(&mut fun.args)?;
        if let Some(over) = &mut fun.over {
            for res in over.partition_by.iter_mut() {
                self.visit_expr(res)?;
            }
            for order_expr in over.order_by.iter_mut() {
                self.visit_expr(&mut order_expr.expr)?;
            }
        }

        Ok(())
    }
}

/// Postgres to_timestamp clashes with Datafusion to_timestamp so we replace it with str_to_date
#[derive(Debug)]
pub struct ToTimestampReplacer {}

impl ToTimestampReplacer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn replace(mut self, stmt: ast::Statement) -> ast::Statement {
        let mut result = stmt;

        self.visit_statement(&mut result).unwrap();

        result
    }
}

impl<'ast> Visitor<'ast, ConnectionError> for ToTimestampReplacer {
    fn visit_identifier(&mut self, identifier: &mut Ident) -> Result<(), ConnectionError> {
        if identifier.value.to_lowercase() == "to_timestamp" {
            identifier.value = "str_to_date".to_string()
        };

        Ok(())
    }
}
// Some Postgres UDFs accept rows (records) as arguments.
// We simplify the expression, passing only the required values
pub struct UdfWildcardArgReplacer {}

impl UdfWildcardArgReplacer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn replace(mut self, stmt: ast::Statement) -> ast::Statement {
        let mut result = stmt;

        self.visit_statement(&mut result).unwrap();

        result
    }

    pub fn get_new_args_for_fn(
        &self,
        name: &str,
        args: &Vec<ast::FunctionArg>,
    ) -> Option<Vec<ast::FunctionArg>> {
        match name {
            "information_schema._pg_truetypid" => self.replace_simple(
                args,
                vec![(0, "atttypid"), (1, "typtype"), (1, "typbasetype")],
            ),
            "information_schema._pg_truetypmod" => self.replace_simple(
                args,
                vec![(0, "atttypmod"), (1, "typtype"), (1, "typtypmod")],
            ),
            _ => None,
        }
    }

    pub fn replace_simple(
        &self,
        args: &Vec<ast::FunctionArg>,
        mapping: Vec<(usize, &str)>,
    ) -> Option<Vec<ast::FunctionArg>> {
        let max_index = mapping.iter().map(|(index, _)| index).max()?;
        if args.len() <= *max_index {
            return None;
        }

        let new_args = mapping
            .iter()
            .map(|(index, column)| match &args[*index] {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::QualifiedWildcard(
                    ast::ObjectName(idents),
                )) => {
                    let mut new_idents = idents.clone();
                    new_idents.push(ast::Ident {
                        value: column.to_string(),
                        quote_style: None,
                    });
                    let new_arg = ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                        ast::Expr::CompoundIdentifier(new_idents),
                    ));
                    Some(new_arg)
                }
                _ => None,
            })
            .collect::<Option<_>>();

        new_args
    }
}

impl<'a> Visitor<'a, ConnectionError> for UdfWildcardArgReplacer {
    fn visit_function(&mut self, fun: &mut ast::Function) -> Result<(), ConnectionError> {
        if let Some(new_args) = self.get_new_args_for_fn(&fun.name.to_string(), &fun.args) {
            fun.args = new_args
        }
        for res in fun.name.0.iter_mut() {
            self.visit_identifier(res)?;
        }
        self.visit_function_args(&mut fun.args)?;
        if let Some(over) = &mut fun.over {
            for res in over.partition_by.iter_mut() {
                self.visit_expr(res)?;
            }
            for order_expr in over.order_by.iter_mut() {
                self.visit_expr(&mut order_expr.expr)?;
            }
        }

        Ok(())
    }
}

pub struct ApproximateCountDistinctVisitor {}

impl ApproximateCountDistinctVisitor {
    pub fn new() -> Self {
        Self {}
    }

    pub fn replace(mut self, stmt: ast::Statement) -> ast::Statement {
        let mut result = stmt;

        self.visit_statement(&mut result).unwrap();

        result
    }
}

impl<'a> Visitor<'a, ConnectionError> for ApproximateCountDistinctVisitor {
    fn visit_function(&mut self, fun: &mut ast::Function) -> Result<(), ConnectionError> {
        if fun.approximate && fun.distinct && &fun.name.to_string().to_uppercase() == "COUNT" {
            fun.name = ast::ObjectName(vec![ast::Ident::new("APPROX_DISTINCT")]);
            fun.approximate = false;
            fun.distinct = false;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct SensitiveDataSanitizer {}

impl SensitiveDataSanitizer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn replace(mut self, stmt: ast::Statement) -> ast::Statement {
        let mut result = stmt;

        self.visit_statement(&mut result).unwrap();

        result
    }
}

impl<'ast> Visitor<'ast, ConnectionError> for SensitiveDataSanitizer {
    fn visit_value(
        &mut self,
        val: &mut ast::Value,
        _pt: PlaceholderType,
    ) -> Result<(), ConnectionError> {
        match val {
            ast::Value::SingleQuotedString(str)
            | ast::Value::DoubleQuotedString(str)
            | ast::Value::NationalStringLiteral(str) => {
                if ["false", "true"].contains(&str.as_str()) || str.len() < 4 {
                    return Ok(());
                }
                *str = "[REPLACED]".to_string();
            }
            _ => (),
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CubeError;
    use pg_srv::{DateValue, TimestampValue};
    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    fn run_cast_replacer(input: &str, output: &str) -> Result<(), CubeError> {
        let stmt = Parser::parse_sql(&PostgreSqlDialect {}, &input)
            .unwrap()
            .pop()
            .expect("must contain at least one statement");

        let replacer = CastReplacer::new();
        let res = replacer.replace(stmt);

        assert_eq!(res.to_string(), output);

        Ok(())
    }

    #[test]
    fn test_cast_replacer() -> Result<(), CubeError> {
        run_cast_replacer("SELECT 'pg_class'::regclass", "SELECT 1259")?;
        run_cast_replacer("SELECT 'pg_class'::regclass::oid", "SELECT 1259")?;
        run_cast_replacer("SELECT 64::information_schema.cardinal_number", "SELECT 64")?;
        run_cast_replacer(
            "SELECT NOW()::timestamptz",
            "SELECT CAST(NOW() AS TIMESTAMP)",
        )?;
        run_cast_replacer(
            "SELECT CAST(1 + 1 as Regclass);",
            "SELECT __cube_regclass_cast(1 + 1)",
        )?;

        Ok(())
    }

    fn run_redshift_date_part_replacer(input: &str, output: &str) -> Result<(), CubeError> {
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, &input).unwrap();

        let replacer = RedshiftDatePartReplacer::new();
        let res = replacer.replace(stmts[0].clone());

        assert_eq!(res.to_string(), output);

        Ok(())
    }

    #[test]
    fn test_redshift_date_part_replacer() -> Result<(), CubeError> {
        run_redshift_date_part_replacer(
            r#"SELECT DATEDIFF(day, DATE '1970-01-01', "ta_1"."createdAt")"#,
            r#"SELECT DATEDIFF('day', DATE '1970-01-01', "ta_1"."createdAt")"#,
        )?;

        run_redshift_date_part_replacer(
            r#"SELECT DATEADD(week, '2009-01-01', '2009-12-31')"#,
            r#"SELECT DATEADD('week', '2009-01-01', '2009-12-31')"#,
        )?;

        run_redshift_date_part_replacer(
            r#"SELECT DATEDIFF(day, DATEADD(week, '2009-01-01', '2009-12-31'), "ta_1"."createdAt")"#,
            r#"SELECT DATEDIFF('day', DATEADD('week', '2009-01-01', '2009-12-31'), "ta_1"."createdAt")"#,
        )?;

        Ok(())
    }

    fn run_pg_binder(
        input: &str,
        output: &str,
        values: Vec<BindValue>,
    ) -> Result<(), ConnectionError> {
        let stmt = Parser::parse_sql(&PostgreSqlDialect {}, &input)
            .unwrap()
            .pop()
            .expect("must contain at least one statement");

        let binder = PostgresStatementParamsBinder::new(values);
        let mut res = stmt;
        binder.bind(&mut res)?;

        assert_eq!(res.to_string(), output);

        Ok(())
    }

    #[test]
    fn test_pg_binder() -> Result<(), ConnectionError> {
        run_pg_binder(
            "SELECT $1",
            "SELECT 'test'",
            vec![BindValue::String("test".to_string())],
        )?;

        run_pg_binder(
            "SELECT $1 AS t1, $2 AS t2",
            "SELECT 'test1' AS t1, NULL AS t2",
            vec![BindValue::String("test1".to_string()), BindValue::Null],
        )?;

        run_pg_binder(
            "SELECT $1 AS t1, $2 AS t2, $1 as b1, $2 as b2",
            "SELECT 'test1' AS t1, NULL AS t2, 'test1' AS b1, NULL AS b2",
            vec![BindValue::String("test1".to_string()), BindValue::Null],
        )?;

        // binary op
        run_pg_binder(
            r#"
                SELECT *
                FROM testdata
                WHERE fieldA = $1 AND fieldB = $2 OR (fieldC = $3 AND fieldD = $4)
            "#,
            "SELECT * FROM testdata WHERE fieldA = 'test' AND fieldB = 1 OR (fieldC = 2 AND fieldD = 2)",
            vec![
                BindValue::String("test".to_string()),
                BindValue::Int64(1),
                BindValue::Int64(2),
                BindValue::Float64(2.0),
                BindValue::Bool(true),
            ],
        )?;

        // IN
        run_pg_binder(
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
        run_pg_binder(
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

        run_pg_binder(
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

        run_pg_binder(
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

        // test TimestampValue binding in the WHERE clause
        run_pg_binder(
            "SELECT * FROM events WHERE created_at BETWEEN $1 AND $2",
            "SELECT * FROM events WHERE created_at BETWEEN '2022-04-25T12:38:42.000' AND '2025-08-08T09:30:45.123'",
            vec![
                BindValue::Timestamp(TimestampValue::new(1650890322000000000, None)),
                BindValue::Timestamp(TimestampValue::new(1754645445123456000, None)),
            ],
        )?;

        // test DateValue binding in the WHERE clause
        run_pg_binder(
            "SELECT * FROM orders WHERE order_date >= $1 AND order_date <= $2",
            "SELECT * FROM orders WHERE order_date >= '1999-12-31' AND order_date <= '2000-01-01'",
            vec![
                BindValue::Date(DateValue::from_ymd_opt(1999, 12, 31).unwrap()),
                BindValue::Date(DateValue::from_ymd_opt(2000, 1, 1).unwrap()),
            ],
        )?;

        Ok(())
    }

    fn assert_pg_params_finder(
        input: &str,
        expected: Vec<FoundParameter>,
    ) -> Result<(), CubeError> {
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, &input).unwrap();

        let finder = PostgresStatementParamsFinder::new(&[]);
        let result = finder.find(&stmts[0]).unwrap();

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_pg_placeholder_find() -> Result<(), CubeError> {
        assert_pg_params_finder("SELECT $1", vec![FoundParameter::new(ColumnType::String)])?;
        assert_pg_params_finder("SELECT true as true_bool, false as false_bool", vec![])?;
        assert_pg_params_finder(
            "WITH t AS (SELECT $1 AS x) SELECT x FROM t",
            vec![FoundParameter::new(ColumnType::String)],
        )?;
        assert_pg_params_finder(
            "SELECT 1 LIMIT $1",
            vec![FoundParameter::new(ColumnType::Int64)],
        )?;

        assert_pg_params_finder(
            "SELECT $1, $2, $1, $2",
            vec![
                FoundParameter::new(ColumnType::String),
                FoundParameter::new(ColumnType::String),
            ],
        )?;

        assert_pg_params_finder(
            "SELECT $1 LIMIT $2",
            vec![
                FoundParameter::new(ColumnType::String),
                FoundParameter::new(ColumnType::Int64),
            ],
        )?;
        // reverse order
        assert_pg_params_finder(
            "SELECT $2 LIMIT $1",
            vec![
                FoundParameter::new(ColumnType::Int64),
                FoundParameter::new(ColumnType::String),
            ],
        )?;

        assert_pg_params_finder(
            "SELECT 1 OFFSET $1",
            vec![FoundParameter::new(ColumnType::Int64)],
        )?;

        assert_pg_params_finder(
            "SELECT 1 FETCH FIRST $1 ROWS ONLY",
            vec![FoundParameter::new(ColumnType::Int64)],
        )?;

        Ok(())
    }

    fn assert_placeholder_replacer(input: &str, output: &str) -> Result<(), CubeError> {
        let stmt = Parser::parse_sql(&PostgreSqlDialect {}, &input)
            .unwrap()
            .pop()
            .expect("must contain at least one statement");

        let binder = StatementPlaceholderReplacer::new();
        let result = binder.replace(stmt).unwrap();

        assert_eq!(result.to_string(), output);

        Ok(())
    }

    #[test]
    fn test_placeholder_replacer() -> Result<(), CubeError> {
        assert_placeholder_replacer("SELECT ?", "SELECT 'replaced_placeholder'")?;
        assert_placeholder_replacer("SELECT 1 LIMIT ?", "SELECT 1 LIMIT 1")?;
        assert_placeholder_replacer("SELECT 1 OFFSET ?", "SELECT 1 OFFSET 1")?;
        assert_placeholder_replacer(
            "SELECT 1 FETCH FIRST ? ROWS ONLY",
            "SELECT 1 FETCH FIRST 1 ROWS ONLY",
        )?;

        Ok(())
    }

    fn assert_sensitive_data_sanitizer(input: &str, output: &str) -> Result<(), CubeError> {
        let stmt = Parser::parse_sql(&PostgreSqlDialect {}, &input)
            .unwrap()
            .pop()
            .expect("must contain at least one statement");

        let binder = SensitiveDataSanitizer::new();
        let result = binder.replace(stmt);

        assert_eq!(result.to_string(), output);

        Ok(())
    }

    #[test]
    fn test_sensitive_data_sanitizer() -> Result<(), CubeError> {
        assert_sensitive_data_sanitizer(
            "SELECT * FROM testdata WHERE email = 'to@replace.com'",
            "SELECT * FROM testdata WHERE email = '[REPLACED]'",
        )?;

        Ok(())
    }
}
