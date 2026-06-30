use itertools::Itertools;
use log::trace;
use pg_srv::{
    protocol::{ErrorCode, ErrorResponse},
    BindValue, PgType, PgTypeId,
};
use sqlparser::ast::{
    self, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments,
    Ident, ObjectName, ObjectNamePart, Value,
};
use std::{collections::HashMap, error::Error};

use super::types::ColumnType;
use crate::{sql::postgres::ConnectionError, utils::parse_named_timezone_timestamp};

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

fn new_function(name: &str, args: Vec<FunctionArg>) -> Function {
    Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args,
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        approximate: false,
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

    /// Hook invoked for every expression before its children are visited, allowing a visitor to
    /// rewrite/replace the node in place. The default is a no-op; recursion below then descends
    /// into whatever node it has been replaced with.
    fn transform_expr(&mut self, _expr: &mut Expr) -> Result<(), E> {
        Ok(())
    }

    fn visit_expr_with_placeholder_type(
        &mut self,
        expr: &mut Expr,
        placeholder_type: PlaceholderType,
    ) -> Result<(), E> {
        self.transform_expr(expr)?;

        match expr {
            Expr::Value(value) => self.visit_value(&mut value.value, placeholder_type)?,
            Expr::Identifier(identifier) => self.visit_identifier(identifier)?,
            Expr::CompoundIdentifier(identifiers) => {
                for ident in identifiers.iter_mut() {
                    self.visit_identifier(ident)?;
                }
            }
            Expr::Nested(v) => self.visit_expr(&mut *v)?,
            Expr::Cast { .. } => self.visit_cast(expr)?,
            Expr::Between {
                expr, low, high, ..
            } => {
                self.visit_expr(&mut *expr)?;
                self.visit_expr(&mut *low)?;
                self.visit_expr(&mut *high)?;
            }
            Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
                self.visit_expr(&mut *left)?;
                self.visit_expr(&mut *right)?;
            }
            Expr::BinaryOp { left, right, .. } => {
                self.visit_expr(&mut *left)?;
                self.visit_expr(&mut *right)?;
            }
            Expr::Like { expr, pattern, .. }
            | Expr::ILike { expr, pattern, .. }
            | Expr::SimilarTo { expr, pattern, .. }
            | Expr::RLike { expr, pattern, .. } => {
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
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.visit_expr(&mut *op)?;
                }
                for when in conditions.iter_mut() {
                    self.visit_expr(&mut when.condition)?;
                    self.visit_expr(&mut when.result)?;
                }
                if let Some(res) = else_result {
                    self.visit_expr(&mut *res)?;
                }
            }
            Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::IsNormalized { expr, .. } => self.visit_expr(expr)?,
            Expr::IsDistinctFrom(expr_1, expr_2) | Expr::IsNotDistinctFrom(expr_1, expr_2) => {
                self.visit_expr(expr_1)?;
                self.visit_expr(expr_2)?;
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.visit_expr(expr)?;
                self.visit_query(subquery)?;
            }
            Expr::InUnnest {
                expr, array_expr, ..
            } => {
                self.visit_expr(expr)?;
                self.visit_expr(array_expr)?;
            }
            Expr::UnaryOp { expr, .. }
            | Expr::Convert { expr, .. }
            | Expr::Extract { expr, .. }
            | Expr::Ceil { expr, .. }
            | Expr::Floor { expr, .. }
            | Expr::OuterJoin(expr)
            | Expr::Prior(expr)
            | Expr::Named { expr, .. }
            | Expr::Prefixed { value: expr, .. } => self.visit_expr(expr)?,
            Expr::AtTimeZone { timestamp, .. } => self.visit_expr(timestamp)?,
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                self.visit_expr(expr)?;
                if let Some(res) = substring_from {
                    self.visit_expr(res)?;
                }
                if let Some(res) = substring_for {
                    self.visit_expr(res)?;
                }
            }
            Expr::Trim {
                expr,
                trim_what,
                trim_characters,
                ..
            } => {
                self.visit_expr(expr)?;
                if let Some(res) = trim_what {
                    self.visit_expr(res)?;
                }
                if let Some(chars) = trim_characters {
                    for res in chars.iter_mut() {
                        self.visit_expr(res)?;
                    }
                }
            }
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                self.visit_expr(expr)?;
                self.visit_expr(overlay_what)?;
                self.visit_expr(overlay_from)?;
                if let Some(res) = overlay_for {
                    self.visit_expr(res)?;
                }
            }
            Expr::Collate { expr, collation } => {
                self.visit_expr(expr)?;
                for res in collation.0.iter_mut() {
                    if let ObjectNamePart::Identifier(ident) = res {
                        self.visit_identifier(ident)?;
                    }
                }
            }
            Expr::CompoundFieldAccess { root, access_chain } => {
                self.visit_expr(root)?;
                for access in access_chain.iter_mut() {
                    match access {
                        ast::AccessExpr::Dot(expr) => self.visit_expr(expr)?,
                        ast::AccessExpr::Subscript(subscript) => self.visit_subscript(subscript)?,
                    }
                }
            }
            Expr::JsonAccess { value, .. } => self.visit_expr(value)?,
            Expr::Function(fun) => self.visit_function(fun)?,
            Expr::Exists { subquery, .. } => self.visit_query(subquery)?,
            Expr::Subquery(query) => self.visit_query(query)?,
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
            Expr::Array(arr) => {
                for expr in arr.elem.iter_mut() {
                    self.visit_expr(expr)?;
                }
            }
            Expr::Interval(interval) => self.visit_expr(&mut interval.value)?,
            Expr::TypedString { .. } | Expr::Wildcard(_) | Expr::QualifiedWildcard(..) => (),
            Expr::Position { expr, r#in } => {
                self.visit_expr(expr)?;
                self.visit_expr(r#in)?;
            }
            // Exotic / dialect-specific variants (Struct, Map, Dictionary, MatchAgainst,
            // Lambda, MemberOf, ...) carry no placeholders in CubeSQL's workload.
            _ => {}
        };

        Ok(())
    }

    fn visit_subscript(&mut self, subscript: &mut ast::Subscript) -> Result<(), E> {
        match subscript {
            ast::Subscript::Index { index } => self.visit_expr(index)?,
            ast::Subscript::Slice {
                lower_bound,
                upper_bound,
                stride,
            } => {
                if let Some(expr) = lower_bound {
                    self.visit_expr(expr)?;
                }
                if let Some(expr) = upper_bound {
                    self.visit_expr(expr)?;
                }
                if let Some(expr) = stride {
                    self.visit_expr(expr)?;
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
            ast::TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                self.visit_table_with_joins(&mut *table_with_joins)?;
                self.visit_table_alias(alias)?;
            }
            ast::TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                ..
            } => {
                self.visit_object_name(name)?;
                self.visit_table_alias(alias)?;
                if let Some(args) = args {
                    self.visit_function_args(&mut args.args)?;
                }
                for hint in with_hints.iter_mut() {
                    self.visit_expr(hint)?;
                }
            }
            ast::TableFactor::Function {
                name, args, alias, ..
            } => {
                self.visit_object_name(name)?;
                self.visit_function_args(args)?;
                self.visit_table_alias(alias)?;
            }
            // UNNEST, JSON_TABLE, OPENJSON, PIVOT, ... are not produced by CubeSQL's workload.
            _ => {}
        };

        Ok(())
    }

    fn visit_object_name(&mut self, name: &mut ObjectName) -> Result<(), E> {
        for part in name.0.iter_mut() {
            if let ObjectNamePart::Identifier(ident) = part {
                self.visit_identifier(ident)?;
            }
        }

        Ok(())
    }

    fn visit_join(&mut self, join: &mut ast::Join) -> Result<(), E> {
        self.visit_table_factor(&mut join.relation)?;

        match &mut join.join_operator {
            ast::JoinOperator::Join(constr)
            | ast::JoinOperator::Inner(constr)
            | ast::JoinOperator::Left(constr)
            | ast::JoinOperator::LeftOuter(constr)
            | ast::JoinOperator::Right(constr)
            | ast::JoinOperator::RightOuter(constr)
            | ast::JoinOperator::FullOuter(constr)
            | ast::JoinOperator::CrossJoin(constr)
            | ast::JoinOperator::Semi(constr)
            | ast::JoinOperator::LeftSemi(constr)
            | ast::JoinOperator::RightSemi(constr)
            | ast::JoinOperator::Anti(constr)
            | ast::JoinOperator::LeftAnti(constr)
            | ast::JoinOperator::RightAnti(constr)
            | ast::JoinOperator::StraightJoin(constr) => self.visit_join_constraint(constr)?,
            ast::JoinOperator::AsOf {
                match_condition,
                constraint,
            } => {
                self.visit_expr(match_condition)?;
                self.visit_join_constraint(constraint)?;
            }
            ast::JoinOperator::CrossApply
            | ast::JoinOperator::OuterApply
            | ast::JoinOperator::ArrayJoin
            | ast::JoinOperator::LeftArrayJoin
            | ast::JoinOperator::InnerArrayJoin => (),
        };

        Ok(())
    }

    fn visit_join_constraint(&mut self, constr: &mut ast::JoinConstraint) -> Result<(), E> {
        match constr {
            ast::JoinConstraint::On(expr) => {
                self.visit_expr(expr)?;
            }
            ast::JoinConstraint::Using(names) => {
                for name in names.iter_mut() {
                    self.visit_object_name(name)?;
                }
            }
            ast::JoinConstraint::Natural | ast::JoinConstraint::None => (),
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
            ast::SelectItem::ExprWithAliases { expr, .. } => self.visit_expr(expr)?,
            ast::SelectItem::UnnamedExpr(expr) => self.visit_expr(expr)?,
            ast::SelectItem::QualifiedWildcard(kind, _) => match kind {
                ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                    self.visit_object_name(name)?
                }
                ast::SelectItemQualifiedWildcardKind::Expr(expr) => self.visit_expr(expr)?,
            },
            ast::SelectItem::Wildcard(_) => (),
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

        if let ast::GroupByExpr::Expressions(exprs, _) = &mut select.group_by {
            for group_by in exprs.iter_mut() {
                self.visit_expr(group_by)?;
            }
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
                for row in vals.rows.iter_mut() {
                    for expr in row.content.iter_mut() {
                        self.visit_expr(expr)?;
                    }
                }
            }
            ast::SetExpr::Insert(_)
            | ast::SetExpr::Update(_)
            | ast::SetExpr::Delete(_)
            | ast::SetExpr::Merge(_)
            | ast::SetExpr::Table(_) => (),
        };

        Ok(())
    }

    fn visit_query(&mut self, query: &mut Box<ast::Query>) -> Result<(), E> {
        self.visit_set_expr(&mut query.body)?;
        if let Some(with) = query.with.as_mut() {
            self.visit_with(with)?;
        }
        if let Some(order_by) = query.order_by.as_mut() {
            if let ast::OrderByKind::Expressions(exprs) = &mut order_by.kind {
                for order_expr in exprs.iter_mut() {
                    self.visit_expr(&mut order_expr.expr)?;
                }
            }
        }
        match query.limit_clause.as_mut() {
            Some(ast::LimitClause::LimitOffset { limit, offset, .. }) => {
                if let Some(limit) = limit {
                    self.visit_expr_with_placeholder_type(limit, PlaceholderType::Number)?;
                }
                if let Some(offset) = offset {
                    self.visit_expr_with_placeholder_type(
                        &mut offset.value,
                        PlaceholderType::Number,
                    )?;
                }
            }
            Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
                self.visit_expr_with_placeholder_type(offset, PlaceholderType::Number)?;
                self.visit_expr_with_placeholder_type(limit, PlaceholderType::Number)?;
            }
            None => {}
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
            ast::Statement::Declare { stmts } => {
                for declare in stmts.iter_mut() {
                    if let Some(query) = declare.for_query.as_mut() {
                        self.visit_query(query)?;
                    }
                }
            }
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
        self.visit_object_name(&mut fun.name)?;
        self.visit_function_arguments(&mut fun.parameters)?;
        self.visit_function_arguments(&mut fun.args)?;
        if let Some(filter) = &mut fun.filter {
            self.visit_expr(filter)?;
        }
        for order_expr in fun.within_group.iter_mut() {
            self.visit_expr(&mut order_expr.expr)?;
        }
        if let Some(ast::WindowType::WindowSpec(spec)) = &mut fun.over {
            self.visit_window_spec(spec)?;
        }

        Ok(())
    }

    fn visit_window_spec(&mut self, spec: &mut ast::WindowSpec) -> Result<(), E> {
        for res in spec.partition_by.iter_mut() {
            self.visit_expr(res)?;
        }
        for order_expr in spec.order_by.iter_mut() {
            self.visit_expr(&mut order_expr.expr)?;
        }

        Ok(())
    }

    fn visit_function_arguments(&mut self, args: &mut FunctionArguments) -> Result<(), E> {
        match args {
            FunctionArguments::None => {}
            FunctionArguments::Subquery(query) => self.visit_query(query)?,
            FunctionArguments::List(list) => self.visit_function_args(&mut list.args)?,
        };

        Ok(())
    }

    fn visit_function_args(&mut self, args: &mut Vec<ast::FunctionArg>) -> Result<(), E> {
        for a in args.iter_mut() {
            match a {
                ast::FunctionArg::Named { name, arg, .. } => {
                    self.visit_identifier(name)?;
                    self.visit_function_arg_expr(arg)?;
                }
                ast::FunctionArg::ExprNamed { name, arg, .. } => {
                    self.visit_expr(name)?;
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
            ast::FunctionArgExpr::QualifiedWildcard(name) => self.visit_object_name(name)?,
            ast::FunctionArgExpr::Wildcard | ast::FunctionArgExpr::WildcardWithOptions(_) => (),
        };

        Ok(())
    }

    fn visit_table_alias(&mut self, alias: &mut Option<ast::TableAlias>) -> Result<(), E> {
        if let Some(a) = alias {
            self.visit_identifier(&mut a.name)?;
            for col in a.columns.iter_mut() {
                self.visit_identifier(&mut col.name)?;
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

#[derive(Debug)]
pub struct PlainTimestampTimezoneSuffixReplacer {}

impl PlainTimestampTimezoneSuffixReplacer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn replace(mut self, stmt: ast::Statement) -> ast::Statement {
        let mut result = stmt;

        self.visit_statement(&mut result).unwrap();

        result
    }

    fn is_plain_timestamp_data_type(data_type: &ast::DataType) -> bool {
        matches!(
            data_type,
            ast::DataType::Timestamp(
                _,
                ast::TimezoneInfo::None | ast::TimezoneInfo::WithoutTimeZone
            )
        )
    }

    fn strip_plain_timestamp_timezone_suffix(value: &mut ast::Value) {
        // Postgres ignores zones in timestamp without time zone literals.
        let timestamp = match value {
            Value::SingleQuotedString(str) | Value::DoubleQuotedString(str) => {
                parse_named_timezone_timestamp(str).map(|(timestamp, _)| timestamp)
            }
            _ => None,
        };

        if let Some(timestamp) = timestamp {
            *value = Value::SingleQuotedString(timestamp);
        }
    }

    fn strip_plain_timestamp_timezone_literal(expr: &mut Expr) {
        if let Expr::Value(value) = expr {
            Self::strip_plain_timestamp_timezone_suffix(&mut value.value);
        }
    }
}

impl<'ast> Visitor<'ast, ConnectionError> for PlainTimestampTimezoneSuffixReplacer {
    fn transform_expr(&mut self, expr: &mut Expr) -> Result<(), ConnectionError> {
        if let Expr::TypedString(typed_string) = expr {
            if Self::is_plain_timestamp_data_type(&typed_string.data_type) {
                Self::strip_plain_timestamp_timezone_suffix(&mut typed_string.value.value);
            }
        }

        Ok(())
    }

    fn visit_cast(&mut self, expr: &mut Expr) -> Result<(), ConnectionError> {
        if let Expr::Cast {
            expr: cast_expr,
            data_type,
            ..
        } = expr
        {
            self.visit_expr(&mut *cast_expr)?;
            if Self::is_plain_timestamp_data_type(data_type) {
                Self::strip_plain_timestamp_timezone_literal(cast_expr);
            }
        }

        Ok(())
    }
}

impl<'ast> Visitor<'ast, ConnectionError> for CastReplacer {
    fn visit_cast(&mut self, expr: &mut Expr) -> Result<(), ConnectionError> {
        if let Expr::Cast {
            expr: cast_expr,
            data_type,
            ..
        } = expr
        {
            match data_type {
                ast::DataType::Custom(name, _) => match name.to_string().to_lowercase().as_str() {
                    "name" | "oid" | "information_schema.cardinal_number" | "regproc" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *expr = *cast_expr.clone();
                    }
                    "xid" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *data_type = ast::DataType::IntUnsigned(None);
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

                        *data_type = ast::DataType::Double(ast::ExactNumberInfo::None);
                    }
                    "bool" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *data_type = ast::DataType::Boolean;
                    }
                    "timestamptz" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *data_type = ast::DataType::Timestamp(None, ast::TimezoneInfo::None);
                    }
                    "regtype" => {
                        self.visit_expr(&mut *cast_expr)?;

                        if let Expr::Identifier(_) = &**cast_expr {
                            *expr = Expr::Function(new_function(
                                "format_type",
                                vec![
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(*cast_expr.clone())),
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                        Value::Null.into(),
                                    ))),
                                ],
                            ))
                        }
                    }
                    "\"char\"" => {
                        self.visit_expr(&mut *cast_expr)?;

                        *data_type = ast::DataType::Text;
                    }
                    // TODO:
                    _ => (),
                },
                // Postgres `timestamptz` now parses as a built-in `Timestamp(_, Tz)`
                // (not a `Custom` type as in sqlparser 0.16); drop the timezone so DataFusion
                // casts to a plain timestamp.
                ast::DataType::Timestamp(_, ast::TimezoneInfo::Tz) => {
                    self.visit_expr(&mut *cast_expr)?;

                    *data_type = ast::DataType::Timestamp(None, ast::TimezoneInfo::None);
                }
                ast::DataType::Bool => {
                    self.visit_expr(&mut *cast_expr)?;

                    *data_type = ast::DataType::Boolean;
                }
                ast::DataType::Int2(_) => {
                    self.visit_expr(&mut *cast_expr)?;

                    *data_type = ast::DataType::SmallInt(None);
                }
                ast::DataType::Int4(_) => {
                    self.visit_expr(&mut *cast_expr)?;

                    *data_type = ast::DataType::Int(None);
                }
                ast::DataType::Int8(_) => {
                    self.visit_expr(&mut *cast_expr)?;

                    *data_type = ast::DataType::BigInt(None);
                }
                ast::DataType::Int2Unsigned(_) => {
                    self.visit_expr(&mut *cast_expr)?;

                    *data_type = ast::DataType::SmallIntUnsigned(None);
                }
                ast::DataType::Int4Unsigned(_) | ast::DataType::IntegerUnsigned(_) => {
                    self.visit_expr(&mut *cast_expr)?;

                    *data_type = ast::DataType::IntUnsigned(None);
                }
                ast::DataType::Int8Unsigned(_) => {
                    self.visit_expr(&mut *cast_expr)?;

                    *data_type = ast::DataType::BigIntUnsigned(None);
                }
                ast::DataType::Float8 => {
                    self.visit_expr(&mut *cast_expr)?;

                    *data_type = ast::DataType::Double(ast::ExactNumberInfo::None);
                }
                ast::DataType::Numeric(info) => {
                    let info = *info;
                    self.visit_expr(&mut *cast_expr)?;

                    *data_type = ast::DataType::Decimal(info);
                }
                ast::DataType::CharacterVarying(len) => {
                    let len = *len;
                    self.visit_expr(&mut *cast_expr)?;

                    *data_type = ast::DataType::Varchar(len);
                }
                ast::DataType::Regclass => match &**cast_expr {
                    Expr::Value(val) => {
                        let str_val = self.parse_value_to_str(&val.value);
                        let Some(str_val) = str_val else {
                            return Ok(());
                        };
                        let str_val = str_val.strip_prefix("pg_catalog.").unwrap_or(&str_val);

                        for typ in PgType::get_all() {
                            if typ.typname == str_val {
                                *expr = Expr::Value(
                                    Value::Number(typ.typrelid.to_string(), false).into(),
                                );
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

                        *expr = ast::Expr::Function(new_function(
                            "__cube_regclass_cast",
                            vec![ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                *cast_expr.clone(),
                            ))],
                        ))
                    }
                },
                _ => self.visit_expr(&mut *cast_expr)?,
            }
        };

        Ok(())
    }
}

/// Normalizes a handful of expression nodes that sqlparser 0.62 introduced/renamed back into the
/// shapes our DataFusion 7 fork understands:
/// - `FLOOR(expr)` / `CEIL(expr)` are now dedicated `Expr::Floor` / `Expr::Ceil` nodes (they used
///   to parse as ordinary function calls); convert the plain numeric form back to `floor(expr)` /
///   `ceil(expr)` function calls.
/// - the `^` operator now parses as `BinaryOperator::PGExp` instead of `BinaryOperator::BitwiseXor`;
///   the fork maps `BitwiseXor` to exponentiation, so rewrite it back.
#[derive(Debug)]
pub struct SqlParser062Normalizer {}

impl SqlParser062Normalizer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn replace(mut self, stmt: ast::Statement) -> ast::Statement {
        let mut result = stmt;

        self.visit_statement(&mut result).unwrap();

        result
    }
}

impl<'a> Visitor<'a, ConnectionError> for SqlParser062Normalizer {
    fn transform_expr(&mut self, expr: &mut Expr) -> Result<(), ConnectionError> {
        match expr {
            Expr::Floor {
                expr: inner,
                field: ast::CeilFloorKind::DateTimeField(ast::DateTimeField::NoDateTime),
            } => {
                *expr = Expr::Function(new_function(
                    "floor",
                    vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        (**inner).clone(),
                    ))],
                ));
            }
            Expr::Ceil {
                expr: inner,
                field: ast::CeilFloorKind::DateTimeField(ast::DateTimeField::NoDateTime),
            } => {
                *expr = Expr::Function(new_function(
                    "ceil",
                    vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        (**inner).clone(),
                    ))],
                ));
            }
            Expr::BinaryOp {
                op: op @ ast::BinaryOperator::PGExp,
                ..
            } => {
                *op = ast::BinaryOperator::BitwiseXor;
            }
            // `EXTRACT('YEAR' FROM ...)` parses the quoted field as `DateTimeField::Custom` keeping
            // its quote style, so it stringifies as `'year'` and breaks date-part resolution. Drop
            // the quotes to restore the bare `year` form expected downstream.
            Expr::Extract {
                field: ast::DateTimeField::Custom(ident),
                ..
            } => {
                ident.quote_style = None;
            }
            // sqlparser 0.62 already unescapes `U&'..'` literals at tokenize time, but our
            // DataFusion fork unescapes them a second time and rejects any literal backslash.
            // Downgrade to a plain string literal carrying the already-decoded value.
            Expr::Value(value) => {
                if let Value::UnicodeStringLiteral(s) = &value.value {
                    value.value = Value::SingleQuotedString(s.clone());
                }
            }
            _ => {}
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
        self.visit_object_name(&mut fun.name)?;
        let fn_name = fun.name.to_string().to_lowercase();
        if fn_name == "datediff" || fn_name == "dateadd" {
            if let FunctionArguments::List(list) = &mut fun.args {
                if list.args.len() == 3 {
                    if let ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        arg @ Expr::Identifier(_),
                    )) = &mut list.args[0]
                    {
                        if let Expr::Identifier(ident) = &*arg {
                            let granularity_in_identifier = ident.value.to_lowercase();
                            match granularity_in_identifier.as_str() {
                                "second" | "minute" | "hour" | "day" | "qtr" | "week" | "month"
                                | "year" => {
                                    *arg = Expr::Value(
                                        Value::SingleQuotedString(granularity_in_identifier).into(),
                                    );
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        self.visit_function_arguments(&mut fun.args)?;
        if let Some(ast::WindowType::WindowSpec(spec)) = &mut fun.over {
            self.visit_window_spec(spec)?;
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
                    ast::ObjectName(parts),
                )) => {
                    let mut new_idents = parts
                        .iter()
                        .map(|part| part.as_ident().cloned())
                        .collect::<Option<Vec<ast::Ident>>>()?;
                    new_idents.push(ast::Ident::new(column.to_string()));
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
        if let FunctionArguments::List(list) = &mut fun.args {
            if let Some(new_args) = self.get_new_args_for_fn(&fun.name.to_string(), &list.args) {
                list.args = new_args
            }
        }
        self.visit_object_name(&mut fun.name)?;
        self.visit_function_arguments(&mut fun.args)?;
        if let Some(ast::WindowType::WindowSpec(spec)) = &mut fun.over {
            self.visit_window_spec(spec)?;
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
        let is_distinct = matches!(
            &fun.args,
            FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: Some(ast::DuplicateTreatment::Distinct),
                ..
            })
        );
        if fun.approximate && is_distinct && &fun.name.to_string().to_uppercase() == "COUNT" {
            fun.name = ObjectName::from(vec![ast::Ident::new("APPROX_DISTINCT")]);
            fun.approximate = false;
            if let FunctionArguments::List(list) = &mut fun.args {
                list.duplicate_treatment = None;
            }
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

    fn run_plain_timestamp_timezone_suffix_replacer(
        input: &str,
        output: &str,
    ) -> Result<(), CubeError> {
        let stmt = Parser::parse_sql(&PostgreSqlDialect {}, &input)
            .unwrap()
            .pop()
            .expect("must contain at least one statement");

        let replacer = PlainTimestampTimezoneSuffixReplacer::new();
        let res = replacer.replace(stmt);

        assert_eq!(res.to_string(), output);

        Ok(())
    }

    fn run_plain_timestamp_timezone_suffix_replacer_unchanged(
        input: &str,
    ) -> Result<(), CubeError> {
        let stmt = Parser::parse_sql(&PostgreSqlDialect {}, &input)
            .unwrap()
            .pop()
            .expect("must contain at least one statement");
        let expected = stmt.to_string();

        let replacer = PlainTimestampTimezoneSuffixReplacer::new();
        let res = replacer.replace(stmt);

        assert_eq!(res.to_string(), expected);

        Ok(())
    }

    #[test]
    fn test_cast_replacer() -> Result<(), CubeError> {
        run_cast_replacer("SELECT 'pg_class'::regclass", "SELECT 1259")?;
        run_cast_replacer("SELECT 'pg_class'::regclass::oid", "SELECT 1259")?;
        run_cast_replacer("SELECT 64::information_schema.cardinal_number", "SELECT 64")?;
        run_cast_replacer("SELECT NOW()::timestamptz", "SELECT NOW()::TIMESTAMP")?;
        run_cast_replacer(
            "SELECT CAST(1 + 1 as Regclass);",
            "SELECT __cube_regclass_cast(1 + 1)",
        )?;
        run_cast_replacer(
            "SELECT CAST(1 as INTEGER UNSIGNED)",
            "SELECT CAST(1 AS INT UNSIGNED)",
        )?;
        run_cast_replacer(
            "SELECT CAST(1 as INT4 UNSIGNED)",
            "SELECT CAST(1 AS INT UNSIGNED)",
        )?;
        run_cast_replacer(
            "SELECT CAST(1 as INT2 UNSIGNED)",
            "SELECT CAST(1 AS SMALLINT UNSIGNED)",
        )?;
        run_cast_replacer(
            "SELECT CAST(1 as INT8 UNSIGNED)",
            "SELECT CAST(1 AS BIGINT UNSIGNED)",
        )?;

        Ok(())
    }

    #[test]
    fn test_plain_timestamp_timezone_suffix_replacer() -> Result<(), CubeError> {
        run_plain_timestamp_timezone_suffix_replacer(
            "SELECT TIMESTAMP '2026-06-14 00:00 America/Los_Angeles'",
            "SELECT TIMESTAMP '2026-06-14 00:00'",
        )?;
        run_plain_timestamp_timezone_suffix_replacer(
            "SELECT CAST('2026-06-14 00:00 America/Los_Angeles' AS TIMESTAMP)",
            "SELECT CAST('2026-06-14 00:00' AS TIMESTAMP)",
        )?;
        run_plain_timestamp_timezone_suffix_replacer_unchanged(
            "SELECT TIMESTAMP 'not a timestamp America/Los_Angeles'",
        )?;

        Ok(())
    }

    #[test]
    fn test_plain_timestamp_timezone_suffix_replacer_leaves_timestamptz_unchanged(
    ) -> Result<(), CubeError> {
        run_plain_timestamp_timezone_suffix_replacer_unchanged(
            "SELECT TIMESTAMP WITH TIME ZONE '2026-06-14T00:00:00 America/Los_Angeles'",
        )?;
        run_plain_timestamp_timezone_suffix_replacer_unchanged(
            "SELECT CAST('2026-06-14 00:00 America/Los_Angeles' AS TIMESTAMPTZ)",
        )?;
        run_plain_timestamp_timezone_suffix_replacer_unchanged(
            "SELECT CAST(order_date AS TIMESTAMPTZ)",
        )?;
        run_plain_timestamp_timezone_suffix_replacer_unchanged(
            "SELECT CAST(order_date AS TIMESTAMP WITH TIME ZONE)",
        )?;
        run_plain_timestamp_timezone_suffix_replacer_unchanged(
            "SELECT TIMESTAMP WITH TIME ZONE '2026-06-14 00:00:00+02:00'",
        )?;
        run_plain_timestamp_timezone_suffix_replacer_unchanged(
            "SELECT CAST('2026-06-14 00:00:00+02:00' AS TIMESTAMPTZ)",
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
        assert_placeholder_replacer("SELECT $1", "SELECT 'replaced_placeholder'")?;
        assert_placeholder_replacer("SELECT 1 LIMIT $1", "SELECT 1 LIMIT 1")?;
        assert_placeholder_replacer("SELECT 1 OFFSET $1", "SELECT 1 OFFSET 1")?;
        assert_placeholder_replacer(
            "SELECT 1 FETCH FIRST $1 ROWS ONLY",
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
