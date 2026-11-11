//! Named `:name` parameters of the SQL API. A parameter value is a literal of its type, or a
//! SQL expression that is parsed on its own; either replaces the placeholder in the statement.
//! [`describe_parameters`] reports the type each placeholder is expected to have.

use std::{collections::HashMap, ops::ControlFlow, sync::Arc};

use chrono::DateTime;

use datafusion::{
    arrow::{
        compute::kernels::cast_utils::string_to_timestamp_nanos,
        datatypes::{DataType, TimeUnit},
    },
    error::DataFusionError,
    logical_expr::{
        aggregate_function, function, ReturnTypeFunction, ScalarUDF, Signature, TypeSignature,
        Volatility,
    },
    logical_plan::{
        DFSchema, Expr as DFExpr, ExprSchemable, GroupingSet, LogicalPlan, Operator, PlanVisitor,
    },
    physical_plan::functions::make_scalar_function,
    scalar::ScalarValue,
    sql::{parser::Statement as DFStatement, planner::SqlToRel},
};
use serde::{Deserialize, Serialize};
use sqlparser::{
    ast::{
        self, visit_expressions, visit_expressions_mut, Expr, Function, FunctionArg, Query, Value,
        ValueWithSpan, Visit, Visitor,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
    tokenizer::{Location, Token},
};

use super::{
    router::rewrite_statement, CompilationError, CompilationResult, QueryEngine, SqlQueryEngine,
};
use itertools::Itertools;

use crate::{sql::Session, transport::MetaContext};

/// Name of the stand-in function placeholders are planned as when describing parameters.
const PARAMETER_UDF: &str = "__cube_parameter";

fn parameter_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Placeholder(placeholder) => placeholder.strip_prefix(':'),
            _ => None,
        },
        _ => None,
    }
}

/// Rejects subqueries and placeholders inside a parameter value.
struct ValueValidator;

impl Visitor for ValueValidator {
    type Break = &'static str;

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        ControlFlow::Break("a subquery")
    }

    fn pre_visit_value(&mut self, value: &ValueWithSpan) -> ControlFlow<Self::Break> {
        match value.value {
            Value::Placeholder(_) => ControlFlow::Break("a parameter"),
            _ => ControlFlow::Continue(()),
        }
    }
}

/// Fails on a `:name` placeholder left where no expression can stand, e.g. `ESCAPE :e`.
struct LeftoverPlaceholderFinder;

impl Visitor for LeftoverPlaceholderFinder {
    type Break = String;

    fn pre_visit_value(&mut self, value: &ValueWithSpan) -> ControlFlow<Self::Break> {
        match &value.value {
            Value::Placeholder(p) if p.starts_with(':') => ControlFlow::Break(p.clone()),
            _ => ControlFlow::Continue(()),
        }
    }
}

fn check_no_leftover_placeholders(stmt: &ast::Statement) -> CompilationResult<()> {
    match stmt.visit(&mut LeftoverPlaceholderFinder) {
        ControlFlow::Break(p) => Err(CompilationError::user(format!(
            "Parameter {p} is not supported in this position"
        ))),
        ControlFlow::Continue(()) => Ok(()),
    }
}

/// `DATE :d` and other typed literals hold their placeholder as a bare value; makes them
/// `CAST(:d AS DATE)`, so the placeholder is an expression like any other.
fn typed_placeholders_to_casts(stmt: &mut ast::Statement) {
    let _ = visit_expressions_mut(stmt, |expr| {
        if let Expr::TypedString(typed) = expr {
            if matches!(&typed.value.value, Value::Placeholder(p) if p.starts_with(':')) {
                *expr = Expr::Cast {
                    kind: ast::CastKind::Cast,
                    expr: Box::new(Expr::Value(typed.value.clone())),
                    data_type: typed.data_type.clone(),
                    array: false,
                    format: None,
                };
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Parses the value of parameter `name` as a single SQL expression.
pub fn parse_parameter_value(name: &str, sql: &str) -> CompilationResult<Expr> {
    let invalid = |reason: String| {
        CompilationError::user(format!("Invalid value of parameter :{name}: {reason}"))
    };

    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|err| invalid(err.to_string()))?;
    let expr = parser
        .parse_expr()
        .map_err(|err| invalid(err.to_string()))?;
    parser
        .expect_token(&Token::EOF)
        .map_err(|err| invalid(err.to_string()))?;

    if let ControlFlow::Break(found) = expr.visit(&mut ValueValidator) {
        return Err(invalid(format!("{found} is not allowed")));
    }

    // Parentheses keep the value one operand when the statement is printed, e.g. in logs
    Ok(match expr {
        Expr::Value(_) | Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Nested(_) => {
            expr
        }
        expr => Expr::Nested(Box::new(expr)),
    })
}

/// Type of a parameter value: an `expr` is SQL, any other type is a literal.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParameterValueType {
    Number,
    String,
    Time,
    Boolean,
    #[serde(rename = "expr")]
    Expression,
}

/// Value of a `:name` parameter as the API receives it.
#[derive(Debug, Clone, Deserialize)]
pub struct ParameterValue {
    pub name: String,
    pub value: serde_json::Value,
    #[serde(rename = "type")]
    pub value_type: ParameterValueType,
}

enum BoundValue<'a> {
    Literal(Box<Expr>),
    Sql(&'a str),
}

/// Checks that `param.value` is of its type and makes a literal of it; an `expr`
/// is parsed only once bound, so an unused one is never parsed.
fn prepare_value(param: &ParameterValue) -> CompilationResult<BoundValue<'_>> {
    use serde_json::Value as Json;

    let invalid = || {
        let expected = match param.value_type {
            ParameterValueType::Number => "a number",
            ParameterValueType::String => "a string",
            ParameterValueType::Time => "a timestamp string, e.g. 2024-01-31T09:30:00Z",
            ParameterValueType::Boolean => "a boolean",
            ParameterValueType::Expression => "a string with a SQL expression",
        };
        CompilationError::user(format!(
            "Invalid value of parameter :{}: expected {expected}",
            param.name
        ))
    };
    let literal =
        |value: Value| BoundValue::Literal(Box::new(Expr::Value(value.with_empty_span())));
    Ok(match (param.value_type, &param.value) {
        (_, Json::Null) => literal(Value::Null),
        (ParameterValueType::Expression, Json::String(sql)) => BoundValue::Sql(sql),
        (ParameterValueType::Number, Json::Number(n)) => {
            let n = n.to_string();
            let negative = n.starts_with('-');
            let value = Expr::Value(Value::Number(n, false).with_empty_span());
            // Like an expression, so `-:n` prints as `-(-1)`, not the comment `--1`
            BoundValue::Literal(Box::new(if negative {
                Expr::Nested(Box::new(value))
            } else {
                value
            }))
        }
        (ParameterValueType::String, Json::String(s)) => {
            literal(Value::SingleQuotedString(s.clone()))
        }
        (ParameterValueType::Boolean, Json::Bool(b)) => literal(Value::Boolean(*b)),
        (ParameterValueType::Time, Json::String(s)) => {
            // Bound as UTC without an offset, which a data source can't drop or reinterpret
            let nanos = string_to_timestamp_nanos(s).map_err(|_| invalid())?;
            let utc = DateTime::from_timestamp_nanos(nanos)
                .naive_utc()
                .format("%Y-%m-%dT%H:%M:%S%.f")
                .to_string();
            BoundValue::Literal(Box::new(Expr::Cast {
                kind: ast::CastKind::Cast,
                expr: Box::new(Expr::Value(
                    Value::SingleQuotedString(utc).with_empty_span(),
                )),
                data_type: ast::DataType::Timestamp(None, ast::TimezoneInfo::None),
                array: false,
                format: None,
            }))
        }
        _ => return Err(invalid()),
    })
}

/// Bound values may repeat, so their total size is capped to keep one request from
/// growing into a huge statement.
const MAX_BOUND_PARAMETERS_SIZE: usize = 1024 * 1024;

/// Replaces every `:name` placeholder of `stmt` with its value from `values`.
/// Values without a placeholder are ignored; a placeholder without a value is an error.
pub fn bind_parameters(
    stmt: &mut ast::Statement,
    values: &[ParameterValue],
) -> CompilationResult<()> {
    let mut prepared = HashMap::<&str, (BoundValue, usize)>::new();
    for param in values {
        let size = match &param.value {
            serde_json::Value::String(s) => s.len(),
            value => value.to_string().len(),
        };
        let value = prepare_value(param)?;
        if prepared.insert(&param.name, (value, size)).is_some() {
            return Err(CompilationError::user(format!(
                "Duplicate value for parameter :{}",
                param.name
            )));
        }
    }

    typed_placeholders_to_casts(stmt);

    let mut parsed = HashMap::<String, Expr>::new();
    let mut bound_size = 0;
    let result = visit_expressions_mut(stmt, |expr| {
        let Some(name) = parameter_name(expr).map(str::to_string) else {
            return ControlFlow::Continue(());
        };
        let Some((value, size)) = prepared.get(name.as_str()) else {
            return ControlFlow::Break(CompilationError::user(format!(
                "Missing value for parameter :{name}"
            )));
        };
        bound_size += size;
        if bound_size > MAX_BOUND_PARAMETERS_SIZE {
            return ControlFlow::Break(CompilationError::user(format!(
                "Parameter values exceed {MAX_BOUND_PARAMETERS_SIZE} bytes in total"
            )));
        }
        *expr = match value {
            BoundValue::Literal(value) => value.as_ref().clone(),
            BoundValue::Sql(sql) => match parsed.get(&name) {
                Some(value) => value.clone(),
                None => match parse_parameter_value(&name, sql) {
                    Ok(value) => parsed.entry(name).or_insert(value).clone(),
                    Err(err) => return ControlFlow::Break(err),
                },
            },
        };
        ControlFlow::Continue(())
    });

    match result {
        ControlFlow::Break(err) => Err(err),
        ControlFlow::Continue(()) => check_no_leftover_placeholders(stmt),
    }
}

/// Cube type of a parameter. `Unknown` stands for anything else, and for no type implied.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ParameterType {
    Number,
    String,
    Time,
    Boolean,
    Unknown,
}

impl ParameterType {
    fn from_data_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => Self::Number,
            DataType::Utf8 | DataType::LargeUtf8 => Self::String,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => Self::Time,
            DataType::Boolean => Self::Boolean,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub struct DescribedParameter {
    pub name: String,
    #[serde(rename = "type")]
    pub parameter_type: ParameterType,
}

/// Types of the placeholders known from the syntax alone (`LIMIT`, `OFFSET`, `FETCH`);
/// every other placeholder is replaced with a call of [`PARAMETER_UDF`] for planning.
struct PlanningPlaceholderReplacer {
    types: HashMap<String, DataType>,
}

impl PlanningPlaceholderReplacer {
    /// Makes a `LIMIT` or `OFFSET` placeholder an integer literal the planner accepts.
    fn replace_count(&mut self, expr: &mut Expr) {
        if let Some(name) = parameter_name(expr).map(str::to_string) {
            self.types.entry(name).or_insert(DataType::Int64);
            *expr = Expr::value(Value::Number("1".to_string(), false));
        }
    }
}

impl ast::VisitorMut for PlanningPlaceholderReplacer {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        match &mut query.limit_clause {
            Some(ast::LimitClause::LimitOffset { limit, offset, .. }) => {
                if let Some(limit) = limit {
                    self.replace_count(limit);
                }
                if let Some(offset) = offset {
                    self.replace_count(&mut offset.value);
                }
            }
            Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
                self.replace_count(offset);
                self.replace_count(limit);
            }
            None => {}
        }
        if let Some(quantity) = query.fetch.as_mut().and_then(|f| f.quantity.as_mut()) {
            self.replace_count(quantity);
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Some(name) = parameter_name(expr).map(str::to_string) {
            *expr = Expr::Function(Function {
                name: ast::ObjectName::from(vec![ast::Ident::new(PARAMETER_UDF)]),
                uses_odbc_syntax: false,
                parameters: ast::FunctionArguments::None,
                args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                        Expr::value(Value::SingleQuotedString(name)),
                    ))],
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
                approximate: false,
            });
        }
        ControlFlow::Continue(())
    }
}

/// A function of type `Null`, which coerces to whatever its surroundings need, so planning
/// the statement types everything around the placeholders. It is never executed.
fn create_parameter_udf() -> ScalarUDF {
    let fun = make_scalar_function(|_| {
        Err(DataFusionError::Internal(format!(
            "{PARAMETER_UDF} is not executable"
        )))
    });
    let return_type: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Null)));

    ScalarUDF::new(
        PARAMETER_UDF,
        &Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

fn udf_parameter_name(expr: &DFExpr) -> Option<&str> {
    match expr {
        DFExpr::ScalarUDF { fun, args } if fun.name == PARAMETER_UDF => match args.as_slice() {
            [DFExpr::Literal(ScalarValue::Utf8(Some(name)))] => Some(name.as_str()),
            _ => None,
        },
        _ => None,
    }
}

/// Infers the type of each placeholder from the expression it is an operand of.
struct TypeInferrer<'a> {
    schema: &'a DFSchema,
    types: &'a mut HashMap<String, DataType>,
}

impl TypeInferrer<'_> {
    fn type_of(&self, expr: &DFExpr) -> Option<DataType> {
        expr.get_type(self.schema)
            .ok()
            .filter(|t| *t != DataType::Null)
    }

    /// The type of the first of `exprs` whose type is known.
    fn common_type<'e>(&self, exprs: impl IntoIterator<Item = &'e DFExpr>) -> Option<DataType> {
        exprs.into_iter().find_map(|e| self.type_of(e))
    }

    fn infer(&mut self, expr: &DFExpr, expected: Option<DataType>) {
        if let Some(name) = udf_parameter_name(expr) {
            if let Some(expected) = expected {
                self.types.entry(name.to_string()).or_insert(expected);
            }
            return;
        }

        match expr {
            DFExpr::Alias(expr, _) | DFExpr::Negative(expr) => self.infer(expr, expected),
            DFExpr::Not(expr) => self.infer(expr, Some(DataType::Boolean)),
            DFExpr::IsNull(expr) | DFExpr::IsNotNull(expr) | DFExpr::Sort { expr, .. } => {
                self.infer(expr, None)
            }
            DFExpr::BinaryExpr { left, op, right } => match op {
                Operator::And | Operator::Or => {
                    self.infer(left, Some(DataType::Boolean));
                    self.infer(right, Some(DataType::Boolean));
                }
                Operator::StringConcat
                | Operator::Like
                | Operator::NotLike
                | Operator::ILike
                | Operator::NotILike
                | Operator::RegexMatch
                | Operator::RegexIMatch
                | Operator::RegexNotMatch
                | Operator::RegexNotIMatch => {
                    self.infer(left, Some(DataType::Utf8));
                    self.infer(right, Some(DataType::Utf8));
                }
                Operator::Plus
                | Operator::Minus
                | Operator::Multiply
                | Operator::Divide
                | Operator::Modulo => {
                    let (left_type, right_type) = (self.type_of(left), self.type_of(right));
                    self.infer(
                        left,
                        arithmetic_operand_type(op, right_type, expected.clone()),
                    );
                    self.infer(right, arithmetic_operand_type(op, left_type, expected));
                }
                _ => {
                    let (left_type, right_type) = (self.type_of(left), self.type_of(right));
                    self.infer(left, right_type);
                    self.infer(right, left_type);
                }
            },
            DFExpr::AnyExpr { left, right, .. } => {
                let element_type = match self.type_of(right) {
                    Some(DataType::List(field)) => Some(field.data_type().clone()),
                    _ => None,
                };
                self.infer(left, element_type);
                self.infer(right, None);
            }
            DFExpr::Like(like) | DFExpr::ILike(like) | DFExpr::SimilarTo(like) => {
                self.infer(&like.expr, Some(DataType::Utf8));
                self.infer(&like.pattern, Some(DataType::Utf8));
            }
            DFExpr::Between {
                expr, low, high, ..
            } => {
                let operand_type = self.common_type([expr.as_ref(), low, high]);
                for e in [expr, low, high] {
                    self.infer(e, operand_type.clone());
                }
            }
            DFExpr::InList { expr, list, .. } => {
                let operand_type = self.common_type(std::iter::once(expr.as_ref()).chain(list));
                self.infer(expr, operand_type.clone());
                for e in list {
                    self.infer(e, operand_type.clone());
                }
            }
            DFExpr::InSubquery { expr, subquery, .. } => {
                // The subquery's column is typed as a list of its values
                let operand_type = match self.type_of(subquery) {
                    Some(DataType::List(field)) => Some(field.data_type().clone()),
                    other => other,
                };
                self.infer(expr, operand_type);
            }
            DFExpr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                let condition_type = match expr {
                    Some(expr) => {
                        let operand_type = self.common_type(
                            std::iter::once(expr.as_ref())
                                .chain(when_then_expr.iter().map(|(w, _)| w.as_ref())),
                        );
                        self.infer(expr, operand_type.clone());
                        operand_type
                    }
                    None => Some(DataType::Boolean),
                };
                let result_type = self
                    .common_type(
                        when_then_expr
                            .iter()
                            .map(|(_, t)| t.as_ref())
                            .chain(else_expr.as_deref()),
                    )
                    .or(expected);
                for (when, then) in when_then_expr {
                    self.infer(when, condition_type.clone());
                    self.infer(then, result_type.clone());
                }
                if let Some(else_expr) = else_expr {
                    self.infer(else_expr, result_type);
                }
            }
            DFExpr::Cast { expr, data_type } | DFExpr::TryCast { expr, data_type } => {
                self.infer(expr, Some(data_type.clone()))
            }
            DFExpr::GetIndexedField { expr, key } => {
                self.infer(expr, None);
                self.infer(key, None);
            }
            DFExpr::ScalarFunction { fun, args } => {
                self.infer_args(&function::signature(fun), args, expected)
            }
            DFExpr::ScalarUDF { fun, args } => self.infer_args(&fun.signature, args, expected),
            DFExpr::AggregateFunction { fun, args, .. } => {
                self.infer_args(&aggregate_function::signature(fun), args, expected)
            }
            DFExpr::AggregateUDF { fun, args, .. } => {
                self.infer_args(&fun.signature, args, expected)
            }
            DFExpr::TableUDF { args, .. } => {
                for arg in args {
                    self.infer(arg, None);
                }
            }
            DFExpr::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for e in args.iter().chain(partition_by).chain(order_by) {
                    self.infer(e, None);
                }
            }
            DFExpr::GroupingSet(set) => {
                let exprs: Vec<&DFExpr> = match set {
                    GroupingSet::Rollup(exprs) | GroupingSet::Cube(exprs) => exprs.iter().collect(),
                    GroupingSet::GroupingSets(sets) => sets.iter().flatten().collect(),
                };
                for e in exprs {
                    self.infer(e, None);
                }
            }
            DFExpr::OuterColumn(..)
            | DFExpr::Column(_)
            | DFExpr::ScalarVariable(..)
            | DFExpr::Literal(_)
            | DFExpr::Wildcard
            | DFExpr::QualifiedWildcard { .. } => {}
        }
    }

    /// `expected` is the type the call's result should have; it is taken for arguments
    /// the signature leaves open, as `COALESCE` and the like return their argument type.
    fn infer_args(&mut self, signature: &Signature, args: &[DFExpr], expected: Option<DataType>) {
        let arg_types = args.iter().map(|a| self.type_of(a)).collect::<Vec<_>>();
        for (i, arg) in args.iter().enumerate() {
            let arg_type =
                signature_arg_type(&signature.type_signature, &arg_types, i, expected.as_ref());
            self.infer(arg, arg_type);
        }
    }
}

/// The type of an arithmetic operand whose other operand has type `other`.
fn arithmetic_operand_type(
    op: &Operator,
    other: Option<DataType>,
    expected: Option<DataType>,
) -> Option<DataType> {
    match other {
        // `:start + INTERVAL '1 day'`, `:days * INTERVAL '1 day'`
        Some(DataType::Interval(_)) => match op {
            Operator::Plus | Operator::Minus => {
                expected.or(Some(DataType::Timestamp(TimeUnit::Nanosecond, None)))
            }
            _ => Some(DataType::Float64),
        },
        // An interval or another date and time, which `unknown` covers either way
        Some(DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)) => None,
        other => other.or(expected),
    }
}

/// The type `signature` takes for argument `index`, given the known types of all the
/// arguments. Of several alternatives, the first one the known types match is used.
fn signature_arg_type(
    signature: &TypeSignature,
    arg_types: &[Option<DataType>],
    index: usize,
    expected: Option<&DataType>,
) -> Option<DataType> {
    let sibling_type = || arg_types.iter().flatten().next().cloned();
    // The sibling's type, which planning accepted, or the expected type when valid, else
    // the one Cube type all valid types share
    let pick = |valid: &[DataType]| {
        sibling_type()
            .or_else(|| expected.filter(|t| valid.contains(t)).cloned())
            .or_else(|| {
                let first = valid.first()?;
                let cube_type = ParameterType::from_data_type(first);
                valid
                    .iter()
                    .all(|t| ParameterType::from_data_type(t) == cube_type)
                    .then(|| first.clone())
            })
    };
    match signature {
        TypeSignature::Exact(types) if types.len() == arg_types.len() => types.get(index).cloned(),
        TypeSignature::Uniform(n, valid) if *n == arg_types.len() => pick(valid),
        TypeSignature::Variadic(valid) => pick(valid),
        TypeSignature::VariadicEqual => sibling_type().or_else(|| expected.cloned()),
        TypeSignature::OneOf(signatures) => {
            let matches = |s: &TypeSignature| match s {
                TypeSignature::Exact(types) => {
                    types.len() == arg_types.len()
                        && types
                            .iter()
                            .zip(arg_types)
                            .all(|(t, a)| a.as_ref().is_none_or(|a| a == t))
                }
                _ => false,
            };
            signatures
                .iter()
                .find(|s| matches(s))
                .and_then(|s| signature_arg_type(s, arg_types, index, expected))
                .or_else(|| {
                    signatures
                        .iter()
                        .find_map(|s| signature_arg_type(s, arg_types, index, expected))
                })
        }
        _ => None,
    }
}

struct PlanTypeInferrer {
    types: HashMap<String, DataType>,
}

impl PlanVisitor for PlanTypeInferrer {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
        let inputs = plan.inputs();
        let schema = match inputs.split_first() {
            Some((first, rest)) => {
                let mut schema = first.schema().as_ref().clone();
                for input in rest {
                    schema.merge(input.schema());
                }
                schema
            }
            None => plan.schema().as_ref().clone(),
        };

        let predicate_type = matches!(plan, LogicalPlan::Filter(_)).then_some(DataType::Boolean);
        let mut inferrer = TypeInferrer {
            schema: &schema,
            types: &mut self.types,
        };
        for expr in plan.expressions() {
            inferrer.infer(&expr, predicate_type.clone());
        }

        Ok(true)
    }
}

/// The `:name` placeholders of `stmt` in the order they first appear, each with the type
/// the expression around it expects.
pub fn describe_parameters(
    stmt: &ast::Statement,
    meta: Arc<MetaContext>,
    session: Arc<Session>,
) -> CompilationResult<Vec<DescribedParameter>> {
    let mut stmt = stmt.clone();
    typed_placeholders_to_casts(&mut stmt);

    let mut first_seen = HashMap::<String, Location>::new();
    let _ = visit_expressions(&stmt, |expr| {
        if let (Some(name), Expr::Value(value)) = (parameter_name(expr), expr) {
            let start = value.span.start;
            first_seen
                .entry(name.to_string())
                .and_modify(|seen| *seen = (*seen).min(start))
                .or_insert(start);
        }
        ControlFlow::<()>::Continue(())
    });
    let names = first_seen
        .into_iter()
        .sorted_by_key(|(_, start)| *start)
        .map(|(name, _)| name)
        .collect::<Vec<_>>();

    let mut replacer = PlanningPlaceholderReplacer {
        types: HashMap::new(),
    };
    let _ = ast::VisitMut::visit(&mut stmt, &mut replacer);
    check_no_leftover_placeholders(&stmt)?;
    let mut types = replacer.types;

    // Planned even when every type is known, so an invalid query is reported either way
    let engine = SqlQueryEngine::new(session.session_manager.clone());
    let mut ctx = engine.create_session_ctx(session.state.clone())?;
    ctx.register_udf(create_parameter_udf());
    let cube_ctx = engine.create_cube_ctx(session.state.clone(), meta, ctx)?;

    let stmt = rewrite_statement(stmt);
    let plan = SqlToRel::new_with_options(&cube_ctx, true)
        .statement_to_plan(DFStatement::Statement(Box::new(stmt)))
        .map_err(|err| {
            // Errors print the stand-in calls; show the placeholders instead
            let message = names.iter().fold(err.to_string(), |message, name| {
                message.replace(
                    &format!("{PARAMETER_UDF}(Utf8(\"{name}\"))"),
                    &format!(":{name}"),
                )
            });
            CompilationError::user(format!("Initial planning error: {message}"))
        })?;

    let mut inferrer = PlanTypeInferrer {
        types: HashMap::new(),
    };
    plan.accept(&mut inferrer)
        .map_err(|err| CompilationError::internal(err.to_string()))?;
    for (name, data_type) in inferrer.types {
        types.entry(name).or_insert(data_type);
    }

    Ok(names
        .into_iter()
        .map(|name| {
            let parameter_type = types.remove(&name).map_or(ParameterType::Unknown, |t| {
                ParameterType::from_data_type(&t)
            });
            DescribedParameter {
                name,
                parameter_type,
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::{
        parser::parse_sql_to_statement,
        test::{get_test_session, get_test_tenant_ctx},
        DatabaseProtocol,
    };

    fn parse(sql: &str) -> ast::Statement {
        parse_sql_to_statement(sql, DatabaseProtocol::PostgreSQL, &mut None).unwrap()
    }

    fn bind(
        sql: &str,
        values: &[(&str, serde_json::Value, ParameterValueType)],
    ) -> CompilationResult<String> {
        let mut stmt = parse(sql);
        let values = values
            .iter()
            .map(|(name, value, value_type)| ParameterValue {
                name: name.to_string(),
                value: value.clone(),
                value_type: *value_type,
            })
            .collect::<Vec<_>>();
        bind_parameters(&mut stmt, &values)?;
        Ok(stmt.to_string())
    }

    #[test]
    fn test_bind_parameters() {
        use serde_json::json;
        use ParameterValueType::*;

        assert_eq!(
            bind(
                "SELECT :total + 1 FROM t WHERE a = :a AND b > :b AND d >= DATE :d LIMIT :n",
                &[
                    ("total", json!("SUM(x) * 2"), Expression),
                    ("a", json!("'it''s'"), Expression),
                    ("b", json!("-1"), Expression),
                    ("d", json!("'2024-01-01'"), Expression),
                    ("n", json!("10"), Expression),
                    ("unused", json!("'unterminated"), Expression),
                ],
            )
            .unwrap(),
            "SELECT (SUM(x) * 2) + 1 FROM t WHERE a = 'it''s' AND b > (-1) AND d >= CAST('2024-01-01' AS DATE) LIMIT 10"
        );
        assert_eq!(
            bind(
                "SELECT * FROM t WHERE a = :a AND b > -:b AND c = :c AND d >= :d AND e = :e AND f IS NOT DISTINCT FROM :f AND g = COALESCE(:g, 1) LIMIT :n",
                &[
                    ("a", json!("It's summer time"), String),
                    ("b", json!(-1.5), Number),
                    ("c", json!(true), Boolean),
                    ("d", json!("2024-01-31T09:30:00+03:00"), Time),
                    ("e", json!("'x'"), Expression),
                    ("f", json!(null), Time),
                    ("g", json!(null), Expression),
                    ("n", json!(10), Number),
                ],
            )
            .unwrap(),
            "SELECT * FROM t WHERE a = 'It''s summer time' AND b > -(-1.5) AND c = true AND d >= CAST('2024-01-31T06:30:00' AS TIMESTAMP) AND e = 'x' AND f IS NOT DISTINCT FROM NULL AND g = COALESCE(NULL, 1) LIMIT 10"
        );

        let err = |sql: &str, values: &[(&str, serde_json::Value, ParameterValueType)]| {
            bind(sql, values).unwrap_err().to_string()
        };
        assert!(err("SELECT :a, :b", &[("a", json!("1"), Expression)])
            .contains("Missing value for parameter :b"));
        assert!(err(
            "SELECT 1",
            &[("a", json!(1), Number), ("a", json!(2), Number)]
        )
        .contains("Duplicate value for parameter :a"));
        for value in [
            "(SELECT 1)",
            "x IN (SELECT 1)",
            ":b",
            "$1",
            "TEXT :b",
            "1; DROP TABLE t",
            "1 1",
        ] {
            assert!(
                err("SELECT :a", &[("a", json!(value), Expression)])
                    .contains("Invalid value of parameter :a"),
                "{}",
                value
            );
        }
        for (value, value_type) in [
            (json!(123), Boolean),
            (json!(false), String),
            (json!("1"), Number),
            (json!("yesterday"), Time),
            (json!(1), Expression),
        ] {
            assert!(
                err("SELECT 1", &[("a", value.clone(), value_type)])
                    .contains("Invalid value of parameter :a: expected"),
                "{} {:?}",
                value,
                value_type
            );
        }
        assert!(err(
            "SELECT 'a' LIKE :p ESCAPE :e",
            &[
                ("p", json!("'a'"), Expression),
                ("e", json!("'!'"), Expression)
            ]
        )
        .contains("Parameter :e is not supported in this position"));
        let big = "1".repeat(MAX_BOUND_PARAMETERS_SIZE / 2 + 1);
        assert!(err("SELECT :a, :a", &[("a", json!(big), Expression)])
            .contains("Parameter values exceed"));
    }

    async fn describe(sql: &str) -> Vec<(String, ParameterType)> {
        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
        describe_parameters(&parse(sql), meta, session)
            .unwrap()
            .into_iter()
            .map(|p| (p.name, p.parameter_type))
            .collect()
    }

    fn expected(params: &[(&str, ParameterType)]) -> Vec<(String, ParameterType)> {
        params.iter().map(|(n, t)| (n.to_string(), *t)).collect()
    }

    #[tokio::test]
    async fn test_describe_parameters() {
        use ParameterType::*;

        let described = describe(
            r#"
            SELECT :unknown, CASE WHEN :flag THEN customer_gender ELSE :fallback END
            FROM KibanaSampleDataEcommerce
            WHERE :cond
                AND taxful_total_price > :price
                AND SUBSTR(:text, 0, 5) = 'abc'
                AND customer_gender IN (:g1, :g2)
                AND order_date BETWEEN :from AND :to
                AND NOT :negated
            GROUP BY 1, 2
            OFFSET :skip LIMIT :n
            "#,
        )
        .await;
        assert_eq!(
            described,
            expected(&[
                ("unknown", Unknown),
                ("flag", Boolean),
                ("fallback", String),
                ("cond", Boolean),
                ("price", Number),
                ("text", String),
                ("g1", String),
                ("g2", String),
                ("from", Time),
                ("to", Time),
                ("negated", Boolean),
                ("skip", Number),
                ("n", Number),
            ])
        );

        let meta = get_test_tenant_ctx();
        let session = get_test_session(DatabaseProtocol::PostgreSQL, meta.clone()).await;
        let stmt = parse("SELECT bad_col FROM KibanaSampleDataEcommerce LIMIT :n");
        assert!(describe_parameters(&stmt, meta, session)
            .unwrap_err()
            .to_string()
            .contains("Initial planning error"));

        let described = describe(
            r#"
            SELECT COALESCE(:a, :b), GREATEST(order_date, :latest)
            FROM KibanaSampleDataEcommerce
            WHERE order_date >= COALESCE(:start, order_date)
                AND order_date < :end + INTERVAL '1 day'
                AND order_date + :shift > NOW()
                AND order_date >= CURRENT_DATE - :days * INTERVAL '1 day'
                AND :g IN (SELECT customer_gender FROM KibanaSampleDataEcommerce)
                AND order_date >= DATE :day
                AND taxful_total_price > :x::numeric
            "#,
        )
        .await;
        assert_eq!(
            described,
            expected(&[
                ("a", Unknown),
                ("b", Unknown),
                ("latest", Time),
                ("start", Time),
                ("end", Time),
                ("shift", Unknown),
                ("days", Number),
                ("g", String),
                ("day", Time),
                ("x", Number),
            ])
        );
    }
}
