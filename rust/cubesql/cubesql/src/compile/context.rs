use std::collections::HashMap;

use cubeclient::models::{V1CubeMeta, V1CubeMetaDimension, V1CubeMetaMeasure, V1CubeMetaSegment};
use regex::Regex;
use sqlparser::ast;

use crate::{
    compile::CompilationError,
    transport::{V1CubeMetaDimensionExt, V1CubeMetaMeasureExt, V1CubeMetaSegmentExt},
};

use super::CompilationResult;

#[derive(Debug, Clone, PartialEq)]
pub enum Selection {
    TimeDimension(V1CubeMetaDimension, String),
    Dimension(V1CubeMetaDimension),
    Measure(V1CubeMetaMeasure),
    Segment(V1CubeMetaSegment),
}

#[derive(Debug)]
pub struct QueryContext {
    pub meta: V1CubeMeta,
    aliases: HashMap<String, Selection>,
}

impl QueryContext {
    pub fn new(meta: &V1CubeMeta) -> QueryContext {
        QueryContext {
            meta: meta.clone(),
            aliases: HashMap::new(),
        }
    }

    pub fn find_selection_for_identifier(
        &self,
        identifier: &String,
        check_alias: bool,
    ) -> Option<Selection> {
        for dimension in self.meta.dimensions.iter() {
            if dimension.get_real_name().eq(identifier) {
                return Some(Selection::Dimension(dimension.clone()));
            }
        }

        for measure in self.meta.measures.iter() {
            if measure.get_real_name().eq(identifier) {
                return Some(Selection::Measure(measure.clone()));
            }
        }

        for segment in self.meta.segments.iter() {
            if segment.get_real_name().eq(identifier) {
                return Some(Selection::Segment(segment.clone()));
            }
        }

        if check_alias {
            self.aliases
                .get(identifier)
                .map(|selection| selection.clone())
        } else {
            None
        }
    }

    pub fn find_dimension_for_identifier(
        &self,
        identifier: &String,
    ) -> Option<V1CubeMetaDimension> {
        for dimension in self.meta.dimensions.iter() {
            if dimension.get_real_name().eq_ignore_ascii_case(identifier) {
                return Some(dimension.clone());
            }
        }

        None
    }

    fn find_selection_for_binary_op(
        &self,
        expr_as_str: &String,
    ) -> CompilationResult<Option<Selection>> {
        // Quarter granularity from Superset
        // MAKEDATE(YEAR(order_date), 1) + INTERVAL QUARTER(order_date) QUARTER - INTERVAL 1 QUARTER
        {
            let left_regexp = Regex::new(r"MAKEDATE\(YEAR\((?P<column>[a-z_]+)\), 1\) \+ INTERVAL QUARTER\([a-zA-Z_]+\) QUARTER - INTERVAL 1 QUARTER").unwrap();
            if let Some(identifiers) = left_regexp.captures(expr_as_str) {
                let identifier = identifiers.name("column").unwrap().as_str();
                let result = self
                    .find_dimension_for_identifier(&identifier.to_string())
                    .map(|dimension| Selection::TimeDimension(dimension, "quarter".to_string()));

                return Ok(result);
            };
        }

        Ok(None)
    }

    // Compile any expression from any part of query to Selection
    // This method is can be used in GROUP BY, ORDER BY, except projection
    pub fn compile_selection(&self, expr: &ast::Expr) -> CompilationResult<Option<Selection>> {
        match expr {
            ast::Expr::Function(f) => Ok(Some(self.find_selection_for_function(f)?)),
            ast::Expr::CompoundIdentifier(i) => {
                // @todo We need a context with main table rel
                if i.len() == 2 {
                    Ok(self.find_selection_for_identifier(&i[1].value.to_string(), true))
                } else {
                    Err(CompilationError::unsupported(format!(
                        "Unsupported compound identifier: {:?}",
                        expr
                    )))
                }
            }
            ast::Expr::Identifier(i) => {
                Ok(self.find_selection_for_identifier(&i.value.to_string(), true))
            }
            _ => {
                return Err(CompilationError::unsupported(format!(
                    "Unable to find selection in selection: {:?}",
                    expr
                )));
            }
        }
    }

    // Compile any expression from projection to Selection
    pub fn compile_selection_from_projection(
        &self,
        expr: &ast::Expr,
    ) -> CompilationResult<Option<Selection>> {
        match expr {
            ast::Expr::BinaryOp { .. } => self.find_selection_for_binary_op(&expr.to_string()),
            ast::Expr::Function(f) => Ok(Some(self.find_selection_for_function(f)?)),
            ast::Expr::CompoundIdentifier(i) => {
                // @todo We need a context with main table rel
                if i.len() == 2 {
                    Ok(self.find_selection_for_identifier(&i[1].value.to_string(), false))
                } else {
                    Err(CompilationError::unsupported(format!(
                        "Unsupported compound identifier: {:?}",
                        expr
                    )))
                }
            }
            ast::Expr::Identifier(i) => {
                Ok(self.find_selection_for_identifier(&i.value.to_string(), false))
            }
            _ => {
                return Err(CompilationError::unsupported(format!(
                    "Unable to find selection in selection: {:?}",
                    expr
                )));
            }
        }
    }

    fn unpack_identifier_from_arg(&self, arg: &ast::FunctionArg) -> CompilationResult<String> {
        let argument = match arg {
            ast::FunctionArg::Named { arg, .. } => arg,
            ast::FunctionArg::Unnamed(expr) => expr,
        };

        let identifier = match argument {
            ast::FunctionArgExpr::Wildcard => "*".to_string(),
            ast::FunctionArgExpr::Expr(expr) => match expr {
                ast::Expr::Identifier(i) => i.value.to_string().to_lowercase(),
                ast::Expr::CompoundIdentifier(i) => {
                    // @todo We need a context with main table rel
                    if i.len() == 2 {
                        i[1].value.to_string()
                    } else {
                        return Err(CompilationError::unsupported(format!(
                            "Unsupported compound identifier in argument: {:?}",
                            argument
                        )));
                    }
                }
                _ => {
                    return Err(CompilationError::unsupported(format!(
                        "type of argument {:?}",
                        argument
                    )))
                }
            },
            _ => {
                return Err(CompilationError::unsupported(format!(
                    "type of argument {:?}",
                    argument
                )));
            }
        };

        Ok(identifier)
    }

    pub fn find_selection_for_date_add_fn(
        &self,
        f: &ast::Function,
    ) -> CompilationResult<Selection> {
        let [left_fn, right_fn] = match f.args.as_slice() {
            [left_arg, right_arg] => {
                let left_arg_fn = match left_arg {
                    ast::FunctionArg::Unnamed(l) => l,
                    ast::FunctionArg::Named { arg, .. } => arg,
                };

                let right_arg_fn = match right_arg {
                    ast::FunctionArg::Unnamed(l) => l,
                    ast::FunctionArg::Named { arg, .. } => arg,
                };

                [left_arg_fn, right_arg_fn]
            }
            _ => {
                return Err(CompilationError::user(format!(
                    "Unable to unpack function: {:?}",
                    f
                )));
            }
        };

        let time_dimension_opt = match left_fn {
            ast::FunctionArgExpr::Expr(ast::Expr::Function(f)) => {
                if !f.name.to_string().to_lowercase().eq("date") {
                    return Err(CompilationError::user(format!(
                        "Unable to detect granularity (left side must be date): {:?}",
                        left_fn
                    )));
                }

                let possible_dimension_name = self.unpack_identifier_from_arg(&f.args[0])?;

                self.find_dimension_for_identifier(&possible_dimension_name)
            }
            _ => {
                return Err(CompilationError::user(format!(
                    "Unable to detect granularity: {:?}",
                    left_fn
                )));
            }
        };

        if let Some(time_dimension) = time_dimension_opt {
            // Convert AST back to string to reduce variants of formating
            let right_as_str = right_fn.to_string();

            let second_regexp = Regex::new(
                r"INTERVAL \(HOUR\([a-zA-Z_`]+\) \* 60 \* 60 \+ MINUTE\([a-zA-Z_`]+\) \* 60 \+ SECOND\([a-zA-Z_`]+\)\)",
            )?;
            let minute_regexp = Regex::new(
                r"INTERVAL \(HOUR\([a-zA-Z_`]+\) \* 60 \+ MINUTE\([a-zA-Z_`]+\)\) MINUTE",
            )?;
            let hour_regexp = Regex::new(r"INTERVAL HOUR\([a-zA-Z_`]+\) HOUR")?;

            let granularity = if second_regexp.is_match(&right_as_str) {
                "second".to_string()
            } else if minute_regexp.is_match(&right_as_str) {
                "minute".to_string()
            } else if hour_regexp.is_match(&right_as_str) {
                "hour".to_string()
            } else {
                return Err(CompilationError::user(format!(
                    "Unable to detect granularity: {} ({:?})",
                    right_fn.to_string(),
                    right_fn
                )));
            };

            Ok(Selection::TimeDimension(time_dimension, granularity))
        } else {
            Err(CompilationError::unsupported(format!(
                "Unsupported variation of arguments passed to date_add function: {}",
                f
            )))
        }
    }

    pub fn find_selection_for_date_trunc_fn(
        &self,
        f: &ast::Function,
    ) -> CompilationResult<Selection> {
        match f.args.as_slice() {
            [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Value(ast::Value::SingleQuotedString(granularity)))), ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Identifier(column)))] => {
                let possible_dimension_name = column.value.to_string();

                let granularity_value = match granularity.as_str() {
                    "second" | "minute" | "hour" | "day" | "week" | "month" | "quarter" | "year" => granularity.clone(),
                    "qtr" => "quarter".to_string(),
                    _ => {
                        return Err(CompilationError::user(format!(
                            "Unsupported granularity {:?}",
                            granularity
                        )));
                    }
                };

                if let Some(r) = self.find_dimension_for_identifier(&possible_dimension_name) {
                    if r.is_time() {
                        Ok(Selection::TimeDimension(r, granularity_value))
                    } else {
                        Err(CompilationError::user(format!(
                            "Unable to use non time dimension \"{}\" as a column in date_trunc, please specify time dimension",
                            possible_dimension_name
                        )))
                    }
                } else {
                    Err(CompilationError::user(format!(
                        "Unknown dimension '{}' passed as a column in date_trunc",
                        possible_dimension_name
                    )))
                }
            }
            _ => Err(CompilationError::user(
                "Unsupported variation of arguments passed to date_trunc function, correct date_trunc(string, column)".to_string()
            )),
        }
    }

    pub fn find_selection_for_date_fn(&self, f: &ast::Function) -> CompilationResult<Selection> {
        match f.args.as_slice() {
            [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Function(
                date_sub,
            )))] => {
                if !date_sub.name.to_string().to_lowercase().eq("date_sub") {
                    return Err(CompilationError::user(format!(
                        "Unable to detect heuristics: {}",
                        f
                    )));
                }

                let column_identifier = date_sub.args[0].to_string();
                // isoweek is called week in cube.js
                let iso_week_test = format!(
                    "INTERVAL DAYOFWEEK(DATE_SUB({}, INTERVAL 1 DAY)) - 1 DAY",
                    column_identifier
                );
                // week is not supported in
                let week_test = format!("INTERVAL DAYOFWEEK({}) - 1 DAY", column_identifier);
                let month_test = format!("INTERVAL DAYOFMONTH({}) - 1 DAY", column_identifier);
                let year_test = format!("INTERVAL DAYOFYEAR({}) - 1 DAY", column_identifier);

                let right_part = date_sub.args[1].to_string();
                let granularity = if right_part.eq(&iso_week_test) {
                    "week".to_string()
                } else if right_part.eq(&week_test) {
                    return Err(CompilationError::unsupported(
                        "date granularity, week is not supported in Cube.js, please use ISOWEEK"
                            .to_string(),
                    ));
                } else if right_part.eq(&month_test) {
                    "month".to_string()
                } else if right_part.eq(&year_test) {
                    "year".to_string()
                } else {
                    return Err(CompilationError::user(format!(
                        "Unable to detect granularity: {:?}",
                        right_part
                    )));
                };

                let possible_dimension_name = self.unpack_identifier_from_arg(&date_sub.args[0])?;

                if let Some(r) = self.find_dimension_for_identifier(&possible_dimension_name) {
                    if r.is_time() {
                        Ok(Selection::TimeDimension(r, granularity))
                    } else {
                        Err(CompilationError::user(format!(
                            "Unable to use non time dimension '{}' in date manipulations, please specify time dimension",
                            possible_dimension_name
                        )))
                    }
                } else {
                    Err(CompilationError::user(format!(
                        "Unknown dimension '{}'",
                        possible_dimension_name
                    )))
                }
            }
            [ast::FunctionArg::Unnamed(_)] => {
                let possible_dimension_name = self.unpack_identifier_from_arg(&f.args[0])?;

                if let Some(r) = self.find_dimension_for_identifier(&possible_dimension_name) {
                    Ok(Selection::TimeDimension(r, "day".to_string()))
                } else {
                    return Err(CompilationError::user(format!(
                        "Unable to find dimension '{}' from expression: {}",
                        possible_dimension_name, f
                    )));
                }
            }
            _ => Err(CompilationError::user(format!(
                "Unsupported variation of arguments passed to date function: {}",
                f
            ))),
        }
    }

    pub fn find_selection_for_aggregation_fn(
        &self,
        f: &ast::Function,
    ) -> CompilationResult<Selection> {
        if f.args.is_empty() {
            return Err(CompilationError::user(format!(
                "Unable to use aggregation function '{}()' without arguments",
                f.name.to_string(),
            )));
        } else if f.args.len() > 1 {
            return Err(CompilationError::user(format!(
                "Unable to use aggregation function '{}()' with more then one argument",
                f.name.to_string(),
            )));
        };

        let argument = match &f.args[0] {
            ast::FunctionArg::Named { arg, .. } => arg,
            ast::FunctionArg::Unnamed(expr) => expr,
        };

        let measure_name = match argument {
            ast::FunctionArgExpr::Wildcard => "*".to_string(),
            ast::FunctionArgExpr::Expr(expr) => match expr {
                ast::Expr::Value(ast::Value::Number(n, is_negative)) => {
                    let prefix = if *is_negative {
                        "-".to_string()
                    } else {
                        "".to_string()
                    };

                    let number = prefix + n;

                    if &number != "1" {
                        return Err(CompilationError::user(format!(
                            "Unable to use number '{}' as argument to aggregation function",
                            number
                        )));
                    }

                    "*".to_string()
                }
                ast::Expr::Identifier(i) => i.value.to_string(),
                ast::Expr::CompoundIdentifier(i) => {
                    // @todo We need a context with main table rel
                    if i.len() == 2 {
                        i[1].value.to_string()
                    } else {
                        return Err(CompilationError::unsupported(format!(
                            "Unsupported compound identifier in argument: {:?}",
                            argument
                        )));
                    }
                }
                _ => {
                    return Err(CompilationError::unsupported(format!(
                        "type of argument {:?}",
                        argument
                    )));
                }
            },
            _ => {
                return Err(CompilationError::unsupported(format!(
                    "type of argument {:?}",
                    argument
                )));
            }
        };

        let fn_name = f.name.to_string().to_ascii_lowercase();
        let (selection_opt, call_agg_type) = if fn_name.eq(&"count".to_string()) && !f.distinct {
            if &measure_name == "*" {
                let measure_for_argument = self.meta.measures.iter().find(|measure| {
                    if measure.agg_type.is_some() {
                        let agg_type = measure.agg_type.clone().unwrap();
                        agg_type.eq(&"count".to_string())
                    } else {
                        false
                    }
                });

                if let Some(measure) = measure_for_argument {
                    (
                        Some(Selection::Measure(measure.clone())),
                        "count".to_string(),
                    )
                } else {
                    return Err(CompilationError::user(format!(
                        "Unable to find measure with count type: {}",
                        f
                    )));
                }
            } else {
                (
                    self.find_selection_for_identifier(&measure_name, true),
                    "count".to_string(),
                )
            }
        } else {
            let mut call_agg_type = fn_name;

            if f.distinct {
                call_agg_type += &"Distinct".to_string();
            };

            if measure_name == "*" {
                return Err(CompilationError::user(format!(
                    "Unable to use '{}' as argument to aggregation function '{}()' (only COUNT() supported)",
                    measure_name,
                    f.name.to_string(),
                )));
            }

            (
                self.find_selection_for_identifier(&measure_name, true),
                call_agg_type,
            )
        };

        let selection = selection_opt.ok_or_else(|| {
            CompilationError::user(format!(
                "Unable to find measure with name '{}' which is used as argument to aggregation function '{}()'",
                measure_name,
                f.name.to_string(),
            ))
        })?;
        match selection {
            Selection::Measure(measure) => {
                if measure.agg_type.is_some()
                    && !measure.is_same_agg_type(&call_agg_type)
                {
                    return Err(CompilationError::user(format!(
                        "Measure aggregation type doesn't match. The aggregation type for '{}' is '{}()' but '{}()' was provided",
                        measure.get_real_name(),
                        measure.agg_type.unwrap_or("unknown".to_string()).to_uppercase(),
                        f.name.to_string(),
                    )));
                } else {
                    // @todo Should we throw an exception?
                };

                Ok(Selection::Measure(measure))
            }
            Selection::Dimension(t) | Selection::TimeDimension(t, _) => {
                Err(CompilationError::user(format!(
                    "Dimension '{}' was used with the aggregate function '{}()'. Please use a measure instead",
                    t.get_real_name(),
                    f.name.to_string(),
                )))
            }
            Selection::Segment(s) => Err(CompilationError::user(format!(
                "Unable to use segment '{}' as measure in aggregation function '{}()'",
                s.get_real_name(),
                f.name.to_string(),
            ))),
        }
    }

    pub fn find_selection_for_measure_fn(&self, f: &ast::Function) -> CompilationResult<Selection> {
        if f.args.len() == 1 {
            let possible_measure_name = self.unpack_identifier_from_arg(&f.args[0])?;

            if let Some(r) = self.meta.measures.iter().find(|measure| {
                measure
                    .get_real_name()
                    .eq_ignore_ascii_case(&possible_measure_name)
            }) {
                Ok(Selection::Measure(r.clone()))
            } else {
                Err(CompilationError::user(format!(
                    "Unable to find measure with name '{}' for {}",
                    possible_measure_name, f
                )))
            }
        } else {
            Err(CompilationError::user(format!(
                "Unsupported variation of arguments passed to measure function: {}",
                f
            )))
        }
    }

    pub fn find_selection_for_function(&self, f: &ast::Function) -> CompilationResult<Selection> {
        let fn_name = f.name.to_string().to_ascii_lowercase();
        match fn_name.as_str() {
            "date_add" => self.find_selection_for_date_add_fn(f),
            "date_trunc" => self.find_selection_for_date_trunc_fn(f),
            "date" => self.find_selection_for_date_fn(f),
            "measure" => self.find_selection_for_measure_fn(f),
            "sum" | "min" | "max" | "avg" | "count" => self.find_selection_for_aggregation_fn(f),
            _ => Err(CompilationError::unsupported(format!(
                "Unsupported function: {}",
                f
            ))),
        }
    }

    pub fn with_alias(&mut self, alias: String, selection: Selection) {
        self.aliases.insert(alias, selection);
    }
}
