use std::collections::HashMap;

use cubeclient::models::{V1CubeMeta, V1CubeMetaDimension, V1CubeMetaMeasure, V1CubeMetaSegment};
use regex::Regex;
use sqlparser::ast;

use crate::{
    compile::CompilationError,
    schema::{V1CubeMetaDimensionExt, V1CubeMetaMeasureExt, V1CubeMetaSegmentExt},
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
        column_name: &String,
        check_alias: bool,
    ) -> Option<Selection> {
        for dimension in self.meta.dimensions.iter() {
            if dimension.get_real_name().eq(column_name) {
                return Some(Selection::Dimension(dimension.clone()));
            }
        }

        for measure in self.meta.measures.iter() {
            if measure.get_real_name().eq(column_name) {
                return Some(Selection::Measure(measure.clone()));
            }
        }

        for segment in self.meta.segments.iter() {
            if segment.get_real_name().eq(column_name) {
                return Some(Selection::Segment(segment.clone()));
            }
        }

        if check_alias {
            if let Some(r) = self.aliases.get(column_name) {
                // @todo Resolve without match!
                match r {
                    Selection::Dimension(d) => Some(Selection::Dimension(d.clone())),
                    Selection::Measure(d) => Some(Selection::Measure(d.clone())),
                    Selection::TimeDimension(d, g) => {
                        Some(Selection::TimeDimension(d.clone(), g.clone()))
                    }
                    s => panic!("Unable to map this selection type: {:?}", s),
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn find_dimension_for_identifier(
        &self,
        column_name: &String,
    ) -> Option<V1CubeMetaDimension> {
        for dimension in self.meta.dimensions.iter() {
            let (_, dimension_name) = dimension.name.split_once('.').unwrap();

            if dimension_name.eq(column_name) {
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

    pub fn find_selection_for_expr(
        &self,
        expr: &ast::Expr,
    ) -> CompilationResult<Option<Selection>> {
        match expr {
            ast::Expr::BinaryOp { .. } => self.find_selection_for_binary_op(&expr.to_string()),
            ast::Expr::Function(f) => self.find_selection_for_function(f),
            ast::Expr::Identifier(i) => {
                Ok(self.find_selection_for_identifier(&i.value.to_string(), false))
            }
            _ => {
                return Err(CompilationError::Unsupported(format!(
                    "Expression in selection: {:?}",
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
            ast::Expr::Wildcard => "*".to_string(),
            ast::Expr::Identifier(i) => i.value.to_string().to_lowercase(),
            _ => {
                return Err(CompilationError::Unsupported(format!(
                    "type of argument {:?}",
                    argument
                )));
            }
        };

        Ok(identifier)
    }

    pub fn find_selection_for_function(
        &self,
        f: &ast::Function,
    ) -> CompilationResult<Option<Selection>> {
        let aggregate_functions = vec!["sum", "min", "max", "avg", "count"];

        let fn_name = f.name.to_string().to_lowercase();
        if fn_name.eq("date_add") {
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
                    return Err(CompilationError::User(format!(
                        "Unable to unpack function: {:?}",
                        f
                    )));
                }
            };

            let time_dimension_opt = match left_fn {
                ast::Expr::Function(f) => {
                    if !f.name.to_string().to_lowercase().eq("date") {
                        return Err(CompilationError::User(format!(
                            "Unable to detect granularity (left side must be date): {:?}",
                            left_fn
                        )));
                    }

                    let possible_dimension_name = self.unpack_identifier_from_arg(&f.args[0])?;

                    self.meta.dimensions.iter().find(|dimension| {
                        dimension
                            .get_real_name()
                            .to_lowercase()
                            .eq(&possible_dimension_name)
                    })
                }
                _ => {
                    return Err(CompilationError::User(format!(
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
                    return Err(CompilationError::User(format!(
                        "Unable to detect granularity: {} ({:?})",
                        right_fn.to_string(),
                        right_fn
                    )));
                };

                Ok(Some(Selection::TimeDimension(
                    time_dimension.clone(),
                    granularity,
                )))
            } else {
                Ok(None)
            }
        } else if fn_name.eq("date") {
            match f.args.as_slice() {
                [ast::FunctionArg::Unnamed(ast::Expr::Function(date_sub))] => {
                    if !date_sub.name.to_string().to_lowercase().eq("date_sub") {
                        return Err(CompilationError::User(format!(
                            "Unable to detect heuristics in selection: {}",
                            f.to_string()
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
                        return Err(CompilationError::Unsupported("date granularity, week is not supported in Cube.js, please use ISOWEEK".to_string()));
                    } else if right_part.eq(&month_test) {
                        "month".to_string()
                    } else if right_part.eq(&year_test) {
                        "year".to_string()
                    } else {
                        return Err(CompilationError::User(format!(
                            "Unable to detect granularity: {:?}",
                            right_part
                        )));
                    };

                    let possible_dimension_name =
                        self.unpack_identifier_from_arg(&date_sub.args[0])?;

                    if let Some(r) = self.meta.dimensions.iter().find(|dimension| {
                        dimension
                            .get_real_name()
                            .to_lowercase()
                            .eq(&possible_dimension_name)
                    }) {
                        Ok(Some(Selection::TimeDimension(r.clone(), granularity)))
                    } else {
                        Ok(None)
                    }
                }
                [ast::FunctionArg::Unnamed(ast::Expr::Identifier(_i))] => {
                    let possible_dimension_name = self.unpack_identifier_from_arg(&f.args[0])?;

                    if let Some(r) = self.meta.dimensions.iter().find(|dimension| {
                        dimension
                            .get_real_name()
                            .to_lowercase()
                            .eq(&possible_dimension_name)
                    }) {
                        Ok(Some(Selection::TimeDimension(r.clone(), "day".to_string())))
                    } else {
                        return Err(CompilationError::User(format!(
                            "Unable to find dimension {} from expression: {}",
                            possible_dimension_name,
                            f.to_string()
                        )));
                    }
                }
                _ => Ok(None),
            }
        } else if aggregate_functions.contains(&fn_name.as_str()) {
            if f.args.is_empty() {
                return Err(CompilationError::User(
                    "Unable to use aggregation function without arguments".to_string(),
                ));
            } else if f.args.len() > 1 {
                return Err(CompilationError::User(
                    "Unable to use aggregation function with more then one argument".to_string(),
                ));
            };

            let argument = match &f.args[0] {
                ast::FunctionArg::Named { arg, .. } => arg,
                ast::FunctionArg::Unnamed(expr) => expr,
            };

            let measure_name = match argument {
                ast::Expr::Wildcard => "*".to_string(),
                ast::Expr::Identifier(i) => i.value.to_string(),
                _ => {
                    return Err(CompilationError::Unsupported(format!(
                        "type of argument {:?}",
                        argument
                    )));
                }
            };

            if measure_name == "*" && !(fn_name.eq(&"count".to_string()) && !f.distinct) {
                return Err(CompilationError::User(
                    "Unable to use * as argument to aggregation function (only count supported)"
                        .to_string(),
                ));
            }

            let mut call_agg_type = fn_name;

            if f.distinct {
                call_agg_type += &"Distinct".to_string();
            };

            if call_agg_type.eq(&"count".to_string()) {
                let measure_for_argument = self.meta.measures.iter().find(|measure| {
                    if measure.agg_type.is_some() {
                        let agg_type = measure.agg_type.clone().unwrap();
                        agg_type.eq(&"count".to_string())
                    } else {
                        false
                    }
                });

                if let Some(measure) = measure_for_argument {
                    Ok(Some(Selection::Measure(measure.clone())))
                } else {
                    Ok(None)
                }
            } else {
                let selection_opt = self.find_selection_for_identifier(&measure_name, true);
                if let Some(selection) = selection_opt {
                    match selection {
                        Selection::Measure(measure) => {
                            if measure.agg_type.is_some()
                                && !measure.is_same_agg_type(&call_agg_type)
                            {
                                return Err(CompilationError::User(format!(
                                    "Unable to use measure {} with type {:?} as argument in {} (required {})",
                                    measure.get_real_name(),
                                    measure.agg_type,
                                    f.to_string(),
                                    call_agg_type,
                                )));
                            } else {
                                // @todo Should we throw an exception?
                            };

                            Ok(Some(Selection::Measure(measure)))
                        }
                        Selection::Dimension(t) | Selection::TimeDimension(t, _) => {
                            Err(CompilationError::User(format!(
                                "Unable to use dimension {} as measure in aggregation function {}",
                                t.get_real_name(),
                                f.to_string(),
                            )))
                        }
                        Selection::Segment(s) => Err(CompilationError::User(format!(
                            "Unable to use segment {} as measure in aggregation function {}",
                            s.get_real_name(),
                            f.to_string(),
                        ))),
                    }
                } else {
                    Ok(None)
                }
            }
        } else if fn_name.to_lowercase().eq("measure") {
            if f.args.len() == 1 {
                let possible_measure_name = self.unpack_identifier_from_arg(&f.args[0])?;

                if let Some(r) = self.meta.measures.iter().find(|measure| {
                    measure
                        .get_real_name()
                        .to_lowercase()
                        .eq(&possible_measure_name)
                }) {
                    Ok(Some(Selection::Measure(r.clone())))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn with_alias(&mut self, alias: String, selection: Selection) {
        self.aliases.insert(alias, selection);
    }
}
