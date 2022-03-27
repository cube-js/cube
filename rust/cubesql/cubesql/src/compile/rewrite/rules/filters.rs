use crate::compile::engine::provider::CubeContext;
use crate::compile::rewrite::analysis::{ConstantData, LogicalPlanAnalysis};
use crate::compile::rewrite::rewriter::RewriteRules;
use crate::compile::rewrite::FilterMemberOp;
use crate::compile::rewrite::FilterMemberValues;
use crate::compile::rewrite::FilterReplacerCube;
use crate::compile::rewrite::InListExprNegated;
use crate::compile::rewrite::LiteralExprValue;
use crate::compile::rewrite::LogicalPlanLanguage;
use crate::compile::rewrite::TableScanSourceTableName;
use crate::compile::rewrite::TimeDimensionDateRange;
use crate::compile::rewrite::TimeDimensionDateRangeReplacerDateRange;
use crate::compile::rewrite::TimeDimensionDateRangeReplacerMember;
use crate::compile::rewrite::TimeDimensionGranularity;
use crate::compile::rewrite::TimeDimensionName;
use crate::compile::rewrite::{between_expr, FilterMemberMember};
use crate::compile::rewrite::{
    binary_expr, column_expr, cube_scan, cube_scan_filters, filter, filter_member, filter_op,
    filter_op_filters, filter_replacer, literal_expr, rewrite, transforming_rewrite,
};
use crate::compile::rewrite::{
    cube_scan_filters_empty_tail, cube_scan_members, dimension_expr, measure_expr,
    time_dimension_date_range_replacer, time_dimension_expr, BetweenExprNegated,
};
use crate::compile::rewrite::{inlist_expr, BinaryExprOp};
use crate::compile::rewrite::{is_not_null_expr, is_null_expr, ColumnExprColumn};
use crate::transport::ext::V1CubeMetaExt;
use crate::transport::MemberType;
use crate::var;
use crate::var_iter;
use chrono::{SecondsFormat, TimeZone, Utc};
use datafusion::logical_plan::{Column, Operator};
use datafusion::scalar::ScalarValue;
use egg::{EGraph, Rewrite, Subst};
use std::fmt::Display;
use std::ops::Index;
use std::sync::Arc;

pub struct FilterRules {
    cube_context: Arc<CubeContext>,
}

impl RewriteRules for FilterRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        vec![
            transforming_rewrite(
                "push-down-filter",
                filter(
                    "?expr",
                    cube_scan(
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "?order",
                        "?limit",
                        "?offset",
                    ),
                ),
                cube_scan(
                    "?source_table_name",
                    "?members",
                    cube_scan_filters("?filters", filter_replacer("?expr", "?cube")),
                    "?order",
                    "?limit",
                    "?offset",
                ),
                self.push_down_filter("?source_table_name", "?expr", "?cube"),
            ),
            transforming_rewrite(
                "filter-replacer",
                filter_replacer(
                    binary_expr(column_expr("?column"), "?op", literal_expr("?literal")),
                    "?cube",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_filter(
                    "?column",
                    "?op",
                    "?literal",
                    "?cube",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                ),
            ),
            transforming_rewrite(
                "filter-replacer-in-filter",
                filter_replacer(
                    inlist_expr(column_expr("?column"), "?list", "?negated"),
                    "?cube",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_in_filter(
                    "?column",
                    "?list",
                    "?negated",
                    "?cube",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                ),
            ),
            transforming_rewrite(
                "filter-replacer-is-null",
                filter_replacer(is_null_expr(column_expr("?column")), "?cube"),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_is_null(
                    "?column",
                    "?cube",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    true,
                ),
            ),
            transforming_rewrite(
                "filter-replacer-is-not-null",
                filter_replacer(is_not_null_expr(column_expr("?column")), "?cube"),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_is_null(
                    "?column",
                    "?cube",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                    false,
                ),
            ),
            transforming_rewrite(
                "filter-replacer-between",
                filter_replacer(
                    between_expr(column_expr("?column"), "?negated", "?low", "?high"),
                    "?cube",
                ),
                filter_member("?filter_member", "?filter_op", "?filter_values"),
                self.transform_between(
                    "?column",
                    "?negated",
                    "?low",
                    "?high",
                    "?cube",
                    "?filter_member",
                    "?filter_op",
                    "?filter_values",
                ),
            ),
            rewrite(
                "filter-replacer-and",
                filter_replacer(binary_expr("?left", "AND", "?right"), "?cube"),
                filter_op(
                    filter_op_filters(
                        filter_replacer("?left", "?cube"),
                        filter_replacer("?right", "?cube"),
                    ),
                    "and",
                ),
            ),
            rewrite(
                "filter-replacer-or",
                filter_replacer(binary_expr("?left", "OR", "?right"), "?cube"),
                filter_op(
                    filter_op_filters(
                        filter_replacer("?left", "?cube"),
                        filter_replacer("?right", "?cube"),
                    ),
                    "or",
                ),
            ),
            rewrite(
                "filter-flatten-upper-and-left",
                cube_scan_filters(
                    filter_op(filter_op_filters("?left", "?right"), "and"),
                    "?tail",
                ),
                cube_scan_filters(cube_scan_filters("?left", "?right"), "?tail"),
            ),
            rewrite(
                "filter-flatten-upper-and-right",
                cube_scan_filters(
                    "?tail",
                    filter_op(filter_op_filters("?left", "?right"), "and"),
                ),
                cube_scan_filters("?tail", cube_scan_filters("?left", "?right")),
            ),
            rewrite(
                "filter-flatten-upper-and-left-reduce",
                cube_scan_filters(filter_op_filters("?left", "?right"), "?tail"),
                cube_scan_filters(cube_scan_filters("?left", "?right"), "?tail"),
            ),
            rewrite(
                "filter-flatten-upper-and-right-reduce",
                cube_scan_filters("?tail", filter_op_filters("?left", "?right")),
                cube_scan_filters("?tail", cube_scan_filters("?left", "?right")),
            ),
            filter_flatten_rewrite_left("or"),
            filter_flatten_rewrite_right("or"),
            filter_flatten_rewrite_left("and"),
            filter_flatten_rewrite_right("and"),
            // TODO changes filter ordering which fail tests
            // rewrite(
            //     "filter-commute",
            //     cube_scan_filters("?left", "?right"),
            //     cube_scan_filters("?right", "?left"),
            // ),
            transforming_rewrite(
                "filter-replacer-in-date-range",
                filter_op(
                    filter_op_filters(
                        filter_member("?member", "FilterMemberOp:afterDate", "?date_range_start"),
                        filter_member("?member", "FilterMemberOp:beforeDate", "?date_range_end"),
                    ),
                    "and",
                ),
                filter_member("?member", "FilterMemberOp:inDateRange", "?date_range"),
                self.merge_date_range("?date_range_start", "?date_range_end", "?date_range"),
            ),
            transforming_rewrite(
                "in-date-range-to-time-dimension",
                filter_member("?member", "FilterMemberOp:inDateRange", "?date_range"),
                time_dimension_date_range_replacer(
                    cube_scan_filters_empty_tail(),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                self.filter_to_time_dimension_range(
                    "?member",
                    "?date_range",
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
            ),
            rewrite(
                "in-date-range-to-time-dimension-pull-up-left",
                cube_scan_filters(
                    time_dimension_date_range_replacer(
                        "?filters",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                    "?right",
                ),
                time_dimension_date_range_replacer(
                    cube_scan_filters("?filters", "?right"),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
            ),
            rewrite(
                "in-date-range-to-time-dimension-pull-up-right",
                cube_scan_filters(
                    "?left",
                    time_dimension_date_range_replacer(
                        "?filters",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                ),
                time_dimension_date_range_replacer(
                    cube_scan_filters("?left", "?filters"),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
            ),
            rewrite(
                "in-date-range-to-time-dimension-swap-to-members",
                cube_scan(
                    "?source_table_name",
                    "?members",
                    time_dimension_date_range_replacer(
                        "?filters",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                    "?order",
                    "?limit",
                    "?offset",
                ),
                cube_scan(
                    "?source_table_name",
                    time_dimension_date_range_replacer(
                        "?members",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                    "?filters",
                    "?order",
                    "?limit",
                    "?offset",
                ),
            ),
            transforming_rewrite(
                "time-dimension-date-range-replacer-push-down-left",
                time_dimension_date_range_replacer(
                    cube_scan_members("?left", "?right"),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                cube_scan_members(
                    time_dimension_date_range_replacer(
                        "?left",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                    "?right",
                ),
                self.push_down_time_dimension_replacer("?left", "?time_dimension_member"),
            ),
            transforming_rewrite(
                "time-dimension-date-range-replacer-push-down-right",
                time_dimension_date_range_replacer(
                    cube_scan_members("?left", "?right"),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                cube_scan_members(
                    "?left",
                    time_dimension_date_range_replacer(
                        "?right",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                ),
                self.push_down_time_dimension_replacer("?right", "?time_dimension_member"),
            ),
            transforming_rewrite(
                "time-dimension-date-range-replacer-push-down-new-time-dimension",
                time_dimension_date_range_replacer(
                    "?members",
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                cube_scan_members(
                    time_dimension_expr("?member", "?granularity", "?date_range", "?expr"),
                    "?members",
                ),
                self.push_down_time_dimension_replacer_new_time_dimension(
                    "?members",
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                    "?member",
                    "?granularity",
                    "?date_range",
                    "?expr",
                ),
            ),
            rewrite(
                "time-dimension-date-range-replacer-skip-measure",
                time_dimension_date_range_replacer(
                    cube_scan_members(measure_expr("?measure", "?expr"), "?tail"),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                cube_scan_members(
                    measure_expr("?measure", "?expr"),
                    time_dimension_date_range_replacer(
                        "?tail",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                ),
            ),
            rewrite(
                "time-dimension-date-range-replacer-skip-dimension",
                time_dimension_date_range_replacer(
                    cube_scan_members(dimension_expr("?dimension", "?expr"), "?tail"),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                cube_scan_members(
                    dimension_expr("?dimension", "?expr"),
                    time_dimension_date_range_replacer(
                        "?tail",
                        "?time_dimension_member",
                        "?time_dimension_date_range",
                    ),
                ),
            ),
            transforming_rewrite(
                "time-dimension-date-range-replacer-time-dimension",
                time_dimension_date_range_replacer(
                    cube_scan_members(
                        time_dimension_expr("?member", "?granularity", "?date_range", "?expr"),
                        "?tail",
                    ),
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                ),
                cube_scan_members(
                    time_dimension_expr("?member", "?granularity", "?output_date_range", "?expr"),
                    "?tail",
                ),
                self.replace_time_dimension_date_range(
                    "?member",
                    "?date_range",
                    "?time_dimension_member",
                    "?time_dimension_date_range",
                    "?output_date_range",
                ),
            ),
        ]
    }
}

impl FilterRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self { cube_context }
    }

    fn push_down_filter(
        &self,
        table_name_var: &'static str,
        exp_var: &'static str,
        cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let table_name_var = var!(table_name_var);
        let exp_var = var!(exp_var);
        let cube_var = var!(cube_var);
        move |egraph, subst| {
            for table_name in var_iter!(egraph[subst[table_name_var]], TableScanSourceTableName) {
                if let Some(_referenced_expr) = &egraph.index(subst[exp_var]).data.referenced_expr {
                    let table_name = table_name.to_string();
                    // TODO check referenced_expr
                    subst.insert(
                        cube_var,
                        egraph.add(LogicalPlanLanguage::FilterReplacerCube(FilterReplacerCube(
                            Some(table_name.to_string()),
                        ))),
                    );
                    return true;
                }
            }
            false
        }
    }

    fn transform_filter(
        &self,
        column_var: &'static str,
        op_var: &'static str,
        literal_var: &'static str,
        cube_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = column_var.parse().unwrap();
        let op_var = op_var.parse().unwrap();
        let literal_var = literal_var.parse().unwrap();
        let cube_var = cube_var.parse().unwrap();
        let filter_member_var = filter_member_var.parse().unwrap();
        let filter_op_var = filter_op_var.parse().unwrap();
        let filter_values_var = filter_values_var.parse().unwrap();
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for cube in var_iter!(egraph[subst[cube_var]], FilterReplacerCube) {
                for expr_op in var_iter!(egraph[subst[op_var]], BinaryExprOp) {
                    for literal in var_iter!(egraph[subst[literal_var]], LiteralExprValue) {
                        if let Some(cube) = cube
                            .as_ref()
                            .and_then(|cube| meta_context.find_cube_with_name(cube.to_string()))
                        {
                            for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn) {
                                let member_name = format!("{}.{}", cube.name, column.name);
                                if let Some(member_type) = cube.member_type(&member_name) {
                                    let op = match expr_op {
                                        Operator::Eq => "equals",
                                        Operator::NotEq => "notEquals",
                                        Operator::Lt => "lt",
                                        Operator::LtEq => "lte",
                                        Operator::Gt => "gt",
                                        Operator::GtEq => "gte",
                                        Operator::Like => "contains",
                                        Operator::NotLike => "notContains",
                                        _ => {
                                            continue;
                                        }
                                    };

                                    let op = match member_type {
                                        MemberType::String => op,
                                        MemberType::Number => op,
                                        MemberType::Boolean => op,
                                        MemberType::Time => match expr_op {
                                            Operator::Lt => "beforeDate",
                                            Operator::LtEq => "beforeDate",
                                            Operator::Gt => "afterDate",
                                            Operator::GtEq => "afterDate",
                                            _ => op,
                                        },
                                    };

                                    let value = match literal {
                                        ScalarValue::Utf8(Some(value)) => value.to_string(),
                                        ScalarValue::Int64(Some(value)) => value.to_string(),
                                        ScalarValue::Boolean(Some(value)) => value.to_string(),
                                        ScalarValue::Float64(Some(value)) => value.to_string(),
                                        ScalarValue::TimestampNanosecond(Some(value)) => {
                                            let minus_one = Utc
                                                .timestamp_nanos(*value - 1000)
                                                .to_rfc3339_opts(SecondsFormat::Millis, true);
                                            let value = Utc
                                                .timestamp_nanos(*value)
                                                .to_rfc3339_opts(SecondsFormat::Millis, true);

                                            match expr_op {
                                                Operator::Lt => minus_one,
                                                Operator::LtEq => minus_one,
                                                Operator::Gt => value,
                                                Operator::GtEq => value,
                                                _ => {
                                                    continue;
                                                }
                                            }
                                        }
                                        x => panic!("Unsupported filter scalar: {:?}", x),
                                    };

                                    subst.insert(
                                        filter_member_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberMember(
                                            FilterMemberMember(member_name.to_string()),
                                        )),
                                    );

                                    subst.insert(
                                        filter_op_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberOp(
                                            FilterMemberOp(op.to_string()),
                                        )),
                                    );

                                    subst.insert(
                                        filter_values_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                            FilterMemberValues(vec![value.to_string()]),
                                        )),
                                    );

                                    return true;
                                }
                            }
                        }
                    }
                }
            }

            false
        }
    }

    fn transform_in_filter(
        &self,
        column_var: &'static str,
        list_var: &'static str,
        negated_var: &'static str,
        cube_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let list_var = var!(list_var);
        let negated_var = var!(negated_var);
        let cube_var = var!(cube_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for cube in var_iter!(egraph[subst[cube_var]], FilterReplacerCube) {
                if let Some(cube) = cube
                    .as_ref()
                    .and_then(|cube| meta_context.find_cube_with_name(cube.to_string()))
                {
                    if let Some(ConstantData::Intermediate(list)) =
                        &egraph[subst[list_var]].data.constant
                    {
                        let values = list
                            .into_iter()
                            .map(|literal| FilterRules::scalar_to_value(literal))
                            .collect::<Vec<_>>();

                        for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn) {
                            let member_name = format!("{}.{}", cube.name, column.name);
                            if cube.contains_member(&member_name) {
                                for negated in
                                    var_iter!(egraph[subst[negated_var]], InListExprNegated)
                                {
                                    let negated = *negated;
                                    subst.insert(
                                        filter_member_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberMember(
                                            FilterMemberMember(member_name.to_string()),
                                        )),
                                    );

                                    subst.insert(
                                        filter_op_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberOp(
                                            FilterMemberOp(if negated {
                                                "notEquals".to_string()
                                            } else {
                                                "equals".to_string()
                                            }),
                                        )),
                                    );

                                    subst.insert(
                                        filter_values_var,
                                        egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                            FilterMemberValues(values),
                                        )),
                                    );

                                    return true;
                                }
                            }
                        }
                    }
                }
            }

            false
        }
    }

    fn scalar_to_value(literal: &ScalarValue) -> String {
        match literal {
            ScalarValue::Utf8(Some(value)) => value.to_string(),
            ScalarValue::Int64(Some(value)) => value.to_string(),
            ScalarValue::Boolean(Some(value)) => value.to_string(),
            ScalarValue::Float64(Some(value)) => value.to_string(),
            x => panic!("Unsupported filter scalar: {:?}", x),
        }
    }

    fn transform_is_null(
        &self,
        column_var: &'static str,
        cube_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
        is_null_op: bool,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let cube_var = var!(cube_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for cube in var_iter!(egraph[subst[cube_var]], FilterReplacerCube) {
                if let Some(cube) = cube
                    .as_ref()
                    .and_then(|cube| meta_context.find_cube_with_name(cube.to_string()))
                {
                    for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn) {
                        let member_name = format!("{}.{}", cube.name, column.name);
                        if cube.contains_member(&member_name) {
                            subst.insert(
                                filter_member_var,
                                egraph.add(LogicalPlanLanguage::FilterMemberMember(
                                    FilterMemberMember(member_name.to_string()),
                                )),
                            );

                            subst.insert(
                                filter_op_var,
                                egraph.add(LogicalPlanLanguage::FilterMemberOp(FilterMemberOp(
                                    if is_null_op {
                                        "notSet".to_string()
                                    } else {
                                        "set".to_string()
                                    },
                                ))),
                            );

                            subst.insert(
                                filter_values_var,
                                egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                    FilterMemberValues(Vec::new()),
                                )),
                            );

                            return true;
                        }
                    }
                }
            }

            false
        }
    }

    fn transform_between(
        &self,
        column_var: &'static str,
        negated_var: &'static str,
        low_var: &'static str,
        high_var: &'static str,
        cube_var: &'static str,
        filter_member_var: &'static str,
        filter_op_var: &'static str,
        filter_values_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let negated_var = var!(negated_var);
        let low_var = var!(low_var);
        let high_var = var!(high_var);
        let cube_var = var!(cube_var);
        let filter_member_var = var!(filter_member_var);
        let filter_op_var = var!(filter_op_var);
        let filter_values_var = var!(filter_values_var);
        let meta_context = self.cube_context.meta.clone();
        move |egraph, subst| {
            for cube in var_iter!(egraph[subst[cube_var]], FilterReplacerCube) {
                if let Some(cube) = cube
                    .as_ref()
                    .and_then(|cube| meta_context.find_cube_with_name(cube.to_string()))
                {
                    for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn) {
                        let member_name = format!("{}.{}", cube.name, column.name);
                        if let Some(_) = cube.lookup_dimension(&member_name) {
                            for negated in var_iter!(egraph[subst[negated_var]], BetweenExprNegated)
                            {
                                let negated = *negated;
                                if let Some(ConstantData::Intermediate(low)) =
                                    &egraph[subst[low_var]].data.constant
                                {
                                    if let Some(ConstantData::Intermediate(high)) =
                                        &egraph[subst[high_var]].data.constant
                                    {
                                        let values = vec![
                                            FilterRules::scalar_to_value(&low[0]),
                                            FilterRules::scalar_to_value(&high[0]),
                                        ];

                                        subst.insert(
                                            filter_member_var,
                                            egraph.add(LogicalPlanLanguage::FilterMemberMember(
                                                FilterMemberMember(member_name.to_string()),
                                            )),
                                        );

                                        subst.insert(
                                            filter_op_var,
                                            egraph.add(LogicalPlanLanguage::FilterMemberOp(
                                                FilterMemberOp(if negated {
                                                    "notInDateRange".to_string()
                                                } else {
                                                    "inDateRange".to_string()
                                                }),
                                            )),
                                        );

                                        subst.insert(
                                            filter_values_var,
                                            egraph.add(LogicalPlanLanguage::FilterMemberValues(
                                                FilterMemberValues(values),
                                            )),
                                        );

                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            false
        }
    }

    fn merge_date_range(
        &self,
        date_range_start_var: &'static str,
        date_range_end_var: &'static str,
        date_range_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let date_range_start_var = date_range_start_var.parse().unwrap();
        let date_range_end_var = date_range_end_var.parse().unwrap();
        let date_range_var = date_range_var.parse().unwrap();
        move |egraph, subst| {
            for date_range_start in
                var_iter!(egraph[subst[date_range_start_var]], FilterMemberValues)
            {
                for date_range_end in
                    var_iter!(egraph[subst[date_range_end_var]], FilterMemberValues)
                {
                    let mut result = Vec::new();
                    result.extend(date_range_start.clone().into_iter());
                    result.extend(date_range_end.clone().into_iter());
                    subst.insert(
                        date_range_var,
                        egraph.add(LogicalPlanLanguage::FilterMemberValues(FilterMemberValues(
                            result,
                        ))),
                    );
                    return true;
                }
            }

            false
        }
    }

    fn filter_to_time_dimension_range(
        &self,
        member_var: &'static str,
        date_range_var: &'static str,
        time_dimension_member_var: &'static str,
        time_dimension_date_range_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let member_var = member_var.parse().unwrap();
        let date_range_var = date_range_var.parse().unwrap();
        let time_dimension_member_var = time_dimension_member_var.parse().unwrap();
        let time_dimension_date_range_var = time_dimension_date_range_var.parse().unwrap();
        move |egraph, subst| {
            for member in var_iter!(egraph[subst[member_var]], FilterMemberMember) {
                let member = member.to_string();
                for date_range in var_iter!(egraph[subst[date_range_var]], FilterMemberValues) {
                    let date_range = date_range.clone();
                    subst.insert(
                        time_dimension_member_var,
                        egraph.add(LogicalPlanLanguage::TimeDimensionDateRangeReplacerMember(
                            TimeDimensionDateRangeReplacerMember(member.to_string()),
                        )),
                    );

                    subst.insert(
                        time_dimension_date_range_var,
                        egraph.add(
                            LogicalPlanLanguage::TimeDimensionDateRangeReplacerDateRange(
                                TimeDimensionDateRangeReplacerDateRange(date_range.clone()),
                            ),
                        ),
                    );

                    return true;
                }
            }

            false
        }
    }

    fn push_down_time_dimension_replacer_new_time_dimension(
        &self,
        members_var: &'static str,
        time_dimension_member_var: &'static str,
        time_dimension_date_range_var: &'static str,
        member_var: &'static str,
        granularity_var: &'static str,
        date_range_var: &'static str,
        expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let members_var = var!(members_var);
        let time_dimension_member_var = var!(time_dimension_member_var);
        let time_dimension_date_range_var = var!(time_dimension_date_range_var);
        let member_var = var!(member_var);
        let granularity_var = var!(granularity_var);
        let date_range_var = var!(date_range_var);
        let expr_var = var!(expr_var);
        move |egraph, subst| {
            for member in var_iter!(
                egraph[subst[time_dimension_member_var]],
                TimeDimensionDateRangeReplacerMember
            ) {
                let member = member.to_string();
                if let Some(member_name_to_expr) =
                    &egraph.index(subst[members_var]).data.member_name_to_expr
                {
                    if member_name_to_expr.iter().all(|(m, _)| m != &member) {
                        let date_range = var_iter!(
                            egraph[subst[time_dimension_date_range_var]],
                            TimeDimensionDateRangeReplacerDateRange
                        )
                        .next()
                        .unwrap()
                        .clone();

                        subst.insert(
                            member_var,
                            egraph.add(LogicalPlanLanguage::TimeDimensionName(TimeDimensionName(
                                member.to_string(),
                            ))),
                        );

                        subst.insert(
                            granularity_var,
                            egraph.add(LogicalPlanLanguage::TimeDimensionGranularity(
                                TimeDimensionGranularity(None),
                            )),
                        );

                        subst.insert(
                            date_range_var,
                            egraph.add(LogicalPlanLanguage::TimeDimensionDateRange(
                                TimeDimensionDateRange(Some(date_range)),
                            )),
                        );

                        let column = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                            ColumnExprColumn(Column::from_name(member.to_string())),
                        ));

                        subst.insert(
                            expr_var,
                            egraph.add(LogicalPlanLanguage::ColumnExpr([column])),
                        );

                        return true;
                    }
                }
            }

            false
        }
    }

    fn push_down_time_dimension_replacer(
        &self,
        members_var: &'static str,
        time_dimension_member_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let members_var = var!(members_var);
        let time_dimension_member_var = var!(time_dimension_member_var);
        move |egraph, subst| {
            for member in var_iter!(
                egraph[subst[time_dimension_member_var]],
                TimeDimensionDateRangeReplacerMember
            ) {
                if let Some(member_name_to_expr) =
                    &egraph.index(subst[members_var]).data.member_name_to_expr
                {
                    if member_name_to_expr.iter().any(|(m, _)| m == member) {
                        return true;
                    }
                }
            }

            false
        }
    }

    fn replace_time_dimension_date_range(
        &self,
        member_var: &'static str,
        date_range_var: &'static str,
        time_dimension_member_var: &'static str,
        time_dimension_date_range_var: &'static str,
        output_date_range_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let member_var = var!(member_var);
        let date_range_var = var!(date_range_var);
        let time_dimension_member_var = var!(time_dimension_member_var);
        let time_dimension_date_range_var = var!(time_dimension_date_range_var);
        let output_date_range_var = var!(output_date_range_var);
        move |egraph, subst| {
            for date_range in var_iter!(egraph[subst[date_range_var]], TimeDimensionDateRange) {
                if date_range.is_none() {
                    for member in var_iter!(egraph[subst[member_var]], TimeDimensionName) {
                        for time_dimension_member in var_iter!(
                            egraph[subst[time_dimension_member_var]],
                            TimeDimensionDateRangeReplacerMember
                        ) {
                            if member == time_dimension_member {
                                for time_dimension_date_range in var_iter!(
                                    egraph[subst[time_dimension_date_range_var]],
                                    TimeDimensionDateRangeReplacerDateRange
                                ) {
                                    let time_dimension_date_range =
                                        time_dimension_date_range.clone();
                                    subst.insert(
                                        output_date_range_var,
                                        egraph.add(LogicalPlanLanguage::TimeDimensionDateRange(
                                            TimeDimensionDateRange(Some(
                                                time_dimension_date_range.clone(),
                                            )),
                                        )),
                                    );

                                    return true;
                                }
                            }
                        }
                    }
                }
            }

            false
        }
    }
}

fn filter_flatten_rewrite_left(
    op: impl Display + Copy,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
    rewrite(
        &format!("filter-flatten-{}-left", op),
        filter_op(
            filter_op_filters(filter_op(filter_op_filters("?left", "?right"), op), "?tail"),
            op,
        ),
        filter_op(
            filter_op_filters(filter_op_filters("?left", "?right"), "?tail"),
            op,
        ),
    )
}

fn filter_flatten_rewrite_right(
    op: impl Display + Copy,
) -> Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis> {
    rewrite(
        &format!("filter-flatten-{}-right", op),
        filter_op(
            filter_op_filters("?tail", filter_op(filter_op_filters("?left", "?right"), op)),
            op,
        ),
        filter_op(
            filter_op_filters("?tail", filter_op_filters("?left", "?right")),
            op,
        ),
    )
}
