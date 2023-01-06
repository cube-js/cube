use super::utils::parse_granularity;
use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            agg_fun_expr, analysis::LogicalPlanAnalysis, binary_expr, cast_expr,
            cast_expr_explicit, column_expr, fun_expr, literal_expr, literal_int, literal_string,
            negative_expr, rewrite, rewriter::RewriteRules, to_day_interval_expr,
            transforming_rewrite, udf_expr, CastExprDataType, LiteralExprValue,
            LogicalPlanLanguage,
        },
    },
    var, var_iter,
};
use datafusion::{arrow::datatypes::DataType, scalar::ScalarValue};
use egg::{EGraph, Rewrite, Subst};
use std::{convert::TryFrom, sync::Arc};

pub struct DateRules {
    _cube_context: Arc<CubeContext>,
}

impl RewriteRules for DateRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        vec![
            // TODO ?interval
            rewrite(
                "superset-quarter-to-date-trunc",
                binary_expr(
                    binary_expr(
                        udf_expr(
                            "makedate",
                            vec![
                                udf_expr("year", vec![column_expr("?column")]),
                                literal_int(1),
                            ],
                        ),
                        "+",
                        fun_expr(
                            "ToMonthInterval",
                            vec![
                                udf_expr("quarter", vec![column_expr("?column")]),
                                literal_string("quarter"),
                            ],
                        ),
                    ),
                    "-",
                    literal_expr("?interval"),
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("quarter"), column_expr("?column")],
                ),
            ),
            // TODO ?interval
            rewrite(
                "superset-week-to-date-trunc",
                udf_expr(
                    "date",
                    vec![udf_expr(
                        "date_sub",
                        vec![
                            column_expr("?column"),
                            to_day_interval_expr(
                                binary_expr(
                                    udf_expr(
                                        "dayofweek",
                                        vec![udf_expr(
                                            "date_sub",
                                            vec![column_expr("?column"), literal_expr("?interval")],
                                        )],
                                    ),
                                    "-",
                                    literal_int(1),
                                ),
                                literal_string("day"),
                            ),
                        ],
                    )],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("week"), column_expr("?column")],
                ),
            ),
            rewrite(
                "superset-month-to-date-trunc",
                udf_expr(
                    "date",
                    vec![udf_expr(
                        "date_sub",
                        vec![
                            column_expr("?column"),
                            to_day_interval_expr(
                                binary_expr(
                                    udf_expr("dayofmonth", vec![column_expr("?column")]),
                                    "-",
                                    literal_int(1),
                                ),
                                literal_string("day"),
                            ),
                        ],
                    )],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("month"), column_expr("?column")],
                ),
            ),
            rewrite(
                "superset-year-to-date-trunc",
                udf_expr(
                    "date",
                    vec![udf_expr(
                        "date_sub",
                        vec![
                            column_expr("?column"),
                            to_day_interval_expr(
                                binary_expr(
                                    udf_expr("dayofyear", vec![column_expr("?column")]),
                                    "-",
                                    literal_int(1),
                                ),
                                literal_string("day"),
                            ),
                        ],
                    )],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("year"), column_expr("?column")],
                ),
            ),
            rewrite(
                "superset-hour-to-date-trunc",
                udf_expr(
                    "date_add",
                    vec![
                        udf_expr("date", vec![column_expr("?column")]),
                        to_day_interval_expr(
                            udf_expr("hour", vec![column_expr("?column")]),
                            literal_string("hour"),
                        ),
                    ],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("hour"), column_expr("?column")],
                ),
            ),
            rewrite(
                "superset-minute-to-date-trunc",
                udf_expr(
                    "date_add",
                    vec![
                        udf_expr("date", vec![column_expr("?column")]),
                        to_day_interval_expr(
                            binary_expr(
                                binary_expr(
                                    udf_expr("hour", vec![column_expr("?column")]),
                                    "*",
                                    literal_int(60),
                                ),
                                "+",
                                udf_expr("minute", vec![column_expr("?column")]),
                            ),
                            literal_string("minute"),
                        ),
                    ],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("minute"), column_expr("?column")],
                ),
            ),
            rewrite(
                "superset-second-to-date-trunc",
                udf_expr(
                    "date_add",
                    vec![
                        udf_expr("date", vec![column_expr("?column")]),
                        to_day_interval_expr(
                            binary_expr(
                                binary_expr(
                                    binary_expr(
                                        udf_expr("hour", vec![column_expr("?column")]),
                                        "*",
                                        literal_int(60),
                                    ),
                                    "*",
                                    literal_int(60),
                                ),
                                "+",
                                binary_expr(
                                    binary_expr(
                                        udf_expr("minute", vec![column_expr("?column")]),
                                        "*",
                                        literal_int(60),
                                    ),
                                    "+",
                                    udf_expr("second", vec![column_expr("?column")]),
                                ),
                            ),
                            literal_string("second"),
                        ),
                    ],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("second"), column_expr("?column")],
                ),
            ),
            rewrite(
                "date-to-date-trunc",
                udf_expr("date", vec![column_expr("?column")]),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("day"), column_expr("?column")],
                ),
            ),
            rewrite(
                "cast-in-date-trunc",
                fun_expr(
                    "DateTrunc",
                    // TODO check data_type?
                    vec![
                        "?granularity".to_string(),
                        cast_expr(column_expr("?column"), "?data_type"),
                    ],
                ),
                fun_expr(
                    "DateTrunc",
                    vec!["?granularity".to_string(), column_expr("?column")],
                ),
            ),
            rewrite(
                "current-timestamp-to-now",
                udf_expr("current_timestamp", Vec::<String>::new()),
                fun_expr("UtcTimestamp", Vec::<String>::new()),
            ),
            rewrite(
                "localtimestamp-to-now",
                udf_expr("localtimestamp", Vec::<String>::new()),
                fun_expr("UtcTimestamp", Vec::<String>::new()),
            ),
            rewrite(
                "tableau-week",
                binary_expr(
                    fun_expr(
                        "DateTrunc",
                        vec!["?granularity".to_string(), column_expr("?column")],
                    ),
                    "+",
                    negative_expr(binary_expr(
                        fun_expr(
                            "DatePart",
                            vec![literal_string("DOW"), column_expr("?column")],
                        ),
                        "*",
                        // TODO match
                        literal_expr("?interval_one_day"),
                    )),
                ),
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("week"), column_expr("?column")],
                ),
            ),
            rewrite(
                "metabase-interval-date-range",
                binary_expr(
                    cast_expr(fun_expr("Now", Vec::<String>::new()), "?data_type"),
                    "+",
                    literal_expr("?interval"),
                ),
                udf_expr(
                    "date_add",
                    vec![
                        fun_expr("Now", Vec::<String>::new()),
                        literal_expr("?interval"),
                    ],
                ),
            ),
            transforming_rewrite(
                "binary-expr-interval-add-right",
                binary_expr("?left", "+", literal_expr("?interval")),
                udf_expr(
                    "date_add",
                    vec!["?left".to_string(), literal_expr("?interval")],
                ),
                self.transform_interval_binary_expr("?interval"),
            ),
            transforming_rewrite(
                "binary-expr-interval-add-left",
                binary_expr(literal_expr("?interval"), "+", "?right"),
                udf_expr(
                    "date_add",
                    vec!["?right".to_string(), literal_expr("?interval")],
                ),
                self.transform_interval_binary_expr("?interval"),
            ),
            transforming_rewrite(
                "binary-expr-interval-sub",
                binary_expr("?left", "-", literal_expr("?interval")),
                udf_expr(
                    "date_sub",
                    vec!["?left".to_string(), literal_expr("?interval")],
                ),
                self.transform_interval_binary_expr("?interval"),
            ),
            transforming_rewrite(
                "binary-expr-interval-mul-right",
                binary_expr("?left", "*", literal_expr("?interval")),
                udf_expr(
                    "interval_mul",
                    vec![literal_expr("?interval"), "?left".to_string()],
                ),
                self.transform_interval_binary_expr("?interval"),
            ),
            transforming_rewrite(
                "binary-expr-interval-mul-left",
                binary_expr(literal_expr("?interval"), "*", "?right"),
                udf_expr(
                    "interval_mul",
                    vec![literal_expr("?interval"), "?right".to_string()],
                ),
                self.transform_interval_binary_expr("?interval"),
            ),
            transforming_rewrite(
                "binary-expr-interval-neg",
                negative_expr(literal_expr("?interval")),
                udf_expr(
                    "interval_mul",
                    vec![literal_expr("?interval"), literal_int(-1)],
                ),
                self.transform_interval_binary_expr("?interval"),
            ),
            rewrite(
                "redshift-dateadd-to-interval-cast-unwrap",
                udf_expr(
                    "dateadd",
                    vec![
                        literal_expr("?datepart"),
                        cast_expr_explicit(literal_expr("?interval_int"), DataType::Int32),
                        "?expr".to_string(),
                    ],
                ),
                udf_expr(
                    "dateadd",
                    vec![
                        literal_expr("?datepart"),
                        literal_expr("?interval_int"),
                        "?expr".to_string(),
                    ],
                ),
            ),
            transforming_rewrite(
                "redshift-dateadd-to-interval",
                udf_expr(
                    "dateadd",
                    vec![
                        literal_expr("?datepart"),
                        literal_expr("?interval_int"),
                        "?expr".to_string(),
                    ],
                ),
                udf_expr(
                    "date_add",
                    vec!["?expr".to_string(), literal_expr("?interval")],
                ),
                self.transform_interval_parts_to_interval(
                    "?datepart",
                    "?interval_int",
                    "?interval",
                ),
            ),
            transforming_rewrite(
                "redshift-dateadd-literal-date32-to-interval",
                udf_expr(
                    "dateadd",
                    vec![
                        literal_expr("?datepart"),
                        literal_expr("?interval_int"),
                        cast_expr_explicit(literal_expr("?date_string"), DataType::Date32),
                    ],
                ),
                udf_expr(
                    "date_add",
                    vec![
                        udf_expr("date_to_timestamp", vec![literal_expr("?date_string")]),
                        literal_expr("?interval"),
                    ],
                ),
                self.transform_interval_parts_to_interval(
                    "?datepart",
                    "?interval_int",
                    "?interval",
                ),
            ),
            rewrite(
                "thoughtspot-to-date-to-timestamp",
                udf_expr(
                    "to_date",
                    vec![literal_expr("?date"), literal_string("YYYY-MM-DD")],
                ),
                udf_expr("date_to_timestamp", vec![literal_expr("?date")]),
            ),
            rewrite(
                "datastudio-dates",
                fun_expr(
                    "DateTrunc",
                    vec![
                        "?granularity".to_string(),
                        fun_expr(
                            "DateTrunc",
                            vec![literal_string("SECOND"), column_expr("?column")],
                        ),
                    ],
                ),
                fun_expr(
                    "DateTrunc",
                    vec!["?granularity".to_string(), column_expr("?column")],
                ),
            ),
            transforming_rewrite(
                "unwrap-cast-to-date",
                agg_fun_expr(
                    "?aggr_fun",
                    vec![cast_expr(column_expr("?column"), "?data_type")],
                    "?distinct",
                ),
                agg_fun_expr("?aggr_fun", vec![column_expr("?column")], "?distinct"),
                self.unwrap_cast_to_date("?data_type"),
            ),
        ]
    }
}

impl DateRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self {
            _cube_context: cube_context,
        }
    }

    fn transform_interval_binary_expr(
        &self,
        interval_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let interval_var = var!(interval_var);
        move |egraph, subst| {
            for interval in var_iter!(egraph[subst[interval_var]], LiteralExprValue) {
                match interval {
                    ScalarValue::IntervalYearMonth(_)
                    | ScalarValue::IntervalDayTime(_)
                    | ScalarValue::IntervalMonthDayNano(_) => return true,
                    _ => (),
                }
            }

            false
        }
    }

    fn transform_interval_parts_to_interval(
        &self,
        datepart_var: &'static str,
        interval_int_var: &'static str,
        interval_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let datepart_var = var!(datepart_var);
        let interval_int_var = var!(interval_int_var);
        let interval_var = var!(interval_var);
        move |egraph, subst| {
            for interval_int in var_iter!(egraph[subst[interval_int_var]], LiteralExprValue) {
                let interval_int = match interval_int {
                    ScalarValue::Int32(Some(interval_int)) => *interval_int,
                    ScalarValue::Int64(Some(interval_int)) => match i32::try_from(*interval_int) {
                        Ok(interval_int) => interval_int,
                        _ => continue,
                    },
                    _ => continue,
                };

                for datepart in var_iter!(egraph[subst[datepart_var]], LiteralExprValue) {
                    let interval = match parse_granularity(datepart, false).as_deref() {
                        Some("millisecond") => {
                            ScalarValue::IntervalDayTime(Some(i64::from(interval_int)))
                        }
                        Some("second") => {
                            ScalarValue::IntervalDayTime(Some(1000 * i64::from(interval_int)))
                        }
                        Some("minute") => {
                            ScalarValue::IntervalDayTime(Some(60_000 * i64::from(interval_int)))
                        }
                        Some("hour") => {
                            ScalarValue::IntervalDayTime(Some(3_600_000 * i64::from(interval_int)))
                        }
                        Some("day") => ScalarValue::IntervalDayTime(Some(
                            4_294_967_296 * i64::from(interval_int),
                        )),
                        Some("week") => ScalarValue::IntervalDayTime(Some(
                            30_064_771_072 * i64::from(interval_int),
                        )),
                        Some("month") => ScalarValue::IntervalYearMonth(Some(interval_int)),
                        Some("quarter") => ScalarValue::IntervalYearMonth(Some(3 * interval_int)),
                        Some("year") => ScalarValue::IntervalYearMonth(Some(12 * interval_int)),
                        _ => continue,
                    };

                    subst.insert(
                        interval_var,
                        egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                            interval,
                        ))),
                    );

                    return true;
                }
            }

            false
        }
    }

    fn unwrap_cast_to_date(
        &self,
        data_type_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let data_type_var = var!(data_type_var);
        move |egraph, subst| {
            for node in egraph[subst[data_type_var]].nodes.iter() {
                match node {
                    LogicalPlanLanguage::CastExprDataType(expr) => match expr {
                        CastExprDataType(DataType::Date32) | CastExprDataType(DataType::Date64) => {
                            return true
                        }
                        _ => (),
                    },
                    _ => (),
                }
            }

            false
        }
    }
}
