use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            agg_fun_expr, analysis::LogicalPlanAnalysis, binary_expr, cast_expr, column_expr,
            fun_expr, literal_expr, literal_string, negative_expr, rewrite, rewriter::RewriteRules,
            to_day_interval_expr, transforming_rewrite, udf_expr, CastExprDataType,
            LiteralExprValue, LogicalPlanLanguage,
        },
    },
    var, var_iter,
};
use datafusion::{arrow::datatypes::DataType, scalar::ScalarValue};
use egg::{EGraph, Rewrite, Subst};
use std::sync::Arc;

pub struct DateRules {
    _cube_context: Arc<CubeContext>,
}

impl RewriteRules for DateRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        vec![
            // TODO ?interval ?one
            rewrite(
                "superset-quarter-to-date-trunc",
                binary_expr(
                    binary_expr(
                        udf_expr(
                            "makedate",
                            vec![
                                udf_expr("year", vec![column_expr("?column")]),
                                literal_expr("?one"),
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
            // TODO ?one ?interval
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
                                    literal_expr("?one"),
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
            // TODO ?one ?interval
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
                                    literal_expr("?one"),
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
            // TODO ?one ?interval
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
                                    literal_expr("?one"),
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
            // TODO ?one ?interval
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
            // TODO ?sixty
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
                                    "?sixty",
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
            // TODO ?sixty
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
                                        "?sixty",
                                    ),
                                    "*",
                                    "?sixty",
                                ),
                                "+",
                                binary_expr(
                                    binary_expr(
                                        udf_expr("minute", vec![column_expr("?column")]),
                                        "*",
                                        "?sixty",
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
                "binary-expr-interval-right",
                binary_expr("?left", "+", literal_expr("?interval")),
                udf_expr(
                    "date_add",
                    vec!["?left".to_string(), literal_expr("?interval")],
                ),
                self.transform_interval_binary_expr("?interval"),
            ),
            transforming_rewrite(
                "binary-expr-interval-left",
                binary_expr(literal_expr("?interval"), "+", "?right"),
                udf_expr(
                    "date_add",
                    vec!["?right".to_string(), literal_expr("?interval")],
                ),
                self.transform_interval_binary_expr("?interval"),
            ),
            transforming_rewrite(
                "interval-binary-expr-minus",
                binary_expr("?left", "-", literal_expr("?interval")),
                udf_expr(
                    "date_sub",
                    vec!["?left".to_string(), literal_expr("?interval")],
                ),
                self.transform_interval_binary_expr("?interval"),
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
