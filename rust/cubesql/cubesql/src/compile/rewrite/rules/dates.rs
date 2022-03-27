use crate::compile::engine::provider::CubeContext;
use crate::compile::rewrite::analysis::LogicalPlanAnalysis;
use crate::compile::rewrite::rewriter::RewriteRules;
use crate::compile::rewrite::LogicalPlanLanguage;
use crate::compile::rewrite::{binary_expr, column_expr, literal_expr, rewrite};
use crate::compile::rewrite::{fun_expr, literal_string, to_day_interval_expr, udf_expr};
use egg::Rewrite;
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
        ]
    }
}

impl DateRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self {
            _cube_context: cube_context,
        }
    }
}
