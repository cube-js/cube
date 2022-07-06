use crate::compile::{
    engine::provider::CubeContext,
    rewrite::{
        analysis::LogicalPlanAnalysis, binary_expr, cast_expr, column_expr, fun_expr, literal_expr,
        literal_string, negative_expr, rewrite, rewriter::RewriteRules, to_day_interval_expr,
        udf_expr, LogicalPlanLanguage,
    },
};
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
            rewrite(
                "metabase-cast-date-tranc-with-two-binary-exprs-to-date-trunc-with-two-date-add",
                binary_expr(
                    cast_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![
                                "?granularity".to_string(),
                                binary_expr(
                                    binary_expr(
                                        fun_expr("Now", Vec::<String>::new()),
                                        "+",
                                        literal_expr("?innermost_interval"),
                                    ),
                                    "+",
                                    literal_expr("?inner_interval"),
                                ),
                            ],
                        ),
                        "?data_type",
                    ),
                    "+",
                    literal_expr("?outer_interval"),
                ),
                udf_expr(
                    "date_add",
                    vec![
                        fun_expr(
                            "DateTrunc",
                            vec![
                                "?granularity".to_string(),
                                udf_expr(
                                    "date_add",
                                    vec![
                                        udf_expr(
                                            "date_add",
                                            vec![
                                                fun_expr("Now", Vec::<String>::new()),
                                                literal_expr("?innermost_interval"),
                                            ],
                                        ),
                                        literal_expr("?inner_interval"),
                                    ],
                                ),
                            ],
                        ),
                        literal_expr("?outer_interval"),
                    ],
                ),
            ),
            rewrite(
                "metabase-cast-date-tranc-with-binary-expr-to-date-trunc-with-date-add",
                binary_expr(
                    cast_expr(
                        fun_expr(
                            "DateTrunc",
                            vec![
                                "?granularity".to_string(),
                                binary_expr(
                                    fun_expr("Now", Vec::<String>::new()),
                                    "+",
                                    literal_expr("?inner_interval"),
                                ),
                            ],
                        ),
                        "?data_type",
                    ),
                    "+",
                    literal_expr("?outer_interval"),
                ),
                udf_expr(
                    "date_add",
                    vec![
                        fun_expr(
                            "DateTrunc",
                            vec![
                                "?granularity".to_string(),
                                udf_expr(
                                    "date_add",
                                    vec![
                                        fun_expr("Now", Vec::<String>::new()),
                                        literal_expr("?inner_interval"),
                                    ],
                                ),
                            ],
                        ),
                        literal_expr("?outer_interval"),
                    ],
                ),
            ),
            rewrite(
                "metabase-now-plus-interval-in-date-trunc-to-date-add-in-date-trunc",
                fun_expr(
                    "DateTrunc",
                    vec![
                        "?granularity".to_string(),
                        binary_expr(
                            fun_expr("Now", Vec::<String>::new()),
                            "+",
                            literal_expr("?interval"),
                        ),
                    ],
                ),
                fun_expr(
                    "DateTrunc",
                    vec![
                        "?granularity".to_string(),
                        udf_expr(
                            "date_add",
                            vec![
                                fun_expr("Now", Vec::<String>::new()),
                                literal_expr("?interval"),
                            ],
                        ),
                    ],
                ),
            ),
            rewrite(
                "metabase-cast-now-plus-interval-to-date-add",
                cast_expr(
                    binary_expr(
                        fun_expr("Now", Vec::<String>::new()),
                        "+",
                        literal_expr("?interval"),
                    ),
                    "?data_type",
                ),
                udf_expr(
                    "date_add",
                    vec![
                        fun_expr("Now", Vec::<String>::new()),
                        literal_expr("?interval"),
                    ],
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
