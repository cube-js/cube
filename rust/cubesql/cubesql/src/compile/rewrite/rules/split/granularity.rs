use crate::{
    compile::rewrite::{
        alias_expr,
        analysis::ConstantFolding,
        binary_expr, cast_expr, cast_expr_explicit, column_expr, literal_expr, literal_float,
        literal_int, literal_string,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::split::SplitRules,
        udf_expr,
    },
    var,
};
use datafusion::{arrow::datatypes::DataType as ArrowDataType, scalar::ScalarValue};

impl SplitRules {
    pub fn granularity_rules(&self, rules: &mut Vec<CubeRewrite>) {
        // CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."LO_COMMITDATE") * 100) + 1) * 100) + 1) AS varchar) AS date)
        self.single_arg_split_point_rules(
            "thoughtspot-year",
            || {
                cast_expr_explicit(
                    cast_expr_explicit(
                        binary_expr(
                            binary_expr(
                                binary_expr(
                                    binary_expr(
                                        self.fun_expr(
                                            "DatePart",
                                            vec![literal_string("year"), column_expr("?column")],
                                        ),
                                        "*",
                                        literal_int(100),
                                    ),
                                    "+",
                                    literal_int(1),
                                ),
                                "*",
                                literal_int(100),
                            ),
                            "+",
                            literal_int(1),
                        ),
                        ArrowDataType::Utf8,
                    ),
                    ArrowDataType::Date32,
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_string("year"), column_expr("?column")],
                )
            },
            |alias_column| alias_column,
            |_, _, _| true,
            true,
            rules,
        );
        // DATE_TRUNC('month', CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."createdAt") * 100) + 1) * 100) + 1) AS CHARACTER VARYING) AS timestamp))
        self.single_arg_split_point_rules(
            "thoughtspot-extract-year",
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![
                        // Any granularity will do as long as our max is YEAR.
                        // If maximum raises to DECADE, ?granularity needs to be checked
                        literal_expr("?granularity"),
                        cast_expr(
                            cast_expr_explicit(
                                binary_expr(
                                    binary_expr(
                                        binary_expr(
                                            binary_expr(
                                                self.fun_expr(
                                                    "DatePart",
                                                    vec![
                                                        literal_string("year"),
                                                        "?column".to_string(),
                                                    ],
                                                ),
                                                "*",
                                                literal_int(100),
                                            ),
                                            "+",
                                            literal_int(1),
                                        ),
                                        "*",
                                        literal_int(100),
                                    ),
                                    "+",
                                    literal_int(1),
                                ),
                                ArrowDataType::Utf8,
                            ),
                            "?timestamp_type",
                        ),
                    ],
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_string("year"), "?column".to_string()],
                )
            },
            |alias_column| alias_column,
            |_, _, _| true,
            true,
            rules,
        );
        // FLOOR(((EXTRACT(DAY FROM DATEADD(day, CAST((4 - (((DATEDIFF(day, DATE '1970-01-01', "ta_1"."LO_COMMITDATE") + 3) % 7) + 1)) AS int), "ta_1"."LO_COMMITDATE")) + 6) / NULLIF(CAST(7 AS FLOAT8),0.0)))
        self.single_arg_split_point_rules(
            "thoughtspot-week-num-in-month",
            || {
                self.fun_expr(
                    "Floor",
                    vec![binary_expr(
                        binary_expr(
                            self.fun_expr(
                                "DatePart",
                                vec![
                                    literal_string("day"),
                                    udf_expr(
                                        "dateadd",
                                        vec![
                                            literal_string("day"),
                                            cast_expr_explicit(
                                                binary_expr(
                                                    literal_int(4),
                                                    "-",
                                                    binary_expr(
                                                        binary_expr(
                                                            binary_expr(
                                                                udf_expr(
                                                                    "datediff",
                                                                    vec![
                                                                        literal_string("day"),
                                                                        cast_expr_explicit(
                                                                            literal_string(
                                                                                "1970-01-01",
                                                                            ),
                                                                            ArrowDataType::Date32,
                                                                        ),
                                                                        column_expr("?column"),
                                                                    ],
                                                                ),
                                                                "+",
                                                                literal_int(3),
                                                            ),
                                                            "%",
                                                            literal_int(7),
                                                        ),
                                                        "+",
                                                        literal_int(1),
                                                    ),
                                                ),
                                                ArrowDataType::Int32,
                                            ),
                                            column_expr("?column"),
                                        ],
                                    ),
                                ],
                            ),
                            "+",
                            literal_int(6),
                        ),
                        "/",
                        self.fun_expr(
                            "NullIf",
                            vec![
                                cast_expr_explicit(literal_int(7), ArrowDataType::Float64),
                                literal_float(0.0),
                            ],
                        ),
                    )],
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_string("week"), column_expr("?column")],
                )
            },
            |alias_column| {
                self.fun_expr(
                    "Ceil",
                    vec![binary_expr(
                        self.fun_expr(
                            "DatePart",
                            vec![
                                literal_string("day"),
                                udf_expr(
                                    "dateadd",
                                    vec![literal_string("day"), literal_int(3), alias_column],
                                ),
                            ],
                        ),
                        "/",
                        literal_float(7.0),
                    )],
                )
            },
            |_, _, _| true,
            false,
            rules,
        );

        // (CAST("ta_1"."completedAt" AS date) - CAST((CAST(EXTRACT(YEAR FROM "ta_1"."completedAt") || '-' || EXTRACT(MONTH FROM "ta_1"."completedAt") || '-01' AS DATE) + (((MOD(CAST((EXTRACT(MONTH FROM "ta_1"."completedAt") - 1) AS numeric), 3) + 1) - 1) * -1) * INTERVAL '1 month') AS date) + 1)
        self.single_arg_split_point_rules(
            "thoughtspot-pg-extract-day-of-quarter",
            || {
                binary_expr(
                    binary_expr(
                        cast_expr_explicit(column_expr("?column"), ArrowDataType::Date32),
                        "-",
                        cast_expr_explicit(
                            binary_expr(
                                cast_expr_explicit(
                                    binary_expr(
                                        binary_expr(
                                            binary_expr(
                                                self.fun_expr(
                                                    "DatePart",
                                                    vec![
                                                        literal_string("year"),
                                                        column_expr("?column"),
                                                    ],
                                                ),
                                                "||",
                                                literal_string("-"),
                                            ),
                                            "||",
                                            self.fun_expr(
                                                "DatePart",
                                                vec![
                                                    literal_string("month"),
                                                    column_expr("?column"),
                                                ],
                                            ),
                                        ),
                                        "||",
                                        literal_string("-01"),
                                    ),
                                    ArrowDataType::Date32,
                                ),
                                "+",
                                binary_expr(
                                    binary_expr(
                                        binary_expr(
                                            binary_expr(
                                                alias_expr(
                                                    binary_expr(
                                                        cast_expr(
                                                            binary_expr(
                                                                self.fun_expr(
                                                                    "DatePart",
                                                                    vec![
                                                                        literal_string("month"),
                                                                        column_expr("?column"),
                                                                    ],
                                                                ),
                                                                "-",
                                                                literal_int(1),
                                                            ),
                                                            // TODO: explicitly test Decimal(38, 10)
                                                            "?numeric",
                                                        ),
                                                        "%",
                                                        literal_int(3),
                                                    ),
                                                    "?mod_alias",
                                                ),
                                                "+",
                                                literal_int(1),
                                            ),
                                            "-",
                                            literal_int(1),
                                        ),
                                        "*",
                                        literal_int(-1),
                                    ),
                                    "*",
                                    literal_expr("?interval"),
                                ),
                            ),
                            ArrowDataType::Date32,
                        ),
                    ),
                    "+",
                    literal_int(1),
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_string("day"), column_expr("?column")],
                )
            },
            |alias_column| {
                binary_expr(
                    binary_expr(
                        cast_expr_explicit(alias_column.clone(), ArrowDataType::Date32),
                        "-",
                        cast_expr_explicit(
                            self.fun_expr(
                                "DateTrunc",
                                vec![literal_string("quarter"), alias_column],
                            ),
                            ArrowDataType::Date32,
                        ),
                    ),
                    "+",
                    literal_int(1),
                )
            },
            |_, _, _| true,
            false,
            rules,
        );

        // CAST(EXTRACT(YEAR FROM "ta_1"."completedAt") || '-' || ((FLOOR(((EXTRACT(MONTH FROM "ta_1"."completedAt") - 1) / NULLIF(3,0))) * 3) + 1) || '-01' AS DATE) AS "ca_2"
        self.single_arg_split_point_rules(
            "thoughtspot-pg-date-trunc-quarter-inner-replacer",
            || {
                cast_expr_explicit(
                    binary_expr(
                        binary_expr(
                            binary_expr(
                                self.fun_expr(
                                    "DatePart",
                                    vec![literal_string("year"), column_expr("?column")],
                                ),
                                "||",
                                literal_string("-"),
                            ),
                            "||",
                            binary_expr(
                                binary_expr(
                                    self.fun_expr(
                                        "Floor",
                                        vec![binary_expr(
                                            binary_expr(
                                                self.fun_expr(
                                                    "DatePart",
                                                    vec![
                                                        literal_string("month"),
                                                        column_expr("?column"),
                                                    ],
                                                ),
                                                "-",
                                                literal_int(1),
                                            ),
                                            "/",
                                            self.fun_expr(
                                                "NullIf",
                                                vec![literal_int(3), literal_int(0)],
                                            ),
                                        )],
                                    ),
                                    "*",
                                    literal_int(3),
                                ),
                                "+",
                                literal_int(1),
                            ),
                        ),
                        "||",
                        literal_string("-01"),
                    ),
                    ArrowDataType::Date32,
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_string("quarter"), column_expr("?column")],
                )
            },
            |alias_column| cast_expr_explicit(alias_column, ArrowDataType::Date32),
            |_, _, _| true,
            true,
            rules,
        );
        // (DATEDIFF(day, DATEADD(month, CAST(((EXTRACT(MONTH FROM "ta_1"."createdAt") - 1) * -1) AS int), CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."createdAt") * 100) + EXTRACT(MONTH FROM "ta_1"."createdAt")) * 100) + 1) AS varchar) AS date)), "ta_1"."createdAt")
        self.single_arg_split_point_rules(
            "thoughtspot-pg-datediff-month",
            || {
                binary_expr(
                    udf_expr(
                        "datediff",
                        vec![
                            literal_string("day"),
                            udf_expr(
                                "dateadd",
                                vec![
                                    literal_string("month"),
                                    cast_expr_explicit(
                                        binary_expr(
                                            binary_expr(
                                                self.fun_expr(
                                                    "DatePart",
                                                    vec![
                                                        literal_string("month"),
                                                        column_expr("?column"),
                                                    ],
                                                ),
                                                "-",
                                                literal_int(1),
                                            ),
                                            "*",
                                            literal_int(-1),
                                        ),
                                        ArrowDataType::Int32,
                                    ),
                                    cast_expr_explicit(
                                        cast_expr_explicit(
                                            binary_expr(
                                                binary_expr(
                                                    binary_expr(
                                                        binary_expr(
                                                            self.fun_expr(
                                                                "DatePart",
                                                                vec![
                                                                    literal_string("year"),
                                                                    column_expr("?column"),
                                                                ],
                                                            ),
                                                            "*",
                                                            literal_int(100),
                                                        ),
                                                        "+",
                                                        self.fun_expr(
                                                            "DatePart",
                                                            vec![
                                                                literal_string("month"),
                                                                column_expr("?column"),
                                                            ],
                                                        ),
                                                    ),
                                                    "*",
                                                    literal_int(100),
                                                ),
                                                "+",
                                                literal_int(1),
                                            ),
                                            ArrowDataType::Utf8,
                                        ),
                                        ArrowDataType::Date32,
                                    ),
                                ],
                            ),
                            column_expr("?column"),
                        ],
                    ),
                    "+",
                    literal_int(1),
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_string("day"), column_expr("?column")],
                )
            },
            |alias_column| self.fun_expr("DatePart", vec![literal_string("doy"), alias_column]),
            |_, _, _| true,
            false,
            rules,
        );

        // (DATEDIFF(day, DATEADD(month, CAST((((((EXTRACT(MONTH FROM "ta_1"."LO_COMMITDATE") - 1) % 3) + 1) - 1) * -1) AS int), CAST(CAST(((((EXTRACT(YEAR FROM "ta_1"."LO_COMMITDATE") * 100) + EXTRACT(MONTH FROM "ta_1"."LO_COMMITDATE")) * 100) + 1) AS varchar) AS date)), "ta_1"."LO_COMMITDATE") + 1)
        self.single_arg_split_point_rules(
            "thoughtspot-day-in-quarter",
            || {
                binary_expr(
                    udf_expr(
                        "datediff",
                        vec![
                            literal_string("day"),
                            udf_expr(
                                "dateadd",
                                vec![
                                    literal_string("month"),
                                    cast_expr_explicit(
                                        binary_expr(
                                            binary_expr(
                                                binary_expr(
                                                    binary_expr(
                                                        binary_expr(
                                                            self.fun_expr(
                                                                "DatePart",
                                                                vec![
                                                                    literal_string("month"),
                                                                    column_expr("?column"),
                                                                ],
                                                            ),
                                                            "-",
                                                            literal_int(1),
                                                        ),
                                                        "%",
                                                        literal_int(3),
                                                    ),
                                                    "+",
                                                    literal_int(1),
                                                ),
                                                "-",
                                                literal_int(1),
                                            ),
                                            "*",
                                            literal_int(-1),
                                        ),
                                        ArrowDataType::Int32,
                                    ),
                                    cast_expr_explicit(
                                        cast_expr_explicit(
                                            binary_expr(
                                                binary_expr(
                                                    binary_expr(
                                                        binary_expr(
                                                            self.fun_expr(
                                                                "DatePart",
                                                                vec![
                                                                    literal_string("year"),
                                                                    column_expr("?column"),
                                                                ],
                                                            ),
                                                            "*",
                                                            literal_int(100),
                                                        ),
                                                        "+",
                                                        self.fun_expr(
                                                            "DatePart",
                                                            vec![
                                                                literal_string("month"),
                                                                column_expr("?column"),
                                                            ],
                                                        ),
                                                    ),
                                                    "*",
                                                    literal_int(100),
                                                ),
                                                "+",
                                                literal_int(1),
                                            ),
                                            ArrowDataType::Utf8,
                                        ),
                                        ArrowDataType::Date32,
                                    ),
                                ],
                            ),
                            column_expr("?column"),
                        ],
                    ),
                    "+",
                    literal_int(1),
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_string("day"), column_expr("?column")],
                )
            },
            |alias_column| {
                binary_expr(
                    udf_expr(
                        "datediff",
                        vec![
                            literal_string("day"),
                            self.fun_expr(
                                "DateTrunc",
                                vec![literal_string("quarter"), alias_column.clone()],
                            ),
                            alias_column,
                        ],
                    ),
                    "+",
                    literal_int(1),
                )
            },
            |_, _, _| true,
            false,
            rules,
        );
        // date_trunc('week', (order_date :: timestamptz + cast(1 || ' day' as interval))) + cast(-1 || ' day' as interval)
        self.single_arg_split_point_rules(
            "sunday-week",
            || {
                binary_expr(
                    self.fun_expr(
                        "DateTrunc",
                        vec![
                            literal_string("week"),
                            binary_expr(
                                cast_expr(column_expr("?column"), "?data_type"),
                                "+",
                                "?day_interval",
                            ),
                        ],
                    ),
                    "+",
                    "?neg_day_interval",
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_string("day"), column_expr("?column")],
                )
            },
            |alias_column| {
                udf_expr(
                    "date_add",
                    vec![
                        self.fun_expr(
                            "DateTrunc",
                            vec![
                                literal_string("week"),
                                udf_expr(
                                    "date_add",
                                    vec![alias_column, "?day_interval".to_string()],
                                ),
                            ],
                        ),
                        "?neg_day_interval".to_string(),
                    ],
                )
            },
            self.transform_sunday_week("?day_interval", "?neg_day_interval"),
            false,
            rules,
        );
        // DATE_TRUNC('month', DATE_TRUNC('month', "ta_1"."order_date"))
        self.single_arg_split_point_rules(
            "date-trunc-within-date-trunc-same-granularity",
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![
                        literal_expr("?inner_granularity"),
                        self.fun_expr(
                            "DateTrunc",
                            vec![literal_expr("?inner_granularity"), column_expr("?column")],
                        ),
                    ],
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?inner_granularity"), column_expr("?column")],
                )
            },
            |alias_column| alias_column,
            |_, _, _| true,
            true,
            rules,
        );
    }

    fn transform_sunday_week(
        &self,
        day_interval_var: &str,
        neg_day_interval_var: &str,
    ) -> impl Fn(bool, &mut CubeEGraph, &mut egg::Subst) -> bool + Sync + Send + Clone + 'static
    {
        let day_interval = var!(day_interval_var);
        let neg_day_interval = var!(neg_day_interval_var);
        move |_, egraph, subst| {
            if let Some(ConstantFolding::Scalar(value)) =
                &egraph[subst[day_interval]].data.constant.clone()
            {
                if let Some(ConstantFolding::Scalar(neg_value)) =
                    &egraph[subst[neg_day_interval]].data.constant.clone()
                {
                    match (value, neg_value) {
                        (
                            ScalarValue::IntervalMonthDayNano(Some(value)),
                            ScalarValue::IntervalMonthDayNano(Some(neg_value)),
                        ) => {
                            if value & 0xFFFFFFFFFFFFFFFF == 0
                                && neg_value & 0xFFFFFFFFFFFFFFFF == 0
                            {
                                return true;
                            }
                        }
                        _ => {}
                    }
                }
            }
            false
        }
    }
}
