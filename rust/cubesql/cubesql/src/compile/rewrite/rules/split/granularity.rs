use crate::compile::rewrite::{
    alias_expr, analysis::LogicalPlanAnalysis, binary_expr, cast_expr, cast_expr_explicit,
    column_expr, fun_expr, literal_expr, literal_float, literal_int, literal_string,
    rules::split::SplitRules, udf_expr, LogicalPlanLanguage,
};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use egg::Rewrite;

impl SplitRules {
    pub fn granularity_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
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
                                        fun_expr(
                                            "DatePart",
                                            vec![literal_string("YEAR"), column_expr("?column")],
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
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("year"), column_expr("?column")],
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
                fun_expr(
                    "Floor",
                    vec![binary_expr(
                        binary_expr(
                            fun_expr(
                                "DatePart",
                                vec![
                                    literal_string("DAY"),
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
                        fun_expr(
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
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("week"), column_expr("?column")],
                )
            },
            |alias_column| {
                fun_expr(
                    "Ceil",
                    vec![binary_expr(
                        fun_expr(
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
                                                fun_expr(
                                                    "DatePart",
                                                    vec![
                                                        literal_string("YEAR"),
                                                        column_expr("?column"),
                                                    ],
                                                ),
                                                "||",
                                                literal_string("-"),
                                            ),
                                            "||",
                                            fun_expr(
                                                "DatePart",
                                                vec![
                                                    literal_string("MONTH"),
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
                                                                fun_expr(
                                                                    "DatePart",
                                                                    vec![
                                                                        literal_string("MONTH"),
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
                fun_expr(
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
                            fun_expr("DateTrunc", vec![literal_string("quarter"), alias_column]),
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
                                fun_expr(
                                    "DatePart",
                                    vec![literal_string("YEAR"), column_expr("?column")],
                                ),
                                "||",
                                literal_string("-"),
                            ),
                            "||",
                            binary_expr(
                                binary_expr(
                                    fun_expr(
                                        "Floor",
                                        vec![binary_expr(
                                            binary_expr(
                                                fun_expr(
                                                    "DatePart",
                                                    vec![
                                                        literal_string("MONTH"),
                                                        column_expr("?column"),
                                                    ],
                                                ),
                                                "-",
                                                literal_int(1),
                                            ),
                                            "/",
                                            fun_expr(
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
                fun_expr(
                    "DateTrunc",
                    vec![literal_string("quarter"), column_expr("?column")],
                )
            },
            |alias_column| cast_expr_explicit(alias_column, ArrowDataType::Date32),
            |_, _, _| true,
            true,
            rules,
        );
    }
}
