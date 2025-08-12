use super::utils;
use crate::{
    compile::rewrite::{
        alias_expr,
        analysis::{ConstantFolding, OriginalExpr},
        binary_expr, cast_expr, cast_expr_explicit, column_expr, fun_expr, literal_expr,
        literal_int, literal_string, negative_expr, original_expr_name, rewrite,
        rewriter::{CubeEGraph, CubeRewrite, RewriteRules},
        rules::utils::DeltaTimeUnitToken,
        to_day_interval_expr, transform_original_expr_to_alias, transforming_rewrite,
        transforming_rewrite_with_root, udf_expr, AliasExprAlias, CastExprDataType,
        LiteralExprValue, LogicalPlanLanguage,
    },
    config::ConfigObj,
    var, var_iter,
};
use datafusion::{
    arrow::datatypes::{DataType, TimeUnit},
    logical_plan::DFSchema,
    scalar::ScalarValue,
};
use egg::{Id, Subst};
use std::{convert::TryFrom, fmt::Display, sync::Arc};

pub struct DateRules {
    config_obj: Arc<dyn ConfigObj>,
}

impl RewriteRules for DateRules {
    fn rewrite_rules(&self) -> Vec<CubeRewrite> {
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
                        self.fun_expr(
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
                self.fun_expr(
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
                            self.to_day_interval_expr(
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
                self.fun_expr(
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
                            self.to_day_interval_expr(
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
                self.fun_expr(
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
                            self.to_day_interval_expr(
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
                self.fun_expr(
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
                        self.to_day_interval_expr(
                            udf_expr("hour", vec![column_expr("?column")]),
                            literal_string("hour"),
                        ),
                    ],
                ),
                self.fun_expr(
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
                        self.to_day_interval_expr(
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
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_string("minute"), column_expr("?column")],
                ),
            ),
            rewrite(
                "date-to-date-trunc",
                udf_expr("date", vec![column_expr("?column")]),
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_string("day"), column_expr("?column")],
                ),
            ),
            // TODO
            transforming_rewrite_with_root(
                "cast-in-date-trunc",
                self.fun_expr(
                    "DateTrunc",
                    vec![
                        literal_expr("?granularity"),
                        cast_expr(column_expr("?column"), "?data_type"),
                    ],
                ),
                alias_expr(
                    self.fun_expr(
                        "DateTrunc",
                        vec![literal_expr("?granularity"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                self.unwrap_cast_to_timestamp("?data_type", "?granularity", "?alias"),
            ),
            transforming_rewrite_with_root(
                "cast-in-date-trunc-double",
                self.fun_expr(
                    "DateTrunc",
                    vec![
                        literal_expr("?granularity"),
                        cast_expr(
                            self.fun_expr(
                                "DateTrunc",
                                vec![
                                    literal_expr("?granularity"),
                                    cast_expr("?expr", "?data_type"),
                                ],
                            ),
                            "?data_type",
                        ),
                    ],
                ),
                alias_expr(
                    self.fun_expr(
                        "DateTrunc",
                        vec![
                            literal_expr("?granularity"),
                            self.fun_expr(
                                "DateTrunc",
                                vec![literal_expr("?granularity"), "?expr".to_string()],
                            ),
                        ],
                    ),
                    "?alias",
                ),
                self.unwrap_cast_to_timestamp("?data_type", "?granularity", "?alias"),
            ),
            transforming_rewrite_with_root(
                "current-timestamp-to-now",
                udf_expr("current_timestamp", Vec::<String>::new()),
                alias_expr(
                    self.fun_expr("UtcTimestamp", Vec::<String>::new()),
                    "?alias",
                ),
                transform_original_expr_to_alias("?alias"),
            ),
            transforming_rewrite_with_root(
                "localtimestamp-to-now",
                udf_expr("localtimestamp", Vec::<String>::new()),
                alias_expr(
                    self.fun_expr("UtcTimestamp", Vec::<String>::new()),
                    "?alias",
                ),
                transform_original_expr_to_alias("?alias"),
            ),
            transforming_rewrite_with_root(
                "tableau-week",
                binary_expr(
                    alias_expr(
                        self.fun_expr(
                            "DateTrunc",
                            vec!["?granularity".to_string(), column_expr("?column")],
                        ),
                        "?date_trunc_alias",
                    ),
                    "+",
                    binary_expr(
                        negative_expr(self.fun_expr(
                            "DatePart",
                            vec![literal_string("dow"), column_expr("?column")],
                        )),
                        "*",
                        // TODO match
                        literal_expr("?interval_one_day"),
                    ),
                ),
                alias_expr(
                    self.fun_expr(
                        "DateTrunc",
                        vec![literal_string("week"), column_expr("?column")],
                    ),
                    "?alias",
                ),
                self.transform_root_alias("?alias"),
            ),
            rewrite(
                "metabase-interval-date-range",
                binary_expr(
                    cast_expr(self.fun_expr("Now", Vec::<String>::new()), "?data_type"),
                    "+",
                    literal_expr("?interval"),
                ),
                udf_expr(
                    "date_add",
                    vec![
                        self.fun_expr("Now", Vec::<String>::new()),
                        literal_expr("?interval"),
                    ],
                ),
            ),
            transforming_rewrite_with_root(
                "redshift-dateadd-to-interval-cast-unwrap",
                udf_expr(
                    "dateadd",
                    vec![
                        literal_expr("?datepart"),
                        cast_expr_explicit(literal_expr("?interval_int"), DataType::Int32),
                        "?expr".to_string(),
                    ],
                ),
                alias_expr(
                    udf_expr(
                        "date_add",
                        vec![
                            literal_expr("?datepart"),
                            literal_expr("?interval_int"),
                            "?expr".to_string(),
                        ],
                    ),
                    "?alias",
                ),
                transform_original_expr_to_alias("?alias"),
            ),
            transforming_rewrite_with_root(
                "redshift-dateadd-to-interval",
                udf_expr(
                    "dateadd",
                    vec![
                        literal_expr("?datepart"),
                        "?interval_int".to_string(),
                        "?expr".to_string(),
                    ],
                ),
                alias_expr(
                    udf_expr(
                        "date_add",
                        vec!["?expr".to_string(), literal_expr("?interval")],
                    ),
                    "?alias",
                ),
                self.transform_interval_parts_to_interval(
                    "?datepart",
                    "?interval_int",
                    "?interval",
                    "?alias",
                ),
            ),
            transforming_rewrite_with_root(
                "redshift-dateadd-literal-date32-to-interval",
                udf_expr(
                    "dateadd",
                    vec![
                        literal_expr("?datepart"),
                        literal_expr("?interval_int"),
                        cast_expr_explicit(literal_expr("?date_string"), DataType::Date32),
                    ],
                ),
                alias_expr(
                    udf_expr(
                        "date_add",
                        vec![
                            udf_expr("date_to_timestamp", vec![literal_expr("?date_string")]),
                            literal_expr("?interval"),
                        ],
                    ),
                    "?alias",
                ),
                self.transform_interval_parts_to_interval(
                    "?datepart",
                    "?interval_int",
                    "?interval",
                    "?alias",
                ),
            ),
            // TODO: TO_DATE should return Date32, but Timestamp works for all supported cases
            transforming_rewrite(
                "thoughtspot-to-date-to-timestamp",
                udf_expr(
                    "to_date",
                    vec![literal_expr("?date"), literal_expr("?format")],
                ),
                udf_expr("date_to_timestamp", vec![literal_expr("?date")]),
                self.transform_to_date_to_timestamp("?format"),
            ),
            // TODO turn this rule into generic DateTrunc merge
            transforming_rewrite(
                "datastudio-dates",
                self.fun_expr(
                    "DateTrunc",
                    vec![
                        "?outer_granularity".to_string(),
                        self.fun_expr(
                            "DateTrunc",
                            vec!["?inner_granularity".to_string(), column_expr("?column")],
                        ),
                    ],
                ),
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?new_granularity"), column_expr("?column")],
                ),
                self.transform_datastudio_date_trunc_merge(
                    "?outer_granularity",
                    "?inner_granularity",
                    "?new_granularity",
                ),
            ),
            // AGE function seems to be a popular choice for this date arithmetic,
            // but it is not supported in SQL push down by most dialects.
            transforming_rewrite_with_root(
                "thoughtspot-date-part-over-age-as-datediff-month",
                binary_expr(
                    binary_expr(
                        self.fun_expr(
                            "DatePart",
                            vec![
                                literal_string("year"),
                                udf_expr("age", vec!["?newer_date", "?older_date"]),
                            ],
                        ),
                        "*",
                        literal_int(12),
                    ),
                    "+",
                    self.fun_expr(
                        "DatePart",
                        vec![
                            literal_string("month"),
                            udf_expr("age", vec!["?newer_date", "?older_date"]),
                        ],
                    ),
                ),
                alias_expr(
                    udf_expr(
                        "datediff",
                        vec![
                            literal_string("month"),
                            "?older_date".to_string(),
                            "?newer_date".to_string(),
                        ],
                    ),
                    "?alias",
                ),
                self.transform_root_alias("?alias"),
            ),
        ]
    }
}

impl DateRules {
    pub fn new(config_obj: Arc<dyn ConfigObj>) -> Self {
        Self { config_obj }
    }

    fn fun_expr(&self, fun_name: impl Display, args: Vec<impl Display>) -> String {
        fun_expr(fun_name, args, self.config_obj.push_down_pull_up_split())
    }

    fn to_day_interval_expr<D: Display>(&self, period: D, unit: D) -> String {
        to_day_interval_expr(period, unit, self.config_obj.push_down_pull_up_split())
    }

    fn transform_interval_parts_to_interval(
        &self,
        datepart_var: &'static str,
        interval_int_var: &'static str,
        interval_var: &'static str,
        alias_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, Id, &mut Subst) -> bool {
        let datepart_var = var!(datepart_var);
        let interval_int_var = var!(interval_int_var);
        let interval_var = var!(interval_var);
        let alias_var = var!(alias_var);
        move |egraph, root, subst| {
            if let Some(ConstantFolding::Scalar(interval_int)) =
                egraph[subst[interval_int_var]].data.constant.clone()
            {
                let interval_int = match interval_int {
                    ScalarValue::Int32(Some(interval_int)) => interval_int,
                    ScalarValue::Int64(Some(interval_int)) => match i32::try_from(interval_int) {
                        Ok(interval_int) => interval_int,
                        _ => return false,
                    },
                    _ => return false,
                };

                for datepart in var_iter!(egraph[subst[datepart_var]], LiteralExprValue).cloned() {
                    let interval = match utils::parse_granularity(&datepart, false).as_deref() {
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

                    if let Some(original_expr) = original_expr_name(egraph, root) {
                        let alias = egraph.add(LogicalPlanLanguage::AliasExprAlias(
                            AliasExprAlias(original_expr),
                        ));
                        subst.insert(alias_var, alias);

                        subst.insert(
                            interval_var,
                            egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                                interval,
                            ))),
                        );
                        return true;
                    }
                }
            }

            false
        }
    }

    fn transform_to_date_to_timestamp(
        &self,
        format_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let format_var = var!(format_var);
        move |egraph, subst| {
            for format in var_iter!(egraph[subst[format_var]], LiteralExprValue) {
                match format {
                    ScalarValue::Utf8(Some(format)) => match format.as_str() {
                        "YYYY-MM-DD" | "yyyy-MM-dd" => return true,
                        _ => (),
                    },
                    _ => (),
                }
            }
            false
        }
    }

    // TODO turn this transform into generic DateTrunc merge
    fn transform_datastudio_date_trunc_merge(
        &self,
        outer_granularity_var: &'static str,
        inner_granularity_var: &'static str,
        new_granularity_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let outer_granularity_var = var!(outer_granularity_var);
        let inner_granularity_var = var!(inner_granularity_var);
        let new_granularity_var = var!(new_granularity_var);
        move |egraph, subst| match (
            &egraph[subst[outer_granularity_var]].data.constant,
            &egraph[subst[inner_granularity_var]].data.constant,
        ) {
            (
                Some(ConstantFolding::Scalar(ScalarValue::Utf8(Some(outer_granularity)))),
                Some(ConstantFolding::Scalar(ScalarValue::Utf8(Some(inner_granularity)))),
            ) => {
                let Ok(outer_granularity) = outer_granularity.parse::<DeltaTimeUnitToken>() else {
                    return false;
                };
                let Ok(inner_granularity) = inner_granularity.parse::<DeltaTimeUnitToken>() else {
                    return false;
                };

                use DeltaTimeUnitToken::*;

                if !matches!(inner_granularity, Second) {
                    return false;
                }

                let new_granularity = match outer_granularity {
                    // Outer granularity is finer that inner seconds
                    Microseconds | Milliseconds => Second,

                    // Outer granularity is coarser, but aligned with inner seconds
                    Second | Minute | Hour | Day | Week | Month | Quarter | Year | Decade
                    | Century | Millennium => outer_granularity,

                    // Invalid token for date_trunc
                    Timezone | TimezoneHour | TimezoneMinute => return false,
                };

                let new_granularity =
                    egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                        ScalarValue::Utf8(Some(new_granularity.as_str().to_string())),
                    )));
                subst.insert(new_granularity_var, new_granularity);
                return true;
            }
            _ => false,
        }
    }

    fn unwrap_cast_to_timestamp(
        &self,
        data_type_var: &'static str,
        granularity_var: &'static str,
        alias_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, Id, &mut Subst) -> bool {
        let data_type_var = var!(data_type_var);
        let granularity_var = var!(granularity_var);
        let alias_var = var!(alias_var);
        move |egraph, root, subst| {
            for data_type in var_iter!(egraph[subst[data_type_var]], CastExprDataType) {
                if let Some(OriginalExpr::Expr(original_expr)) =
                    egraph[root].data.original_expr.as_ref()
                {
                    let alias = original_expr.name(&DFSchema::empty()).unwrap();
                    match data_type {
                        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                            subst.insert(
                                alias_var,
                                egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(
                                    alias.to_string(),
                                ))),
                            );
                            return true;
                        }
                        DataType::Date32 => {
                            for granularity in
                                var_iter!(egraph[subst[granularity_var]], LiteralExprValue)
                            {
                                if let ScalarValue::Utf8(Some(granularity)) = granularity {
                                    if let (Some(original_granularity), Some(day_granularity)) = (
                                        utils::granularity_str_to_int_order(
                                            &granularity,
                                            Some(false),
                                        ),
                                        utils::granularity_str_to_int_order("day", Some(false)),
                                    ) {
                                        if original_granularity >= day_granularity {
                                            subst.insert(
                                                alias_var,
                                                egraph.add(LogicalPlanLanguage::AliasExprAlias(
                                                    AliasExprAlias(alias.to_string()),
                                                )),
                                            );
                                            return true;
                                        }
                                    }
                                }
                            }
                        }
                        _ => (),
                    }
                }
            }
            false
        }
    }

    pub fn transform_root_alias(
        &self,
        alias_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, Id, &mut Subst) -> bool {
        let alias_var = var!(alias_var);
        move |egraph, root, subst| {
            if let Some(OriginalExpr::Expr(original_expr)) =
                egraph[root].data.original_expr.as_ref()
            {
                let alias = original_expr.name(&DFSchema::empty()).unwrap();
                subst.insert(
                    alias_var,
                    egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(
                        alias.to_string(),
                    ))),
                );
                return true;
            }
            false
        }
    }
}
