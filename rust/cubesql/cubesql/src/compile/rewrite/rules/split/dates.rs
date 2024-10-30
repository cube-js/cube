use crate::{
    compile::rewrite::{
        analysis::{ConstantFolding, LogicalPlanAnalysis},
        cast_expr, literal_expr,
        rules::{
            members::min_granularity,
            split::SplitRules,
            utils::{DatePartToken, SpecialTimeUnitToken},
        },
        LiteralExprValue, LogicalPlanLanguage,
    },
    var,
};
use datafusion::scalar::ScalarValue;
use egg::Rewrite;

impl SplitRules {
    pub fn date_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
        // TODO check for time dimension before push down to optimize performance
        // TODO use pass-through instead point rules for epoch
        self.single_arg_split_point_rules(
            "date-part-epoch",
            || self.fun_expr("DatePart", vec!["?date_part", "?expr"]),
            || "?expr".to_string(),
            |alias_column| {
                self.fun_expr(
                    "DatePart",
                    vec![literal_expr("?new_date_part"), alias_column],
                )
            },
            self.transform_date_part_epoch("?date_part", "?new_date_part"),
            // epoch return essentially same timestamp, but as a floating point, so projections should be fine
            // TODO recheck how projections behave on timestamp with time zone
            true,
            rules,
        );
        self.single_arg_split_point_rules(
            "date-part",
            || {
                self.fun_expr(
                    "DatePart",
                    vec!["?date_part".to_string(), "?expr".to_string()],
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?new_trunc_granularity"), "?expr".to_string()],
                )
            },
            |alias_column| {
                self.fun_expr(
                    "DatePart",
                    vec![literal_expr("?new_date_part"), alias_column],
                )
            },
            self.transform_date_part("?date_part", "?new_date_part", "?new_trunc_granularity"),
            false,
            rules,
        );
        self.single_arg_split_point_rules(
            "date-trunc-within-date-part",
            || {
                self.fun_expr(
                    "DatePart",
                    vec![
                        "?outer_granularity".to_string(),
                        self.fun_expr(
                            "DateTrunc",
                            vec!["?inner_granularity".to_string(), "?expr".to_string()],
                        ),
                    ],
                )
            },
            || self.fun_expr("DateTrunc", vec!["?new_inner_granularity", "?expr"]),
            |alias_column| {
                self.fun_expr(
                    "DatePart",
                    vec!["?new_outer_granularity".to_string(), alias_column],
                )
            },
            self.transform_date_part_within_date_trunc(
                "?outer_granularity",
                "?inner_granularity",
                "?new_outer_granularity",
                "?new_inner_granularity",
            ),
            false,
            rules,
        );
        self.single_arg_split_point_rules(
            "date-trunc-within-date-part-with-cast",
            || {
                self.fun_expr(
                    "DatePart",
                    vec![
                        "?outer_granularity".to_string(),
                        cast_expr(
                            self.fun_expr(
                                "DateTrunc",
                                vec!["?inner_granularity".to_string(), "?expr".to_string()],
                            ),
                            "?cast_type",
                        ),
                    ],
                )
            },
            || self.fun_expr("DateTrunc", vec!["?new_inner_granularity", "?expr"]),
            |alias_column| {
                self.fun_expr(
                    "DatePart",
                    vec![
                        "?new_outer_granularity".to_string(),
                        cast_expr(alias_column, "?cast_type"),
                    ],
                )
            },
            self.transform_date_part_within_date_trunc(
                "?outer_granularity",
                "?inner_granularity",
                "?new_outer_granularity",
                "?new_inner_granularity",
            ),
            false,
            rules,
        );
        self.single_arg_split_point_rules(
            "date-trunc",
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), "?expr".to_string()],
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), "?expr".to_string()],
                )
            },
            |alias_column| alias_column,
            |_, _, _| true,
            false,
            rules,
        );
    }

    fn transform_date_part_epoch(
        &self,
        date_part_var: &str,
        new_date_part_var: &str,
    ) -> impl Fn(
        bool,
        &mut egg::EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        &mut egg::Subst,
    ) -> bool
           + Sync
           + Send
           + Clone {
        let date_part_var = var!(date_part_var);
        let new_date_part_var = var!(new_date_part_var);
        move |_, egraph, subst| {
            // We are going to split DatePart(epoch, ...), leaving original expression inside

            if let Some(ConstantFolding::Scalar(ScalarValue::Utf8(Some(date_part)))) =
                &egraph[subst[date_part_var]].data.constant
            {
                let Ok(date_part) = date_part.parse::<DatePartToken>() else {
                    return false;
                };
                if !matches!(
                    date_part,
                    // Julian dates are very similar to epoch, so we handle it here as well
                    DatePartToken::Special(SpecialTimeUnitToken::Epoch)
                        | DatePartToken::Special(SpecialTimeUnitToken::Julian)
                ) {
                    return false;
                }

                let new_date_part = egraph.add(LogicalPlanLanguage::LiteralExprValue(
                    LiteralExprValue(ScalarValue::Utf8(Some(date_part.as_str().to_string()))),
                ));
                subst.insert(new_date_part_var, new_date_part);

                return true;
            }
            false
        }
    }

    fn transform_date_part(
        &self,
        date_part_var: &str,
        new_date_part_var: &str,
        new_trunc_granularity_var: &str,
    ) -> impl Fn(
        bool,
        &mut egg::EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        &mut egg::Subst,
    ) -> bool
           + Sync
           + Send
           + Clone {
        let date_part_var = var!(date_part_var);
        let new_date_part_var = var!(new_date_part_var);
        let new_trunc_granularity_var = var!(new_trunc_granularity_var);
        move |_, egraph, subst| {
            // We are going to split single DatePart to DatePart(DateTrunc)
            // Parse token as DatePartToken and use DatePartToken::delta_for_trunc to determine appropriate argument for DateTrunc
            // Also there are several different date parts with same granularity, but different semantic
            // date_part(dow) and date_part(doy) should both use date_trunc(day), but they return different ranges of values
            // So we keep original part for date_part after split

            if let Some(ConstantFolding::Scalar(ScalarValue::Utf8(Some(date_part)))) =
                &egraph[subst[date_part_var]].data.constant
            {
                let Ok(date_part) = date_part.parse::<DatePartToken>() else {
                    return false;
                };
                let Some(trunc_granularity) = date_part.delta_for_trunc() else {
                    return false;
                };

                let new_date_part = egraph.add(LogicalPlanLanguage::LiteralExprValue(
                    LiteralExprValue(ScalarValue::Utf8(Some(date_part.as_str().to_string()))),
                ));
                let trunc_granularity =
                    egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                        ScalarValue::Utf8(Some(trunc_granularity.as_str().to_string())),
                    )));
                subst.insert(new_date_part_var, new_date_part);
                subst.insert(new_trunc_granularity_var, trunc_granularity);
                return true;
            }
            false
        }
    }

    fn transform_date_part_within_date_trunc(
        &self,
        outer_var: &str,
        inner_var: &str,
        new_outer_var: &str,
        new_inner_var: &str,
    ) -> impl Fn(
        bool,
        &mut egg::EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        &mut egg::Subst,
    ) -> bool
           + Sync
           + Send
           + Clone {
        let outer_var = var!(outer_var);
        let inner_var = var!(inner_var);
        let new_outer_var = var!(new_outer_var);
        let new_inner_var = var!(new_inner_var);
        move |_, egraph, subst| {
            if let Some(ConstantFolding::Scalar(ScalarValue::Utf8(Some(outer_granularity)))) =
                &egraph[subst[outer_var]].data.constant
            {
                if let Some(ConstantFolding::Scalar(ScalarValue::Utf8(Some(inner_granularity)))) =
                    &egraph[subst[inner_var]].data.constant
                {
                    let Some(min_granularity) =
                        min_granularity(&outer_granularity, &inner_granularity)
                    else {
                        subst.insert(new_outer_var, subst[outer_var]);
                        subst.insert(new_inner_var, subst[inner_var]);
                        return true;
                    };

                    let date_trunc_granularity =
                        if min_granularity.to_lowercase() == inner_granularity.to_lowercase() {
                            outer_granularity
                        } else {
                            inner_granularity
                        };

                    let granularity_value =
                        egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                            ScalarValue::Utf8(Some(date_trunc_granularity.to_string())),
                        )));
                    let granularity =
                        egraph.add(LogicalPlanLanguage::LiteralExpr([granularity_value]));
                    subst.insert(new_outer_var, granularity);
                    subst.insert(new_inner_var, granularity);
                    return true;
                }
            }
            false
        }
    }
}
