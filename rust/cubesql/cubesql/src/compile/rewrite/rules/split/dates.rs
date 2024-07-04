use crate::{
    compile::rewrite::{
        analysis::{ConstantFolding, LogicalPlanAnalysis},
        column_expr, literal_expr,
        rules::{members::min_granularity, split::SplitRules, utils::parse_granularity_string},
        LiteralExprValue, LogicalPlanLanguage,
    },
    var,
};
use datafusion::scalar::ScalarValue;
use egg::Rewrite;

impl SplitRules {
    pub fn date_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
        // TODO check for time dimension before push down to optimize performance
        self.single_arg_split_point_rules(
            "date-part",
            || {
                self.fun_expr(
                    "DatePart",
                    vec!["?granularity".to_string(), column_expr("?column")],
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?output_granularity"), column_expr("?column")],
                )
            },
            |alias_column| {
                self.fun_expr(
                    "DatePart",
                    vec![literal_expr("?output_granularity"), alias_column],
                )
            },
            self.transform_date_part("?granularity", "?output_granularity"),
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
                            vec!["?inner_granularity".to_string(), column_expr("?column")],
                        ),
                    ],
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?output_granularity"), column_expr("?column")],
                )
            },
            |alias_column| {
                self.fun_expr(
                    "DatePart",
                    vec![literal_expr("?output_granularity"), alias_column],
                )
            },
            self.transform_date_part_within_date_trunc(
                "?outer_granularity",
                "?inner_granularity",
                "?output_granularity",
            ),
            false,
            rules,
        );
        self.single_arg_split_point_rules(
            "date-trunc",
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), column_expr("?column")],
                )
            },
            || {
                self.fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), column_expr("?column")],
                )
            },
            |alias_column| alias_column,
            |_, _, _| true,
            false,
            rules,
        );
    }

    fn transform_date_part(
        &self,
        granularity_var: &str,
        output_granularity_var: &str,
    ) -> impl Fn(
        bool,
        &mut egg::EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        &mut egg::Subst,
    ) -> bool
           + Sync
           + Send
           + Clone {
        let granularity_var = var!(granularity_var);
        let output_granularity_var = var!(output_granularity_var);
        move |_, egraph, subst| {
            if let Some(ConstantFolding::Scalar(ScalarValue::Utf8(Some(granularity)))) =
                &egraph[subst[granularity_var]].data.constant
            {
                if let Some(out_granularity) = parse_granularity_string(&granularity, true) {
                    let output_granularity = egraph.add(LogicalPlanLanguage::LiteralExprValue(
                        LiteralExprValue(ScalarValue::Utf8(Some(out_granularity.to_string()))),
                    ));
                    subst.insert(output_granularity_var, output_granularity);
                    return true;
                }
            }
            false
        }
    }

    fn transform_date_part_within_date_trunc(
        &self,
        outer_var: &str,
        inner_var: &str,
        granularity_var: &str,
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
        let granularity_var = var!(granularity_var);
        move |_, egraph, subst| {
            if let Some(ConstantFolding::Scalar(ScalarValue::Utf8(Some(outer_granularity)))) =
                &egraph[subst[outer_var]].data.constant
            {
                if let Some(ConstantFolding::Scalar(ScalarValue::Utf8(Some(inner_granularity)))) =
                    &egraph[subst[inner_var]].data.constant
                {
                    let date_trunc_granularity =
                        match min_granularity(&outer_granularity, &inner_granularity) {
                            Some(granularity) => {
                                if granularity.to_lowercase() == inner_granularity.to_lowercase() {
                                    outer_granularity
                                } else {
                                    inner_granularity
                                }
                            }
                            None => return false,
                        };

                    let granularity =
                        egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                            ScalarValue::Utf8(Some(date_trunc_granularity.to_string())),
                        )));
                    subst.insert(granularity_var, granularity);
                    return true;
                }
            }
            false
        }
    }
}
