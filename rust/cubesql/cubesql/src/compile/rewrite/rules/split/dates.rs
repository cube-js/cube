use crate::{
    compile::rewrite::{
        analysis::{ConstantFolding, LogicalPlanAnalysis},
        column_expr, fun_expr, literal_expr,
        rules::{split::SplitRules, utils::parse_granularity_string},
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
                fun_expr(
                    "DatePart",
                    vec!["?granularity".to_string(), column_expr("?column")],
                )
            },
            || {
                fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?output_granularity"), column_expr("?column")],
                )
            },
            |alias_column| {
                fun_expr(
                    "DatePart",
                    vec![literal_expr("?output_granularity"), alias_column],
                )
            },
            self.transform_date_part("?granularity", "?output_granularity"),
            false,
            rules,
        );
        self.single_arg_split_point_rules(
            "date-trunc",
            || {
                fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), column_expr("?column")],
                )
            },
            || {
                fun_expr(
                    "DateTrunc",
                    vec![literal_expr("?granularity"), column_expr("?column")],
                )
            },
            |alias_column| alias_column,
            |_, _, _| true,
            false,
            rules,
        );
        self.single_arg_pass_through_rules(
            "date-trunc-pass-through",
            |expr| fun_expr("DateTrunc", vec![literal_expr("?granularity"), expr]),
            false,
            rules,
        );
        // self.single_arg_pass_through_rules(
        //     "date-part-pass-through",
        //     |expr| fun_expr("DatePart", vec![literal_expr("?granularity"), expr]),
        //     false,
        //     rules,
        // );
        // self.single_arg_pass_through_rules(
        //     "date-add-pass-through",
        //     |expr| udf_expr("date_add", vec![expr, "?interval".to_string()]),
        //     false,
        //     rules,
        // );
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
}
