use crate::{
    compile::rewrite::{
        case_expr_var_arg, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context,
    },
    var,
};
use egg::Subst;

impl WrapperRules {
    pub fn case_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-case",
                wrapper_pushdown_replacer(case_expr_var_arg("?when", "?then", "?else"), "?context"),
                case_expr_var_arg(
                    wrapper_pushdown_replacer("?when", "?context"),
                    wrapper_pushdown_replacer("?then", "?context"),
                    wrapper_pushdown_replacer("?else", "?context"),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-case",
                case_expr_var_arg(
                    wrapper_pullup_replacer(
                        "?when",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?then",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?else",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                ),
                wrapper_pullup_replacer(
                    case_expr_var_arg("?when", "?then", "?else"),
                    wrapper_replacer_context(
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                        "?ungrouped_scan",
                        "?input_data_source",
                    ),
                ),
                self.transform_case_expr("?input_data_source"),
            ),
        ]);

        Self::expr_list_pushdown_pullup_rules(rules, "wrapper-case-expr", "CaseExprExpr");

        Self::expr_list_pushdown_pullup_rules(
            rules,
            "wrapper-case-when-expr",
            "CaseExprWhenThenExpr",
        );

        Self::expr_list_pushdown_pullup_rules(rules, "wrapper-case-else-expr", "CaseExprElseExpr");
    }

    fn transform_case_expr(
        &self,
        input_data_source_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
            else {
                return false;
            };

            Self::can_rewrite_template(&data_source, &meta, "expressions/case")
        }
    }
}
