use crate::{
    compile::rewrite::{
        inlist_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context,
    },
    var,
};
use egg::Subst;

impl WrapperRules {
    pub fn in_list_expr_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-in-list-only-consts-push-down",
                wrapper_pushdown_replacer(inlist_expr("?expr", "?list", "?negated"), "?context"),
                inlist_expr(
                    wrapper_pushdown_replacer("?expr", "?context"),
                    wrapper_pullup_replacer("?list", "?context"),
                    "?negated",
                ),
                self.transform_in_list_only_consts("?list"),
            ),
            rewrite(
                "wrapper-in-list-push-down",
                wrapper_pushdown_replacer(inlist_expr("?expr", "?list", "?negated"), "?context"),
                inlist_expr(
                    wrapper_pushdown_replacer("?expr", "?context"),
                    wrapper_pushdown_replacer("?list", "?context"),
                    "?negated",
                ),
            ),
            transforming_rewrite(
                "wrapper-in-list-pull-up",
                inlist_expr(
                    wrapper_pullup_replacer(
                        "?expr",
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
                        "?list",
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
                    "?negated",
                ),
                wrapper_pullup_replacer(
                    inlist_expr("?expr", "?list", "?negated"),
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
                self.transform_in_list_expr("?input_data_source"),
            ),
        ]);

        // TODO: support for flatten list
        Self::expr_list_pushdown_pullup_rules(rules, "wrapper-in-list-exprs", "InListExprList");
    }

    fn transform_in_list_expr(
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

            Self::can_rewrite_template(&data_source, &meta, "expressions/in_list")
        }
    }

    fn transform_in_list_only_consts(
        &self,
        list_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let list_var = var!(list_var);
        move |egraph: &mut CubeEGraph, subst| {
            return egraph[subst[list_var]].data.constant_in_list.is_some();
        }
    }
}
