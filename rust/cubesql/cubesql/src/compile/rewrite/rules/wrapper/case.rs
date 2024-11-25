use crate::{
    compile::rewrite::{
        case_expr_var_arg, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn case_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-case",
                wrapper_pushdown_replacer(
                    case_expr_var_arg("?when", "?then", "?else"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                ),
                case_expr_var_arg(
                    wrapper_pushdown_replacer(
                        "?when",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?then",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?else",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-case",
                case_expr_var_arg(
                    wrapper_pullup_replacer(
                        "?when",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?then",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?else",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
                wrapper_pullup_replacer(
                    case_expr_var_arg("?when", "?then", "?else"),
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_case_expr("?alias_to_cube"),
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
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                WrapperPullupReplacerAliasToCube
            )
            .cloned()
            {
                if let Some(sql_generator) = meta.sql_generator_by_alias_to_cube(&alias_to_cube) {
                    if sql_generator
                        .get_sql_templates()
                        .templates
                        .contains_key("expressions/case")
                    {
                        return true;
                    }
                }
            }
            false
        }
    }
}
