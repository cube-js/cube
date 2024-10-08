use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, like_expr, rewrite, rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        LikeExprEscapeChar, LikeExprLikeType, LikeType, LogicalPlanLanguage,
        WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn like_expr_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-like-expr",
                wrapper_pushdown_replacer(
                    like_expr(
                        "?like_type",
                        "?negated",
                        "?expr",
                        "?pattern",
                        "?escape_char",
                    ),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                like_expr(
                    "?like_type",
                    "?negated",
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?pattern",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?escape_char",
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-like-expr",
                like_expr(
                    "?like_type",
                    "?negated",
                    wrapper_pullup_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?pattern",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?escape_char",
                ),
                wrapper_pullup_replacer(
                    like_expr(
                        "?like_type",
                        "?negated",
                        "?expr",
                        "?pattern",
                        "?escape_char",
                    ),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_like_expr("?alias_to_cube", "?like_type", "?escape_char"),
            ),
        ]);
    }

    fn transform_like_expr(
        &self,
        alias_to_cube_var: &'static str,
        like_type_var: &'static str,
        escape_char_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let like_type_var = var!(like_type_var);
        let escape_char_var = var!(escape_char_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                WrapperPullupReplacerAliasToCube
            ) {
                let Some(sql_generator) = meta.sql_generator_by_alias_to_cube(&alias_to_cube)
                else {
                    continue;
                };

                let templates = &sql_generator.get_sql_templates().templates;

                for escape_char in var_iter!(egraph[subst[escape_char_var]], LikeExprEscapeChar) {
                    if escape_char.is_some() {
                        if !templates.contains_key("expressions/like_escape") {
                            continue;
                        }
                    }

                    for like_type in var_iter!(egraph[subst[like_type_var]], LikeExprLikeType) {
                        let expression_name = match like_type {
                            LikeType::Like => "like",
                            LikeType::ILike => "ilike",
                            _ => continue,
                        };
                        if templates.contains_key(&format!("expressions/{}", expression_name)) {
                            return true;
                        }
                    }
                }
            }
            false
        }
    }
}
