use crate::{
    compile::rewrite::{
        like_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context, LikeExprEscapeChar, LikeExprLikeType, LikeType,
    },
    var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn like_expr_rules(&self, rules: &mut Vec<CubeRewrite>) {
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
                    "?context",
                ),
                like_expr(
                    "?like_type",
                    "?negated",
                    wrapper_pushdown_replacer("?expr", "?context"),
                    wrapper_pushdown_replacer("?pattern", "?context"),
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
                        "?pattern",
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
                self.transform_like_expr("?input_data_source", "?like_type", "?escape_char"),
            ),
        ]);
    }

    fn transform_like_expr(
        &self,
        input_data_source_var: &'static str,
        like_type_var: &'static str,
        escape_char_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let like_type_var = var!(like_type_var);
        let escape_char_var = var!(escape_char_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
            else {
                return false;
            };

            for escape_char in var_iter!(egraph[subst[escape_char_var]], LikeExprEscapeChar) {
                if escape_char.is_some() {
                    if !Self::can_rewrite_template(&data_source, &meta, "expressions/like_escape") {
                        continue;
                    }
                }

                for like_type in var_iter!(egraph[subst[like_type_var]], LikeExprLikeType) {
                    let expression_name = match like_type {
                        LikeType::Like => "like",
                        LikeType::ILike => "ilike",
                        _ => continue,
                    };
                    if Self::can_rewrite_template(
                        &data_source,
                        &meta,
                        &format!("expressions/{}", expression_name),
                    ) {
                        return true;
                    }
                }
            }
            false
        }
    }
}
