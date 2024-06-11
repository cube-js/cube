use crate::{
    compile::rewrite::{
        analysis::{LogicalPlanAnalysis, OriginalExpr},
        inlist_expr, rewrite,
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer, CastExprDataType,
        LogicalPlanLanguage, WrapperPullupReplacerAliasToCube,
    },
    var, var_iter,
};
use datafusion::{
    arrow::datatypes::{DataType, TimeUnit},
    logical_plan::Expr,
    physical_plan::functions::BuiltinScalarFunction,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn in_list_expr_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-in-list-only-consts-push-down",
                wrapper_pushdown_replacer(
                    inlist_expr("?expr", "?list", "?negated"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                inlist_expr(
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?list",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?negated",
                ),
                self.transform_in_list_only_consts("?list"),
            ),
            rewrite(
                "wrapper-in-list-push-down",
                wrapper_pushdown_replacer(
                    inlist_expr("?expr", "?list", "?negated"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                inlist_expr(
                    wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pushdown_replacer(
                        "?list",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?negated",
                ),
            ),
            transforming_rewrite(
                "wrapper-in-list-pull-up",
                inlist_expr(
                    wrapper_pullup_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    wrapper_pullup_replacer(
                        "?list",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?negated",
                ),
                wrapper_pullup_replacer(
                    inlist_expr("?expr", "?new_list", "?negated"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_in_list_expr_with_cast(
                    "?expr",
                    "?list",
                    "?new_list",
                    "?alias_to_cube",
                ),
            ),
        ]);

        // TODO: support for flatten list
        Self::expr_list_pushdown_pullup_rules(rules, "wrapper-in-list-exprs", "InListExprList");
    }

    fn transform_in_list_expr_with_cast(
        &self,
        expr_var: &'static str,
        list_var: &'static str,
        new_list_var: &'static str,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let expr_var = var!(expr_var);
        let list_var = var!(list_var);
        let new_list_var = var!(new_list_var);
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
                    let sql_templates = sql_generator.get_sql_templates();
                    if !sql_templates.templates.contains_key("expressions/in_list") {
                        continue;
                    }

                    if sql_templates.cast_in_list_time_dimension {
                        if let Some(OriginalExpr::Expr(expr)) =
                            &egraph[subst[expr_var]].data.original_expr
                        {
                            if let Expr::ScalarFunction { fun, .. } = expr {
                                if fun == &BuiltinScalarFunction::DateTrunc {
                                    for node in egraph[subst[list_var]].nodes.iter().cloned() {
                                        if let LogicalPlanLanguage::InListExprList(list) = node {
                                            let new_list_ids = list
                                                .into_iter()
                                                .map(|elem_id| {
                                                    let cast_expr_data_type_id = egraph.add(
                                                        LogicalPlanLanguage::CastExprDataType(
                                                            CastExprDataType(DataType::Timestamp(
                                                                TimeUnit::Nanosecond,
                                                                None,
                                                            )),
                                                        ),
                                                    );
                                                    let cast_expr =
                                                        egraph.add(LogicalPlanLanguage::CastExpr(
                                                            [elem_id, cast_expr_data_type_id],
                                                        ));
                                                    cast_expr
                                                })
                                                .collect();
                                            subst.insert(
                                                new_list_var,
                                                egraph.add(LogicalPlanLanguage::InListExprList(
                                                    new_list_ids,
                                                )),
                                            );
                                            return true;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    subst.insert(new_list_var, subst[list_var]);
                    return true;
                }
            }
            false
        }
    }

    fn transform_in_list_only_consts(
        &self,
        list_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let list_var = var!(list_var);
        move |egraph: &mut EGraph<_, LogicalPlanAnalysis>, subst| {
            return egraph[subst[list_var]].data.constant_in_list.is_some();
        }
    }
}
