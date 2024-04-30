use crate::{
    compile::{
        rewrite::{
            analysis::LogicalPlanAnalysis, cube_scan_wrapper, rewrite,
            rules::wrapper::WrapperRules, subquery_node, subquery_pushdown_holder,
            transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
            LogicalPlanLanguage, WrapperPullupReplacerAliasToCube,
        },
        MetaContext,
    },
    var, var_iter, var_list_iter,
};
use egg::{EGraph, Rewrite, Subst, Var};
use std::sync::Arc;

impl WrapperRules {
    pub fn subquery_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-subqueries-wrapped-scan-to-pull",
                subquery_pushdown_holder(
                    subquery_node(cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            "?inner_alias_to_cube",
                            "?nner_ungrouped",
                            "?inner_in_projection",
                            "?inner_cube_members",
                        ),
                        "CubeScanWrapperFinalized:false",
                    )),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    subquery_node("?cube_scan_input"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_check_subquery_wrapped("?cube_scan_input"),
            ),
            transforming_rewrite(
                "wrapper-subqueries-wrapped-scan-to-pull-upkskip-pushdown", //Non EmptyRalation case
                subquery_pushdown_holder(
                    subquery_node(wrapper_pushdown_replacer(
                        cube_scan_wrapper(
                            wrapper_pullup_replacer(
                                "?cube_scan_input",
                                "?inner_alias_to_cube",
                                "?nner_ungrouped",
                                "?inner_in_projection",
                                "?inner_cube_members",
                            ),
                            "CubeScanWrapperFinalized:false",
                        ),
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    )),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    subquery_node("?cube_scan_input"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_check_subquery_wrapped("?cube_scan_input"),
            ),
            rewrite(
                "wrapper-subqueries-add-input-pushdown",
                wrapper_pushdown_replacer(
                    subquery_node("?input"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                subquery_pushdown_holder(
                    subquery_node(wrapper_pushdown_replacer(
                        "?input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    )),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
            ),
        ]);
        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-subqueries",
            "SubquerySubqueries",
            "WrappedSelectSubqueries",
        );
    }

    pub fn transform_check_subquery_allowed(
        egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        subst: &mut Subst,
        meta: Arc<MetaContext>,
        alias_to_cube_var: Var,
    ) -> bool {
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
                    .contains_key("expressions/subquery")
                {
                    return true;
                }
            }
        }
        false
    }

    fn transform_check_subquery_wrapped(
        &self,
        cube_scan_input_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let cube_scan_input_var = var!(cube_scan_input_var);
        move |egraph, subst| {
            for _ in var_list_iter!(egraph[subst[cube_scan_input_var]], WrappedSelect).cloned() {
                return true;
            }
            false
        }
    }
}
