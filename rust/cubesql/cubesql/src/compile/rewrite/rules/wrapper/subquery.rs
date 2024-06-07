use crate::{
    compile::{
        rewrite::{
            analysis::LogicalPlanAnalysis, cube_scan_wrapper, empty_relation,
            rules::wrapper::WrapperRules, transforming_rewrite, wrapper_pullup_replacer,
            wrapper_pushdown_replacer, EmptyRelationDerivedSourceTableName, LogicalPlanLanguage,
            WrapperPullupReplacerAliasToCube,
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
                wrapper_pushdown_replacer(
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
                ),
                wrapper_pullup_replacer(
                    "?cube_scan_input",
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_check_subquery_wrapped("?cube_scan_input"),
            ),
            transforming_rewrite(
                "wrapper-subqueries-wrap-empty-rel",
                empty_relation(
                    "?produce_one_row",
                    "?derived_source_table_name",
                    "EmptyRelationIsWrappable:true",
                ),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        empty_relation(
                            "?produce_one_row",
                            "?derived_source_table_name",
                            "EmptyRelationIsWrappable:true",
                        ),
                        "?alias_to_cube",
                        "WrapperPullupReplacerUngrouped:false",
                        "WrapperPullupReplacerInProjection:true",
                        "CubeScanMembers",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_wrap_empty_rel("?derived_source_table_name", "?alias_to_cube"),
            ),
        ]);
        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-subqueries",
            "SubquerySubqueries",
            "WrappedSelectSubqueries",
        );
    }
    pub fn transform_wrap_empty_rel(
        &self,
        source_table_name_var: &'static str,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let source_table_name_var = var!(source_table_name_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            for name in var_iter!(
                egraph[subst[source_table_name_var]],
                EmptyRelationDerivedSourceTableName
            ) {
                if let Some(name) = name {
                    if let Some(cube) = meta_context
                        .cubes
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(name))
                    {
                        subst.insert(
                            alias_to_cube_var,
                            egraph.add(LogicalPlanLanguage::WrapperPullupReplacerAliasToCube(
                                WrapperPullupReplacerAliasToCube(vec![(
                                    "".to_string(),
                                    cube.name.to_string(),
                                )]),
                            )),
                        );
                        return true;
                    }
                }
            }

            false
        }
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
