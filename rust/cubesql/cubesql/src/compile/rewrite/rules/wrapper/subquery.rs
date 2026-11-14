use crate::{
    compile::rewrite::{
        cube_scan_wrapper, empty_relation, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::{replacer_push_down_node, wrapper::WrapperRules},
        transforming_rewrite, wrapped_subquery, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context, EmptyRelationDerivedSourceTableName, LogicalPlanLanguage,
        WrapperReplacerContextAliasToCube, WrapperReplacerContextGroupedSubqueries,
    },
    transport::{MetaContext, CUBE_STORE_DATA_SOURCE},
    var, var_iter, var_list_iter,
};
use egg::{Subst, Var};

impl WrapperRules {
    pub fn subquery_rules(&self, rules: &mut Vec<CubeRewrite>) {
        // A pulled up form of a subquery, put into the class the subquery is pushed down in.
        // That class gets every form and nothing else that renders, so the subqueries list is
        // built once over it, and extraction picks the form there
        let subquery_form = wrapped_subquery("?cube_scan_input", "?input_data_source");
        let pushed_down_subquery = |inner_input_data_source: &str| {
            wrapper_pushdown_replacer(
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        wrapper_replacer_context(
                            "?inner_alias_to_cube",
                            "?inner_push_to_cube",
                            "?inner_in_projection",
                            "?inner_cube_members",
                            "?inner_grouped_subqueries",
                            "?inner_ungrouped_scan",
                            inner_input_data_source,
                        ),
                    ),
                    "CubeScanWrapperFinalized:false",
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
            )
        };
        rules.extend(vec![
            // Only a form reaching the data source of the query it is pulled into: one over a
            // pre-aggregation renders for Cube Store and can't be inlined into anything else
            transforming_rewrite(
                "wrapper-subqueries-wrapped-scan-to-pull",
                pushed_down_subquery("?input_data_source"),
                subquery_form.clone(),
                self.transform_check_subquery_wrapped("?cube_scan_input"),
            ),
            // A form reaching no data source, like one over an empty relation, fits any query
            transforming_rewrite(
                "wrapper-subqueries-wrapped-scan-to-pull-unrestricted",
                pushed_down_subquery("WrapperReplacerContextInputDataSource:None"),
                subquery_form.clone(),
                self.transform_check_subquery_wrapped("?cube_scan_input"),
            ),
            // TODO: forms of two data sources other than Cube Store are inlined into each
            // other, which is only sound when they share a database; only a form over a
            // pre-aggregation is kept in a Cube Store query for now
            transforming_rewrite(
                "wrapper-subqueries-wrapped-scan-to-pull-other-data-source",
                pushed_down_subquery("?inner_input_data_source"),
                subquery_form.clone(),
                self.transform_check_subquery_outside_cube_store(
                    "?cube_scan_input",
                    "?inner_input_data_source",
                    "?input_data_source",
                ),
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
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:false",
                            "WrapperReplacerContextInProjection:true",
                            "CubeScanMembers",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "WrapperReplacerContextInputDataSource:None",
                        ),
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_wrap_empty_rel(
                    "?derived_source_table_name",
                    "?alias_to_cube",
                    "?grouped_subqueries",
                ),
            ),
        ]);
        rules.extend(replacer_push_down_node(
            "wrapper-subqueries",
            "SubquerySubqueries",
            |node| wrapper_pushdown_replacer(node, "?context"),
            false,
        ));
        rules.extend(vec![
            // The form is spelled out again rather than bound, so it resolves to the class all
            // forms share and the list is one node whichever form matched
            rewrite(
                "wrapper-subqueries-pull-up",
                format!(
                    "(SubquerySubqueries {} {})",
                    subquery_form,
                    wrapper_pullup_replacer("?right", "?context")
                ),
                wrapper_pullup_replacer(
                    format!("(WrappedSelectSubqueries {} ?right)", subquery_form),
                    "?context",
                ),
            ),
            rewrite(
                "wrapper-subqueries-tail",
                wrapper_pushdown_replacer("SubquerySubqueries", "?context"),
                wrapper_pullup_replacer("WrappedSelectSubqueries", "?context"),
            ),
        ]);
    }
    pub fn transform_wrap_empty_rel(
        &self,
        source_table_name_var: &'static str,
        alias_to_cube_var: &'static str,
        grouped_subqueries_out_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let source_table_name_var = var!(source_table_name_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let grouped_subqueries_out_var = var!(grouped_subqueries_out_var);
        let meta_context = self.meta_context.clone();
        move |egraph, subst| {
            for name in var_iter!(
                egraph[subst[source_table_name_var]],
                EmptyRelationDerivedSourceTableName
            )
            .flatten()
            {
                if let Some(cube) = meta_context
                    .cubes
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(name))
                {
                    subst.insert(
                        alias_to_cube_var,
                        egraph.add(LogicalPlanLanguage::WrapperReplacerContextAliasToCube(
                            WrapperReplacerContextAliasToCube(vec![(
                                "".to_string(),
                                cube.name.to_string(),
                            )]),
                        )),
                    );
                    // We don't want to mark current query as a grouped, because we create pullup replacer out of thin air here
                    // And it would need to match other replacers later
                    // At the same time, this pullup replacer have no subqueries on its own
                    // So whoever want to treat this as subquery would introduce it to grouped_subqueries
                    subst.insert(
                        grouped_subqueries_out_var,
                        egraph.add(
                            LogicalPlanLanguage::WrapperReplacerContextGroupedSubqueries(
                                WrapperReplacerContextGroupedSubqueries(vec![]),
                            ),
                        ),
                    );
                    return true;
                }
            }

            false
        }
    }

    pub fn transform_check_subquery_allowed(
        egraph: &mut CubeEGraph,
        subst: &mut Subst,
        meta: &MetaContext,
        input_data_source_var: Var,
    ) -> bool {
        let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var) else {
            return false;
        };
        Self::can_rewrite_template(&data_source, &meta, "expressions/subquery")
    }

    fn transform_check_subquery_outside_cube_store(
        &self,
        cube_scan_input_var: &'static str,
        inner_input_data_source_var: &'static str,
        input_data_source_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let check_wrapped = self.transform_check_subquery_wrapped(cube_scan_input_var);
        let inner_input_data_source_var = var!(inner_input_data_source_var);
        let input_data_source_var = var!(input_data_source_var);
        move |egraph, subst| {
            let mut outside_cube_store = |var| {
                Self::get_data_source(egraph, subst, var)
                    .is_ok_and(|data_source| data_source.specific() != Some(CUBE_STORE_DATA_SOURCE))
            };
            outside_cube_store(inner_input_data_source_var)
                && outside_cube_store(input_data_source_var)
                && check_wrapped(egraph, subst)
        }
    }

    fn transform_check_subquery_wrapped(
        &self,
        cube_scan_input_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let cube_scan_input_var = var!(cube_scan_input_var);
        move |egraph, subst| {
            for _ in var_list_iter!(egraph[subst[cube_scan_input_var]], WrappedSelect).cloned() {
                return true;
            }
            false
        }
    }
}
