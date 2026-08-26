use crate::{
    compile::rewrite::{
        cube_scan_wrapper, distinct, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, union, union_inputs, union_inputs_empty_tail, wrapped_union,
        wrapped_union_inputs, wrapped_union_inputs_empty_tail, wrapper_pullup_replacer,
        wrapper_replacer_context, LogicalPlanLanguage, UnionAlias, WrappedUnionAlias,
        WrapperReplacerContextAliasToCube, WrapperReplacerContextGroupedSubqueries,
        WrapperReplacerContextInputDataSource,
    },
    transport::DataSource,
    var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn union_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            // The queries of a union arrive already pulled up, so there is nothing to push
            // into them and the list carries a pull-up replacer rather than a push-down one.
            //
            // Only the head of the list is matched, to bind the data source that every
            // other query has to agree with; `wrapper-union-inputs-pull-up` is what checks
            // the rest, one query at a time.
            transforming_rewrite(
                "wrapper-push-down-union-to-cube-scan",
                union(
                    union_inputs(
                        cube_scan_wrapper(
                            wrapper_pullup_replacer(
                                "?head",
                                wrapper_replacer_context(
                                    "?head_alias_to_cube",
                                    "?head_push_to_cube",
                                    "?head_in_projection",
                                    "?head_cube_members",
                                    "?head_grouped_subqueries",
                                    "?head_ungrouped_scan",
                                    "?input_data_source",
                                ),
                            ),
                            "CubeScanWrapperFinalized:false",
                        ),
                        "?tail",
                    ),
                    "?union_alias",
                ),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_union(
                            wrapper_pullup_replacer(
                                union_inputs(
                                    cube_scan_wrapper(
                                        wrapper_pullup_replacer(
                                            "?head",
                                            wrapper_replacer_context(
                                                "?head_alias_to_cube",
                                                "?head_push_to_cube",
                                                "?head_in_projection",
                                                "?head_cube_members",
                                                "?head_grouped_subqueries",
                                                "?head_ungrouped_scan",
                                                "?input_data_source",
                                            ),
                                        ),
                                        "CubeScanWrapperFinalized:false",
                                    ),
                                    "?tail",
                                ),
                                wrapper_replacer_context(
                                    "?out_alias_to_cube",
                                    // A set operation is evaluated by the data source, so
                                    // nothing above it reaches a Cube load query anymore
                                    "WrapperReplacerContextPushToCube:false",
                                    "WrapperReplacerContextInProjection:false",
                                    "?out_cube_members",
                                    "?out_grouped_subqueries",
                                    "WrapperReplacerContextUngroupedScan:false",
                                    "?input_data_source",
                                ),
                            ),
                            "WrappedUnionDistinct:false",
                            "?wrapped_union_alias",
                        ),
                        wrapper_replacer_context(
                            "?out_alias_to_cube",
                            "WrapperReplacerContextPushToCube:false",
                            "WrapperReplacerContextInProjection:false",
                            "?out_cube_members",
                            "?out_grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_union(
                    "?union_alias",
                    "?input_data_source",
                    "?wrapped_union_alias",
                    "?out_alias_to_cube",
                    "?out_cube_members",
                    "?out_grouped_subqueries",
                ),
            ),
            // One query of the set operation, peeled off the head of the list. The data
            // source of the query and of the list are the same variable, so a query that
            // reaches another one leaves a replacer behind rather than a union, and the
            // plain post processing plan outprices it.
            rewrite(
                "wrapper-union-inputs-pull-up",
                wrapper_pullup_replacer(
                    union_inputs(
                        cube_scan_wrapper(
                            wrapper_pullup_replacer(
                                "?head",
                                wrapper_replacer_context(
                                    "?head_alias_to_cube",
                                    "?head_push_to_cube",
                                    "?head_in_projection",
                                    "?head_cube_members",
                                    "?head_grouped_subqueries",
                                    "?head_ungrouped_scan",
                                    "?input_data_source",
                                ),
                            ),
                            "CubeScanWrapperFinalized:false",
                        ),
                        "?tail",
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
                wrapped_union_inputs(
                    "?head",
                    wrapper_pullup_replacer(
                        "?tail",
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
            ),
            rewrite(
                "wrapper-union-inputs-tail",
                wrapper_pullup_replacer(union_inputs_empty_tail(), "?context"),
                wrapped_union_inputs_empty_tail(),
            ),
            // `UNION` is `UNION ALL` with the duplicates dropped, so the data source can do
            // both at once and the deduplication does not have to happen in DataFusion
            rewrite(
                "wrapper-push-down-distinct-to-union",
                distinct(cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_union("?inputs", "WrappedUnionDistinct:false", "?alias"),
                        "?context",
                    ),
                    "CubeScanWrapperFinalized:false",
                )),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_union("?inputs", "WrappedUnionDistinct:true", "?alias"),
                        "?context",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
            ),
        ]);
    }

    fn transform_union(
        &self,
        union_alias_var: &'static str,
        input_data_source_var: &'static str,
        wrapped_union_alias_var: &'static str,
        alias_to_cube_out_var: &'static str,
        cube_members_out_var: &'static str,
        grouped_subqueries_out_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let union_alias_var = var!(union_alias_var);
        let input_data_source_var = var!(input_data_source_var);
        let wrapped_union_alias_var = var!(wrapped_union_alias_var);
        let alias_to_cube_out_var = var!(alias_to_cube_out_var);
        let cube_members_out_var = var!(cube_members_out_var);
        let grouped_subqueries_out_var = var!(grouped_subqueries_out_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Some(data_source) = var_iter!(
                egraph[subst[input_data_source_var]],
                WrapperReplacerContextInputDataSource
            )
            .flat_map(|data_source| data_source.clone())
            .next() else {
                return false;
            };

            // A data source that has no union template cannot render a set operation, and
            // there is no way back to post processing once this plan is chosen
            if !Self::can_rewrite_template(
                &DataSource::Specific(&data_source),
                &meta,
                "statements/union",
            ) {
                return false;
            }

            let Some(alias) = var_iter!(egraph[subst[union_alias_var]], UnionAlias)
                .cloned()
                .next()
            else {
                return false;
            };
            subst.insert(
                wrapped_union_alias_var,
                egraph.add(LogicalPlanLanguage::WrappedUnionAlias(WrappedUnionAlias(
                    alias,
                ))),
            );

            // The queries are complete by now, so nothing above the union can reach the
            // cubes and members they were built from
            subst.insert(
                alias_to_cube_out_var,
                egraph.add(LogicalPlanLanguage::WrapperReplacerContextAliasToCube(
                    WrapperReplacerContextAliasToCube(vec![]),
                )),
            );
            subst.insert(
                cube_members_out_var,
                egraph.add(LogicalPlanLanguage::CubeScanMembers(vec![])),
            );
            subst.insert(
                grouped_subqueries_out_var,
                egraph.add(
                    LogicalPlanLanguage::WrapperReplacerContextGroupedSubqueries(
                        WrapperReplacerContextGroupedSubqueries(vec![]),
                    ),
                ),
            );

            true
        }
    }
}
