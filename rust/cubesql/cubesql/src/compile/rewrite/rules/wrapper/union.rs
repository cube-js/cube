use crate::{
    compile::rewrite::{
        cube_scan_wrapper, distinct, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_list_rewrite_with_lists_and_vars, union, wrapped_union,
        wrapper_pullup_replacer, wrapper_replacer_context, ListApplierListPattern, ListPattern,
        ListType, LogicalPlanLanguage, UnionAlias, WrappedUnionAlias,
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
            // into them: the whole list is matched at once and every query is unwrapped into
            // the set operation. `?input_data_source` is a top level element variable, so
            // every query has to reach the same data source for this to match at all — a
            // union spanning two of them is left to post processing without a check of its
            // own.
            transforming_list_rewrite_with_lists_and_vars(
                "wrapper-pull-up-union",
                ListType::UnionInputs,
                ListPattern {
                    pattern: union("?list", "?union_alias"),
                    list_var: "?list".to_string(),
                    elem: cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?elem",
                            wrapper_replacer_context(
                                "?elem_alias_to_cube",
                                "?elem_push_to_cube",
                                "?elem_in_projection",
                                "?elem_cube_members",
                                "?elem_grouped_subqueries",
                                "?elem_ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                },
                &cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_union(
                            "?new_list",
                            "WrappedUnionDistinct:false",
                            "?wrapped_union_alias",
                        ),
                        wrapper_replacer_context(
                            "?out_alias_to_cube",
                            // A set operation is evaluated by the data source, so nothing
                            // above it reaches a Cube load query anymore
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
                [ListApplierListPattern::new(
                    ListType::WrappedUnionInputs,
                    "?new_list",
                    "?elem",
                )],
                &["?input_data_source"],
                // One query is not a union, and folding a `Distinct` into a single query
                // one would leave the deduplication with no operator to render it on
                2,
                &[
                    "?wrapped_union_alias",
                    "?out_alias_to_cube",
                    "?out_cube_members",
                    "?out_grouped_subqueries",
                ],
                self.transform_union(
                    "?union_alias",
                    "?input_data_source",
                    "?wrapped_union_alias",
                    "?out_alias_to_cube",
                    "?out_cube_members",
                    "?out_grouped_subqueries",
                ),
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
