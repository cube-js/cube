use crate::{
    compile::rewrite::{
        cube_scan, cube_scan_wrapper, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_replacer_context,
        CubeScanAliasToCube, CubeScanLimit, CubeScanOffset, CubeScanUngrouped, LogicalPlanLanguage,
        WrapperReplacerContextAliasToCube, WrapperReplacerContextGroupedSubqueries,
        WrapperReplacerContextInputDataSource, WrapperReplacerContextPushToCube,
        WrapperReplacerContextUngroupedScan,
    },
    copy_flag,
    transport::DataSource,
    var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn cube_scan_wrapper_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-cube-scan-wrap",
                cube_scan(
                    "?alias_to_cube",
                    "?members",
                    "?filters",
                    "?order",
                    "?limit",
                    "?offset",
                    "CubeScanSplit:false",
                    "?can_pushdown_join",
                    "CubeScanWrapped:false",
                    "?ungrouped",
                    "?join_hints",
                ),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        cube_scan(
                            "?alias_to_cube",
                            "?members",
                            "?filters",
                            "?order",
                            "?limit",
                            "?offset",
                            "CubeScanSplit:false",
                            "?can_pushdown_join",
                            "CubeScanWrapped:true",
                            "?ungrouped",
                            "?join_hints",
                        ),
                        wrapper_replacer_context(
                            "?alias_to_cube_out",
                            "?push_to_cube_out",
                            "WrapperReplacerContextInProjection:false",
                            "?members",
                            "?grouped_subqueries_out",
                            "?ungrouped_scan_out",
                            "?input_data_source_out",
                        ),
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_wrap_cube_scan(
                    "?members",
                    "?alias_to_cube",
                    "?limit",
                    "?offset",
                    "?ungrouped",
                    "?alias_to_cube_out",
                    "?push_to_cube_out",
                    "?grouped_subqueries_out",
                    "?ungrouped_scan_out",
                    "?input_data_source_out",
                ),
            ),
            rewrite(
                "wrapper-finalize-pull-up-replacer",
                cube_scan_wrapper(
                    wrapper_pullup_replacer("?cube_scan_input", "?context"),
                    "CubeScanWrapperFinalized:false",
                ),
                cube_scan_wrapper("?cube_scan_input", "CubeScanWrapperFinalized:true"),
            ),
        ]);
    }

    fn transform_wrap_cube_scan(
        &self,
        members_var: &'static str,
        alias_to_cube_var: &'static str,
        limit_var: &'static str,
        offset_var: &'static str,
        ungrouped_cube_var: &'static str,
        alias_to_cube_var_out: &'static str,
        push_to_cube_out_var: &'static str,
        grouped_subqueries_out_var: &'static str,
        ungrouped_scan_out_var: &'static str,
        input_data_source_out_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let members_var = var!(members_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let limit_var = var!(limit_var);
        let offset_var = var!(offset_var);
        let ungrouped_cube_var = var!(ungrouped_cube_var);
        let alias_to_cube_var_out = var!(alias_to_cube_var_out);
        let push_to_cube_out_var = var!(push_to_cube_out_var);
        let grouped_subqueries_out_var = var!(grouped_subqueries_out_var);
        let ungrouped_scan_out_var = var!(ungrouped_scan_out_var);
        let input_data_source_out_var = var!(input_data_source_out_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let mut has_no_limit_or_offset = true;
            for limit in var_iter!(egraph[subst[limit_var]], CubeScanLimit).cloned() {
                has_no_limit_or_offset &= limit.is_none();
            }
            for offset in var_iter!(egraph[subst[offset_var]], CubeScanOffset).cloned() {
                has_no_limit_or_offset &= offset.is_none();
            }

            if !copy_flag!(
                egraph,
                subst,
                ungrouped_cube_var,
                CubeScanUngrouped,
                ungrouped_scan_out_var,
                WrapperReplacerContextUngroupedScan
            ) {
                return false;
            }

            // This rule would wrap CubeScan, which would try to generate data source SQL for it
            // This means that CubeScan should allow for this
            // View can reference cubes from different data sources, and should be disallowed here
            // But
            // During rewrites we can generate representations like CubeScan(AllMembers(view))
            // And that would be an issue for representations like this: Aggregate(CSW(CubeScan(AllMembers(view), ungrouped=true)))
            // In this plan aggregate can be pushed into wrapper, and it would limit members
            // that would be actually referenced later, during SQL generation
            // But this rule would see only AllMembers, and disallow SQL pushdown for views like that
            // Rewriting views like this would require further limiting data sources during later rewrtie stages
            // And, probably, penalizing multi-datasource representations in cost
            // TODO find a clever-er way to allow SQL pushdown for multi-datasource views

            let data_source = {
                let Some(members) = &egraph[subst[members_var]].data.member_name_to_expr else {
                    return false;
                };

                let member_names = members
                    .list
                    .iter()
                    .filter_map(|(_, member, _)| member.name());
                // TODO get all referenced members (from members, filters, order etc.)
                let every_member_name = member_names;

                meta.data_source_for_member_names(every_member_name)
            };
            // See comment above about wrapping CubeScan(AllMembers(view), ungrouped=true)
            let Ok(data_source) = data_source else {
                return false;
            };

            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                for ungrouped in
                    var_iter!(egraph[subst[ungrouped_cube_var]], CubeScanUngrouped).cloned()
                {
                    // When CubeScan already has limit or offset, it's unsafe to allow to push
                    // anything on top to Cube.
                    // Especially aggregation: aggregate does not commute with limit,
                    // so it would be incorrect to join them to single CubeScan
                    let push_to_cube_out = ungrouped && has_no_limit_or_offset;

                    let data_source_out = match data_source {
                        DataSource::Unrestricted => None,
                        DataSource::Specific(data_source) => Some(data_source.to_string()),
                    };

                    subst.insert(
                        input_data_source_out_var,
                        egraph.add(LogicalPlanLanguage::WrapperReplacerContextInputDataSource(
                            WrapperReplacerContextInputDataSource(data_source_out),
                        )),
                    );
                    subst.insert(
                        push_to_cube_out_var,
                        egraph.add(LogicalPlanLanguage::WrapperReplacerContextPushToCube(
                            WrapperReplacerContextPushToCube(push_to_cube_out),
                        )),
                    );
                    subst.insert(
                        alias_to_cube_var_out,
                        egraph.add(LogicalPlanLanguage::WrapperReplacerContextAliasToCube(
                            WrapperReplacerContextAliasToCube(alias_to_cube),
                        )),
                    );
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
}
