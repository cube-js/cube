use crate::{
    compile::rewrite::{
        cube_scan, cube_scan_wrapper, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite_multi, wrapper_pullup_replacer, wrapper_replacer_context,
        CubeScanAliasToCube, CubeScanLimit, CubeScanOffset, CubeScanUngrouped, LogicalPlanLanguage,
        WrapperReplacerContextAliasToCube, WrapperReplacerContextGroupedSubqueries,
        WrapperReplacerContextInputDataSource, WrapperReplacerContextPushToCube,
        WrapperReplacerContextUngroupedScan,
    },
    copy_flag, var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn cube_scan_wrapper_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            transforming_rewrite_multi(
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
    ) -> impl Fn(&mut CubeEGraph, &Subst) -> Vec<Subst> {
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
            let mut subst = subst.clone();

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
                return vec![];
            }

            let Some(alias_to_cube) =
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube)
                    .next()
                    .cloned()
            else {
                return vec![];
            };
            let Some(ungrouped) = var_iter!(egraph[subst[ungrouped_cube_var]], CubeScanUngrouped)
                .next()
                .cloned()
            else {
                return vec![];
            };
            // When CubeScan already has limit or offset, it's unsafe to allow to push
            // anything on top to Cube.
            // Especially aggregation: aggregate does not commute with limit,
            // so it would be incorrect to join them to single CubeScan
            let push_to_cube_out = ungrouped && has_no_limit_or_offset;

            // This rule would wrap CubeScan, which would try to generate data source SQL for it
            // This means that CubeScan should allow for this
            let data_sources = {
                let Some(members) = &egraph[subst[members_var]].data.member_name_to_expr else {
                    return vec![];
                };

                let member_names = members
                    .list
                    .iter()
                    .filter_map(|(_, member, _)| member.name());
                // TODO get all referenced members (from members, filters, order etc.)
                let every_member_name = member_names;

                let Ok(data_sources) = meta.data_sources_for_member_names(every_member_name) else {
                    return vec![];
                };
                data_sources
            };
            // A scan that nothing is pushed into is rendered as is, so several data sources
            // among its members cannot be resolved to one. A scan that is pushed into is
            // narrowed later, so each data source gets its own context (`member_fits_data_source`)
            if data_sources.len() > 1 && !push_to_cube_out {
                return vec![];
            }
            let data_sources_out: Vec<Option<String>> = if data_sources.is_empty() {
                vec![None]
            } else {
                data_sources
                    .iter()
                    .map(|data_source| Some(data_source.to_string()))
                    .collect()
            };

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

            data_sources_out
                .into_iter()
                .map(|data_source_out| {
                    let mut subst = subst.clone();
                    subst.insert(
                        input_data_source_out_var,
                        egraph.add(LogicalPlanLanguage::WrapperReplacerContextInputDataSource(
                            WrapperReplacerContextInputDataSource(data_source_out),
                        )),
                    );
                    subst
                })
                .collect()
        }
    }
}
