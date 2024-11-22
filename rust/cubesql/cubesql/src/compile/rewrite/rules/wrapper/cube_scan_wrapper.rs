use crate::{
    compile::rewrite::{
        cube_scan, cube_scan_wrapper, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, CubeScanAliasToCube, CubeScanUngrouped,
        LogicalPlanLanguage, WrapperPullupReplacerAliasToCube, WrapperPullupReplacerPushToCube,
    },
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
                        ),
                        "?alias_to_cube_out",
                        "?push_to_cube_out",
                        "WrapperPullupReplacerInProjection:false",
                        "?members",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_wrap_cube_scan(
                    "?members",
                    "?alias_to_cube",
                    "?ungrouped",
                    "?alias_to_cube_out",
                    "?push_to_cube_out",
                ),
            ),
            rewrite(
                "wrapper-finalize-pull-up-replacer",
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                    ),
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
        ungrouped_cube_var: &'static str,
        alias_to_cube_var_out: &'static str,
        push_to_cube_out_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let members_var = var!(members_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let ungrouped_cube_var = var!(ungrouped_cube_var);
        let alias_to_cube_var_out = var!(alias_to_cube_var_out);
        let push_to_cube_out_var = var!(push_to_cube_out_var);
        move |egraph, subst| {
            if let Some(_) = egraph[subst[members_var]].data.member_name_to_expr {
                for alias_to_cube in
                    var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
                {
                    for ungrouped in
                        var_iter!(egraph[subst[ungrouped_cube_var]], CubeScanUngrouped).cloned()
                    {
                        subst.insert(
                            push_to_cube_out_var,
                            egraph.add(LogicalPlanLanguage::WrapperPullupReplacerPushToCube(
                                WrapperPullupReplacerPushToCube(ungrouped),
                            )),
                        );
                        subst.insert(
                            alias_to_cube_var_out,
                            egraph.add(LogicalPlanLanguage::WrapperPullupReplacerAliasToCube(
                                WrapperPullupReplacerAliasToCube(alias_to_cube),
                            )),
                        );
                        return true;
                    }
                }
            }

            false
        }
    }
}
