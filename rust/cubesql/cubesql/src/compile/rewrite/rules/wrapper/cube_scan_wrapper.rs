use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, cube_scan, cube_scan_wrapper, rewrite,
        rules::wrapper::WrapperRules, transforming_rewrite, wrapper_pullup_replacer,
        CubeScanAliasToCube, CubeScanUngrouped, LogicalPlanLanguage,
        WrapperPullupReplacerAliasToCube, WrapperPullupReplacerUngrouped,
    },
    var, var_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn cube_scan_wrapper_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
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
                        "?ungrouped_out",
                        "?members",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_wrap_cube_scan(
                    "?alias_to_cube",
                    "?ungrouped",
                    "?alias_to_cube_out",
                    "?ungrouped_out",
                ),
            ),
            rewrite(
                "wrapper-finalize-pull-up-replacer",
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        "?alias_to_cube",
                        "?ungrouped",
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
        alias_to_cube_var: &'static str,
        ungrouped_cube_var: &'static str,
        alias_to_cube_var_out: &'static str,
        ungrouped_cube_var_out: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let ungrouped_cube_var = var!(ungrouped_cube_var);
        let alias_to_cube_var_out = var!(alias_to_cube_var_out);
        let ungrouped_cube_var_out = var!(ungrouped_cube_var_out);
        move |egraph, subst| {
            for alias_to_cube in
                var_iter!(egraph[subst[alias_to_cube_var]], CubeScanAliasToCube).cloned()
            {
                for ungrouped in
                    var_iter!(egraph[subst[ungrouped_cube_var]], CubeScanUngrouped).cloned()
                {
                    subst.insert(
                        ungrouped_cube_var_out,
                        egraph.add(LogicalPlanLanguage::WrapperPullupReplacerUngrouped(
                            WrapperPullupReplacerUngrouped(ungrouped),
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
            false
        }
    }
}
