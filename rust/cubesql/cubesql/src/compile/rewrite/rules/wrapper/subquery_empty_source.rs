use crate::{
    compile::rewrite::{
        aggregate, analysis::LogicalPlanAnalysis, cube_scan_wrapper, empty_relation, filter, limit,
        projection, rewrite, rules::wrapper::WrapperRules, sort, transforming_rewrite,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, LogicalPlanLanguage,
    },
    var,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn subquery_empty_source_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            rewrite(
                "wrapper-subqueries-empty-rel-wrap",
                wrapper_pushdown_replacer(
                    empty_relation("?produce_one_row"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        empty_relation("?produce_one_row"),
                        "?alias_to_cube",
                        "WrapperPullupReplacerUngrouped:false",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
            ),
            transforming_rewrite(
                "wrapper-push-down-projection-in-subquery",
                wrapper_pushdown_replacer(
                    projection(
                        "?expr",
                        "?input",
                        "?projection_alias",
                        "ProjectionSplit:false",
                    ),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                projection(
                    "?expr",
                    wrapper_pushdown_replacer(
                        "?input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?projection_alias",
                    "ProjectionSplit:false",
                ),
                self.transform_push_down_to_empty_rel_check_input("?input"),
            ),
            transforming_rewrite(
                "wrapper-push-down-limit-in-subquery",
                wrapper_pushdown_replacer(
                    limit("?offset", "?limit", "?input"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                limit(
                    "?offset",
                    "?limit",
                    wrapper_pushdown_replacer(
                        "?input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
                self.transform_push_down_to_empty_rel_check_input("?input"),
            ),
            transforming_rewrite(
                "wrapper-push-down-filter-in-subquery",
                wrapper_pushdown_replacer(
                    filter("?filter_expr", "?input"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                filter(
                    "?filter_expr",
                    wrapper_pushdown_replacer(
                        "?input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
                self.transform_push_down_to_empty_rel_check_input("?input"),
            ),
            transforming_rewrite(
                "wrapper-push-down-order-in-subquery",
                wrapper_pushdown_replacer(
                    sort("?order_expr", "?input"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                sort(
                    "?order_expr",
                    wrapper_pushdown_replacer(
                        "?input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                ),
                self.transform_push_down_to_empty_rel_check_input("?input"),
            ),
            transforming_rewrite(
                "wrapper-push-down-aggregate-in-subquery",
                wrapper_pushdown_replacer(
                    aggregate(
                        "?input",
                        "?group_expr",
                        "?aggr_expr",
                        "AggregateSplit:false",
                    ),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                aggregate(
                    wrapper_pushdown_replacer(
                        "?input",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?group_expr",
                    "?aggr_expr",
                    "AggregateSplit:false",
                ),
                self.transform_push_down_to_empty_rel_check_input("?input"),
            ),
        ]);
    }
    fn transform_push_down_to_empty_rel_check_input(
        &self,
        input_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let input_var = var!(input_var);
        move |egraph, subst| {
            let allowed_inputs = egraph[subst[input_var]]
                .nodes
                .iter()
                .filter(|node| match node {
                    LogicalPlanLanguage::EmptyRelation(_)
                    | LogicalPlanLanguage::Projection(_)
                    | LogicalPlanLanguage::Filter(_)
                    | LogicalPlanLanguage::Sort(_)
                    | LogicalPlanLanguage::Aggregate(_)
                    | LogicalPlanLanguage::Limit(_) => true,
                    _ => false,
                });
            for _ in allowed_inputs {
                return true;
            }

            false
        }
    }
}
