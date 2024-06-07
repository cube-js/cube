use egg::{EGraph, Rewrite, Subst};

use crate::{
    compile::rewrite::{
        aggr_aggr_expr_empty_tail, aggr_group_expr_empty_tail,
        aggr_group_expr_legacy as aggr_group_expr, case_expr_else_expr, case_expr_expr,
        case_expr_replacer, case_expr_var_arg, case_expr_when_then_expr,
        case_expr_when_then_expr_empty_tail, column_expr, group_aggregate_split_replacer,
        group_expr_split_replacer, inner_aggregate_split_replacer, is_not_null_expr, is_null_expr,
        literal_expr, outer_aggregate_split_replacer, rewrite, rewriter::RewriteRules,
        transforming_rewrite, CaseExprReplacerAliasToCube, InnerAggregateSplitReplacerAliasToCube,
        LogicalPlanAnalysis, LogicalPlanLanguage,
    },
    var, var_iter,
};

pub struct CaseRules {}

impl RewriteRules for CaseRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        vec![
            // Case replacer takes place of inner replacer
            transforming_rewrite(
                "case-inner-replacer-to-case-replacer",
                inner_aggregate_split_replacer(
                    case_expr_var_arg("?expr", "?when_then", "?else"),
                    "?inner_alias_to_cube",
                ),
                case_expr_replacer(
                    case_expr_var_arg("?expr", "?when_then", "?else"),
                    "?case_alias_to_cube",
                ),
                self.transform_inner_replacer_to_case_replacer(
                    "?inner_alias_to_cube",
                    "?case_alias_to_cube",
                ),
            ),
            // Split rules to aggregate by whole CASE expression
            rewrite(
                "split-push-down-case-expr-outer-aggr-replacer",
                outer_aggregate_split_replacer(
                    case_expr_var_arg("?expr", "?when_then", "?else"),
                    "?alias_to_cube",
                ),
                case_expr_var_arg("?expr", "?when_then", "?else"),
            ),
            rewrite(
                "split-push-down-case-expr-group-expr-replacer",
                group_expr_split_replacer(
                    case_expr_var_arg("?expr", "?when_then", "?else"),
                    "?alias_to_cube",
                ),
                case_expr_var_arg("?expr", "?when_then", "?else"),
            ),
            rewrite(
                "split-push-down-case-expr-group-aggregate-replacer",
                group_aggregate_split_replacer(
                    case_expr_var_arg("?expr", "?when_then", "?else"),
                    "?alias_to_cube",
                ),
                aggr_aggr_expr_empty_tail(),
            ),
            // Case replacer pushdowns -- no change to member amount
            rewrite(
                "case-unwrap-column-expr-push-down",
                case_expr_replacer(column_expr("?column"), "?alias_to_cube"),
                column_expr("?column"),
            ),
            rewrite(
                "case-unwrap-is-null-expr-push-down",
                case_expr_replacer(is_null_expr("?expr"), "?alias_to_cube"),
                case_expr_replacer("?expr", "?alias_to_cube"),
            ),
            rewrite(
                "case-unwrap-is-not-null-expr-push-down",
                case_expr_replacer(is_not_null_expr("?expr"), "?alias_to_cube"),
                case_expr_replacer("?expr", "?alias_to_cube"),
            ),
            // Cube scan members pushdowns -- adds or drops members
            // TODO replace aggr_group_expr with appropriate list type based InnerAggregateSplitReplacer type
            rewrite(
                "case-expr-expr-push-down-cube-scan-members",
                case_expr_replacer(
                    case_expr_var_arg(
                        case_expr_expr(Some("?expr".to_string())),
                        "?when_then",
                        "?else",
                    ),
                    "?case_alias_to_cube",
                ),
                aggr_group_expr(
                    case_expr_replacer("?expr", "?case_alias_to_cube"),
                    case_expr_replacer(
                        case_expr_var_arg(case_expr_expr(None), "?when_then", "?else"),
                        "?case_alias_to_cube",
                    ),
                ),
            ),
            rewrite(
                "case-expr-when-then-expr-push-down-cube-scan-members",
                case_expr_replacer(
                    case_expr_var_arg(
                        case_expr_expr(None),
                        case_expr_when_then_expr(
                            "?when",
                            case_expr_when_then_expr("?then", "?when_then_tail"),
                        ),
                        "?else",
                    ),
                    "?case_alias_to_cube",
                ),
                aggr_group_expr(
                    case_expr_replacer("?when", "?case_alias_to_cube"),
                    aggr_group_expr(
                        case_expr_replacer("?then", "?case_alias_to_cube"),
                        case_expr_replacer(
                            case_expr_var_arg(case_expr_expr(None), "?when_then_tail", "?else"),
                            "?case_alias_to_cube",
                        ),
                    ),
                ),
            ),
            rewrite(
                "case-expr-else-expr-push-down-cube-scan-members",
                case_expr_replacer(
                    case_expr_var_arg(
                        case_expr_expr(None),
                        case_expr_when_then_expr_empty_tail(),
                        case_expr_else_expr(Some("?else".to_string())),
                    ),
                    "?case_alias_to_cube",
                ),
                aggr_group_expr(
                    case_expr_replacer("?else", "?case_alias_to_cube"),
                    case_expr_replacer(
                        case_expr_var_arg(
                            case_expr_expr(None),
                            case_expr_when_then_expr_empty_tail(),
                            case_expr_else_expr(None),
                        ),
                        "?case_alias_to_cube",
                    ),
                ),
            ),
            rewrite(
                "case-empty-push-down-cube-scan-members",
                case_expr_replacer(
                    case_expr_var_arg(
                        case_expr_expr(None),
                        case_expr_when_then_expr_empty_tail(),
                        case_expr_else_expr(None),
                    ),
                    "?case_alias_to_cube",
                ),
                aggr_group_expr_empty_tail(),
            ),
            rewrite(
                "case-literal-push-down-cube-scan-members",
                case_expr_replacer(literal_expr("?literal"), "?case_alias_to_cube"),
                aggr_group_expr_empty_tail(),
            ),
        ]
    }
}

impl CaseRules {
    pub fn new() -> Self {
        Self {}
    }

    fn transform_inner_replacer_to_case_replacer(
        &self,
        inner_alias_to_cube_var: &'static str,
        case_alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let inner_alias_to_cube_var = var!(inner_alias_to_cube_var);
        let case_alias_to_cube_var = var!(case_alias_to_cube_var);
        move |egraph, subst| {
            for inner_alias_to_cube in var_iter!(
                egraph[subst[inner_alias_to_cube_var]],
                InnerAggregateSplitReplacerAliasToCube
            )
            .cloned()
            {
                subst.insert(
                    case_alias_to_cube_var,
                    egraph.add(LogicalPlanLanguage::CaseExprReplacerAliasToCube(
                        CaseExprReplacerAliasToCube(inner_alias_to_cube),
                    )),
                );
                return true;
            }
            false
        }
    }
}
