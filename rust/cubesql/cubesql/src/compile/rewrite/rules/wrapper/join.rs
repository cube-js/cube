use crate::{
    compile::rewrite::{
        cube_scan_wrapper, join, rewrite, rewriter::CubeRewrite, rules::wrapper::WrapperRules,
        transforming_rewrite, wrapped_select, wrapped_select_aggr_expr_empty_tail,
        wrapped_select_filter_expr_empty_tail, wrapped_select_group_expr_empty_tail,
        wrapped_select_having_expr_empty_tail, wrapped_select_join, wrapped_select_joins,
        wrapped_select_joins_empty_tail, wrapped_select_order_expr_empty_tail,
        wrapped_select_projection_expr_empty_tail, wrapped_select_subqueries_empty_tail,
        wrapped_select_window_expr_empty_tail, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context, BinaryExprOp, ColumnExprColumn, CubeEGraph, JoinLeftOn,
        JoinRightOn, LogicalPlanLanguage, WrappedSelectJoinJoinType,
        WrapperReplacerContextGroupedSubqueries,
    },
    var, var_iter, var_list_iter,
};

use crate::compile::rewrite::analysis::Member;
use datafusion::{logical_expr::Operator, logical_plan::Column};
use egg::{Id, Subst};

impl WrapperRules {
    pub fn join_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-pull-up-single-select-join",
                wrapped_select_join(
                    wrapper_pullup_replacer("?input", "?context"),
                    wrapper_pullup_replacer("?join_expr", "?context"),
                    "?out_join_type",
                ),
                wrapper_pullup_replacer(
                    wrapped_select_join("?input", "?join_expr", "?out_join_type"),
                    "?context",
                ),
            ),
            // TODO handle CrossJoin and Filter(CrossJoin) as well
            transforming_rewrite(
                "wrapper-push-down-ungrouped-join-grouped",
                join(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?left_cube_scan_input",
                            wrapper_replacer_context(
                                // Going to use this in RHS of rule
                                // RHS of join is grouped, so it shouldn't have any cubes or members
                                "?left_alias_to_cube",
                                // This check is important
                                // Rule would place ?left_cube_scan_input to `from` position of WrappedSelect(WrappedSelectPushToCube:true)
                                // So it need to support push-to-Cube
                                "WrapperReplacerContextPushToCube:true",
                                "?left_in_projection",
                                // Going to use this in RHS of rule
                                // RHS of join is grouped, so it shouldn't have any cubes or members
                                "?left_cube_members",
                                "?left_grouped_subqueries",
                                "?left_ungrouped_scan",
                            ),
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?right_input",
                            wrapper_replacer_context(
                                // Going to ignore this
                                "?right_alias_to_cube",
                                "?right_push_to_cube",
                                "?right_in_projection",
                                // Going to ignore this
                                "?right_cube_members",
                                "?right_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                            ),
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                    "?left_on",
                    "?right_on",
                    "?in_join_type",
                    "?join_constraint",
                    "JoinNullEqualsNull:false",
                ),
                // RHS is using WrapperReplacerContextInProjection:false because only part
                // that should have push down replacer is join condition, and it should only contain dimensions
                // Other way of thinking about it: join condition is more like filter than projection
                cube_scan_wrapper(
                    wrapped_select(
                        "WrappedSelectSelectType:Projection",
                        wrapper_pullup_replacer(
                            wrapped_select_projection_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                // Can use it, because we've checked that left input allows push-to-Cube,
                                // so it must be ungrouped, making this whole plan ungrouped
                                "WrapperReplacerContextUngroupedScan:true",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_subqueries_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:true",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_group_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:true",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_aggr_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:true",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_window_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:true",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            // Can move left_cube_scan_input here without checking if it's actually CubeScan
                            // Check for WrapperReplacerContextPushToCube:true should be enough
                            "?left_cube_scan_input",
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:true",
                            ),
                        ),
                        // We don't want to use list rules here, because ?right_input is already done
                        wrapped_select_joins(
                            wrapped_select_join(
                                wrapper_pullup_replacer(
                                    "?right_input",
                                    wrapper_replacer_context(
                                        "?left_alias_to_cube",
                                        "WrapperReplacerContextPushToCube:true",
                                        "WrapperReplacerContextInProjection:false",
                                        "?left_cube_members",
                                        "?out_grouped_subqueries",
                                        "WrapperReplacerContextUngroupedScan:true",
                                    ),
                                ),
                                wrapper_pushdown_replacer(
                                    "?out_join_expr",
                                    wrapper_replacer_context(
                                        "?left_alias_to_cube",
                                        // On one hand, this should be PushToCube:true, so we would only join on dimensions
                                        // On other: RHS is grouped, so any column is just a column
                                        // Right now, it is relying on grouped_subqueries + PushToCube:true, to allow both dimensions and grouped columns
                                        "WrapperReplacerContextPushToCube:true",
                                        "WrapperReplacerContextInProjection:false",
                                        "?left_cube_members",
                                        "?out_grouped_subqueries",
                                        "WrapperReplacerContextUngroupedScan:true",
                                    ),
                                ),
                                "?out_join_type",
                            ),
                            // pullup(tail) just so it could be easily picked up by pullup rules
                            wrapper_pullup_replacer(
                                wrapped_select_joins_empty_tail(),
                                wrapper_replacer_context(
                                    "?left_alias_to_cube",
                                    "WrapperReplacerContextPushToCube:true",
                                    "WrapperReplacerContextInProjection:false",
                                    "?left_cube_members",
                                    "?out_grouped_subqueries",
                                    "WrapperReplacerContextUngroupedScan:true",
                                ),
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:true",
                            ),
                        ),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapper_pullup_replacer(
                            wrapped_select_order_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:true",
                            ),
                        ),
                        "WrappedSelectAlias:None",
                        "WrappedSelectDistinct:false",
                        // left input has WrapperReplacerContextPushToCube:true
                        // Meaning that left input itself is ungrouped CubeScan
                        // Keep it in result, rely on pull-up rules to drop it, and on flattening rules to pick it up
                        "WrappedSelectPushToCube:true",
                        // left input is WrapperReplacerContextPushToCube:true, so result must be ungrouped
                        "WrappedSelectUngroupedScan:true",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_ungrouped_join_grouped(
                    "?left_cube_members",
                    "?left_on",
                    "?right_on",
                    "?in_join_type",
                    "?out_join_expr",
                    "?out_join_type",
                    "?out_grouped_subqueries",
                ),
            ),
        ]);

        // TODO only pullup is necessary here
        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-joins",
            "WrappedSelectJoins",
            "WrappedSelectJoins",
        );
    }

    fn are_join_members_supported<'egraph, 'columns>(
        egraph: &'egraph mut CubeEGraph,
        members: Id,
        join_on: impl IntoIterator<Item = &'columns Column>,
    ) -> bool {
        let members_data = &mut egraph[members].data;

        for column in join_on {
            if let Some(((_, member, _), _)) = members_data.find_member_by_column(column) {
                match member {
                    Member::Dimension { .. } => {
                        // do nothing
                    }
                    _ => {
                        // Unsupported member
                        return false;
                    }
                }
            }
        }

        true
    }

    fn build_join_expr(
        egraph: &mut CubeEGraph,
        left_join_on: impl IntoIterator<Item = Column>,
        right_join_on: impl IntoIterator<Item = Column>,
    ) -> Option<Id> {
        let join_on_pairs = left_join_on
            .into_iter()
            .zip(right_join_on.into_iter())
            .collect::<Vec<_>>();

        let result_expr =
            join_on_pairs
                .into_iter()
                .fold(None, |acc, (left_column, right_column)| {
                    let left_expr = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                        ColumnExprColumn(left_column),
                    ));
                    let right_expr = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                        ColumnExprColumn(right_column),
                    ));
                    let eq_expr = LogicalPlanLanguage::BinaryExpr([
                        egraph.add(LogicalPlanLanguage::ColumnExpr([left_expr])),
                        egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(
                            Operator::Eq,
                        ))),
                        egraph.add(LogicalPlanLanguage::ColumnExpr([right_expr])),
                    ]);
                    let eq_expr = egraph.add(eq_expr);

                    let result = if let Some(acc) = acc {
                        let chained_expr = LogicalPlanLanguage::BinaryExpr([
                            acc,
                            egraph.add(LogicalPlanLanguage::BinaryExprOp(BinaryExprOp(
                                Operator::And,
                            ))),
                            eq_expr,
                        ]);
                        egraph.add(chained_expr)
                    } else {
                        eq_expr
                    };

                    Some(result)
                });

        result_expr
    }

    fn transform_ungrouped_join_grouped(
        &self,
        left_members_var: &'static str,
        left_on_var: &'static str,
        right_on_var: &'static str,
        in_join_type_var: &'static str,
        out_join_expr_var: &'static str,
        out_join_type_var: &'static str,
        out_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let left_members_var = var!(left_members_var);
        let left_on_var = var!(left_on_var);

        let right_on_var = var!(right_on_var);

        let in_join_type_var = var!(in_join_type_var);

        let out_join_expr_var = var!(out_join_expr_var);
        let out_join_type_var = var!(out_join_type_var);
        let out_grouped_subqueries_var = var!(out_grouped_subqueries_var);

        // Only left is allowed to be ungrouped query, so right would be a subquery join for left ungrouped CubeScan
        // It means we don't care about just a "single cube" in LHS, and there's essentially no cubes by this moment in RHS

        move |egraph, subst| {
            // We are going to generate join with grouped subquery
            // TODO Do we have to check stuff like `transform_check_subquery_allowed` is checking:
            // * Both inputs depend on a single data source
            // * SQL generator for that data source have `expressions/subquery` template
            // It could be checked later, in WrappedSelect as well

            for left_join_on in var_iter!(egraph[subst[left_on_var]], JoinLeftOn).cloned() {
                for right_join_on in var_iter!(egraph[subst[right_on_var]], JoinRightOn).cloned() {
                    // Don't check right, as it is already grouped

                    for in_join_type in
                        var_list_iter!(egraph[subst[in_join_type_var]], JoinJoinType).cloned()
                    {
                        if !Self::are_join_members_supported(
                            egraph,
                            subst[left_members_var],
                            &left_join_on,
                        ) {
                            return false;
                        }

                        // TODO what's a proper way to find table expression alias?
                        let right_join_alias = right_join_on
                            .iter()
                            .filter_map(|c| c.relation.as_ref())
                            .next()
                            .cloned();
                        let Some(right_join_alias) = right_join_alias else {
                            return false;
                        };

                        let out_join_expr =
                            Self::build_join_expr(egraph, left_join_on, right_join_on);
                        let Some(out_join_expr) = out_join_expr else {
                            return false;
                        };

                        // LHS is ungrouped, RHS is grouped
                        // Don't pass ungrouped queries from below, their qualifiers should not be accessible during join condition rewrite
                        let out_grouped_subqueries = vec![right_join_alias];

                        subst.insert(out_join_expr_var, out_join_expr);
                        subst.insert(
                            out_join_type_var,
                            egraph.add(LogicalPlanLanguage::WrappedSelectJoinJoinType(
                                WrappedSelectJoinJoinType(in_join_type.0),
                            )),
                        );
                        subst.insert(
                            out_grouped_subqueries_var,
                            egraph.add(
                                LogicalPlanLanguage::WrapperReplacerContextGroupedSubqueries(
                                    WrapperReplacerContextGroupedSubqueries(out_grouped_subqueries),
                                ),
                            ),
                        );

                        return true;
                    }
                }
            }

            return false;
        }
    }
}
