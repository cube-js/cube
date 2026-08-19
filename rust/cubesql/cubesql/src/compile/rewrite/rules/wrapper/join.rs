use crate::{
    compile::rewrite::{
        analysis::Member, binary_expr, cross_join, cube_scan_wrapper, filter, fun_expr,
        is_not_null_expr, join, join_check_pull_up, join_check_push_down, join_check_stage,
        rewrite, rewriter::CubeRewrite, rules::wrapper::WrapperRules, transforming_rewrite,
        wrapped_select, wrapped_select_aggr_expr_empty_tail, wrapped_select_filter_expr_empty_tail,
        wrapped_select_group_expr_empty_tail, wrapped_select_having_expr_empty_tail,
        wrapped_select_join, wrapped_select_joins, wrapped_select_joins_empty_tail,
        wrapped_select_order_expr_empty_tail, wrapped_select_projection_expr_empty_tail,
        wrapped_select_subqueries_empty_tail, wrapped_select_window_expr_empty_tail,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, wrapper_replacer_context, BinaryExprOp,
        ColumnExprColumn, CubeEGraph, CubeScanWrapperFinalized, JoinLeftOn, JoinRightOn,
        LogicalPlanLanguage, WrappedSelectJoinJoinType, WrappedSelectPushToCube,
        WrappedSelectSelectType, WrappedSelectType, WrapperReplacerContextAliasToCube,
        WrapperReplacerContextGroupedSubqueries, WrapperReplacerContextUngroupedScan,
        WRAPPED_SELECT_FROM, WRAPPED_SELECT_JOINS, WRAPPED_SELECT_SELECT_TYPE,
    },
    transport::MetaContext,
    var, var_iter, var_list_iter,
};

use std::collections::HashSet;

use datafusion::{
    logical_expr::{Expr, Operator},
    logical_plan::Column,
    prelude::JoinType,
};
use egg::{Id, Subst, Var};
use itertools::Itertools;

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
                                // Data sources must match for both sides
                                // TODO support unrestricted data source on one side
                                "?input_data_source",
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
                                // Data sources must match for both sides
                                // TODO support unrestricted data source on one side
                                "?input_data_source",
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
                                "?input_data_source",
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
                                "?input_data_source",
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
                                "?input_data_source",
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
                                "?input_data_source",
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
                                "?input_data_source",
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
                                "?input_data_source",
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
                                        "?input_data_source",
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
                                        "?input_data_source",
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
                                    "?input_data_source",
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
                                "?input_data_source",
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
                                "?input_data_source",
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
                    "?right_input",
                    "?left_cube_members",
                    "?left_on",
                    "?right_on",
                    "?in_join_type",
                    "?input_data_source",
                    "?out_join_expr",
                    "?out_join_type",
                    "?out_grouped_subqueries",
                ),
            ),
            // TODO handle CrossJoin and Filter(CrossJoin) as well
            transforming_rewrite(
                "wrapper-push-down-grouped-join-grouped",
                join(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?left_input",
                            wrapper_replacer_context(
                                // Going to use this in RHS of rule
                                "?left_alias_to_cube",
                                // Push-to-Cube can have any value when both sides are grouped
                                "?left_push_to_cube",
                                "?left_in_projection",
                                // Going to use this in RHS of rule
                                "?left_cube_members",
                                "?left_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                // Data sources must match for both sides
                                // TODO support unrestricted data source on one side
                                "?input_data_source",
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
                                // Data sources must match for both sides
                                // TODO support unrestricted data source on one side
                                "?input_data_source",
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
                                "?left_push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_subqueries_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "?left_push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_group_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "?left_push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_aggr_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "?left_push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_window_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "?left_push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            // Can move left_input here without checking if it's CubeScan
                            // Check for WrapperReplacerContextUngroupedScan:true should be enough
                            "?left_input",
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "?left_push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        // We don't want to use list rules here, because ?right_input is already done
                        wrapped_select_joins(
                            wrapped_select_join(
                                wrapper_pullup_replacer(
                                    "?right_input",
                                    wrapper_replacer_context(
                                        "?left_alias_to_cube",
                                        "?left_push_to_cube",
                                        "WrapperReplacerContextInProjection:false",
                                        "?left_cube_members",
                                        "?out_grouped_subqueries",
                                        "WrapperReplacerContextUngroupedScan:false",
                                        "?input_data_source",
                                    ),
                                ),
                                wrapper_pushdown_replacer(
                                    "?out_join_expr",
                                    wrapper_replacer_context(
                                        "?left_alias_to_cube",
                                        "?left_push_to_cube",
                                        "WrapperReplacerContextInProjection:false",
                                        "?left_cube_members",
                                        "?out_grouped_subqueries",
                                        "WrapperReplacerContextUngroupedScan:false",
                                        "?input_data_source",
                                    ),
                                ),
                                "?out_join_type",
                            ),
                            // pullup(tail) just so it could be easily picked up by pullup rules
                            wrapper_pullup_replacer(
                                wrapped_select_joins_empty_tail(),
                                wrapper_replacer_context(
                                    "?left_alias_to_cube",
                                    "?left_push_to_cube",
                                    "WrapperReplacerContextInProjection:false",
                                    "?left_cube_members",
                                    "?out_grouped_subqueries",
                                    "WrapperReplacerContextUngroupedScan:false",
                                    "?input_data_source",
                                ),
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "?left_push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapper_pullup_replacer(
                            wrapped_select_order_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "?left_push_to_cube",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?out_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        "WrappedSelectAlias:None",
                        "WrappedSelectDistinct:false",
                        // left push-to-Cube dictates the resulting push-to-Cube
                        "?out_push_to_cube",
                        // both inputs are grouped, so result is grouped as well
                        "WrappedSelectUngroupedScan:false",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_grouped_join_grouped(
                    "?left_input",
                    "?left_on",
                    "?left_push_to_cube",
                    "?right_on",
                    "?in_join_type",
                    "?input_data_source",
                    "?out_join_expr",
                    "?out_join_type",
                    "?out_grouped_subqueries",
                    "?out_push_to_cube",
                ),
            ),
            // A pivot query builder emits one CTE per property, all joined to the same root
            // CTE. The rule above turns the first of those joins into a WrappedSelect; every
            // next join is added to that select's join list here, rather than nesting another
            // select around it. Nesting would repeat the whole select per join, and the number
            // of ways to represent the result grows with every level.
            //
            // The context of the left select is reused as is, including its
            // `grouped_subqueries`: that list is only read by the column rules to pull a column
            // qualified by a joined subquery up as a dimension, and only when pushing members
            // to Cube. This select does not (`WrapperReplacerContextPushToCube:false` below),
            // so the aliases of the subqueries joined here have nothing to do in that list.
            transforming_rewrite(
                "wrapper-push-down-grouped-join-grouped-chain",
                join(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            // The select built by the rule above, before anything was pushed
                            // into it: everything but `from` and `joins` is still empty
                            wrapped_select(
                                "WrappedSelectSelectType:Projection",
                                wrapped_select_projection_expr_empty_tail(),
                                wrapped_select_subqueries_empty_tail(),
                                wrapped_select_group_expr_empty_tail(),
                                wrapped_select_aggr_expr_empty_tail(),
                                wrapped_select_window_expr_empty_tail(),
                                "?left_from",
                                "?left_joins",
                                wrapped_select_filter_expr_empty_tail(),
                                wrapped_select_having_expr_empty_tail(),
                                "WrappedSelectLimit:None",
                                "WrappedSelectOffset:None",
                                wrapped_select_order_expr_empty_tail(),
                                "WrappedSelectAlias:None",
                                "WrappedSelectDistinct:false",
                                // Only a select that joins subqueries as plain SQL is extended
                                // here. A push-to-Cube select carries its joins to the Cube
                                // query as subquery joins, which are rendered under a
                                // uniqueness assumption this rule can not check
                                "WrappedSelectPushToCube:false",
                                "WrappedSelectUngroupedScan:false",
                            ),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                // A select with joins can not push anything else to Cube, so
                                // the join condition needs no member resolution and is built
                                // as plain columns below
                                "WrapperReplacerContextPushToCube:false",
                                "?left_in_projection",
                                "?left_cube_members",
                                "?left_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                    // The joined side is matched as a whole, and its shape is checked in the
                    // transform: a grouped subquery can be represented in several ways at once,
                    // and matching them here would produce a copy of this select per
                    // representation, for every join in a chain
                    "?right_wrapper",
                    "?left_on",
                    "?right_on",
                    "?in_join_type",
                    "?join_constraint",
                    "JoinNullEqualsNull:false",
                ),
                cube_scan_wrapper(
                    wrapped_select(
                        "WrappedSelectSelectType:Projection",
                        wrapper_pullup_replacer(
                            wrapped_select_projection_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:false",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?left_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_subqueries_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:false",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?left_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_group_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:false",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?left_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_aggr_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:false",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?left_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_window_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:false",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?left_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            "?left_from",
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:false",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?left_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        // The join list is built by the transform, with the new join at the end
                        // so that joins keep the order they had in the query. It is built
                        // resolved, in one node: going through push down and pull up per element
                        // would let the e-graph hold every mix of resolved and unresolved
                        // elements of the list.
                        wrapper_pullup_replacer(
                            "?out_joins",
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:false",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?left_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:false",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?left_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        wrapped_select_having_expr_empty_tail(),
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapper_pullup_replacer(
                            wrapped_select_order_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?left_alias_to_cube",
                                "WrapperReplacerContextPushToCube:false",
                                "WrapperReplacerContextInProjection:false",
                                "?left_cube_members",
                                "?left_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                "?input_data_source",
                            ),
                        ),
                        "WrappedSelectAlias:None",
                        "WrappedSelectDistinct:false",
                        "WrappedSelectPushToCube:false",
                        "WrappedSelectUngroupedScan:false",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_grouped_join_grouped_chain(
                    "?left_joins",
                    "?left_on",
                    "?right_wrapper",
                    "?right_on",
                    "?in_join_type",
                    "?input_data_source",
                    "?out_joins",
                ),
            ),
        ]);

        // DataFusion plans complex join conditions as Filter(?join_condition, CrossJoin(...))
        // Handling each and every condition in here is not that easy, so for now
        // it just handles several special cases of conditions actually generated by BI tools
        // Each condition is defined for a single pair of joined columns, like a special equals operator
        // Join condition can join on multiple columns, and per-column conditions will be joined with AND
        // Because AND is binary, we can have arbitrary binary tree, with single column condition in leaves
        // To process outer ANDs join_check_stage in introduced:
        // 1. Push down over ANDs
        // 2. Turn push down to pull up on proper condition for a single column
        // 3. Pull up results over ANDs
        // 4. Start regular wrapper replacer for join expression
        // Each side in single column condition should contain single reference to column
        // But it can contain other expressions. Most notably, it can contain CAST(column AS TEXT)
        // referenced_expr analysis is used to pick up column references during check
        // Different sides of single expression should reference different sides of CROSS JOIN, but
        // it's tricky to do without a proper name resolution, so for now it handles only qualified column expressions

        rules.extend([
            rewrite(
                "wrapper-push-down-ungrouped-join-grouped-start-condition-check",
                filter(
                    "?filter_expr",
                    cross_join(
                        cube_scan_wrapper("?left", "CubeScanWrapperFinalized:false"),
                        cube_scan_wrapper("?right", "CubeScanWrapperFinalized:false"),
                    ),
                ),
                join_check_stage(join_check_push_down(
                    "?filter_expr",
                    cube_scan_wrapper("?left", "CubeScanWrapperFinalized:false"),
                    cube_scan_wrapper("?right", "CubeScanWrapperFinalized:false"),
                )),
            ),
            rewrite(
                "ungrouped-join-grouped-condition-check-pushdown-and",
                join_check_push_down(
                    binary_expr("?left_expr", "AND", "?right_expr"),
                    "?left_input",
                    "?right_input",
                ),
                binary_expr(
                    join_check_push_down("?left_expr", "?left_input", "?right_input"),
                    "AND",
                    join_check_push_down("?right_expr", "?left_input", "?right_input"),
                ),
            ),
            rewrite(
                "ungrouped-join-grouped-condition-check-pull-up-and",
                binary_expr(
                    join_check_pull_up("?left_expr", "?left_input", "?right_input"),
                    "AND",
                    join_check_pull_up("?right_expr", "?left_input", "?right_input"),
                ),
                join_check_pull_up(
                    binary_expr("?left_expr", "AND", "?right_expr"),
                    "?left_input",
                    "?right_input",
                ),
            ),
            transforming_rewrite(
                "wrapper-push-down-ungrouped-join-grouped-finish-condition-check",
                join_check_stage(join_check_pull_up(
                    "?join_expr",
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
                                // Data sources must match for both sides
                                // TODO support unrestricted data source on one side
                                "?input_data_source",
                            ),
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?right_input",
                            wrapper_replacer_context(
                                // Going to ignore this in RHS
                                "?right_alias_to_cube",
                                "?right_push_to_cube",
                                "?right_in_projection",
                                // Going to ignore this
                                "?right_cube_members",
                                "?right_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
                                // Data sources must match for both sides
                                // TODO support unrestricted data source on one side
                                "?input_data_source",
                            ),
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                )),
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
                                "WrapperReplacerContextUngroupedScan:true",
                                "?input_data_source",
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
                                "?input_data_source",
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
                                "?input_data_source",
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
                                "?input_data_source",
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
                                "?input_data_source",
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
                                "?input_data_source",
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
                                        "?input_data_source",
                                    ),
                                ),
                                wrapper_pushdown_replacer(
                                    "?join_expr",
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
                                        "?input_data_source",
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
                                    "?input_data_source",
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
                                "?input_data_source",
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
                                "?input_data_source",
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
                self.transform_ungrouped_join_grouped_after_check(
                    "?right_input",
                    "?right_alias_to_cube",
                    "?out_join_type",
                    "?out_grouped_subqueries",
                ),
            ),
        ]);

        let complex_join_conditions = [
            // This variant is necessary to allow rewrites when join condition is something like this:
            // CAST(left AS TEXT) = right
            // DF will plan those as Filter(CrossJoin) as well, but joining operator is just `=`
            ("equal", binary_expr("?left_expr", "=", "?right_expr")),
            (
                "coalesce",
                Self::coalesce_join_condition("?left_expr", "?right_expr", "?coalesce_value"),
            ),
            (
                "distinct",
                Self::distinct_join_condition("?left_expr", "?right_expr"),
            ),
        ];

        for (name, pattern) in complex_join_conditions {
            rules.push(transforming_rewrite(
                &format!("ungrouped-join-grouped-condition-check-condition-{name}"),
                join_check_push_down(
                    &pattern,
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
                                // Data sources must match for both sides
                                // TODO support unrestricted data source on one side
                                "?input_data_source",
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
                                // Data sources must match for both sides
                                // TODO support unrestricted data source on one side
                                "?input_data_source",
                            ),
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                ),
                join_check_pull_up(
                    &pattern,
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
                                "?input_data_source",
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
                                "?input_data_source",
                            ),
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                ),
                self.transform_ungrouped_join_grouped_check_condition(
                    "?left_cube_members",
                    "?left_expr",
                    "?right_expr",
                ),
            ));
        }

        // TODO only pullup is necessary here
        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-joins",
            "WrappedSelectJoins",
            "WrappedSelectJoins",
        );
    }

    // JOIN ... ON (coalesce(left.foo, '') = coalesce(right.foo, '')) and ((left.foo is not null) = (right.foo is not null))
    fn coalesce_join_condition(left_expr: &str, right_expr: &str, coalesce_value: &str) -> String {
        binary_expr(
            binary_expr(
                fun_expr("Coalesce", vec![left_expr, coalesce_value], true),
                "=",
                fun_expr("Coalesce", vec![right_expr, coalesce_value], true),
            ),
            "AND",
            binary_expr(
                is_not_null_expr(left_expr),
                "=",
                is_not_null_expr(right_expr),
            ),
        )
    }

    // JOIN ... ON left.foo IS NOT DISTINCT FROM right.foo
    fn distinct_join_condition(left_expr: &str, right_expr: &str) -> String {
        binary_expr(left_expr, "IS_NOT_DISTINCT_FROM", right_expr)
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
            .zip(right_join_on)
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

    /// Whether a join subquery with `join_type` can be pushed down to `data_source_var`.
    ///
    /// Inner/Left are always supported. Right/Full are only supported on the non-push-to-Cube
    /// path (`push_to_cube == false`), i.e. when both sides become standalone subqueries joined
    /// together — there the outer-join semantics map directly to SQL. On the push-to-Cube path
    /// the join is folded inside the Cube query alongside its grouping/measures, where NULL-extended
    /// outer rows are not validated, so Right/Full are refused there.
    /// Other join types (semi/anti) are never supported as join subqueries.
    fn is_subquery_join_type_supported(
        egraph: &CubeEGraph,
        subst: &mut Subst,
        meta: &MetaContext,
        data_source_var: Var,
        join_type: &JoinType,
        push_to_cube: bool,
    ) -> bool {
        let template = match join_type {
            JoinType::Inner => "join_types/inner",
            JoinType::Left => "join_types/left",
            JoinType::Right if !push_to_cube => "join_types/right",
            JoinType::Full if !push_to_cube => "join_types/full",
            _ => return false,
        };
        let Ok(data_source) = Self::get_data_source(egraph, subst, data_source_var) else {
            return false;
        };
        Self::can_rewrite_template(&data_source, meta, template)
    }

    fn transform_ungrouped_join_grouped(
        &self,
        right_input_var: &'static str,
        left_members_var: &'static str,
        left_on_var: &'static str,
        right_on_var: &'static str,
        in_join_type_var: &'static str,
        input_data_source_var: &'static str,
        out_join_expr_var: &'static str,
        out_join_type_var: &'static str,
        out_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let right_input_var = var!(right_input_var);
        let left_members_var = var!(left_members_var);
        let left_on_var = var!(left_on_var);

        let right_on_var = var!(right_on_var);

        let in_join_type_var = var!(in_join_type_var);
        let input_data_source_var = var!(input_data_source_var);

        let out_join_expr_var = var!(out_join_expr_var);
        let out_join_type_var = var!(out_join_type_var);
        let out_grouped_subqueries_var = var!(out_grouped_subqueries_var);

        let meta = self.meta_context.clone();

        // Only left is allowed to be ungrouped query, so right would be a subquery join for left ungrouped CubeScan
        // It means we don't care about just a "single cube" in LHS, and there's essentially no cubes by this moment in RHS

        move |egraph, subst| {
            if !Self::can_be_subquery_join(egraph, subst[right_input_var]) {
                return false;
            }

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
                        // Left is an ungrouped CubeScan pushed to Cube, so this is always the
                        // push-to-Cube path: Right/Full are not supported here.
                        if !Self::is_subquery_join_type_supported(
                            egraph,
                            subst,
                            &meta,
                            input_data_source_var,
                            &in_join_type.0,
                            true,
                        ) {
                            return false;
                        }

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

    fn transform_ungrouped_join_grouped_check_condition(
        &self,
        left_members_var: &'static str,
        left_expr_var: &'static str,
        right_expr_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let left_members_var = var!(left_members_var);
        let left_expr_var = var!(left_expr_var);

        let right_expr_var = var!(right_expr_var);

        // Only left is allowed to be ungrouped query, so right would be a subquery join for left ungrouped CubeScan
        // It means we don't care about just a "single cube" in LHS, and there's essentially no cubes by this moment in RHS

        move |egraph, subst| {
            // We are going to generate join with grouped subquery
            // TODO Do we have to check stuff like `transform_check_subquery_allowed` is checking:
            // * Both inputs depend on a single data source
            // * SQL generator for that data source have `expressions/subquery` template
            // It could be checked later, in WrappedSelect as well
            // TODO For views: check that each member is coming from same data source (or even cube?)

            let prepare_columns = |var| {
                let columns = egraph[subst[var]].data.referenced_expr.as_ref();
                let Some(columns) = columns else {
                    return Err("Missing referenced_expr");
                };
                let columns = columns
                    .iter()
                    .map(|column| {
                        let column = match column {
                            Expr::Column(column) => column.clone(),
                            _ => return Err("Unexpected expression in referenced_expr"),
                        };
                        Ok(column)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(columns)
            };

            fn prepare_relation(columns: &[Column]) -> Result<&str, &'static str> {
                let relation = columns
                    .iter()
                    .map(|column| &column.relation)
                    .all_equal_value();
                let Ok(Some(relation)) = relation else {
                    // Outer Err means there's either no values at all, or more than one different value
                    // Inner Err means that all referenced_expr are not columns
                    // Inner None means that all columns are without relation, don't support that ATM
                    return Err("Relation mismatch");
                };
                Ok(relation)
            }

            let Ok(left_columns) = prepare_columns(left_expr_var) else {
                return false;
            };
            let Ok(left_relation) = prepare_relation(&left_columns) else {
                return false;
            };

            let Ok(right_columns) = prepare_columns(right_expr_var) else {
                return false;
            };
            let Ok(right_relation) = prepare_relation(&right_columns) else {
                return false;
            };

            // Simple check that column expressions reference different join sides
            if left_relation == right_relation {
                return false;
            }

            // Don't check right, as it is already grouped

            if !Self::are_join_members_supported(
                egraph,
                subst[left_members_var],
                left_columns.iter(),
            ) {
                return false;
            }

            // TODO check that right column is coming from right crossjoin input

            return true;
        }
    }

    fn transform_ungrouped_join_grouped_after_check(
        &self,
        right_input_var: &'static str,
        right_alias_to_cube_var: &'static str,
        out_join_type_var: &'static str,
        out_grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let right_input_var = var!(right_input_var);
        let right_alias_to_cube_var = var!(right_alias_to_cube_var);
        let out_join_type_var = var!(out_join_type_var);
        let out_grouped_subqueries_var = var!(out_grouped_subqueries_var);

        move |egraph, subst| {
            if !Self::can_be_subquery_join(egraph, subst[right_input_var]) {
                return false;
            }

            for right_alias_to_cube in var_iter!(
                egraph[subst[right_alias_to_cube_var]],
                WrapperReplacerContextAliasToCube
            ) {
                if right_alias_to_cube.len() != 1 {
                    return false;
                }

                let right_alias = &right_alias_to_cube[0].0;
                // LHS is ungrouped, RHS is grouped
                // Don't pass ungrouped queries from below, their qualifiers should not be accessible during join condition rewrite
                let out_grouped_subqueries = vec![right_alias.clone()];

                // TODO why fixed to inner? Check how left join in input is planned
                let out_join_type = JoinType::Inner;

                subst.insert(
                    out_join_type_var,
                    egraph.add(LogicalPlanLanguage::WrappedSelectJoinJoinType(
                        WrappedSelectJoinJoinType(out_join_type),
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

            return false;
        }
    }

    fn transform_grouped_join_grouped(
        &self,
        left_input_var: &'static str,
        left_on_var: &'static str,
        left_push_to_cube_var: &'static str,
        right_on_var: &'static str,
        in_join_type_var: &'static str,
        input_data_source_var: &'static str,
        out_join_expr_var: &'static str,
        out_join_type_var: &'static str,
        out_grouped_subqueries_var: &'static str,
        out_push_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let left_input_var = var!(left_input_var);
        let left_on_var = var!(left_on_var);
        let left_push_to_cube_var = var!(left_push_to_cube_var);

        let right_on_var = var!(right_on_var);

        let in_join_type_var = var!(in_join_type_var);
        let input_data_source_var = var!(input_data_source_var);

        let out_join_expr_var = var!(out_join_expr_var);
        let out_join_type_var = var!(out_join_type_var);
        let out_grouped_subqueries_var = var!(out_grouped_subqueries_var);
        let out_push_to_cube_var = var!(out_push_to_cube_var);

        let meta = self.meta_context.clone();

        move |egraph, subst| {
            // Joins on top of a select that already has joins are handled by
            // `wrapper-push-down-grouped-join-grouped-chain`, which keeps them in a single
            // select instead of nesting one select per join
            if Self::select_has_joins(egraph, subst[left_input_var]) {
                return false;
            }

            // We are going to generate join with grouped subquery
            // TODO Do we have to check stuff like `transform_check_subquery_allowed` is checking:
            // * Both inputs depend on a single data source
            // * SQL generator for that data source have `expressions/subquery` template
            // It could be checked later, in WrappedSelect as well

            for left_join_on in var_iter!(egraph[subst[left_on_var]], JoinLeftOn) {
                for right_join_on in var_iter!(egraph[subst[right_on_var]], JoinRightOn) {
                    // Don't check left and right, they are already grouped

                    for in_join_type in
                        var_list_iter!(egraph[subst[in_join_type_var]], JoinJoinType).cloned()
                    {
                        for left_push_to_cube in var_list_iter!(
                            egraph[subst[left_push_to_cube_var]],
                            WrapperReplacerContextPushToCube
                        )
                        .cloned()
                        {
                            // Right/Full are only supported on the non-push-to-Cube variant.
                            // `continue` rather than `return false` so the non-push variant of
                            // this eclass still gets a chance to match.
                            if !Self::is_subquery_join_type_supported(
                                egraph,
                                subst,
                                &meta,
                                input_data_source_var,
                                &in_join_type.0,
                                left_push_to_cube.0,
                            ) {
                                continue;
                            }

                            // TODO what's a proper way to find table expression alias?
                            let Some(right_join_alias) = right_join_on
                                .iter()
                                .filter_map(|c| c.relation.as_ref())
                                .next()
                                .cloned()
                            else {
                                return false;
                            };

                            let Some(out_join_expr) = Self::build_join_expr(
                                egraph,
                                left_join_on.clone(),
                                right_join_on.clone(),
                            ) else {
                                return false;
                            };

                            // LHS is grouped, RHS is grouped
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
                                        WrapperReplacerContextGroupedSubqueries(
                                            out_grouped_subqueries,
                                        ),
                                    ),
                                ),
                            );
                            subst.insert(
                                out_push_to_cube_var,
                                egraph.add(LogicalPlanLanguage::WrappedSelectPushToCube(
                                    WrappedSelectPushToCube(left_push_to_cube.0),
                                )),
                            );

                            return true;
                        }
                    }
                }
            }

            return false;
        }
    }

    /// Whether `input` can be joined to a Cube query as a subquery join.
    ///
    /// A subquery join is rendered by the schema compiler in two ways - counted into the
    /// measures over the joined rowset, or computed per distinct primary key - which agree
    /// only when the joined subquery is unique on the join keys. A select that is itself a
    /// join of subqueries has no such guarantee, and nothing here can check it, so it is
    /// refused: the query then plans without pushing the join into the Cube query, or fails
    /// with an explicit error, instead of returning numbers that depend on how a measure
    /// happens to be classified.
    fn can_be_subquery_join(egraph: &CubeEGraph, input: Id) -> bool {
        !Self::joins_subqueries(egraph, input, &mut HashSet::new())
    }

    /// Whether `input` produces its rows by joining subqueries, looking through the selects
    /// stacked on top of the join, and through replacers when a select below is not pulled up
    /// yet. A grouping in between is where the search stops: aggregation collapses the rows a
    /// join below it could have duplicated. Note that this does not make the result unique on
    /// the join keys - that also needs the join keys to cover the group keys, which nothing
    /// here checks (TODO: check key coverage, the pre-existing subquery join path needs it as
    /// well).
    ///
    /// `visited` keeps the walk linear and terminating: e-classes hold many equivalent nodes
    /// pointing at shared children, and unions can make the graph cyclic.
    fn joins_subqueries(egraph: &CubeEGraph, input: Id, visited: &mut HashSet<Id>) -> bool {
        if !visited.insert(egraph.find(input)) {
            // Already walked, on this path or another one
            return false;
        }

        for select in var_list_iter!(egraph[input], WrappedSelect) {
            let (Some(select_type), Some(from), Some(joins)) = (
                select.get(WRAPPED_SELECT_SELECT_TYPE),
                select.get(WRAPPED_SELECT_FROM),
                select.get(WRAPPED_SELECT_JOINS),
            ) else {
                continue;
            };

            // Joins of this select, whatever it does with their rows
            if var_list_iter!(egraph[*joins], WrappedSelectJoins).any(|joins| !joins.is_empty()) {
                return true;
            }

            // An aggregation collapses the rows a join below it could have duplicated, so the
            // search stops there. Every node of the e-class has to agree that this is one:
            // a class holding an Aggregate next to something else would otherwise hide the
            // join below from the rest of the walk.
            let mut select_types = var_iter!(egraph[*select_type], WrappedSelectSelectType);
            let aggregate = select_types
                .next()
                .is_some_and(|select_type| matches!(select_type, WrappedSelectType::Aggregate))
                && select_types
                    .all(|select_type| matches!(select_type, WrappedSelectType::Aggregate));
            if aggregate {
                continue;
            }

            if Self::joins_subqueries(egraph, *from, visited) {
                return true;
            }
        }

        // A select that is still wrapped in a replacer hides the same structure one node deeper
        for replacer in var_list_iter!(egraph[input], WrapperPullupReplacer)
            .chain(var_list_iter!(egraph[input], WrapperPushdownReplacer))
        {
            let Some(member) = replacer.first() else {
                continue;
            };
            if Self::joins_subqueries(egraph, *member, visited) {
                return true;
            }
        }

        false
    }

    /// Whether `input` is a select that already carries joins.
    fn select_has_joins(egraph: &CubeEGraph, input: Id) -> bool {
        for select in var_list_iter!(egraph[input], WrappedSelect) {
            let Some(joins) = select.get(WRAPPED_SELECT_JOINS) else {
                continue;
            };
            if var_list_iter!(egraph[*joins], WrappedSelectJoins).any(|joins| !joins.is_empty()) {
                return true;
            }
        }

        false
    }

    fn transform_grouped_join_grouped_chain(
        &self,
        left_joins_var: &'static str,
        left_on_var: &'static str,
        right_wrapper_var: &'static str,
        right_on_var: &'static str,
        in_join_type_var: &'static str,
        input_data_source_var: &'static str,
        out_joins_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let left_joins_var = var!(left_joins_var);
        let left_on_var = var!(left_on_var);
        let right_wrapper_var = var!(right_wrapper_var);
        let right_on_var = var!(right_on_var);
        let in_join_type_var = var!(in_join_type_var);
        let input_data_source_var = var!(input_data_source_var);
        let out_joins_var = var!(out_joins_var);

        let meta = self.meta_context.clone();

        move |egraph, subst| {
            // Only extend a select that is already joining something: a select without joins
            // is the job of `wrapper-push-down-grouped-join-grouped`
            if !var_list_iter!(egraph[subst[left_joins_var]], WrappedSelectJoins)
                .any(|joins| !joins.is_empty())
            {
                return false;
            }

            let Some(right_input) = Self::grouped_join_right_input(
                egraph,
                subst[right_wrapper_var],
                subst[input_data_source_var],
            ) else {
                return false;
            };

            for left_join_on in var_iter!(egraph[subst[left_on_var]], JoinLeftOn) {
                for right_join_on in var_iter!(egraph[subst[right_on_var]], JoinRightOn) {
                    for in_join_type in
                        var_list_iter!(egraph[subst[in_join_type_var]], JoinJoinType).cloned()
                    {
                        // The select this join is added to is not pushing anything to Cube:
                        // it is a join of grouped subqueries, where every join type maps to
                        // SQL directly
                        if !Self::is_subquery_join_type_supported(
                            egraph,
                            subst,
                            &meta,
                            input_data_source_var,
                            &in_join_type.0,
                            false,
                        ) {
                            continue;
                        }

                        let Some(out_join_expr) = Self::build_join_expr(
                            egraph,
                            left_join_on.clone(),
                            right_join_on.clone(),
                        ) else {
                            return false;
                        };

                        let join_type = egraph.add(LogicalPlanLanguage::WrappedSelectJoinJoinType(
                            WrappedSelectJoinJoinType(in_join_type.0),
                        ));
                        let join = egraph.add(LogicalPlanLanguage::WrappedSelectJoin([
                            right_input,
                            out_join_expr,
                            join_type,
                        ]));

                        let Some(out_joins) =
                            Self::append_join(egraph, subst[left_joins_var], join)
                        else {
                            return false;
                        };

                        subst.insert(out_joins_var, out_joins);

                        return true;
                    }
                }
            }

            false
        }
    }

    /// Add `join` to the end of the `joins` list, keeping the order of the joins already
    /// there: a join condition can refer to anything joined before it, but not after.
    fn append_join(egraph: &mut CubeEGraph, joins: Id, join: Id) -> Option<Id> {
        // Unions can make an e-graph list cyclic, and a rule transform must always return
        let max_joins = 64;

        let mut list = vec![];
        let mut current = joins;
        loop {
            if list.len() >= max_joins {
                return None;
            }

            let mut nodes = var_list_iter!(egraph[current], WrappedSelectJoins).cloned();
            let node = nodes.next()?;
            // A single list must have a single representation, otherwise it is not clear
            // which one the join is added to
            if nodes.next().is_some() {
                return None;
            }
            match node.as_slice() {
                [] => break,
                [head, tail] => {
                    list.push(*head);
                    current = *tail;
                }
                _ => return None,
            }
        }

        let mut result = egraph.add(LogicalPlanLanguage::WrappedSelectJoins(vec![]));
        result = egraph.add(LogicalPlanLanguage::WrappedSelectJoins(vec![join, result]));
        for head in list.into_iter().rev() {
            result = egraph.add(LogicalPlanLanguage::WrappedSelectJoins(vec![head, result]));
        }

        Some(result)
    }

    /// The plan to join, taken out of an unfinalized wrapper over a grouped subquery from the
    /// same data source. A grouped subquery can sit in the e-graph in several shapes at once
    /// (a Cube query, a select over it, ...), all interchangeable here, so one is picked
    /// rather than joined once per shape: the plain Cube query when there is one, so that the
    /// generated SQL keeps the fewest levels of nesting.
    fn grouped_join_right_input(
        egraph: &CubeEGraph,
        right_wrapper: Id,
        input_data_source: Id,
    ) -> Option<Id> {
        let mut candidates = vec![];

        for wrapper in var_list_iter!(egraph[right_wrapper], CubeScanWrapper) {
            let [input, finalized] = wrapper.as_slice() else {
                continue;
            };
            if !var_iter!(egraph[*finalized], CubeScanWrapperFinalized).any(|f| !f) {
                continue;
            }

            for pullup in var_list_iter!(egraph[*input], WrapperPullupReplacer) {
                let [member, context] = pullup.as_slice() else {
                    continue;
                };

                for replacer_context in var_list_iter!(egraph[*context], WrapperReplacerContext) {
                    // (alias_to_cube, push_to_cube, in_projection, cube_members,
                    //  grouped_subqueries, ungrouped_scan, input_data_source)
                    let [.., ungrouped_scan, data_source] = replacer_context.as_slice() else {
                        continue;
                    };
                    // Grouped on both sides, and the same data source, like the rules that
                    // match those checks in their pattern
                    if !var_iter!(egraph[*ungrouped_scan], WrapperReplacerContextUngroupedScan)
                        .any(|ungrouped| !ungrouped)
                    {
                        continue;
                    }
                    if *data_source != input_data_source {
                        continue;
                    }

                    candidates.push(*member);
                }
            }
        }

        // Every candidate renders the same rows, so the pick is only about the shape of the
        // generated SQL: a plain Cube query keeps it one level shallower than a select over
        // one. Beyond that the choice is arbitrary, and sorting only makes it repeatable
        // between runs.
        candidates.sort();
        candidates.dedup();
        candidates
            .iter()
            .find(|member| Self::is_cube_scan(egraph, **member))
            .or_else(|| candidates.first())
            .copied()
    }

    fn is_cube_scan(egraph: &CubeEGraph, id: Id) -> bool {
        var_list_iter!(egraph[id], CubeScan).next().is_some()
    }
}
