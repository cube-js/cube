use crate::{
    compile::rewrite::{
        analysis::Member, binary_expr, cross_join, cube_scan_wrapper, filter, fun_expr,
        is_not_null_expr, join, join_check_pull_up, join_check_push_down, join_check_stage,
        rewrite, rewriter::CubeRewrite, rules::wrapper::WrapperRules,
        transforming_list_rewrite_with_lists_and_vars, transforming_rewrite, wrapped_select,
        wrapped_select_aggr_expr_empty_tail, wrapped_select_filter_expr_empty_tail,
        wrapped_select_group_expr_empty_tail, wrapped_select_having_expr_empty_tail,
        wrapped_select_join, wrapped_select_joins, wrapped_select_order_expr_empty_tail,
        wrapped_select_projection_expr_empty_tail, wrapped_select_subqueries_empty_tail,
        wrapped_select_window_expr_empty_tail, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context, BinaryExprOp, ColumnExprColumn, CubeEGraph, JoinLeftOn,
        JoinRightOn, ListApplierListPattern, ListPattern, ListType, LogicalPlanLanguage,
        WrappedSelectJoinJoinType, WrappedSelectPushToCube, WrapperReplacerContextAliasToCube,
        WrapperReplacerContextGroupedSubqueries,
    },
    transport::MetaContext,
    var, var_iter, var_list_iter,
};

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
                        wrapped_select_joins(vec![wrapped_select_join(
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
                        )]),
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
                        wrapped_select_joins(vec![wrapped_select_join(
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
                        )]),
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
        ]);

        // A pivot query builder emits one CTE per property, all joined to the same root CTE.
        // The rule above turns the first of those joins into a WrappedSelect; every next join
        // is added to that select's join list here, rather than nesting another select around
        // it. The nesting is what has to be avoided, not undone: an e-graph only ever adds, so
        // a rule that flattens a stack of selects afterwards leaves every level of it in place
        // to be matched by everything else. Measured on a chain of joins, each level roughly
        // doubles the graph and six joins run out of nodes, whether or not such a rule is
        // there; keeping the joins in one select stays linear.
        //
        // The context of the left select is reused as is, including its `grouped_subqueries`:
        // that list is only read by the column rules to pull a column qualified by a joined
        // subquery up as a dimension, and only when pushing members to Cube. This select does
        // not (`WrapperReplacerContextPushToCube:false` below), so the aliases of the
        // subqueries joined here have nothing to do in that list.

        // Everything the extended select carries over from the one it replaces. The join it
        // gains changes nothing about it: it still pushes nothing to Cube and stays grouped.
        let chain_out_context = wrapper_replacer_context(
            "?left_alias_to_cube",
            "WrapperReplacerContextPushToCube:false",
            "WrapperReplacerContextInProjection:false",
            "?left_cube_members",
            "?left_grouped_subqueries",
            "WrapperReplacerContextUngroupedScan:false",
            "?input_data_source",
        );
        rules.push(transforming_list_rewrite_with_lists_and_vars(
            "wrapper-push-down-grouped-join-grouped-chain",
            ListType::WrappedSelectJoins,
            ListPattern {
                pattern: join(
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
                                // A select without joins is the job of
                                // `wrapper-push-down-grouped-join-grouped`, so the list needs
                                // at least one element - see `min_elements` below
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
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?right_input",
                            wrapper_replacer_context(
                                "?right_alias_to_cube",
                                "?right_push_to_cube",
                                "?right_in_projection",
                                "?right_cube_members",
                                "?right_grouped_subqueries",
                                "WrapperReplacerContextUngroupedScan:false",
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
                list_var: "?left_joins".to_string(),
                elem: "?left_join".to_string(),
            },
            &cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Projection",
                    wrapper_pullup_replacer(
                        wrapped_select_projection_expr_empty_tail(),
                        &chain_out_context,
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_subqueries_empty_tail(),
                        &chain_out_context,
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_group_expr_empty_tail(),
                        &chain_out_context,
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_aggr_expr_empty_tail(),
                        &chain_out_context,
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        &chain_out_context,
                    ),
                    wrapper_pullup_replacer("?left_from", &chain_out_context),
                    wrapper_pullup_replacer("?merged_joins", &chain_out_context),
                    wrapper_pullup_replacer(
                        wrapped_select_filter_expr_empty_tail(),
                        &chain_out_context,
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        &chain_out_context,
                    ),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "WrappedSelectPushToCube:false",
                    "WrappedSelectUngroupedScan:false",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            // The new join goes after the ones already there: a join condition can refer to a
            // subquery joined earlier, so the order the query joins them in is part of what it
            // means. This is what the flat list buys - the whole list is matched and rebuilt
            // one element longer, where a cons list could only be prepended to.
            [ListApplierListPattern::new(
                ListType::WrappedSelectJoins,
                "?merged_joins",
                "?left_join",
            )
            .with_appended([wrapped_select_join(
                // The wrapper of the joined side goes into the join, not the plan inside it: a
                // grouped subquery is represented by several equivalent plans at once, all
                // sharing that wrapper, so every match builds the same join here and which
                // plan is rendered is left to extraction. Referencing the plan directly builds
                // one join per representation, and those multiply with every join in a chain.
                //
                // Extraction reads that choice through `empty_wrappers`, which counts a wrapper
                // with no `WrappedSelect` under it, so it takes the select over the Cube query
                // rather than the Cube query itself and the joined side renders one level
                // deeper than it has to. That is the price of leaving the choice to extraction;
                // the plans that win carry no empty wrappers, so the count keeps saying what it
                // says elsewhere.
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        "?right_input",
                        wrapper_replacer_context(
                            "?right_alias_to_cube",
                            "?right_push_to_cube",
                            "?right_in_projection",
                            "?right_cube_members",
                            "?right_grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source",
                        ),
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                "?out_join_expr",
                "?out_join_type",
            )])],
            // Nothing has to hold across the elements of the list: they are finished joins
            &[],
            // A select without joins is not extended here
            1,
            &["?out_join_expr", "?out_join_type"],
            self.transform_grouped_join_grouped_chain(
                "?left_on",
                "?right_on",
                "?in_join_type",
                "?input_data_source",
                "?out_join_expr",
                "?out_join_type",
            ),
        ));

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
                        wrapped_select_joins(vec![wrapped_select_join(
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
                        )]),
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
        Self::flat_list_pushdown_pullup_rules(
            rules,
            "wrapper-joins",
            ListType::WrappedSelectJoins,
            ListType::WrappedSelectJoins,
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
            // A join of subqueries is not unique on its join keys, so it can not be handed
            // to a Cube query as a subquery join - see `LogicalPlanData::joins_subqueries`
            if egraph[subst[right_input_var]].data.joins_subqueries {
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
            // A join of subqueries is not unique on its join keys, so it can not be handed
            // to a Cube query as a subquery join - see `LogicalPlanData::joins_subqueries`
            if egraph[subst[right_input_var]].data.joins_subqueries {
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
            if egraph[subst[left_input_var]].data.select_with_joins {
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

    /// Everything this rule needs beyond its pattern: the join condition, which is built from
    /// the join keys of the plan, and the join type, which has to be checked against the data
    /// source before it can be used. Both are the same work the rule this extends does.
    fn transform_grouped_join_grouped_chain(
        &self,
        left_on_var: &'static str,
        right_on_var: &'static str,
        in_join_type_var: &'static str,
        input_data_source_var: &'static str,
        out_join_expr_var: &'static str,
        out_join_type_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let left_on_var = var!(left_on_var);
        let right_on_var = var!(right_on_var);
        let in_join_type_var = var!(in_join_type_var);
        let input_data_source_var = var!(input_data_source_var);
        let out_join_expr_var = var!(out_join_expr_var);
        let out_join_type_var = var!(out_join_type_var);

        let meta = self.meta_context.clone();

        move |egraph, subst| {
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

                        subst.insert(out_join_expr_var, out_join_expr);
                        subst.insert(
                            out_join_type_var,
                            egraph.add(LogicalPlanLanguage::WrappedSelectJoinJoinType(
                                WrappedSelectJoinJoinType(in_join_type.0),
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
