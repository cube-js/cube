use crate::{
    compile::rewrite::{
        analysis::{Member, OriginalExpr},
        between_expr, binary_expr, case_expr_var_arg, cast_expr, column_expr, cube_scan,
        cube_scan_filters, cube_scan_wrapper, expr_column_name, filter,
        filter_simplify_push_down_replacer, filter_split_pull_up, filter_split_push_down,
        filter_split_stage, fun_expr_var_arg, inlist_expr, insubquery_expr, is_not_null_expr,
        is_null_expr, like_expr, literal_bool, literal_expr, not_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        subquery, transforming_rewrite, udf_expr_var_arg, wrapped_select,
        wrapped_select_aggr_expr_empty_tail, wrapped_select_filter_expr,
        wrapped_select_filter_expr_empty_tail, wrapped_select_group_expr_empty_tail,
        wrapped_select_having_expr_empty_tail, wrapped_select_joins_empty_tail,
        wrapped_select_order_expr_empty_tail, wrapped_select_projection_expr_empty_tail,
        wrapped_select_subqueries_empty_tail, wrapped_select_window_expr_empty_tail,
        wrapper_pullup_replacer, wrapper_pushdown_replacer, wrapper_replacer_context, BinaryExprOp,
        WrappedSelectPushToCube, WrappedSelectUngroupedScan, WrapperReplacerContextPushToCube,
        WrapperReplacerContextUngroupedScan,
    },
    copy_flag,
    transport::{CubeMetaDimension, MetaContext, V1CubeMetaDimensionExt},
    var, var_iter,
};
use datafusion::{
    arrow::datatypes::DataType,
    logical_plan::{Expr, Operator},
};
use egg::{Id, Subst, Var};
use std::ops::IndexMut;

impl WrapperRules {
    pub fn filter_rules(&self, rules: &mut Vec<CubeRewrite>) {
        // TODO respect having filter for push down to wrapped select
        // rules.extend(vec![rewrite(
        //     "wrapper-push-down-filter-to-wrapped-select",
        //     filter(
        //         "?filter_expr",
        //         cube_scan_wrapper(
        //             wrapper_pullup_replacer(
        //                 wrapped_select(
        //                     "?select_type",
        //                     "?projection_expr",
        //                     "?group_expr",
        //                     "?aggr_expr",
        //                     "?window_expr",
        //                     "?cube_scan_input",
        //                     "?joins",
        //                     "?old_filter_expr",
        //                     "?having_expr",
        //                     "?wrapped_select_limit",
        //                     "?wrapped_select_offset",
        //                     "?order_expr",
        //                     "?select_alias",
        //                     "?select_ungrouped",
        //                 ),
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             "CubeScanWrapperFinalized:false".to_string(),
        //         ),
        //     ),
        //     cube_scan_wrapper(
        //         wrapped_select(
        //             "?select_type",
        //             wrapper_pullup_replacer(
        //                 "?projection_expr",
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             wrapper_pullup_replacer(
        //                 "?group_expr",
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             wrapper_pullup_replacer(
        //                 "?aggr_expr",
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             wrapper_pullup_replacer(
        //                 "?window_expr",
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             wrapper_pullup_replacer(
        //                 "?cube_scan_input",
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             "?joins",
        //             wrapped_select_filter_expr(
        //                 wrapper_pullup_replacer(
        //                     "?old_filter_expr",
        //                     "?alias_to_cube",
        //                     "?ungrouped",
        //                     "?cube_members",
        //                 ),
        //                 wrapper_pushdown_replacer(
        //                     "?filter_expr",
        //                     "?alias_to_cube",
        //                     "?ungrouped",
        //                     "?cube_members",
        //                 ),
        //             ),
        //             "?having_expr",
        //             "?wrapped_select_limit",
        //             "?wrapped_select_offset",
        //             wrapper_pullup_replacer(
        //                 "?order_expr",
        //                 "?alias_to_cube",
        //                 "?ungrouped",
        //                 "?cube_members",
        //             ),
        //             "?select_alias",
        //             "?select_ungrouped",
        //         ),
        //         "CubeScanWrapperFinalized:false",
        //     ),
        // )]);

        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-filter-to-cube-scan",
            filter(
                "?filter_expr",
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
            ),
            cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Projection",
                    wrapper_pullup_replacer(
                        wrapped_select_projection_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_subqueries_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_group_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_aggr_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_joins_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapped_select_filter_expr(
                        wrapper_pushdown_replacer(
                            "?filter_expr",
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "?in_projection",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "?in_projection",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "?select_push_to_cube",
                    "?select_ungrouped_scan",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_filter(
                "?push_to_cube",
                "?ungrouped_scan",
                "?select_push_to_cube",
                "?select_ungrouped_scan",
            ),
        )]);

        Self::list_pushdown_pullup_rules(
            rules,
            "wrapper-filter-expr",
            "WrappedSelectFilterExpr",
            "WrappedSelectFilterExpr",
        );
    }

    /// A filter over a scan pushed to Cube keeps in SQL only what Cube filters can't say: its
    /// other top-level `AND` conjuncts go to the scan, where the filter rules make them member
    /// filters and date ranges, as on the CubeScan path. The select with all of it in SQL stays
    pub fn filter_split_rules(&self, rules: &mut Vec<CubeRewrite>) {
        let scan = |filters: String| {
            cube_scan(
                "?scan_alias_to_cube",
                "?members",
                filters,
                "?order",
                "CubeScanLimit:None",
                "CubeScanOffset:None",
                "?split",
                "?can_pushdown_join",
                "?wrapped",
                "CubeScanUngrouped:true",
                "?join_hints",
            )
        };
        let split_scan = || {
            scan(cube_scan_filters(
                "?filters",
                filter_simplify_push_down_replacer("?push"),
            ))
        };
        let context = |in_projection: &str, ungrouped_scan: &str| {
            wrapper_replacer_context(
                "?alias_to_cube",
                "WrapperReplacerContextPushToCube:true",
                in_projection,
                "?cube_members",
                "?grouped_subqueries",
                ungrouped_scan,
                "?input_data_source",
            )
        };
        let select = |context: &str,
                      subqueries: String,
                      from: String,
                      joins: String,
                      filter: String| {
            cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Projection",
                    wrapper_pullup_replacer(wrapped_select_projection_expr_empty_tail(), context),
                    subqueries,
                    wrapper_pullup_replacer(wrapped_select_group_expr_empty_tail(), context),
                    wrapper_pullup_replacer(wrapped_select_aggr_expr_empty_tail(), context),
                    wrapper_pullup_replacer(wrapped_select_window_expr_empty_tail(), context),
                    wrapper_pullup_replacer(from, context),
                    wrapper_pullup_replacer(joins, context),
                    filter,
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(wrapped_select_order_expr_empty_tail(), context),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "WrappedSelectPushToCube:true",
                    "WrappedSelectUngroupedScan:true",
                ),
                "CubeScanWrapperFinalized:false",
            )
        };
        let no_subqueries = |context: &str| {
            wrapper_pullup_replacer(wrapped_select_subqueries_empty_tail(), context)
        };
        let no_filter = |context: &str| {
            wrapper_pullup_replacer(wrapped_select_filter_expr_empty_tail(), context)
        };
        let push_down = |expr: &str| filter_split_push_down(expr, "?members");
        let pushed = |expr: &str| filter_split_pull_up(expr, literal_bool(true), true, false);
        let kept = |expr: &str| filter_split_pull_up(literal_bool(true), expr, false, true);

        // The filter is over the scan (`wrapper-push-down-filter-to-cube-scan`), reads
        // subqueries (`wrapper-push-down-filter-and-subquery-to-cube-scan`) or is over a select
        // joining the scan (`wrapper-merge-filter-with-inner-wrapped-select`)
        let scan_context = context("?in_projection", "WrapperReplacerContextUngroupedScan:true");
        let join_context = context(
            "WrapperReplacerContextInProjection:false",
            "?ungrouped_scan",
        );
        let merged_context = context(
            "WrapperReplacerContextInProjection:false",
            "WrapperReplacerContextUngroupedScan:true",
        );
        let scan_input = cube_scan_wrapper(
            wrapper_pullup_replacer(scan("?filters".to_string()), &scan_context),
            "CubeScanWrapperFinalized:false",
        );
        let subquery_input = subquery(&scan_input, "?subqueries", "?types");
        let join_input = select(
            &join_context,
            no_subqueries(&join_context),
            scan("?filters".to_string()),
            "?joins".to_string(),
            no_filter(&join_context),
        );
        for (name, input) in [
            ("scan", &scan_input),
            ("subquery", &subquery_input),
            ("join", &join_input),
        ] {
            rules.push(rewrite(
                &format!("wrapper-filter-split-{name}-start"),
                filter("?filter_expr", input),
                filter_split_stage(push_down("?filter_expr"), input),
            ));
        }
        rules.push(rewrite(
            "wrapper-filter-split-push-down-and",
            push_down(&binary_expr("?left", "AND", "?right")),
            binary_expr(push_down("?left"), "AND", push_down("?right")),
        ));

        // Every conjunct lands on one side, so the two rules of a shape decide the opposite
        let binary = binary_expr("?left", "?op", "?right");
        let between = between_expr("?expr", "BetweenExprNegated:false", "?low", "?high");
        let in_list = inlist_expr("?expr", "?list", "InListExprNegated:false");
        for member_filter in [true, false] {
            let name = if member_filter { "member" } else { "other" };
            let pull_up = |expr: &str| {
                if member_filter {
                    pushed(expr)
                } else {
                    kept(expr)
                }
            };
            rules.extend(vec![
                transforming_rewrite(
                    &format!("wrapper-filter-split-binary-{name}"),
                    push_down(&binary),
                    pull_up(&binary),
                    self.transform_filter_split_binary(
                        "?left",
                        "?op",
                        "?right",
                        "?members",
                        member_filter,
                    ),
                ),
                transforming_rewrite(
                    &format!("wrapper-filter-split-between-{name}"),
                    push_down(&between),
                    pull_up(&between),
                    self.transform_filter_split_column(
                        "?expr",
                        vec!["?low", "?high"],
                        "?members",
                        true,
                        member_filter,
                    ),
                ),
                transforming_rewrite(
                    &format!("wrapper-filter-split-in-list-{name}"),
                    push_down(&in_list),
                    pull_up(&in_list),
                    self.transform_filter_split_column(
                        "?expr",
                        vec!["?list"],
                        "?members",
                        false,
                        member_filter,
                    ),
                ),
            ]);
        }
        for (name, conjunct) in [
            ("not", not_expr("?expr")),
            ("is-null", is_null_expr("?expr")),
            ("is-not-null", is_not_null_expr("?expr")),
            (
                "like",
                like_expr(
                    "?like_type",
                    "?negated",
                    "?expr",
                    "?pattern",
                    "?escape_char",
                ),
            ),
            ("case", case_expr_var_arg("?expr", "?when_then", "?else")),
            ("column", column_expr("?column")),
            ("literal", literal_expr("?value")),
            ("cast", cast_expr("?expr", "?data_type")),
            ("function", fun_expr_var_arg("?fun", "?args")),
            ("udf", udf_expr_var_arg("?fun", "?args")),
            (
                "in-subquery",
                insubquery_expr("?expr", "?subquery", "?negated"),
            ),
            (
                "not-between",
                between_expr("?expr", "BetweenExprNegated:true", "?low", "?high"),
            ),
            (
                "not-in-list",
                inlist_expr("?expr", "?list", "InListExprNegated:true"),
            ),
        ] {
            rules.push(rewrite(
                &format!("wrapper-filter-split-{name}"),
                push_down(&conjunct),
                kept(&conjunct),
            ));
        }

        let sides = [(true, false), (false, true), (true, true)];
        for (left_push, left_rest) in sides {
            for (right_push, right_rest) in sides {
                let and = |left: bool, right: bool, left_var: &str, right_var: &str| match (
                    left, right,
                ) {
                    (true, true) => binary_expr(left_var, "AND", right_var),
                    (true, false) => left_var.to_string(),
                    (false, true) => right_var.to_string(),
                    (false, false) => literal_bool(true),
                };
                rules.push(rewrite(
                    &format!(
                        "wrapper-filter-split-pull-up-and-{left_push}-{left_rest}-{right_push}-{right_rest}"
                    ),
                    binary_expr(
                        filter_split_pull_up("?left_push", "?left_rest", left_push, left_rest),
                        "AND",
                        filter_split_pull_up("?right_push", "?right_rest", right_push, right_rest),
                    ),
                    filter_split_pull_up(
                        and(left_push, right_push, "?left_push", "?right_push"),
                        and(left_rest, right_rest, "?left_rest", "?right_rest"),
                        left_push || right_push,
                        left_rest || right_rest,
                    ),
                ));
            }
        }

        let split = |has_rest| filter_split_pull_up("?push", "?rest", true, has_rest);
        let rest = || {
            wrapped_select_filter_expr(
                wrapper_pushdown_replacer("?rest", &scan_context),
                no_filter(&scan_context),
            )
        };
        rules.extend(vec![
            rewrite(
                "wrapper-filter-split-scan-finish",
                filter_split_stage(split(true), &scan_input),
                select(
                    &scan_context,
                    no_subqueries(&scan_context),
                    split_scan(),
                    wrapped_select_joins_empty_tail(),
                    rest(),
                ),
            ),
            transforming_rewrite(
                "wrapper-filter-split-subquery-finish",
                filter_split_stage(split(true), &subquery_input),
                select(
                    &scan_context,
                    wrapper_pushdown_replacer("?subqueries", &scan_context),
                    split_scan(),
                    wrapped_select_joins_empty_tail(),
                    rest(),
                ),
                self.transform_filter_split_subquery("?input_data_source"),
            ),
            rewrite(
                "wrapper-filter-split-join-finish",
                filter_split_stage(split(true), &join_input),
                select(
                    &merged_context,
                    no_subqueries(&merged_context),
                    split_scan(),
                    "?joins".to_string(),
                    wrapper_pushdown_replacer("?rest", &merged_context),
                ),
            ),
            // A filter right over a scan gets there whole by the CubeScan rules. Under a join
            // nothing else takes it to the scan
            rewrite(
                "wrapper-filter-split-join-finish-whole",
                filter_split_stage(split(false), &join_input),
                select(
                    &merged_context,
                    no_subqueries(&merged_context),
                    split_scan(),
                    "?joins".to_string(),
                    no_filter(&merged_context),
                ),
            ),
        ]);
    }

    pub fn filter_rules_subquery(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![transforming_rewrite(
            "wrapper-push-down-filter-and-subquery-to-cube-scan",
            filter(
                "?filter_expr",
                subquery(
                    cube_scan_wrapper(
                        wrapper_pullup_replacer(
                            "?cube_scan_input",
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "?in_projection",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        "CubeScanWrapperFinalized:false",
                    ),
                    "?subqueries",
                    "?types",
                ),
            ),
            cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Projection",
                    wrapper_pullup_replacer(
                        wrapped_select_projection_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pushdown_replacer(
                        "?subqueries",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_group_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_aggr_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?cube_scan_input",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_joins_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    wrapped_select_filter_expr(
                        wrapper_pushdown_replacer(
                            "?filter_expr",
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "?in_projection",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "?push_to_cube",
                                "?in_projection",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                    ),
                    wrapped_select_having_expr_empty_tail(),
                    "WrappedSelectLimit:None",
                    "WrappedSelectOffset:None",
                    wrapper_pullup_replacer(
                        wrapped_select_order_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "?select_push_to_cube",
                    "?select_ungrouped_scan",
                ),
                "CubeScanWrapperFinalized:false",
            ),
            self.transform_filter_subquery(
                "?input_data_source",
                "?push_to_cube",
                "?ungrouped_scan",
                "?select_push_to_cube",
                "?select_ungrouped_scan",
            ),
        )]);
    }

    pub fn filter_merge_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![rewrite(
            "wrapper-merge-filter-with-inner-wrapped-select",
            // Input is not a finished wrapper_pullup_replacer, but WrappedSelect just before pullup
            // After pullup replacer would disable push to cube, because any node on top would have WrappedSelect in `from`
            // So there would be no CubeScan to push to
            // Instead, this rule tries to catch `from` before pulling up, and merge outer Filter into inner WrappedSelect
            filter(
                "?filter_expr",
                cube_scan_wrapper(
                    wrapped_select(
                        "WrappedSelectSelectType:Projection",
                        wrapper_pullup_replacer(
                            wrapped_select_projection_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_subqueries_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_group_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_aggr_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_window_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            "?inner_from",
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            "?inner_joins",
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapper_pullup_replacer(
                            wrapped_select_filter_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        wrapped_select_having_expr_empty_tail(),
                        // Inner must not have limit and offset, because they are not commutative with aggregation
                        "WrappedSelectLimit:None",
                        "WrappedSelectOffset:None",
                        wrapper_pullup_replacer(
                            wrapped_select_order_expr_empty_tail(),
                            wrapper_replacer_context(
                                "?alias_to_cube",
                                "WrapperReplacerContextPushToCube:true",
                                "WrapperReplacerContextInProjection:false",
                                "?cube_members",
                                "?grouped_subqueries",
                                "?ungrouped_scan",
                                "?input_data_source",
                            ),
                        ),
                        "WrappedSelectAlias:None",
                        "WrappedSelectDistinct:false",
                        "WrappedSelectPushToCube:true",
                        "WrappedSelectUngroupedScan:true",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
            ),
            cube_scan_wrapper(
                wrapped_select(
                    "WrappedSelectSelectType:Projection",
                    wrapper_pullup_replacer(
                        wrapped_select_projection_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:true",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_subqueries_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:true",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_group_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:true",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_aggr_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:true",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        wrapped_select_window_expr_empty_tail(),
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:true",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?inner_from",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:true",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pullup_replacer(
                        "?inner_joins",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:true",
                            "?input_data_source",
                        ),
                    ),
                    wrapper_pushdown_replacer(
                        "?filter_expr",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
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
                            "?alias_to_cube",
                            "WrapperReplacerContextPushToCube:true",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members",
                            "?grouped_subqueries",
                            "WrapperReplacerContextUngroupedScan:true",
                            "?input_data_source",
                        ),
                    ),
                    "WrappedSelectAlias:None",
                    "WrappedSelectDistinct:false",
                    "WrappedSelectPushToCube:true",
                    "WrappedSelectUngroupedScan:true",
                ),
                "CubeScanWrapperFinalized:false",
            ),
        )]);
    }

    fn transform_filter(
        &self,
        push_to_cube_var: &'static str,
        ungrouped_scan_var: &'static str,
        select_push_to_cube_var: &'static str,
        select_ungrouped_scan_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let push_to_cube_var = var!(push_to_cube_var);
        let ungrouped_scan_var = var!(ungrouped_scan_var);
        let select_push_to_cube_var = var!(select_push_to_cube_var);
        let select_ungrouped_scan_var = var!(select_ungrouped_scan_var);
        move |egraph, subst| {
            Self::transform_filter_impl(
                egraph,
                subst,
                push_to_cube_var,
                ungrouped_scan_var,
                select_push_to_cube_var,
                select_ungrouped_scan_var,
            )
        }
    }

    fn transform_filter_subquery(
        &self,
        input_data_source_var: &'static str,
        push_to_cube_var: &'static str,
        ungrouped_scan_var: &'static str,
        select_push_to_cube_var: &'static str,
        select_ungrouped_scan_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let push_to_cube_var = var!(push_to_cube_var);
        let ungrouped_scan_var = var!(ungrouped_scan_var);
        let select_push_to_cube_var = var!(select_push_to_cube_var);
        let select_ungrouped_scan_var = var!(select_ungrouped_scan_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            if Self::transform_check_subquery_allowed(egraph, subst, &meta, input_data_source_var) {
                Self::transform_filter_impl(
                    egraph,
                    subst,
                    push_to_cube_var,
                    ungrouped_scan_var,
                    select_push_to_cube_var,
                    select_ungrouped_scan_var,
                )
            } else {
                false
            }
        }
    }

    fn transform_filter_impl(
        egraph: &mut CubeEGraph,
        subst: &mut Subst,
        push_to_cube_var: Var,
        ungrouped_scan_var: Var,
        select_push_to_cube_var: Var,
        select_ungrouped_scan_var: Var,
    ) -> bool {
        if !copy_flag!(
            egraph,
            subst,
            push_to_cube_var,
            WrapperReplacerContextPushToCube,
            select_push_to_cube_var,
            WrappedSelectPushToCube
        ) {
            return false;
        }

        if !copy_flag!(
            egraph,
            subst,
            ungrouped_scan_var,
            WrapperReplacerContextUngroupedScan,
            select_ungrouped_scan_var,
            WrappedSelectUngroupedScan
        ) {
            return false;
        }

        true
    }

    fn transform_filter_split_subquery(
        &self,
        input_data_source_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let input_data_source_var = var!(input_data_source_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            Self::transform_check_subquery_allowed(egraph, subst, &meta, input_data_source_var)
        }
    }

    /// The dimension of the scan `expr` reads: a column, or one in a cast `cast` accepts
    fn filter_split_dimension<'meta>(
        egraph: &mut CubeEGraph,
        expr: Id,
        members: Id,
        cast: fn(&DataType) -> bool,
        meta: &'meta MetaContext,
    ) -> Option<&'meta CubeMetaDimension> {
        let column = match &egraph[expr].data.original_expr {
            Some(OriginalExpr::Expr(column @ Expr::Column(_))) => column.clone(),
            Some(OriginalExpr::Expr(Expr::Cast { expr, data_type }))
                if cast(data_type) && matches!(expr.as_ref(), Expr::Column(_)) =>
            {
                expr.as_ref().clone()
            }
            _ => return None,
        };
        let member = egraph
            .index_mut(members)
            .data
            .find_member_by_alias(&expr_column_name(&column, &None))
            .and_then(|((member, kind, _), _)| match kind {
                Member::Dimension { .. } => member.clone(),
                _ => None,
            })?;
        meta.find_dimension_with_name(&member)
    }

    /// Told by what the expression references, not by its value: a literal is a query param
    /// until the final rewrite, and a conjunct has to land on the same side in both
    fn filter_split_constant(egraph: &CubeEGraph, expr: Id) -> bool {
        egraph[expr]
            .data
            .referenced_expr
            .as_ref()
            .is_some_and(|referenced| referenced.is_empty())
    }

    fn transform_filter_split_binary(
        &self,
        left_var: &'static str,
        op_var: &'static str,
        right_var: &'static str,
        members_var: &'static str,
        member_filter: bool,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let left_var = var!(left_var);
        let op_var = var!(op_var);
        let right_var = var!(right_var);
        let members_var = var!(members_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Some(op) = var_iter!(egraph[subst[op_var]], BinaryExprOp)
                .next()
                .cloned()
            else {
                return false;
            };
            let date_bound = match op {
                // Pushed down over, not a conjunct
                Operator::And => return false,
                Operator::Eq => false,
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => true,
                _ => return !member_filter,
            };
            let (left, right, members) = (subst[left_var], subst[right_var], subst[members_var]);
            let mut compares = |column: Id, constant: Id, cast: fn(&DataType) -> bool| {
                Self::filter_split_constant(egraph, constant)
                    && Self::filter_split_dimension(egraph, column, members, cast, &meta)
                        .is_some_and(|dimension| dimension.is_time() == date_bound)
            };
            // The filter rules swap a literal on the left only for a date bound, and read a
            // date through a cast only on the left
            let is_member_filter = if date_bound {
                compares(left, right, |data_type| {
                    matches!(data_type, DataType::Date32 | DataType::Timestamp(_, None))
                }) || compares(right, left, |data_type| {
                    matches!(data_type, DataType::Timestamp(_, None))
                })
            } else {
                compares(left, right, |_| false)
            };
            is_member_filter == member_filter
        }
    }

    fn transform_filter_split_column(
        &self,
        column_var: &'static str,
        constant_vars: Vec<&'static str>,
        members_var: &'static str,
        date_bound: bool,
        member_filter: bool,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_var = var!(column_var);
        let constant_vars = constant_vars
            .into_iter()
            .map(|constant_var| var!(constant_var))
            .collect::<Vec<Var>>();
        let members_var = var!(members_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let is_member_filter = constant_vars
                .iter()
                .all(|constant_var| Self::filter_split_constant(egraph, subst[*constant_var]))
                && Self::filter_split_dimension(
                    egraph,
                    subst[column_var],
                    subst[members_var],
                    |_| false,
                    &meta,
                )
                .is_some_and(|dimension| dimension.is_time() == date_bound);
            is_member_filter == member_filter
        }
    }
}
