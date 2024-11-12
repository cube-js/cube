mod aggregate;
mod aggregate_function;
mod alias;
mod binary_expr;
mod case;
mod cast;
mod column;
mod cube_scan_wrapper;
mod distinct;
mod extract;
mod filter;
mod in_list_expr;
mod in_subquery_expr;
mod is_null_expr;
mod like_expr;
mod limit;
mod literal;
mod negative_expr;
mod not_expr;
mod order;
mod projection;
mod scalar_function;
mod sort_expr;
mod subquery;
mod udf_function;
mod window;
mod window_function;
mod wrapper_pull_up;

use crate::{
    compile::rewrite::{
        fun_expr,
        rewriter::{CubeEGraph, CubeRewrite, RewriteRules},
        rules::{
            replacer_flat_pull_up_node, replacer_flat_push_down_node, replacer_pull_up_node,
            replacer_push_down_node,
        },
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer, ListType,
        WrapperPullupReplacerUngrouped, WrapperPushdownReplacerPushToCube,
    },
    config::ConfigObj,
    copy_flag,
    transport::MetaContext,
    var,
};
use egg::Subst;
use std::{fmt::Display, sync::Arc};

pub struct WrapperRules {
    meta_context: Arc<MetaContext>,
    config_obj: Arc<dyn ConfigObj>,
}

impl RewriteRules for WrapperRules {
    fn rewrite_rules(&self) -> Vec<CubeRewrite> {
        let mut rules = Vec::new();

        self.cube_scan_wrapper_rules(&mut rules);
        self.wrapper_pull_up_rules(&mut rules);
        self.aggregate_rules(&mut rules);
        self.aggregate_rules_subquery(&mut rules);
        self.projection_rules(&mut rules);
        self.projection_rules_subquery(&mut rules);
        self.limit_rules(&mut rules);
        self.filter_rules(&mut rules);
        self.filter_rules_subquery(&mut rules);
        self.subquery_rules(&mut rules);
        self.order_rules(&mut rules);
        self.window_rules(&mut rules);
        self.aggregate_function_rules(&mut rules);
        self.window_function_rules(&mut rules);
        self.scalar_function_rules(&mut rules);
        self.udf_function_rules(&mut rules);
        self.extract_rules(&mut rules);
        self.alias_rules(&mut rules);
        self.case_rules(&mut rules);
        self.binary_expr_rules(&mut rules);
        self.is_null_expr_rules(&mut rules);
        self.sort_expr_rules(&mut rules);
        self.cast_rules(&mut rules);
        self.column_rules(&mut rules);
        self.literal_rules(&mut rules);
        self.in_list_expr_rules(&mut rules);
        self.in_subquery_expr_rules(&mut rules);
        self.negative_expr_rules(&mut rules);
        self.not_expr_rules(&mut rules);
        self.distinct_rules(&mut rules);
        self.like_expr_rules(&mut rules);

        rules
    }
}

impl WrapperRules {
    pub fn new(meta_context: Arc<MetaContext>, config_obj: Arc<dyn ConfigObj>) -> Self {
        Self {
            meta_context,
            config_obj,
        }
    }

    fn fun_expr(&self, fun_name: impl Display, args: Vec<impl Display>) -> String {
        fun_expr(fun_name, args, self.config_obj.push_down_pull_up_split())
    }

    fn list_pushdown_pullup_rules(
        rules: &mut Vec<CubeRewrite>,
        rule_name: &str,
        list_node: &str,
        substitute_list_node: &str,
    ) {
        rules.extend(replacer_push_down_node(
            rule_name,
            list_node,
            |node| {
                wrapper_pushdown_replacer(
                    node,
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                )
            },
            false,
        ));

        rules.extend(replacer_pull_up_node(
            rule_name,
            list_node,
            substitute_list_node,
            |node| {
                wrapper_pullup_replacer(
                    node,
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                )
            },
        ));

        rules.extend(vec![transforming_rewrite(
            &format!("{}-tail", rule_name),
            wrapper_pushdown_replacer(
                list_node,
                "?alias_to_cube",
                "?push_to_cube",
                "?in_projection",
                "?cube_members",
            ),
            wrapper_pullup_replacer(
                substitute_list_node,
                "?alias_to_cube",
                "?pullup_ungrouped",
                "?in_projection",
                "?cube_members",
            ),
            Self::transform_list_tail("?push_to_cube", "?pullup_ungrouped"),
        )]);
    }

    fn transform_list_tail(
        push_to_cube_var: &str,
        pullup_ungrouped_var: &str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let push_to_cube_var = var!(push_to_cube_var);
        let pullup_ungrouped_var = var!(pullup_ungrouped_var);
        move |egraph, subst| {
            if !copy_flag!(
                egraph,
                subst,
                push_to_cube_var,
                WrapperPushdownReplacerPushToCube,
                pullup_ungrouped_var,
                WrapperPullupReplacerUngrouped
            ) {
                return false;
            }
            true
        }
    }

    fn flat_list_pushdown_pullup_rules(
        rules: &mut Vec<CubeRewrite>,
        rule_name: &str,
        list_type: ListType,
        substitute_list_type: ListType,
    ) {
        rules.extend(replacer_flat_push_down_node(
            rule_name,
            list_type.clone(),
            |node| {
                wrapper_pushdown_replacer(
                    node,
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                )
            },
            false,
        ));

        rules.extend(replacer_flat_pull_up_node(
            rule_name,
            list_type.clone(),
            substitute_list_type.clone(),
            |node| {
                wrapper_pullup_replacer(
                    node,
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                )
            },
            &[
                "?alias_to_cube",
                "?ungrouped",
                "?in_projection",
                "?cube_members",
            ],
        ));

        rules.extend(vec![transforming_rewrite(
            &format!("{}-tail", rule_name),
            wrapper_pushdown_replacer(
                list_type.empty_list(),
                "?alias_to_cube",
                "?push_to_cube",
                "?in_projection",
                "?cube_members",
            ),
            wrapper_pullup_replacer(
                substitute_list_type.empty_list(),
                "?alias_to_cube",
                "?pullup_ungrouped",
                "?in_projection",
                "?cube_members",
            ),
            Self::transform_flat_list_tail("?push_to_cube", "?pullup_ungrouped"),
        )]);
    }

    fn transform_flat_list_tail(
        push_to_cube_var: &str,
        pullup_ungrouped_var: &str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let push_to_cube_var = var!(push_to_cube_var);
        let pullup_ungrouped_var = var!(pullup_ungrouped_var);
        move |egraph, subst| {
            if !copy_flag!(
                egraph,
                subst,
                push_to_cube_var,
                WrapperPushdownReplacerPushToCube,
                pullup_ungrouped_var,
                WrapperPullupReplacerUngrouped
            ) {
                return false;
            }
            true
        }
    }

    fn expr_list_pushdown_pullup_rules(
        rules: &mut Vec<CubeRewrite>,
        rule_name: &str,
        list_node: &str,
    ) {
        rules.extend(replacer_push_down_node(
            rule_name,
            list_node,
            |node| {
                wrapper_pushdown_replacer(
                    node,
                    "?alias_to_cube",
                    "?push_to_cube",
                    "?in_projection",
                    "?cube_members",
                )
            },
            false,
        ));

        rules.extend(replacer_pull_up_node(
            rule_name,
            list_node,
            list_node,
            |node| {
                wrapper_pullup_replacer(
                    node,
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                )
            },
        ));

        rules.extend(vec![transforming_rewrite(
            rule_name,
            wrapper_pushdown_replacer(
                list_node,
                "?alias_to_cube",
                "?push_to_cube",
                "?in_projection",
                "?cube_members",
            ),
            wrapper_pullup_replacer(
                list_node,
                "?alias_to_cube",
                "?pullup_ungrouped",
                "?in_projection",
                "?cube_members",
            ),
            Self::transform_expr_list_tail("?push_to_cube", "?pullup_ungrouped"),
        )]);
    }

    fn transform_expr_list_tail(
        push_to_cube_var: &str,
        pullup_ungrouped_var: &str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let push_to_cube_var = var!(push_to_cube_var);
        let pullup_ungrouped_var = var!(pullup_ungrouped_var);
        move |egraph, subst| {
            if !copy_flag!(
                egraph,
                subst,
                push_to_cube_var,
                WrapperPushdownReplacerPushToCube,
                pullup_ungrouped_var,
                WrapperPullupReplacerUngrouped
            ) {
                return false;
            }
            true
        }
    }
}
