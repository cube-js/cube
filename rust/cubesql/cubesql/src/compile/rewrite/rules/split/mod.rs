pub mod aggregate_function;
pub mod alias;
pub mod binary_expr;
pub mod case;
pub mod cast;
pub mod column;
pub mod dates;
pub mod functions;
pub mod granularity;
pub mod top_level;

use crate::{
    compile::rewrite::{
        aggregate_split_pullup_replacer, aggregate_split_pushdown_replacer, alias_expr, fun_expr,
        fun_expr_var_arg, list_rewrite_with_lists_and_vars, original_expr_name,
        projection_split_pullup_replacer, projection_split_pushdown_replacer, rewrite,
        rewriter::{CubeEGraph, CubeRewrite, RewriteRules},
        rules::{members::MemberRules, replacer_flat_push_down_node, replacer_push_down_node},
        transforming_chain_rewrite, AliasExprAlias, ListApplierListPattern, ListPattern, ListType,
        LogicalPlanLanguage,
    },
    config::ConfigObj,
    transport::MetaContext,
    var,
};
use std::{fmt::Display, sync::Arc};

pub struct SplitRules {
    meta_context: Arc<MetaContext>,
    #[allow(dead_code)]
    config_obj: Arc<dyn ConfigObj>,
}

impl RewriteRules for SplitRules {
    fn rewrite_rules(&self) -> Vec<CubeRewrite> {
        let mut rules = Vec::new();

        self.top_level_rules(&mut rules);
        self.column_rules(&mut rules);
        self.alias_rules(&mut rules);
        self.functions_rules(&mut rules);
        self.date_rules(&mut rules);
        self.aggregate_function_rules(&mut rules);
        self.granularity_rules(&mut rules);
        self.binary_expr_rules(&mut rules);
        self.case_rules(&mut rules);
        self.cast_rules(&mut rules);

        rules
    }
}

impl SplitRules {
    pub fn new(meta_context: Arc<MetaContext>, config_obj: Arc<dyn ConfigObj>) -> Self {
        Self {
            meta_context,
            config_obj,
        }
    }

    fn fun_expr(&self, fun_name: impl Display, args: Vec<impl Display>) -> String {
        fun_expr(fun_name, args, true)
    }

    pub fn single_arg_pass_through_rules(
        &self,
        name: &str,
        node: impl Fn(String) -> String,
        projection_rules: bool,
        rules: &mut Vec<CubeRewrite>,
    ) {
        rules.extend(vec![
            rewrite(
                &format!("split-{}-push-down-aggregate", name),
                aggregate_split_pushdown_replacer(
                    node("?expr".to_string()),
                    "?list_node",
                    "?alias_to_cube",
                ),
                node(aggregate_split_pushdown_replacer(
                    "?expr".to_string(),
                    "?list_node",
                    "?alias_to_cube",
                )),
            ),
            rewrite(
                &format!("split-{}-pull-up-aggregate", name),
                node(aggregate_split_pullup_replacer(
                    "?inner_expr".to_string(),
                    "?outer_expr".to_string(),
                    "?list_node",
                    "?alias_to_cube",
                )),
                aggregate_split_pullup_replacer(
                    "?inner_expr",
                    node("?outer_expr".to_string()),
                    "?list_node",
                    "?alias_to_cube",
                ),
            ),
        ]);

        if projection_rules {
            rules.extend(vec![
                rewrite(
                    &format!("split-{}-push-down-projection", name),
                    projection_split_pushdown_replacer(
                        node("?expr".to_string()),
                        "?list_node",
                        "?alias_to_cube",
                    ),
                    node(projection_split_pushdown_replacer(
                        "?expr".to_string(),
                        "?list_node",
                        "?alias_to_cube",
                    )),
                ),
                rewrite(
                    &format!("split-{}-pull-up-projection", name),
                    node(projection_split_pullup_replacer(
                        "?inner_expr".to_string(),
                        "?outer_expr".to_string(),
                        "?list_node",
                        "?alias_to_cube",
                    )),
                    projection_split_pullup_replacer(
                        "?inner_expr",
                        node("?outer_expr".to_string()),
                        "?list_node",
                        "?alias_to_cube",
                    ),
                ),
            ]);
        }
    }

    pub fn scalar_fn_args_pass_through_rules(
        &self,
        fun_name: &(impl Display + ?Sized),
        projection_rules: bool,
        rules: &mut Vec<CubeRewrite>,
    ) {
        rules.extend(vec![
            rewrite(
                &format!("split-{fun_name}-args-push-down-aggregate"),
                aggregate_split_pushdown_replacer(
                    fun_expr_var_arg(fun_name, "?args"),
                    "?list_node",
                    "?alias_to_cube",
                ),
                fun_expr_var_arg(
                    fun_name,
                    aggregate_split_pushdown_replacer("?args", "?list_node", "?alias_to_cube"),
                ),
            ),
            rewrite(
                &format!("split-{fun_name}-args-pull-up-aggregate"),
                fun_expr_var_arg(
                    fun_name,
                    aggregate_split_pullup_replacer(
                        "?inner_expr",
                        "?outer_expr",
                        "?list_node",
                        "?alias_to_cube",
                    ),
                ),
                aggregate_split_pullup_replacer(
                    "?inner_expr",
                    fun_expr_var_arg(fun_name, "?outer_expr"),
                    "?list_node",
                    "?alias_to_cube",
                ),
            ),
        ]);

        if projection_rules {
            rules.extend(vec![
                rewrite(
                    &format!("split-{fun_name}-args-push-down-projection"),
                    projection_split_pushdown_replacer(
                        fun_expr_var_arg(fun_name, "?args"),
                        "?list_node",
                        "?alias_to_cube",
                    ),
                    fun_expr_var_arg(
                        fun_name,
                        projection_split_pushdown_replacer("?args", "?list_node", "?alias_to_cube"),
                    ),
                ),
                rewrite(
                    &format!("split-{fun_name}-args-pull-up-projection"),
                    fun_expr_var_arg(
                        fun_name,
                        projection_split_pullup_replacer(
                            "?inner_expr",
                            "?outer_expr",
                            "?list_node",
                            "?alias_to_cube",
                        ),
                    ),
                    projection_split_pullup_replacer(
                        "?inner_expr",
                        fun_expr_var_arg(fun_name, "?outer_expr"),
                        "?list_node",
                        "?alias_to_cube",
                    ),
                ),
            ]);
        }
    }

    pub fn single_arg_split_point_rules(
        &self,
        name: &str,
        match_rule: impl Fn() -> String + Clone,
        inner_rule: impl Fn() -> String + Clone,
        outer_rule: impl Fn(String) -> String + Clone,
        transform_fn: impl Fn(bool, &mut CubeEGraph, &mut egg::Subst) -> bool
            + Sync
            + Send
            + Clone
            + 'static,
        projection_rules: bool,
        rules: &mut Vec<CubeRewrite>,
    ) {
        if projection_rules {
            self.single_arg_split_point_rules_projection(
                name,
                match_rule.clone(),
                inner_rule.clone(),
                outer_rule.clone(),
                transform_fn.clone(),
                rules,
            );
        }
        self.single_arg_split_point_rules_aggregate(
            name,
            match_rule,
            inner_rule,
            outer_rule,
            transform_fn,
            rules,
        );
    }

    pub fn single_arg_split_point_rules_aggregate(
        &self,
        name: &str,
        match_rule: impl Fn() -> String,
        inner_rule: impl Fn() -> String,
        outer_rule: impl Fn(String) -> String,
        transform_fn: impl Fn(bool, &mut CubeEGraph, &mut egg::Subst) -> bool
            + Sync
            + Send
            + Clone
            + 'static,
        rules: &mut Vec<CubeRewrite>,
    ) {
        rules.push(transforming_chain_rewrite(
            &format!("split-{}-point-aggregate", name),
            aggregate_split_pushdown_replacer("?match_expr", "?list_node", "?alias_to_cube"),
            vec![("?match_expr", match_rule())],
            aggregate_split_pullup_replacer(
                alias_expr(inner_rule(), "?inner_alias"),
                alias_expr(
                    outer_rule("?outer_alias_column".to_string()),
                    "?inner_alias",
                ),
                "?list_node",
                "?alias_to_cube",
            ),
            self.transform_single_arg_split_point(
                "?match_expr",
                "?inner_alias",
                "?outer_alias_column",
                false,
                transform_fn.clone(),
            ),
        ));
    }

    pub fn single_arg_split_point_rules_projection(
        &self,
        name: &str,
        match_rule: impl Fn() -> String,
        inner_rule: impl Fn() -> String,
        outer_rule: impl Fn(String) -> String,
        transform_fn: impl Fn(bool, &mut CubeEGraph, &mut egg::Subst) -> bool
            + Sync
            + Send
            + Clone
            + 'static,
        rules: &mut Vec<CubeRewrite>,
    ) {
        rules.push(transforming_chain_rewrite(
            &format!("split-{}-point-projection", name),
            projection_split_pushdown_replacer("?match_expr", "?list_node", "?alias_to_cube"),
            vec![("?match_expr", match_rule())],
            projection_split_pullup_replacer(
                alias_expr(inner_rule(), "?inner_alias"),
                alias_expr(
                    outer_rule("?outer_alias_column".to_string()),
                    "?inner_alias",
                ),
                "?list_node",
                "?alias_to_cube",
            ),
            self.transform_single_arg_split_point(
                "?match_expr",
                "?inner_alias",
                "?outer_alias_column",
                true,
                transform_fn.clone(),
            ),
        ));
    }

    fn transform_single_arg_split_point(
        &self,
        match_expr_var: &str,
        inner_alias_var: &str,
        outer_alias_column_var: &str,
        is_projection: bool,
        transform_fn: impl Fn(bool, &mut CubeEGraph, &mut egg::Subst) -> bool + Clone + Send + Sync,
    ) -> impl Fn(&mut CubeEGraph, &mut egg::Subst) -> bool + Sync + Send + Clone {
        let match_expr_var = var!(match_expr_var);
        let inner_alias_var = var!(inner_alias_var);
        let outer_alias_column_var = var!(outer_alias_column_var);

        move |egraph, subst| {
            if let Some(original_expr) = original_expr_name(egraph, subst[match_expr_var]) {
                if transform_fn(is_projection, egraph, subst) {
                    let inner_alias = egraph.add(LogicalPlanLanguage::AliasExprAlias(
                        AliasExprAlias(original_expr.to_string()),
                    ));
                    subst.insert(inner_alias_var, inner_alias);

                    let outer_alias_column =
                        MemberRules::add_alias_column(egraph, original_expr, None);

                    subst.insert(outer_alias_column_var, outer_alias_column);

                    return true;
                }
            }
            false
        }
    }

    fn list_pushdown_pullup_rules(name: &str, list_node: &str, rules: &mut Vec<CubeRewrite>) {
        let possible_inner_list_nodes = Self::possible_inner_list_nodes();

        // Aggregate split replacer
        let rule_name = &format!("split-{}-aggregate", name);
        rules.extend(replacer_push_down_node(
            rule_name,
            list_node,
            |node| aggregate_split_pushdown_replacer(node, "?list_node", "?alias_to_cube"),
            false,
        ));

        rules.extend(Self::replacer_pull_up_node(
            rule_name,
            list_node,
            list_node,
            |inner, outer, inner_list_node| {
                aggregate_split_pullup_replacer(inner, outer, inner_list_node, "?alias_to_cube")
            },
            &possible_inner_list_nodes,
        ));

        rules.extend(Self::replacer_pushdown_pullup_tail(
            rule_name,
            list_node,
            list_node,
            |node, list_node| aggregate_split_pushdown_replacer(node, list_node, "?alias_to_cube"),
            |inner, outer, inner_list_node| {
                aggregate_split_pullup_replacer(inner, outer, inner_list_node, "?alias_to_cube")
            },
            &possible_inner_list_nodes,
        ));

        // Projection split replacer
        let rule_name = &format!("split-{}-projection", name);
        rules.extend(replacer_push_down_node(
            rule_name,
            list_node,
            |node| projection_split_pushdown_replacer(node, "?list_node", "?alias_to_cube"),
            false,
        ));

        let projection_substitute_node = if possible_inner_list_nodes.iter().any(|n| n == list_node)
        {
            "ProjectionExpr"
        } else {
            list_node
        };
        rules.extend(Self::replacer_pull_up_node(
            rule_name,
            list_node,
            projection_substitute_node,
            |inner, outer, inner_list_node| {
                projection_split_pullup_replacer(inner, outer, inner_list_node, "?alias_to_cube")
            },
            &possible_inner_list_nodes,
        ));

        rules.extend(Self::replacer_pushdown_pullup_tail(
            rule_name,
            list_node,
            projection_substitute_node,
            |node, inner_list_node| {
                projection_split_pushdown_replacer(node, inner_list_node, "?alias_to_cube")
            },
            |inner, outer, inner_list_node| {
                projection_split_pullup_replacer(inner, outer, inner_list_node, "?alias_to_cube")
            },
            &possible_inner_list_nodes,
        ));
    }

    fn flat_list_pushdown_pullup_rules(
        name: &str,
        list_type: ListType,
        rules: &mut Vec<CubeRewrite>,
    ) {
        let possible_inner_list_types = Self::possible_inner_flat_list_types();

        // Aggregate split replacer
        let rule_name = &format!("split-{}-aggregate", name);
        rules.extend(replacer_flat_push_down_node(
            rule_name,
            list_type.clone(),
            |node| aggregate_split_pushdown_replacer(node, "?list_node", "?alias_to_cube"),
            false,
        ));

        rules.extend(Self::replacer_flat_pull_up_node(
            rule_name,
            &list_type,
            &list_type,
            |inner, outer, inner_list_node| {
                aggregate_split_pullup_replacer(inner, outer, inner_list_node, "?alias_to_cube")
            },
            &possible_inner_list_types,
            &["?alias_to_cube"],
        ));

        rules.extend(Self::replacer_flat_pushdown_pullup_tail(
            rule_name,
            &list_type,
            &list_type,
            |node, list_node| aggregate_split_pushdown_replacer(node, list_node, "?alias_to_cube"),
            |inner, outer, inner_list_node| {
                aggregate_split_pullup_replacer(inner, outer, inner_list_node, "?alias_to_cube")
            },
            &possible_inner_list_types,
        ));

        // Projection split replacer
        let rule_name = &format!("split-{}-projection", name);
        rules.extend(replacer_flat_push_down_node(
            rule_name,
            list_type.clone(),
            |node| projection_split_pushdown_replacer(node, "?list_node", "?alias_to_cube"),
            false,
        ));

        let projection_substitute_type =
            if possible_inner_list_types.iter().any(|n| n == &list_type) {
                ListType::ProjectionExpr
            } else {
                list_type.clone()
            };
        rules.extend(Self::replacer_flat_pull_up_node(
            rule_name,
            &list_type,
            &projection_substitute_type,
            |inner, outer, inner_list_node| {
                projection_split_pullup_replacer(inner, outer, inner_list_node, "?alias_to_cube")
            },
            &possible_inner_list_types,
            &["?alias_to_cube"],
        ));

        rules.extend(Self::replacer_flat_pushdown_pullup_tail(
            rule_name,
            &list_type,
            &projection_substitute_type,
            |node, inner_list_node| {
                projection_split_pushdown_replacer(node, inner_list_node, "?alias_to_cube")
            },
            |inner, outer, inner_list_node| {
                projection_split_pullup_replacer(inner, outer, inner_list_node, "?alias_to_cube")
            },
            &possible_inner_list_types,
        ));
    }

    fn possible_inner_list_nodes() -> Vec<String> {
        vec![
            "ProjectionExpr".to_string(),
            "AggregateAggrExpr".to_string(),
            "AggregateGroupExpr".to_string(),
        ]
    }

    fn possible_inner_flat_list_types() -> Vec<ListType> {
        vec![
            ListType::ProjectionExpr,
            ListType::AggregateGroupExpr,
            ListType::AggregateAggrExpr,
        ]
    }

    fn replacer_pull_up_node(
        name: &str,
        list_node: &str,
        substitute_list_node: &str,
        replacer_node: impl Fn(String, String, String) -> String,
        possible_inner_list_nodes: &Vec<String>,
    ) -> Vec<CubeRewrite> {
        possible_inner_list_nodes
            .iter()
            .map(|inner_list_node| {
                rewrite(
                    &format!("{}-{}-pull-up", name, inner_list_node),
                    format!(
                        "({} {} {})",
                        list_node,
                        replacer_node(
                            "?inner_left".to_string(),
                            "?outer_left".to_string(),
                            inner_list_node.clone()
                        ),
                        replacer_node(
                            "?inner_right".to_string(),
                            "?outer_right".to_string(),
                            inner_list_node.clone()
                        ),
                    ),
                    replacer_node(
                        format!("({} ?inner_left ?inner_right)", inner_list_node.clone()),
                        format!("({} ?outer_left ?outer_right)", substitute_list_node),
                        inner_list_node.clone(),
                    ),
                )
            })
            .collect()
    }

    fn replacer_flat_pull_up_node(
        name: &str,
        list_type: &ListType,
        substitute_list_type: &ListType,
        replacer_node: impl Fn(String, String, String) -> String,
        possible_inner_list_types: &Vec<ListType>,
        top_level_elem_vars: &[&str],
    ) -> Vec<CubeRewrite> {
        possible_inner_list_types
            .iter()
            .map(|inner_list_type| {
                list_rewrite_with_lists_and_vars(
                    &format!("{}-{}-pull-up", name, inner_list_type),
                    list_type.clone(),
                    ListPattern {
                        pattern: "?list".to_string(),
                        list_var: "?list".to_string(),
                        elem: replacer_node(
                            "?inner".to_string(),
                            "?outer".to_string(),
                            inner_list_type.to_string(),
                        ),
                    },
                    &replacer_node(
                        "?inner_list".to_string(),
                        "?outer_list".to_string(),
                        inner_list_type.to_string(),
                    ),
                    [
                        ListApplierListPattern {
                            list_type: inner_list_type.clone(),
                            new_list_var: "?inner_list".to_string(),
                            elem_pattern: "?inner".to_string(),
                        },
                        ListApplierListPattern {
                            list_type: substitute_list_type.clone(),
                            new_list_var: "?outer_list".to_string(),
                            elem_pattern: "?outer".to_string(),
                        },
                    ],
                    top_level_elem_vars,
                )
            })
            .collect()
    }

    fn replacer_pushdown_pullup_tail(
        name: &str,
        list_node: &str,
        substitute_list_node: &str,
        push_down_replacer_node: impl Fn(String, String) -> String,
        pull_up_replacer_node: impl Fn(String, String, String) -> String,
        possible_inner_list_nodes: &Vec<String>,
    ) -> Vec<CubeRewrite> {
        possible_inner_list_nodes
            .iter()
            .map(|inner_list_node| {
                rewrite(
                    &format!("{}-push-down-pull-up-{}-tail", name, inner_list_node),
                    push_down_replacer_node(list_node.to_string(), inner_list_node.clone()),
                    pull_up_replacer_node(
                        inner_list_node.clone(),
                        substitute_list_node.to_string(),
                        inner_list_node.clone(),
                    ),
                )
            })
            .collect()
    }

    fn replacer_flat_pushdown_pullup_tail(
        name: &str,
        list_type: &ListType,
        substitute_list_type: &ListType,
        push_down_replacer_node: impl Fn(String, String) -> String,
        pull_up_replacer_node: impl Fn(String, String, String) -> String,
        possible_inner_list_types: &Vec<ListType>,
    ) -> Vec<CubeRewrite> {
        possible_inner_list_types
            .iter()
            .map(|inner_list_type| {
                rewrite(
                    &format!("{}-push-down-pull-up-{}-tail", name, inner_list_type),
                    push_down_replacer_node(list_type.empty_list(), inner_list_type.to_string()),
                    pull_up_replacer_node(
                        inner_list_type.empty_list(),
                        substitute_list_type.empty_list(),
                        inner_list_type.to_string(),
                    ),
                )
            })
            .collect()
    }
}
