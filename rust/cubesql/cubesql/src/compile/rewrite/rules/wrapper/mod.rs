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
mod is_null_expr;
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
        analysis::LogicalPlanAnalysis,
        rewrite,
        rewriter::RewriteRules,
        rules::{replacer_pull_up_node, replacer_push_down_node},
        wrapper_pullup_replacer, wrapper_pushdown_replacer, LogicalPlanLanguage,
    },
    config::ConfigObj,
    transport::MetaContext,
};
use egg::Rewrite;
use std::sync::Arc;

pub struct WrapperRules {
    meta_context: Arc<MetaContext>,
    config_obj: Arc<dyn ConfigObj>,
}

impl RewriteRules for WrapperRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        let mut rules = Vec::new();

        self.cube_scan_wrapper_rules(&mut rules);
        self.wrapper_pull_up_rules(&mut rules);
        self.aggregate_rules(&mut rules);
        self.projection_rules(&mut rules);
        self.limit_rules(&mut rules);
        self.filter_rules(&mut rules);
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
        self.negative_expr_rules(&mut rules);
        self.not_expr_rules(&mut rules);
        self.distinct_rules(&mut rules);

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

    fn list_pushdown_pullup_rules(
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
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
                    "?ungrouped",
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

        rules.extend(vec![rewrite(
            rule_name,
            wrapper_pushdown_replacer(
                list_node,
                "?alias_to_cube",
                "?ungrouped",
                "?in_projection",
                "?cube_members",
            ),
            wrapper_pullup_replacer(
                substitute_list_node,
                "?alias_to_cube",
                "?ungrouped",
                "?in_projection",
                "?cube_members",
            ),
        )]);
    }

    fn expr_list_pushdown_pullup_rules(
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
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
                    "?ungrouped",
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

        rules.extend(vec![rewrite(
            rule_name,
            wrapper_pushdown_replacer(
                list_node,
                "?alias_to_cube",
                "?ungrouped",
                "?in_projection",
                "?cube_members",
            ),
            wrapper_pullup_replacer(
                list_node,
                "?alias_to_cube",
                "?ungrouped",
                "?in_projection",
                "?cube_members",
            ),
        )]);
    }
}
