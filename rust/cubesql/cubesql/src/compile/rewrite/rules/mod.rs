use crate::compile::rewrite::{
    analysis::LogicalPlanAnalysis, list_rewrite, list_rewrite_with_lists,
    list_rewrite_with_lists_and_vars, rewrite, ListApplierListPattern, ListPattern, ListType,
    LogicalPlanLanguage,
};
use egg::Rewrite;

pub mod case;
pub mod common;
pub mod dates;
pub mod filters;
pub mod flatten;
pub mod members;
pub mod old_split;
pub mod order;
pub mod split;
pub mod utils;
pub mod wrapper;

pub fn replacer_push_down_node(
    name: &str,
    list_node: &str,
    replacer_node: impl Fn(String) -> String,
    include_tail: bool,
) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
    let push_down_rule = rewrite(
        &format!("{}-push-down", name),
        replacer_node(format!("({} ?left ?right)", list_node)),
        format!(
            "({} {} {})",
            list_node,
            replacer_node("?left".to_string()),
            replacer_node("?right".to_string())
        ),
    );
    if include_tail {
        vec![
            push_down_rule,
            rewrite(
                &format!("{}-tail", name),
                replacer_node(list_node.to_string()),
                list_node.to_string(),
            ),
        ]
    } else {
        vec![push_down_rule]
    }
}

pub fn replacer_flat_push_down_node(
    name: &str,
    list_type: ListType,
    replacer_node: impl Fn(String) -> String,
    include_tail: bool,
) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
    let push_down_rule = list_rewrite(
        &format!("{}-push-down", name),
        list_type.clone(),
        ListPattern {
            pattern: replacer_node("?list".to_string()),
            list_var: "?list".to_string(),
            elem: "?elem".to_string(),
        },
        ListPattern {
            pattern: "?new_list".to_string(),
            list_var: "?new_list".to_string(),
            elem: replacer_node("?elem".to_string()),
        },
    );
    if include_tail {
        vec![
            push_down_rule,
            rewrite(
                &format!("{}-empty", name),
                replacer_node(list_type.empty_list()),
                list_type.empty_list(),
            ),
        ]
    } else {
        vec![push_down_rule]
    }
}

pub fn replacer_pull_up_node(
    name: &str,
    list_node: &str,
    substitute_list_node: &str,
    replacer_node: impl Fn(String) -> String,
) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
    let pull_up_rule = rewrite(
        &format!("{}-pull-up", name),
        format!(
            "({} {} {})",
            list_node,
            replacer_node("?left".to_string()),
            replacer_node("?right".to_string())
        ),
        replacer_node(format!("({} ?left ?right)", substitute_list_node)),
    );
    vec![pull_up_rule]
}

pub fn replacer_flat_pull_up_node(
    name: &str,
    list_type: ListType,
    substitute_list_type: ListType,
    replacer_node: impl Fn(String) -> String,
    top_level_elem_vars: &[&str],
) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
    let pull_up_rule = list_rewrite_with_lists_and_vars(
        &format!("{}-pull-up", name),
        list_type,
        ListPattern {
            pattern: "?list".to_string(),
            list_var: "?list".to_string(),
            elem: replacer_node("?elem".to_string()),
        },
        &replacer_node("?new_list".to_string()),
        [ListApplierListPattern {
            list_type: substitute_list_type,
            new_list_var: "?new_list".to_string(),
            elem_pattern: "?elem".to_string(),
        }],
        top_level_elem_vars,
    );
    vec![pull_up_rule]
}

pub fn replacer_push_down_node_substitute_rules(
    name: &str,
    list_node: &str,
    substitute_node: &str,
    replacer_node: impl Fn(String) -> String,
) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
    vec![
        rewrite(
            &format!("{}-push-down", name),
            replacer_node(format!("({} ?left ?right)", list_node)),
            format!(
                "({} {} {})",
                substitute_node,
                replacer_node("?left".to_string()),
                replacer_node("?right".to_string())
            ),
        ),
        rewrite(
            &format!("{}-tail", name),
            replacer_node(list_node.to_string()),
            substitute_node.to_string(),
        ),
    ]
}

pub fn replacer_flat_push_down_node_substitute_rules(
    name: &str,
    list_type: ListType,
    substitute_type: ListType,
    replacer_node: impl Fn(String) -> String,
) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
    vec![
        list_rewrite_with_lists(
            &format!("{}-push-down", name),
            list_type.clone(),
            ListPattern {
                pattern: replacer_node("?list".to_string()),
                list_var: "?list".to_string(),
                elem: "?elem".to_string(),
            },
            "?new_list",
            [ListApplierListPattern {
                list_type: substitute_type.clone(),
                new_list_var: "?new_list".to_string(),
                elem_pattern: replacer_node("?elem".to_string()),
            }],
        ),
        rewrite(
            &format!("{}-tail", name),
            replacer_node(list_type.empty_list()),
            substitute_type.empty_list(),
        ),
    ]
}
