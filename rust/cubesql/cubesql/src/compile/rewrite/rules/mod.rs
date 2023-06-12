use crate::compile::rewrite::{analysis::LogicalPlanAnalysis, rewrite, LogicalPlanLanguage};
use egg::Rewrite;

pub mod case;
pub mod dates;
pub mod filters;
pub mod members;
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
