mod aggregate;
mod aggregate_function;
mod alias;
mod between_expr;
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
mod join;
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
mod udaf_function;
mod udf_function;
mod union;
mod window;
mod window_function;
mod wrapper_pull_up;

use crate::{
    compile::rewrite::{
        fun_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite, RewriteRules},
        rules::{
            replacer_flat_pull_up_node, replacer_flat_push_down_node, replacer_pull_up_node,
            replacer_push_down_node,
        },
        wrapper_pullup_replacer, wrapper_pushdown_replacer, ListType,
        WrapperReplacerContextInputDataSource,
    },
    config::ConfigObj,
    singular_eclass,
    transport::{DataSource, MetaContext},
};
use egg::{Subst, Var};
use std::{fmt::Display, sync::Arc};

pub struct WrapperRules {
    meta_context: Arc<MetaContext>,
    config_obj: Arc<dyn ConfigObj>,
}

impl RewriteRules for WrapperRules {
    fn rewrite_rules(&self) -> Vec<CubeRewrite> {
        let mut rules = Vec::new();

        self.cube_scan_wrapper_rules(&mut rules);
        self.join_rules(&mut rules);
        self.union_rules(&mut rules);
        self.wrapper_pull_up_rules(&mut rules);
        self.aggregate_rules(&mut rules);
        self.aggregate_rules_subquery(&mut rules);
        self.aggregate_merge_rules(&mut rules);
        self.projection_rules(&mut rules);
        self.projection_rules_subquery(&mut rules);
        self.projection_merge_rules(&mut rules);
        self.limit_rules(&mut rules);
        self.filter_rules(&mut rules);
        self.filter_rules_subquery(&mut rules);
        self.filter_merge_rules(&mut rules);
        self.subquery_rules(&mut rules);
        self.order_rules(&mut rules);
        self.window_rules(&mut rules);
        self.aggregate_function_rules(&mut rules);
        self.window_function_rules(&mut rules);
        self.scalar_function_rules(&mut rules);
        self.udf_function_rules(&mut rules);
        self.udaf_function_rules(&mut rules);
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
        self.between_expr_rules(&mut rules);

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
            |node| wrapper_pushdown_replacer(node, "?context"),
            false,
        ));

        rules.extend(replacer_pull_up_node(
            rule_name,
            list_node,
            substitute_list_node,
            |node| wrapper_pullup_replacer(node, "?context"),
        ));

        rules.extend(vec![rewrite(
            &format!("{}-tail", rule_name),
            wrapper_pushdown_replacer(list_node, "?context"),
            wrapper_pullup_replacer(substitute_list_node, "?context"),
        )]);
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
            |node| wrapper_pushdown_replacer(node, "?context"),
            false,
        ));

        rules.extend(replacer_flat_pull_up_node(
            rule_name,
            list_type.clone(),
            substitute_list_type.clone(),
            |node| wrapper_pullup_replacer(node, "?context"),
            &["?context"],
        ));

        rules.extend(vec![rewrite(
            &format!("{}-tail", rule_name),
            wrapper_pushdown_replacer(list_type.empty_list(), "?context"),
            wrapper_pullup_replacer(substitute_list_type.empty_list(), "?context"),
        )]);
    }

    fn expr_list_pushdown_pullup_rules(
        rules: &mut Vec<CubeRewrite>,
        rule_name: &str,
        list_node: &str,
    ) {
        rules.extend(replacer_push_down_node(
            rule_name,
            list_node,
            |node| wrapper_pushdown_replacer(node, "?context"),
            false,
        ));

        rules.extend(replacer_pull_up_node(
            rule_name,
            list_node,
            list_node,
            |node| wrapper_pullup_replacer(node, "?context"),
        ));

        rules.extend(vec![rewrite(
            rule_name,
            wrapper_pushdown_replacer(list_node, "?context"),
            wrapper_pullup_replacer(list_node, "?context"),
        )]);
    }

    fn get_data_source<'graph>(
        egraph: &'graph CubeEGraph,
        subst: &mut Subst,
        input_data_source_var: Var,
    ) -> Result<DataSource<'graph>, &'static str> {
        let input_data_source = singular_eclass!(
            egraph[subst[input_data_source_var]],
            WrapperReplacerContextInputDataSource
        );
        let input_data_source =
            input_data_source.ok_or("Non-singular eclass for pull up data source")?;
        Ok(match input_data_source {
            None => DataSource::Unrestricted,
            Some(ds) => DataSource::Specific(ds),
        })
    }

    fn can_rewrite_template(data_source: &DataSource, meta: &MetaContext, template: &str) -> bool {
        let sql_generator = match data_source {
            DataSource::Specific(data_source) => {
                let Some(sql_generator) = meta.data_source_to_sql_generator.get(*data_source)
                else {
                    return false;
                };
                sql_generator
            }
            // Data source is not pinned down at rewrite time, but SQL generation will
            // resolve a specific one. Optimistically assume the resolved generator will
            // support the template: a miss surfaces as a generation-time error, never
            // broken SQL. Callers that must not commit to an unrenderable shape should
            // use `is_template_available_on_every_data_source` instead.
            DataSource::Unrestricted => return true,
        };

        sql_generator
            .get_sql_templates()
            .templates
            .contains_key(template)
    }

    /// Whether every registered SQL generator supports `template`. Used for template
    /// checks on plans whose data source is not pinned down at rewrite time
    /// ([`DataSource::Unrestricted`]): whichever generator the plan resolves to must be
    /// able to render the committed shape. An empty generator registry refuses: there
    /// is nothing that could render the template.
    fn is_template_available_on_every_data_source(meta: &MetaContext, template: &str) -> bool {
        !meta.data_source_to_sql_generator.is_empty()
            && meta
                .data_source_to_sql_generator
                .values()
                .all(|sql_generator| {
                    sql_generator
                        .get_sql_templates()
                        .templates
                        .contains_key(template)
                })
    }
}

#[cfg(test)]
mod tests {
    use super::WrapperRules;
    use crate::{compile::test::sql_generator, transport::MetaContext};
    use std::{collections::HashMap, sync::Arc};
    use uuid::Uuid;

    fn meta_with_generators(generators: Vec<(&str, Vec<(String, String)>)>) -> Arc<MetaContext> {
        Arc::new(MetaContext::new(
            vec![],
            HashMap::new(),
            generators
                .into_iter()
                .map(|(data_source, custom_templates)| {
                    (data_source.to_string(), sql_generator(custom_templates))
                })
                .collect(),
            Uuid::new_v4(),
        ))
    }

    /// A template counts as available for an unrestricted data source only when every
    /// registered generator supports it: whichever generator the plan resolves to must
    /// be able to render the committed shape.
    #[test]
    fn test_template_available_on_every_data_source() {
        // Custom template with an empty value removes the base template
        let meta = meta_with_generators(vec![
            ("default", vec![]),
            (
                "no_full_join",
                vec![("join_types/full".to_string(), "".to_string())],
            ),
        ]);
        assert!(WrapperRules::is_template_available_on_every_data_source(
            &meta,
            "join_types/inner"
        ));
        assert!(!WrapperRules::is_template_available_on_every_data_source(
            &meta,
            "join_types/full"
        ));

        // An empty generator registry refuses: nothing could render the template
        let empty_meta = meta_with_generators(vec![]);
        assert!(!WrapperRules::is_template_available_on_every_data_source(
            &empty_meta,
            "join_types/inner"
        ));
    }
}
