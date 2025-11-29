use super::sql_nodes::SqlNode;
use super::{symbols::MemberSymbol, SqlEvaluatorVisitor};
use crate::cube_bridge::member_sql::{FilterParamsColumn, SecutityContextProps, SqlTemplate};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::VisitorContext;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;
use typed_builder::TypedBuilder;

pub struct SqlCallArg;

impl SqlCallArg {
    const ARG_PREFIX: &'static str = "arg";
    const FILTER_PARAM_PREFIX: &'static str = "fp";
    const FILTER_GROUP_PREFIX: &'static str = "fg";
    const SECURITY_VALUE_PREFIX: &'static str = "sv";

    pub fn dependency(i: usize) -> String {
        format!("{{{}:{}}}", Self::ARG_PREFIX, i)
    }
    pub fn filter_param(i: usize) -> String {
        format!("{{{}:{}}}", Self::FILTER_PARAM_PREFIX, i)
    }
    pub fn filter_group(i: usize) -> String {
        format!("{{{}:{}}}", Self::FILTER_GROUP_PREFIX, i)
    }
    pub fn security_value(i: usize) -> String {
        format!("{{{}:{}}}", Self::SECURITY_VALUE_PREFIX, i)
    }
}

#[derive(Debug, Clone)]
pub struct SqlCallDependency {
    pub path: Vec<String>,
    pub symbol: Rc<MemberSymbol>,
}

#[derive(Debug, Clone)]
pub struct SqlCallFilterParamsItem {
    pub filter_symbol_name: String,
    pub column: FilterParamsColumn,
}

#[derive(Clone, Debug)]
pub struct SqlCallFilterGroupItem {
    pub filter_params: Vec<SqlCallFilterParamsItem>,
}

#[derive(Clone, TypedBuilder, Debug)]
pub struct SqlCall {
    template: SqlTemplate,
    deps: Vec<SqlCallDependency>,
    filter_params: Vec<SqlCallFilterParamsItem>,
    filter_groups: Vec<SqlCallFilterGroupItem>,
    security_context: SecutityContextProps,
}

impl SqlCall {
    pub fn eval(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if let SqlTemplate::String(template) = &self.template {
            let (filter_params, filter_groups, deps, context_values) =
                self.prepare_template_params(visitor, node_processor, &query_tools, templates)?;

            // Substitute placeholders in template in a single pass
            Self::substitute_template(
                template,
                &deps,
                &filter_params,
                &filter_groups,
                &context_values,
            )
        } else {
            Err(CubeError::internal(
                "SqlCall::eval called for fuction that return string".to_string(),
            ))
        }
    }

    pub fn eval_vec(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<Vec<String>, CubeError> {
        let (filter_params, filter_groups, deps, context_values) =
            self.prepare_template_params(visitor, node_processor, &query_tools, templates)?;

        let result = match &self.template {
            SqlTemplate::String(template) => {
                vec![Self::substitute_template(
                    template,
                    &deps,
                    &filter_params,
                    &filter_groups,
                    &context_values,
                )?]
            }
            SqlTemplate::StringVec(templates) => templates
                .iter()
                .map(|template| {
                    Self::substitute_template(
                        template,
                        &deps,
                        &filter_params,
                        &filter_groups,
                        &context_values,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(result)
    }

    fn prepare_template_params(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: &Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<(Vec<String>, Vec<String>, Vec<String>, Vec<String>), CubeError> {
        let filter_params = self
            .filter_params
            .iter()
            .map(|itm| {
                Self::eval_filter_group(
                    std::slice::from_ref(&itm),
                    visitor,
                    query_tools.clone(),
                    templates,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let filter_groups = self
            .filter_groups
            .iter()
            .map(|itm| {
                Self::eval_filter_group(&itm.filter_params, visitor, query_tools.clone(), templates)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let deps = self
            .deps
            .iter()
            .map(|dep| visitor.apply(&dep.symbol, node_processor.clone(), templates))
            .collect::<Result<Vec<_>, _>>()?;

        let context_values = self.eval_security_context_values(&query_tools);
        Ok((filter_params, filter_groups, deps, context_values))
    }

    /// Substitute placeholders in template string with computed values in a single pass
    /// Supports placeholders: {arg:N}, {fp:N}, {fg:N}, {sv:N}
    fn substitute_template(
        template: &str,
        deps: &[String],
        filter_params: &[String],
        filter_groups: &[String],
        security_values: &[String],
    ) -> Result<String, CubeError> {
        let mut result = String::with_capacity(template.len());
        let mut chars = template.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '{' {
                // Found potential placeholder - parse it
                let mut placeholder = String::new();
                let mut found_closing = false;

                while let Some(&next_ch) = chars.peek() {
                    chars.next();
                    if next_ch == '}' {
                        found_closing = true;
                        break;
                    }
                    placeholder.push(next_ch);
                }

                if !found_closing {
                    // No closing brace - treat as literal text
                    result.push('{');
                    result.push_str(&placeholder);
                    continue;
                }

                // Parse placeholder format: "type:index"
                if let Some((typ, idx_str)) = placeholder.split_once(':') {
                    if let Ok(idx) = idx_str.parse::<usize>() {
                        let value = match typ {
                            SqlCallArg::ARG_PREFIX => deps.get(idx),
                            SqlCallArg::FILTER_PARAM_PREFIX => filter_params.get(idx),
                            SqlCallArg::FILTER_GROUP_PREFIX => filter_groups.get(idx),
                            SqlCallArg::SECURITY_VALUE_PREFIX => security_values.get(idx),
                            _ => {
                                result.push('{');
                                result.push_str(&placeholder);
                                result.push('}');
                                continue;
                            }
                        };

                        if let Some(val) = value {
                            result.push_str(val);
                        } else {
                            return Err(CubeError::internal(format!(
                                "Placeholder {{{}:{}}} out of bounds",
                                typ, idx
                            )));
                        }
                    } else {
                        result.push('{');
                        result.push_str(&placeholder);
                        result.push('}');
                        continue;
                    }
                } else {
                    result.push('{');
                    result.push_str(&placeholder);
                    result.push('}');
                    continue;
                }
            } else {
                result.push(ch);
            }
        }

        Ok(result)
    }

    fn eval_filter_group(
        items: &[SqlCallFilterParamsItem],
        visitor: &SqlEvaluatorVisitor,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if let Some(all_filters) = visitor.all_filters() {
            if let Some(filter_item) = all_filters.to_filter_item() {
                let symbols = items
                    .iter()
                    .map(|itm| &itm.filter_symbol_name)
                    .collect_vec();
                if let Some(subtree) = filter_item.find_subtree_for_members(&symbols) {
                    let mut filter_params_columns = HashMap::new();
                    for itm in items {
                        filter_params_columns
                            .insert(itm.filter_symbol_name.clone(), itm.column.clone());
                    }

                    let context = Rc::new(VisitorContext::new_for_filter_params(
                        query_tools.clone(),
                        &SqlNodesFactory::new(),
                        filter_params_columns,
                    ));
                    return subtree.to_sql(templates, context);
                }
            }
        }
        templates.always_true()
    }

    fn eval_security_context_values(&self, query_tools: &Rc<QueryTools>) -> Vec<String> {
        self.security_context
            .values
            .iter()
            .map(|itm| query_tools.allocate_param(itm))
            .collect()
    }

    pub fn is_direct_reference(&self) -> bool {
        self.dependencies_count() == 1
            && self.template == SqlTemplate::String(SqlCallArg::dependency(0))
    }

    pub fn resolve_direct_reference(&self) -> Option<Rc<MemberSymbol>> {
        if self.is_direct_reference() {
            Some(self.deps[0].symbol.clone())
        } else {
            None
        }
    }

    pub fn dependencies_count(&self) -> usize {
        self.deps.iter().filter(|d| !d.symbol.is_cube()).count()
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        self.deps.iter().map(|d| d.symbol.clone()).collect()
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        self.deps
            .iter()
            .map(|d| (d.symbol.clone(), d.path.clone()))
            .collect()
    }

    pub fn extract_symbol_deps(&self, result: &mut Vec<Rc<MemberSymbol>>) {
        for dep in self.deps.iter() {
            result.push(dep.symbol.clone())
        }
    }

    pub fn extract_symbol_deps_with_path(&self, result: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        for dep in self.deps.iter() {
            result.push((dep.symbol.clone(), dep.path.clone()))
        }
    }

    pub fn apply_recursive<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Rc<Self>, CubeError> {
        let mut result = self.clone();
        for dep in result.deps.iter_mut() {
            dep.symbol = dep.symbol.apply_recursive(f)?;
        }
        Ok(Rc::new(result))
    }
}

impl crate::utils::debug::DebugSql for SqlCall {
    fn debug_sql(&self, expand_deps: bool) -> String {
        let template_str = match &self.template {
            SqlTemplate::String(s) => s.clone(),
            SqlTemplate::StringVec(vec) => {
                format!("[{}]", vec.join(", "))
            }
        };

        let deps = self
            .deps
            .iter()
            .map(|dep| {
                if expand_deps {
                    dep.symbol.debug_sql(true)
                } else {
                    format!("{{{}}}", dep.symbol.full_name())
                }
            })
            .collect_vec();

        let filter_params = self
            .filter_params
            .iter()
            .enumerate()
            .map(|(i, _filter_param)| format!("{{FILTER_PARAMS[{}]}}", i))
            .collect_vec();

        let filter_groups = self
            .filter_groups
            .iter()
            .enumerate()
            .map(|(i, _filter_group)| format!("{{FILTER_GROUP[{}]}}", i))
            .collect_vec();

        let context_values = self
            .security_context
            .values
            .iter()
            .enumerate()
            .map(|(key, _value)| format!("{{SECURITY_CONTEXT[{}]}}", key))
            .collect_vec();

        Self::substitute_template(
            &template_str,
            &deps,
            &filter_params,
            &filter_groups,
            &context_values,
        )
        .unwrap()
    }
}
