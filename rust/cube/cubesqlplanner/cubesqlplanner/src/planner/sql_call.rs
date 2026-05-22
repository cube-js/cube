use super::symbols::MemberSymbol;
use crate::cube_bridge::member_sql::{FilterParamsColumn, SecutityContextProps, SqlTemplate};
use crate::physical_plan::sql_nodes::{SqlNode, SqlNodesFactory};
use crate::physical_plan::{SqlEvaluatorVisitor, VisitorContext};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{CubeNameSymbol, CubeTableSymbol};
use crate::utils::sql_expression_scanner::analyze_template_arg_contexts;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

/// Reference to a cube from a SQL template.
///
/// - `Name` — the cube as an identifier (rendered from `{CUBE}` or
///   `{TABLE}` placeholders); resolves to the cube's quoted name or
///   alias.
/// - `Table` — the cube's table expression (rendered from
///   `{CUBE.sql()}`); resolves to the result of the cube's `sql:`
///   function, or to a registered pre-aggregation source.
#[derive(Clone, Debug)]
pub enum CubeRef {
    Name(Rc<CubeNameSymbol>),
    Table(Rc<CubeTableSymbol>),
}

impl CubeRef {
    pub fn cube_name(&self) -> &String {
        match self {
            CubeRef::Name(symbol) => symbol.cube_name(),
            CubeRef::Table(symbol) => symbol.cube_name(),
        }
    }

    pub fn path(&self) -> &Vec<String> {
        match self {
            CubeRef::Name(symbol) => symbol.path(),
            CubeRef::Table(symbol) => symbol.path(),
        }
    }

    pub fn as_name(&self) -> Option<&Rc<CubeNameSymbol>> {
        match self {
            CubeRef::Name(symbol) => Some(symbol),
            _ => None,
        }
    }

    pub fn as_table(&self) -> Option<&Rc<CubeTableSymbol>> {
        match self {
            CubeRef::Table(symbol) => Some(symbol),
            _ => None,
        }
    }
}

/// One `{arg:N}` binding inside a `SqlCall` template: either a
/// member symbol or a cube reference.
#[derive(Clone, Debug)]
pub enum SqlDependency {
    Symbol(Rc<MemberSymbol>),
    CubeRef(CubeRef),
}

impl SqlDependency {
    pub fn as_symbol(&self) -> Option<&Rc<MemberSymbol>> {
        match self {
            SqlDependency::Symbol(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_cube_ref(&self) -> Option<&CubeRef> {
        match self {
            SqlDependency::CubeRef(cr) => Some(cr),
            _ => None,
        }
    }

    pub fn is_symbol(&self) -> bool {
        matches!(self, SqlDependency::Symbol(_))
    }

    pub fn is_cube_ref(&self) -> bool {
        matches!(self, SqlDependency::CubeRef(_))
    }
}

/// Namespace for the placeholder prefixes recognised inside a
/// `SqlCall` template:
///
/// - `arg:N` — Nth `SqlDependency` (member symbol or cube ref).
/// - `fp:N` — Nth filter param.
/// - `fg:N` — Nth filter group.
/// - `sv:N` — Nth security-context value.
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

/// One `FILTER_PARAMS` binding from the data-model SQL: the filter
/// member whose predicate should be substituted at render time,
/// together with the column information used to format it.
#[derive(Debug, Clone)]
pub struct SqlCallFilterParamsItem {
    pub filter_symbol_name: String,
    pub column: FilterParamsColumn,
}

/// `FILTER_GROUP` binding from the data-model SQL: several
/// `FILTER_PARAMS` items combined into a single substitution.
#[derive(Clone, Debug)]
pub struct SqlCallFilterGroupItem {
    pub filter_params: Vec<SqlCallFilterParamsItem>,
}

/// Tesseract representation of a SQL-like function declared in the
/// data model (member `sql:`, `mask_sql:`, case branches, measure
/// filters, and so on). Plays two roles: it stores the template's
/// dependencies — already resolved to live member symbols and cube
/// references, not symbolic paths — and it knows how to turn those
/// dependencies into the final SQL. Placeholders `{arg:N}` /
/// `{fp:N}` / `{fg:N}` / `{sv:N}` are substituted with the rendered
/// SQL of the bound dependency, filter param, filter group or
/// security-context value when the call is evaluated (`eval` /
/// `eval_vec`).
#[derive(Clone, Debug)]
pub struct SqlCall {
    template: SqlTemplate,
    deps: Vec<SqlDependency>,
    filter_params: Vec<SqlCallFilterParamsItem>,
    filter_groups: Vec<SqlCallFilterGroupItem>,
    security_context: SecutityContextProps,
    // Per `{arg:N}` index: whether the surrounding context in the template
    // would make a compound substitution unsafe (requiring parentheses).
    // Computed once at construction from the template.
    arg_paren_contexts: HashMap<usize, bool>,
}

impl SqlCall {
    pub(super) fn new(
        template: SqlTemplate,
        deps: Vec<SqlDependency>,
        filter_params: Vec<SqlCallFilterParamsItem>,
        filter_groups: Vec<SqlCallFilterGroupItem>,
        security_context: SecutityContextProps,
    ) -> Self {
        let arg_paren_contexts = match &template {
            SqlTemplate::String(s) => analyze_template_arg_contexts(s),
            SqlTemplate::StringVec(strings) => {
                let mut merged: HashMap<usize, bool> = HashMap::new();
                for s in strings {
                    for (idx, needs_safe) in analyze_template_arg_contexts(s) {
                        let entry = merged.entry(idx).or_insert(false);
                        *entry = *entry || needs_safe;
                    }
                }
                merged
            }
        };
        Self {
            template,
            deps,
            filter_params,
            filter_groups,
            security_context,
            arg_paren_contexts,
        }
    }

    /// Detects the `count(*)` literal pattern: no dependencies, no
    /// filter params / groups, no security-context bindings, and a
    /// single-string template that is `count(*)` (case-insensitive,
    /// leading/trailing whitespace ignored). Used to recognise a
    /// `type: number, sql: count(*)` measure as the same shape as a
    /// `type: count` measure with no `sql`, so the planner can treat
    /// it as additive in a multiplied join.
    pub fn is_count_star(&self) -> bool {
        if !self.deps.is_empty()
            || !self.filter_params.is_empty()
            || !self.filter_groups.is_empty()
            || !self.security_context.values.is_empty()
        {
            return false;
        }
        let SqlTemplate::String(template) = &self.template else {
            return false;
        };
        let trimmed = template.trim();
        trimmed.len() == "count(*)".len() && trimmed.eq_ignore_ascii_case("count(*)")
    }

    /// Build a `SqlCall` that simply proxies to the given member's SQL —
    /// equivalent to a one-arg template `{arg:0}` referencing it. Use when
    /// an API expects a `SqlCall` but the planner already has a symbol and
    /// there is no real template to compile (e.g. a synthetic
    /// `MAX(<time_dim>)` aggregation built ad hoc in the planner).
    pub fn proxy_for_member(member: Rc<MemberSymbol>) -> Rc<Self> {
        Rc::new(Self::new(
            SqlTemplate::String(SqlCallArg::dependency(0)),
            vec![SqlDependency::Symbol(member)],
            vec![],
            vec![],
            SecutityContextProps::default(),
        ))
    }

    /// Renders the template into a single SQL string. Errors when
    /// the template is a `StringVec` — use `eval_vec` for that case.
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

            Self::substitute_template(
                template,
                &deps,
                &filter_params,
                &filter_groups,
                &context_values,
            )
        } else {
            Err(CubeError::internal(
                "SqlCall::eval called for function that returns string".to_string(),
            ))
        }
    }

    /// Renders the template into one SQL string per element when it
    /// is a `StringVec`, or a single-element vector for plain
    /// `String` templates. `StringVec` is produced by pre-aggregation
    /// dimension / measure reference lists, where the data-model
    /// definition returns the SQL of all referenced members at once.
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

    /// True when the SQL belongs to a single cube — either it has no
    /// dependencies at all (a constant expression), or it references
    /// the owning cube directly through a `{CUBE}` placeholder.
    pub fn is_owned_by_cube(&self) -> bool {
        if self.deps.is_empty() {
            true
        } else {
            self.deps.iter().any(|dep| dep.is_cube_ref())
        }
    }

    /// All `CubeRef::Name` dependencies — cube-name placeholders the
    /// template refers to.
    pub fn cube_name_deps(&self) -> Vec<Rc<CubeNameSymbol>> {
        self.deps
            .iter()
            .filter_map(|dep| {
                if let SqlDependency::CubeRef(CubeRef::Name(symbol)) = dep {
                    Some(symbol.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        let mut result = vec![];
        self.extract_cube_refs(&mut result);
        result
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
            .enumerate()
            .map(|(i, dep)| {
                // Each arg's `arg_needs_paren_safe` flag is set by this call's
                // template context, overriding whatever the caller's visitor
                // carried. The caller's flag only governs wrapping of this
                // whole SqlCall's output, handled by an enclosing Parenthesize
                // node up the processor chain.
                let needs_safe = *self.arg_paren_contexts.get(&i).unwrap_or(&false);
                let arg_visitor = visitor.with_arg_needs_paren_safe(needs_safe);
                match dep {
                    SqlDependency::Symbol(m) => {
                        arg_visitor.apply(m, node_processor.clone(), templates)
                    }
                    SqlDependency::CubeRef(cr) => {
                        arg_visitor.evaluate_cube_ref(cr, node_processor.clone(), templates)
                    }
                }
            })
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

                    let context = VisitorContext::new_for_filter_params(
                        query_tools.clone(),
                        &SqlNodesFactory::new(),
                        filter_params_columns,
                    );
                    return crate::physical_plan::filter::render_filter_item(
                        &context, &subtree, templates,
                    );
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

    /// True if the entire template is exactly `{arg:0}` with a single
    /// member-symbol dependency — i.e. the SQL is a plain reference
    /// to another member, with no wrapping expression.
    pub fn is_direct_reference(&self) -> bool {
        self.dependencies_count() == 1
            && self.template == SqlTemplate::String(SqlCallArg::dependency(0))
    }

    /// The single member this call references when `is_direct_reference`
    /// is true; `None` otherwise.
    pub fn resolve_direct_reference(&self) -> Option<Rc<MemberSymbol>> {
        if self.is_direct_reference() {
            self.deps[0].as_symbol().cloned()
        } else {
            None
        }
    }

    /// Number of member-symbol dependencies. Cube refs are not
    /// counted.
    pub fn dependencies_count(&self) -> usize {
        self.deps.iter().filter(|d| d.is_symbol()).count()
    }

    /// Member-symbol dependencies of the call. Cube refs are excluded
    /// — use `get_cube_refs` for those.
    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        self.deps
            .iter()
            .filter_map(|d| d.as_symbol().cloned())
            .collect()
    }

    pub fn extract_symbol_deps(&self, result: &mut Vec<Rc<MemberSymbol>>) {
        for dep in self.deps.iter() {
            if let Some(s) = dep.as_symbol() {
                result.push(s.clone())
            }
        }
    }

    pub fn extract_cube_refs(&self, result: &mut Vec<CubeRef>) {
        for dep in self.deps.iter() {
            if let SqlDependency::CubeRef(cr) = dep {
                result.push(cr.clone());
            }
        }
    }

    /// Returns a new `SqlCall` with `f` applied recursively to every
    /// member-symbol dependency. Cube refs and other placeholders
    /// pass through unchanged.
    pub fn apply_recursive<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Rc<Self>, CubeError> {
        let mut result = self.clone();
        for dep in result.deps.iter_mut() {
            if let SqlDependency::Symbol(ref s) = dep {
                *dep = SqlDependency::Symbol(s.apply_recursive(f)?);
            }
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
            .map(|dep| match dep {
                SqlDependency::Symbol(s) => {
                    if expand_deps {
                        s.debug_sql(true)
                    } else {
                        format!("{{{}}}", s.full_name())
                    }
                }
                SqlDependency::CubeRef(cr) => {
                    if expand_deps {
                        cr.cube_name().clone()
                    } else {
                        format!("{{{}}}", cr.cube_name())
                    }
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
