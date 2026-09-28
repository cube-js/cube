use super::query_tools::QueryTools;
use super::Compiler;
use super::JoinTreeCache;
use crate::cube_bridge::base_query_options::MaskedMemberItem;
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::join_graph::JoinGraph;
use crate::cube_bridge::security_context::SecurityContext;
use crate::planner::filter::compiler::FilterCompiler;
use crate::planner::filter::{FilterGroup, FilterGroupOperator, FilterItem};
use cubenativeutils::CubeError;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;

/// Mutable, per-query planning state. Owns everything that mutates or
/// caches during planning — the symbol `Compiler` and the join-tree
/// cache — and holds the immutable leaf `QueryTools`.
///
/// This split is what keeps the planner free of `Rc` cycles: cached
/// values (`BaseCube`, `TypedFilter`, symbols, …) only ever hold an
/// `Rc<QueryTools>`, and `QueryTools` owns no cache that could point
/// back at them. `State` is the cache owner, and nothing stored in a
/// cache may hold an `Rc<State>` — so the per-query graph drops cleanly
/// when the query finishes. Transient planners hold `Rc<State>`;
/// long-lived/cached values must not.
///
/// `Deref<Target = QueryTools>` lets callers reach the immutable leaf
/// API directly (`state.cube_evaluator()`, `state.alias_name(..)`, …);
/// the mutable state is reached via `compiler()` / `join_tree_cache()`.
pub struct State {
    query_tools: Rc<QueryTools>,
    compiler: Rc<RefCell<Compiler>>,
    join_tree_cache: JoinTreeCache,
}

impl State {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        security_context: Rc<dyn SecurityContext>,
        base_tools: Rc<dyn BaseTools>,
        join_graph: Rc<dyn JoinGraph>,
        timezone_name: Option<String>,
        export_annotated_sql: bool,
        convert_tz_for_raw_time_dimension: bool,
        masked_members: Option<Vec<MaskedMemberItem>>,
        member_to_alias: Option<HashMap<String, String>>,
    ) -> Result<Rc<Self>, CubeError> {
        let query_tools = QueryTools::try_new(
            cube_evaluator.clone(),
            base_tools.clone(),
            join_graph,
            timezone_name,
            export_annotated_sql,
            convert_tz_for_raw_time_dimension,
            masked_members.clone(),
        )?;

        let compiler = Rc::new(RefCell::new(Compiler::new(
            cube_evaluator,
            base_tools,
            security_context,
            query_tools.timezone(),
            member_to_alias,
        )));

        let result = Rc::new(Self {
            query_tools,
            compiler,
            join_tree_cache: JoinTreeCache::default(),
        });

        result
            .compiler
            .borrow_mut()
            .set_query_tools(Rc::downgrade(&result.query_tools));

        // Compile mask filters now that both the Compiler and the
        // Rc<QueryTools> exist; the result is stored back into QueryTools.
        if let Some(items) = masked_members {
            Self::compile_mask_filters(&result, items)?;
        }

        Ok(result)
    }

    pub fn query_tools(&self) -> &Rc<QueryTools> {
        &self.query_tools
    }

    pub fn compiler(&self) -> &Rc<RefCell<Compiler>> {
        &self.compiler
    }

    pub fn join_tree_cache(&self) -> &JoinTreeCache {
        &self.join_tree_cache
    }

    fn compile_mask_filters(
        this: &Rc<Self>,
        items: Vec<MaskedMemberItem>,
    ) -> Result<(), CubeError> {
        let mut compiled = HashMap::new();
        for item in items {
            let Some(native_filter) = item.filter else {
                continue;
            };
            let mut compiler = this.compiler.borrow_mut();
            let mut filter_compiler = FilterCompiler::new(&mut compiler, this.query_tools.clone());
            filter_compiler.add_item(&native_filter)?;
            let (dimension_filters, _, _) = filter_compiler.extract_result();
            if dimension_filters.is_empty() {
                continue;
            }
            let filter_item = if dimension_filters.len() == 1 {
                dimension_filters.into_iter().next().unwrap()
            } else {
                FilterItem::Group(Rc::new(FilterGroup::new(
                    FilterGroupOperator::And,
                    dimension_filters,
                )))
            };
            compiled.insert(item.member, filter_item);
        }
        this.query_tools.set_member_mask_filters(compiled);
        Ok(())
    }
}

impl Deref for State {
    type Target = QueryTools;

    fn deref(&self) -> &Self::Target {
        &self.query_tools
    }
}
