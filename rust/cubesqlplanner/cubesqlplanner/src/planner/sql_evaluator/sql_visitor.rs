use super::sql_nodes::SqlNode;
use super::CubeRefEvaluator;
use super::MemberSymbol;
use crate::plan::Filter;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_call::CubeRef;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct SqlEvaluatorVisitor {
    query_tools: Rc<QueryTools>,
    cube_ref_evaluator: Rc<CubeRefEvaluator>,
    all_filters: Option<Filter>, //To pass to FILTER_PARAMS and FILTER_GROUP
    ignore_tz_convert: bool,
    /// When `true`, the caller (typically a `SqlCall` substitution site) expects
    /// the rendered expression to be safe for embedding next to operators —
    /// i.e. a compound top-level result should be wrapped in parentheses.
    arg_needs_paren_safe: bool,
}

impl SqlEvaluatorVisitor {
    pub fn new(
        query_tools: Rc<QueryTools>,
        cube_ref_evaluator: Rc<CubeRefEvaluator>,
        all_filters: Option<Filter>,
    ) -> Self {
        Self {
            query_tools,
            cube_ref_evaluator,
            all_filters,
            ignore_tz_convert: false,
            arg_needs_paren_safe: false,
        }
    }

    pub fn with_ignore_tz_convert(&self) -> Self {
        let mut self_copy = self.clone();
        self_copy.ignore_tz_convert = true;
        self_copy
    }

    pub fn with_arg_needs_paren_safe(&self, value: bool) -> Self {
        let mut self_copy = self.clone();
        self_copy.arg_needs_paren_safe = value;
        self_copy
    }

    pub fn arg_needs_paren_safe(&self) -> bool {
        self.arg_needs_paren_safe
    }

    pub fn all_filters(&self) -> Option<Filter> {
        self.all_filters.clone()
    }

    pub fn apply(
        &self,
        node: &Rc<MemberSymbol>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let result = node_processor.to_sql(
            self,
            node,
            self.query_tools.clone(),
            node_processor.clone(),
            templates,
        )?;
        Ok(result)
    }

    pub fn ignore_tz_convert(&self) -> bool {
        self.ignore_tz_convert
    }

    pub fn evaluate_cube_ref(
        &self,
        cube_ref: &CubeRef,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        self.cube_ref_evaluator.evaluate(
            cube_ref,
            self,
            node_processor,
            self.query_tools.clone(),
            templates,
        )
    }
}
