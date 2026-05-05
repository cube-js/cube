use super::query_tools::QueryTools;
use super::sql_evaluator::sql_nodes::{SqlNode, SqlNodesFactory};
use super::sql_evaluator::{CubeRefEvaluator, MemberSymbol, SqlCall};
use crate::cube_bridge::member_sql::FilterParamsColumn;
use crate::plan::filter::{Filter, FilterItem};
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Default)]
pub struct FiltersContext {
    pub use_local_tz: bool,
    pub filter_params_columns: HashMap<String, FilterParamsColumn>,
}

pub struct VisitorContext {
    query_tools: Rc<QueryTools>,
    node_processor: Rc<dyn SqlNode>,
    cube_ref_evaluator: Rc<CubeRefEvaluator>,
    all_filters: Option<Filter>, //To pass to FILTER_PARAMS and FILTER_GROUP
    filters_context: FiltersContext,
}

impl VisitorContext {
    pub fn new(
        query_tools: Rc<QueryTools>,
        nodes_factory: &SqlNodesFactory,
        all_filters: Option<Filter>,
    ) -> Self {
        let filters_context = FiltersContext {
            use_local_tz: nodes_factory.use_local_tz_in_date_range(),
            filter_params_columns: HashMap::new(),
        };
        Self {
            query_tools,
            node_processor: nodes_factory.default_node_processor(),
            cube_ref_evaluator: Rc::new(nodes_factory.cube_ref_evaluator()),
            all_filters,
            filters_context,
        }
    }

    pub fn new_for_filter_params(
        query_tools: Rc<QueryTools>,
        nodes_factory: &SqlNodesFactory,
        filter_params_columns: HashMap<String, FilterParamsColumn>,
    ) -> Self {
        let filters_context = FiltersContext {
            use_local_tz: nodes_factory.use_local_tz_in_date_range(),
            filter_params_columns,
        };
        Self {
            query_tools,
            node_processor: nodes_factory.default_node_processor(),
            cube_ref_evaluator: Rc::new(nodes_factory.cube_ref_evaluator()),
            all_filters: None,
            filters_context,
        }
    }

    pub fn make_visitor(&self, query_tools: Rc<QueryTools>) -> SqlEvaluatorVisitor {
        SqlEvaluatorVisitor::new(
            query_tools,
            self.cube_ref_evaluator.clone(),
            self.all_filters.clone(),
        )
    }

    pub fn node_processor(&self) -> Rc<dyn SqlNode> {
        self.node_processor.clone()
    }

    pub fn filters_context(&self) -> &FiltersContext {
        &self.filters_context
    }

    pub fn query_tools(&self) -> Rc<QueryTools> {
        self.query_tools.clone()
    }

    /// Render a top-level Filter (AND of all items) using this context.
    /// Convenience wrapper that unpacks the context into the explicit args
    /// expected by Filter::to_sql.
    pub fn render_filter(
        &self,
        filter: &Filter,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let visitor = self.make_visitor(self.query_tools());
        filter.to_sql(
            &visitor,
            self.node_processor(),
            self.query_tools(),
            templates,
            &self.filters_context,
        )
    }

    /// Render a single FilterItem (or group) using this context.
    pub fn render_filter_item(
        &self,
        item: &FilterItem,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let visitor = self.make_visitor(self.query_tools());
        item.to_sql(
            &visitor,
            self.node_processor(),
            self.query_tools(),
            templates,
            &self.filters_context,
        )
    }
}

pub fn evaluate_with_context(
    node: &Rc<MemberSymbol>,
    context: Rc<VisitorContext>,
    templates: &PlanSqlTemplates,
) -> Result<String, CubeError> {
    let visitor = context.make_visitor(context.query_tools());
    let node_processor = context.node_processor();

    visitor.apply(node, node_processor, templates)
}

pub fn evaluate_sql_call_with_context(
    sql_call: &Rc<SqlCall>,
    context: Rc<VisitorContext>,
    templates: &PlanSqlTemplates,
) -> Result<String, CubeError> {
    let visitor = context.make_visitor(context.query_tools());
    let node_processor = context.node_processor();
    sql_call.eval(&visitor, node_processor, context.query_tools(), templates)
}
