use crate::physical_plan::cube_ref_evaluator::CubeRefEvaluator;
use crate::physical_plan::sql_nodes::{SqlNode, SqlNodesFactory};
use crate::physical_plan::sql_visitor::SqlEvaluatorVisitor;
use crate::planner::filter::Filter;
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::FiltersContext;
use crate::planner::{MemberSymbol, SqlCall};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct VisitorContext {
    query_tools: Rc<QueryTools>,
    node_processor: Rc<dyn SqlNode>,
    cube_ref_evaluator: Rc<CubeRefEvaluator>,
    all_filters: Option<Filter>, //To pass to FILTER_PARAMS and FILTER_GROUP
    time_shifts: TimeShiftState, //To pass to FILTER_PARAMS in time-shifted CTEs
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
            reading_pre_aggregation: nodes_factory.reading_pre_aggregation(),
        };
        let node_processor = nodes_factory.default_node_processor(&query_tools);
        Self {
            query_tools,
            node_processor,
            cube_ref_evaluator: Rc::new(nodes_factory.cube_ref_evaluator()),
            all_filters,
            time_shifts: nodes_factory.time_shifts().clone(),
            filters_context,
        }
    }

    pub fn new_for_filter_params(
        query_tools: Rc<QueryTools>,
        nodes_factory: &SqlNodesFactory,
        filter_params_columns: HashMap<String, crate::cube_bridge::member_sql::FilterParamsColumn>,
        time_shifts: TimeShiftState,
    ) -> Self {
        let filters_context = FiltersContext {
            use_local_tz: nodes_factory.use_local_tz_in_date_range(),
            filter_params_columns,
            reading_pre_aggregation: nodes_factory.reading_pre_aggregation(),
        };
        let node_processor = nodes_factory.default_node_processor(&query_tools);
        Self {
            query_tools,
            node_processor,
            cube_ref_evaluator: Rc::new(nodes_factory.cube_ref_evaluator()),
            all_filters: None,
            time_shifts,
            filters_context,
        }
    }

    pub fn make_visitor(&self, query_tools: Rc<QueryTools>) -> SqlEvaluatorVisitor {
        SqlEvaluatorVisitor::new(
            query_tools,
            self.cube_ref_evaluator.clone(),
            self.all_filters.clone(),
        )
        .with_time_shifts(self.time_shifts.clone())
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
