use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{FullKeyAggregateQuery, SimpleQuery, SimpleQuerySource};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{Expr, Filter, MemberExpression, QueryPlan, Select, SelectBuilder};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseMember, MemberSymbolRef};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct FullKeyAggregateQueryProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, FullKeyAggregateQuery> for FullKeyAggregateQueryProcessor<'a> {
    type PhysycalNode = Rc<Select>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        logical_plan: &FullKeyAggregateQuery,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let query_tools = self.builder.query_tools();
        todo!()
    }
}

impl ProcessableNode for FullKeyAggregateQuery {
    type ProcessorType<'a> = FullKeyAggregateQueryProcessor<'a>;
}
