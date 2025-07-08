use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{FullKeyAggregate, SimpleQuery, SimpleQuerySource};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{Expr, Filter, MemberExpression, QueryPlan, Select, SelectBuilder};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseMember, MemberSymbolRef};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct FullKeyAggregateProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, FullKeyAggregate> for FullKeyAggregateProcessor<'a> {
    type PhysycalNode = Rc<Select>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        logical_plan: &FullKeyAggregate,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let query_tools = self.builder.query_tools();
        todo!()
    }
}

impl ProcessableNode for FullKeyAggregate {
    type ProcessorType<'a> = FullKeyAggregateProcessor<'a>;
}
