use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::QuerySource;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::From;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct QuerySourceProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, QuerySource> for QuerySourceProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        query_source: &QuerySource,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        match query_source {
            QuerySource::LogicalJoin(item) => self.builder.process_node(item.as_ref(), context),
            QuerySource::FullKeyAggregate(item) => {
                self.builder.process_node(item.as_ref(), context)
            }
            QuerySource::PreAggregation(item) => self.builder.process_node(item.as_ref(), context),
        }
    }
}

impl ProcessableNode for QuerySource {
    type ProcessorType<'a> = QuerySourceProcessor<'a>;
}
