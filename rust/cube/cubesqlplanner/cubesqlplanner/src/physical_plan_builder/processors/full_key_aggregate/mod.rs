mod full_join_aggregate_strategy;
mod inner_join_aggregate_strategy;
mod keys_aggregate_strategy;

use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::FullKeyAggregate;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::From;
use cubenativeutils::CubeError;
use full_join_aggregate_strategy::FullJoinFullKeyAggregateStrategy;
use inner_join_aggregate_strategy::InnerJoinFullKeyAggregateStrategy;
use keys_aggregate_strategy::KeysFullKeyAggregateStrategy;
use std::rc::Rc;

trait FullKeyAggregateStrategy {
    fn process(
        &self,
        full_key_aggregate: &FullKeyAggregate,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<From>, CubeError>;
}

pub struct FullKeyAggregateProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, FullKeyAggregate> for FullKeyAggregateProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        full_key_aggregate: &FullKeyAggregate,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let strategy: Rc<dyn FullKeyAggregateStrategy> =
            if !full_key_aggregate.schema().has_dimensions() {
                InnerJoinFullKeyAggregateStrategy::new(self.builder)
            } else if self.builder.templates().supports_full_join() {
                FullJoinFullKeyAggregateStrategy::new(self.builder)
            } else {
                KeysFullKeyAggregateStrategy::new(self.builder)
            };
        strategy.process(full_key_aggregate, context)
    }
}

impl ProcessableNode for FullKeyAggregate {
    type ProcessorType<'a> = FullKeyAggregateProcessor<'a>;
}
