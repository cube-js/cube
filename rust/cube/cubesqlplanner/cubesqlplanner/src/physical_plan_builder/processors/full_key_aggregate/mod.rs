mod full_join_aggregate_strategy;
mod inner_join_aggregate_strategy;
mod keys_aggregate_strategy;
mod union_aggregate_strategy;

use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{FullKeyAggregate, QueryInputs};
use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::{From, FromSource, SelectBuilder, SingleAliasedSource};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;
use full_join_aggregate_strategy::FullJoinFullKeyAggregateStrategy;
use inner_join_aggregate_strategy::InnerJoinFullKeyAggregateStrategy;
use keys_aggregate_strategy::KeysFullKeyAggregateStrategy;
use std::rc::Rc;
use union_aggregate_strategy::UnionFullKeyAggregateStrategy;

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
        if full_key_aggregate.is_empty() {
            let inputs = QueryInputs::builder()
                .multi_stage_dimension(full_key_aggregate.multi_stage_dimension().clone())
                .build();
            return self.builder.process_node(&inputs, context);
        }
        if let ([sole], []) = (
            full_key_aggregate.multi_stage_subquery_refs().as_slice(),
            full_key_aggregate.keys_input().as_slice(),
        ) {
            let source = SingleAliasedSource::new_from_table_reference(
                sole.name().clone(),
                context.get_multi_stage_schema(sole.name())?,
                None,
            );
            let select = SelectBuilder::new(From::new(FromSource::Single(source)))
                .build(self.builder.query_tools().clone(), SqlNodesFactory::new());
            return Ok(From::new_from_subselect(
                Rc::new(select),
                "fk_aggregate".to_string(),
            ));
        }
        let strategy: Rc<dyn FullKeyAggregateStrategy> =
            if !full_key_aggregate.keys_input().is_empty() {
                // JOIN-model: keys side comes from the logical plan.
                // `KeysFullKeyAggregateStrategy` handles both the explicit
                // `keys_input` shape and the derived-from-measure-refs shape.
                KeysFullKeyAggregateStrategy::new(self.builder)
            } else if !full_key_aggregate.schema().has_dimensions() {
                InnerJoinFullKeyAggregateStrategy::new(self.builder)
            } else if self.builder.supports_union_full_key_aggregate()
                && full_key_aggregate.multi_stage_subquery_refs().len() > 1
            {
                UnionFullKeyAggregateStrategy::new(self.builder)
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
