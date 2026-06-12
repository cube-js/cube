use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{MultiStageMemberLogicalType, RootQuery};
use crate::physical_plan::{Cte, Select};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Renders the root of the plan: processes every CTE member in
/// definition order (registering its schema into the context so
/// later members and the body can reference it) and attaches the
/// resulting CTE list to the body select — the only place a `WITH`
/// clause is emitted.
pub struct RootQueryProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, RootQuery> for RootQueryProcessor<'a> {
    type PhysycalNode = Rc<Select>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        logical_plan: &RootQuery,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let mut context = context.clone();
        let mut ctes = vec![];

        for cte_member in logical_plan.ctes().iter() {
            let query = self
                .builder
                .process_node(&cte_member.member_type, &context)?;
            let alias = cte_member.name.clone();
            context.add_multi_stage_schema(alias.clone(), query.schema());
            if let MultiStageMemberLogicalType::DimensionCalculation(dimension_calculation) =
                &cte_member.member_type
            {
                context.add_multi_stage_dimension_schema(
                    dimension_calculation.resolved_dimensions()?,
                    alias.clone(),
                    dimension_calculation.join_dimensions()?,
                    query.schema(),
                );
            }
            ctes.push(Rc::new(Cte::new(Rc::new(query), alias)));
        }

        let select = self
            .builder
            .process_node(logical_plan.query().as_ref(), &context)?;

        if ctes.is_empty() {
            Ok(select)
        } else {
            Ok(Rc::new(select.with_ctes(ctes)))
        }
    }
}

impl ProcessableNode for RootQuery {
    type ProcessorType<'a> = RootQueryProcessor<'a>;
}
