use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{RootQuery, SubqueryBody};
use crate::physical_plan::{Cte, Select};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Renders the root of the plan: processes every subquery in
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

        for subquery in logical_plan.subqueries().iter() {
            if subquery.inline {
                return Err(CubeError::internal(format!(
                    "Inline subquery `{}` is not supported",
                    subquery.name
                )));
            }
            let query = self.builder.process_node(&subquery.body, &context)?;
            let alias = subquery.name.clone();
            context.add_multi_stage_schema(alias.clone(), query.schema());
            // A generated time series is a self-referencing recursive CTE in
            // some dialects (e.g. MySQL), which need the `WITH RECURSIVE` form.
            let is_recursive = matches!(&subquery.body, SubqueryBody::TimeSeries(_))
                && self
                    .builder
                    .templates()
                    .generated_time_series_is_recursive();
            ctes.push(Rc::new(Cte::new(Rc::new(query), alias, is_recursive)));
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
