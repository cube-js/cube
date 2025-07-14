use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{
    AggregateMultipliedSubquery, AggregateMultipliedSubquerySouce, SimpleQuery, SimpleQuerySource,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{
    Expr, Filter, From, JoinBuilder, JoinCondition, MemberExpression, QualifiedColumnName,
    QueryPlan, Select, SelectBuilder,
};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::ReferencesBuilder;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseMember, MemberSymbolRef};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct AggregateMultipliedSubqueryProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, AggregateMultipliedSubquery>
    for AggregateMultipliedSubqueryProcessor<'a>
{
    type PhysycalNode = Rc<Select>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        aggregate_multiplied_subquery: &AggregateMultipliedSubquery,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let mut render_references = HashMap::new();
        let query_tools = self.builder.query_tools();
        let keys_query = self.builder.process_node(
            aggregate_multiplied_subquery.keys_subquery.as_ref(),
            context,
        )?;

        let keys_query_alias = format!("keys");

        let mut join_builder =
            JoinBuilder::new_from_subselect(keys_query.clone(), keys_query_alias.clone());

        let mut context_factory = context.make_sql_nodes_factory();
        let primary_keys_dimensions = &aggregate_multiplied_subquery
            .keys_subquery
            .primary_keys_dimensions;
        let pk_cube = aggregate_multiplied_subquery.pk_cube.clone();
        let pk_cube_alias = pk_cube
            .cube
            .default_alias_with_prefix(&Some(format!("{}_key", pk_cube.cube.default_alias())));

        match aggregate_multiplied_subquery.source.as_ref() {
            AggregateMultipliedSubquerySouce::Cube => {
                let conditions = primary_keys_dimensions
                    .iter()
                    .map(|dim| -> Result<_, CubeError> {
                        let member_ref = dim.clone().as_base_member(query_tools.clone())?;
                        let alias_in_keys_query =
                            keys_query.schema().resolve_member_alias(&member_ref);
                        let keys_query_ref = Expr::Reference(QualifiedColumnName::new(
                            Some(keys_query_alias.clone()),
                            alias_in_keys_query,
                        ));
                        let pk_cube_expr = Expr::Member(MemberExpression::new(member_ref.clone()));
                        Ok(vec![(keys_query_ref, pk_cube_expr)])
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                join_builder.left_join_cube(
                    pk_cube.cube.clone(),
                    Some(pk_cube_alias.clone()),
                    JoinCondition::new_dimension_join(conditions, false),
                );
                /* for dimension_subquery in aggregate_multiplied_subquery.dimension_subqueries.iter()
                {
                    self.add_subquery_join(
                        dimension_subquery.clone(),
                        &mut join_builder,
                        &mut render_references,
                        context,
                    )?;
                } */
            }
            AggregateMultipliedSubquerySouce::MeasureSubquery(measure_subquery) => {
                todo!()
                /* let subquery = self.process_measure_subquery(&measure_subquery, context)?;
                let conditions = primary_keys_dimensions
                    .iter()
                    .map(|dim| -> Result<_, CubeError> {
                        let dim_ref = dim.clone().as_base_member(self.query_tools.clone())?;
                        let alias_in_keys_query =
                            keys_query.schema().resolve_member_alias(&dim_ref);
                        let keys_query_ref = Expr::Reference(QualifiedColumnName::new(
                            Some(keys_query_alias.clone()),
                            alias_in_keys_query,
                        ));
                        let alias_in_measure_subquery =
                            subquery.schema().resolve_member_alias(&dim_ref);
                        let measure_subquery_ref = Expr::Reference(QualifiedColumnName::new(
                            Some(pk_cube_alias.clone()),
                            alias_in_measure_subquery,
                        ));
                        Ok(vec![(keys_query_ref, measure_subquery_ref)])
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut ungrouped_measure_references = HashMap::new();
                for meas in aggregate_multiplied_subquery.schema.measures.iter() {
                    ungrouped_measure_references.insert(
                        meas.full_name(),
                        QualifiedColumnName::new(
                            Some(pk_cube_alias.clone()),
                            subquery.schema().resolve_member_alias(
                                &meas.clone().as_base_member(self.query_tools.clone())?,
                            ),
                        ),
                    );
                }

                context_factory.set_ungrouped_measure_references(ungrouped_measure_references);

                join_builder.left_join_subselect(
                    subquery,
                    pk_cube_alias.clone(),
                    JoinCondition::new_dimension_join(conditions, false),
                ); */
            }
        }

        let from = From::new_from_join(join_builder.build());
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut select_builder = SelectBuilder::new(from.clone());
        let mut group_by = Vec::new();
        for member in aggregate_multiplied_subquery.schema.all_dimensions() {
            references_builder.resolve_references_for_member(
                member.clone(),
                &None,
                &mut render_references,
            )?;
            let alias = references_builder.resolve_alias_for_member(&member.full_name(), &None);
            let member_ref = member.clone().as_base_member(query_tools.clone())?;
            group_by.push(Expr::Member(MemberExpression::new(member_ref.clone())));
            select_builder.add_projection_member(&member_ref, alias);
        }
        for (measure, exists) in self
            .builder
            .extend_measures(&aggregate_multiplied_subquery.schema.measures, &context)
        {
            let member_ref = measure.clone().as_base_member(query_tools.clone())?;
            if exists {
                if matches!(
                    aggregate_multiplied_subquery.source.as_ref(),
                    AggregateMultipliedSubquerySouce::Cube
                ) {
                    references_builder.resolve_references_for_member(
                        measure.clone(),
                        &None,
                        &mut render_references,
                    )?;
                }
                select_builder.add_projection_member(&member_ref, None);
            } else {
                select_builder.add_null_projection(&member_ref, None);
            }
        }
        select_builder.set_group_by(group_by);
        context_factory.set_render_references(render_references);
        context_factory.set_rendered_as_multiplied_measures(
            aggregate_multiplied_subquery
                .schema
                .multiplied_measures
                .clone(),
        );
        Ok(Rc::new(select_builder.build(context_factory)))
    }
}

impl ProcessableNode for AggregateMultipliedSubquery {
    type ProcessorType<'a> = AggregateMultipliedSubqueryProcessor<'a>;
}
