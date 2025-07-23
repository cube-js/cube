use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{AggregateMultipliedSubquery, AggregateMultipliedSubquerySouce};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{
    Expr, From, JoinBuilder, JoinCondition, MemberExpression, QualifiedColumnName, Select,
    SelectBuilder,
};
use crate::planner::sql_evaluator::ReferencesBuilder;
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

        let mut context_factory = context.make_sql_nodes_factory()?;
        let primary_keys_dimensions = &aggregate_multiplied_subquery
            .keys_subquery
            .primary_keys_dimensions;
        let pk_cube = aggregate_multiplied_subquery.keys_subquery.pk_cube.clone();
        let pk_cube_alias = pk_cube
            .cube
            .default_alias_with_prefix(&Some(format!("{}_key", pk_cube.cube.default_alias())));

        match &aggregate_multiplied_subquery.source {
            AggregateMultipliedSubquerySouce::Cube(cube) => {
                let conditions = primary_keys_dimensions
                    .iter()
                    .map(|dim| -> Result<_, CubeError> {
                        let alias_in_keys_query = keys_query.schema().resolve_member_alias(dim);
                        let keys_query_ref = Expr::Reference(QualifiedColumnName::new(
                            Some(keys_query_alias.clone()),
                            alias_in_keys_query,
                        ));
                        let pk_cube_expr = Expr::Member(MemberExpression::new(dim.clone()));
                        Ok(vec![(keys_query_ref, pk_cube_expr)])
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                join_builder.left_join_cube(
                    cube.cube.clone(),
                    Some(pk_cube_alias.clone()),
                    JoinCondition::new_dimension_join(conditions, false),
                );
                for dimension_subquery in aggregate_multiplied_subquery.dimension_subqueries.iter()
                {
                    self.builder.add_subquery_join(
                        dimension_subquery.clone(),
                        &mut join_builder,
                        context,
                    )?;
                }
            }
            AggregateMultipliedSubquerySouce::MeasureSubquery(measure_subquery) => {
                let subquery = self
                    .builder
                    .process_node(measure_subquery.as_ref(), context)?;
                let conditions = primary_keys_dimensions
                    .iter()
                    .map(|dim| -> Result<_, CubeError> {
                        let alias_in_keys_query = keys_query.schema().resolve_member_alias(dim);
                        let keys_query_ref = Expr::Reference(QualifiedColumnName::new(
                            Some(keys_query_alias.clone()),
                            alias_in_keys_query,
                        ));
                        let alias_in_measure_subquery = subquery.schema().resolve_member_alias(dim);
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
                            subquery.schema().resolve_member_alias(meas),
                        ),
                    );
                }

                context_factory.set_ungrouped_measure_references(ungrouped_measure_references);

                join_builder.left_join_subselect(
                    subquery,
                    pk_cube_alias.clone(),
                    JoinCondition::new_dimension_join(conditions, false),
                );
            }
        }

        let from = From::new_from_join(join_builder.build());
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut select_builder = SelectBuilder::new(from.clone());
        let mut group_by = Vec::new();

        self.builder.resolve_subquery_dimensions_references(
            &aggregate_multiplied_subquery.dimension_subqueries,
            &references_builder,
            &mut render_references,
        )?;

        for member in aggregate_multiplied_subquery.schema.all_dimensions() {
            references_builder.resolve_references_for_member(
                member.clone(),
                &None,
                &mut render_references,
            )?;
            let alias = references_builder.resolve_alias_for_member(&member.full_name(), &None);
            group_by.push(Expr::Member(MemberExpression::new(member.clone())));
            select_builder.add_projection_member(&member, alias);
        }
        for (measure, exists) in self
            .builder
            .measures_for_query(&aggregate_multiplied_subquery.schema.measures, &context)
        {
            if exists {
                if matches!(
                    &aggregate_multiplied_subquery.source,
                    AggregateMultipliedSubquerySouce::Cube(_)
                ) {
                    references_builder.resolve_references_for_member(
                        measure.clone(),
                        &None,
                        &mut render_references,
                    )?;
                }
                select_builder.add_projection_member(&measure, None);
            } else {
                select_builder.add_null_projection(&measure, None);
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
        Ok(Rc::new(
            select_builder.build(query_tools.clone(), context_factory),
        ))
    }
}

impl ProcessableNode for AggregateMultipliedSubquery {
    type ProcessorType<'a> = AggregateMultipliedSubqueryProcessor<'a>;
}
