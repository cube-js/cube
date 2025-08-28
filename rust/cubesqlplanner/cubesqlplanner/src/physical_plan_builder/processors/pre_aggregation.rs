use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::{
    PreAggregation, PreAggregationJoin, PreAggregationSource, PreAggregationTable,
    PreAggregationUnion,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{
    From, FromSource, JoinBuilder, JoinCondition, QualifiedColumnName, QueryPlan, Schema,
    SelectBuilder, SingleAliasedSource, SingleSource, Union,
};
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::SqlJoinCondition;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct PreAggregationProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl PreAggregationProcessor<'_> {
    fn make_pre_aggregation_table_source(
        &self,
        table: &PreAggregationTable,
    ) -> Result<SingleAliasedSource, CubeError> {
        let query_tools = self.builder.query_tools();
        let name = table.alias.clone().unwrap_or_else(|| table.name.clone());
        let table_name = query_tools
            .base_tools()
            .pre_aggregation_table_name(table.cube_name.clone(), name.clone())?;
        let alias = PlanSqlTemplates::memeber_alias_name(&table.cube_name, &name, &None);
        let res = SingleAliasedSource::new_from_table_reference(
            table_name,
            Rc::new(Schema::empty()),
            Some(alias),
        );
        Ok(res)
    }

    fn make_pre_aggregation_join_single_source(
        &self,
        source: &PreAggregationSource,
    ) -> Result<SingleAliasedSource, CubeError> {
        match source {
            PreAggregationSource::Single(table) => self.make_pre_aggregation_table_source(table),
            PreAggregationSource::Union(_) => Err(CubeError::user(format!(
                "Lambda rollups not allowed inside join rollups"
            ))),
            PreAggregationSource::Join(_) => {
                Err(CubeError::user(format!("Nested rollup joins not allowed")))
            }
        }
    }

    fn make_pre_aggregation_join_source(
        &self,
        join: &PreAggregationJoin,
    ) -> Result<Rc<From>, CubeError> {
        let root_table_source = self.make_pre_aggregation_join_single_source(&join.root)?;
        let mut join_builder = JoinBuilder::new(root_table_source);
        for item in join.items.iter() {
            let to_table_source = self.make_pre_aggregation_join_single_source(&item.to)?;
            let condition = SqlJoinCondition::try_new(item.on_sql.clone())?;
            let on = JoinCondition::new_base_join(condition);
            join_builder.left_join_aliased_source(to_table_source, on);
        }
        let from = From::new_from_join(join_builder.build());
        Ok(from)
    }

    fn make_pre_aggregation_union_source(
        &self,
        pre_aggregation: &PreAggregation,
        union: &PreAggregationUnion,
    ) -> Result<Rc<From>, CubeError> {
        if union.items.len() == 1 {
            let table_source = self.make_pre_aggregation_table_source(&union.items[0])?;
            return Ok(From::new(FromSource::Single(table_source)));
        }
        let query_tools = self.builder.query_tools();

        let mut union_sources = Vec::new();
        for item in union.items.iter() {
            let table_source = self.make_pre_aggregation_table_source(&item)?;
            let from = From::new(FromSource::Single(table_source));
            let mut select_builder = SelectBuilder::new(from);
            for dim in pre_aggregation.dimensions.iter() {
                let name_in_table =
                    PlanSqlTemplates::memeber_alias_name(&item.cube_alias, &dim.name(), &None);
                let alias = dim.alias();
                select_builder.add_projection_reference_member(
                    &dim,
                    QualifiedColumnName::new(None, name_in_table),
                    Some(alias),
                );
            }
            for (dim, granularity) in pre_aggregation.time_dimensions.iter() {
                let name_in_table = PlanSqlTemplates::memeber_alias_name(
                    &item.cube_alias,
                    &dim.name(),
                    granularity,
                );
                let suffix = if let Some(granularity) = granularity {
                    format!("_{}", granularity.clone())
                } else {
                    "_day".to_string()
                };
                let alias = format!("{}{}", dim.alias(), suffix);
                select_builder.add_projection_reference_member(
                    &dim,
                    QualifiedColumnName::new(None, name_in_table.clone()),
                    Some(alias),
                );
            }
            for meas in pre_aggregation.measures.iter() {
                let name_in_table = PlanSqlTemplates::memeber_alias_name(
                    &item.cube_alias,
                    &meas.name(),
                    &meas.alias_suffix(),
                );
                let alias = meas.alias();
                select_builder.add_projection_reference_member(
                    &meas,
                    QualifiedColumnName::new(None, name_in_table.clone()),
                    Some(alias),
                );
            }
            let context = SqlNodesFactory::new();
            let select = select_builder.build(query_tools.clone(), context);
            let query_plan = QueryPlan::Select(Rc::new(select));
            union_sources.push(query_plan);
        }

        let plan = QueryPlan::Union(Rc::new(Union::new(union_sources)));
        let source = SingleSource::Subquery(Rc::new(plan));
        let alias = PlanSqlTemplates::memeber_alias_name(
            &pre_aggregation.cube_name,
            &pre_aggregation.name,
            &None,
        );
        let aliased_source = SingleAliasedSource { source, alias };
        let from = From::new(FromSource::Single(aliased_source));
        Ok(from)
    }
}

impl<'a> LogicalNodeProcessor<'a, PreAggregation> for PreAggregationProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        pre_aggregation: &PreAggregation,
        _context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let source = &pre_aggregation.source;
        let from = match source.as_ref() {
            PreAggregationSource::Single(table) => {
                let table_source = self.make_pre_aggregation_table_source(table)?;
                From::new(FromSource::Single(table_source))
            }
            PreAggregationSource::Union(union) => {
                let source = self.make_pre_aggregation_union_source(pre_aggregation, union)?;
                source
            }
            PreAggregationSource::Join(join) => {
                let source = self.make_pre_aggregation_join_source(join)?;
                source
            }
        };
        Ok(from)
    }
}

impl ProcessableNode for PreAggregation {
    type ProcessorType<'a> = PreAggregationProcessor<'a>;
}
