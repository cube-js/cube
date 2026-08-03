use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::{
    PreAggregation, PreAggregationJoin, PreAggregationSource, PreAggregationTable,
    PreAggregationUnion,
};
use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::{
    From, FromSource, JoinBuilder, JoinCondition, QualifiedColumnName, QueryPlan, Schema,
    SelectBuilder, SingleAliasedSource, SingleSource, Union,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
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
        let base_table_name = query_tools
            .base_tools()
            .pre_aggregation_table_name(table.cube_name.clone(), name.clone())?;
        let table_name = match table.usage_index {
            Some(idx) => format!("{}__usage_{}", base_table_name, idx),
            None => base_table_name,
        };
        let alias = PlanSqlTemplates::member_alias_name(&table.cube_name, &name, &None);
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

    /// Resolve the column a union branch stores a lambda member under. The
    /// lambda exposes the first member rollup's symbols, but each branch keeps
    /// its own cube aliases (e.g. `requests_stream__tenant_id` vs the lambda's
    /// `requests__tenant_id`).
    fn find_branch_member(
        lambda_member: &Rc<MemberSymbol>,
        branch_members: &[Rc<MemberSymbol>],
        branch_cube_name: &str,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(member) = branch_members
            .iter()
            .find(|m| m.full_name() == lambda_member.full_name())
        {
            return Ok(member.clone());
        }
        let short_name = lambda_member.name();
        if let Some(member) = branch_members
            .iter()
            .find(|m| m.name() == short_name && m.cube_name() == branch_cube_name)
        {
            return Ok(member.clone());
        }
        Err(CubeError::internal(format!(
            "Lambda pre-aggregation member '{}' has no match in union branch '{}'",
            lambda_member.full_name(),
            branch_cube_name
        )))
    }

    fn make_pre_aggregation_union_source(
        &self,
        pre_aggregation: &PreAggregation,
        union: &PreAggregationUnion,
    ) -> Result<Rc<From>, CubeError> {
        if union.items.len() == 1 {
            let table_source = self.make_pre_aggregation_table_source(&union.items[0].table)?;
            return Ok(From::new(FromSource::Single(table_source)));
        }
        let query_tools = self.builder.query_tools();

        let mut union_sources = Vec::new();
        for item in union.items.iter() {
            let branch_cube_name = &item.table.cube_name;
            let table_source = self.make_pre_aggregation_table_source(&item.table)?;
            let from = From::new(FromSource::Single(table_source));
            let mut select_builder = SelectBuilder::new(from);
            for dim in pre_aggregation.dimensions().iter() {
                // Read this branch's stored column for the lambda dimension and
                // project it under the lambda's unified alias.
                let branch_dim = Self::find_branch_member(dim, &item.dimensions, branch_cube_name)?;
                select_builder.add_projection_reference_member(
                    &dim,
                    QualifiedColumnName::new(None, branch_dim.alias()),
                    Some(dim.alias()),
                );
            }
            // Match time dimensions on their base member so the granularity
            // suffix is applied consistently on both the read and output sides.
            let branch_time_bases = item
                .time_dimensions
                .iter()
                .map(|td| {
                    if let Ok(t) = td.as_time_dimension() {
                        t.base_symbol().clone()
                    } else {
                        td.clone()
                    }
                })
                .collect::<Vec<_>>();
            for dim in pre_aggregation.time_dimensions().iter() {
                let (lambda_base, granularity) = if let Ok(td) = dim.as_time_dimension() {
                    (td.base_symbol().clone(), td.granularity().clone())
                } else {
                    (dim.clone(), None)
                };

                let branch_base =
                    Self::find_branch_member(&lambda_base, &branch_time_bases, branch_cube_name)?;

                let read_suffix = if let Some(granularity) = &granularity {
                    format!("_{}", granularity)
                } else {
                    String::new()
                };
                let name_in_table = format!("{}{}", branch_base.alias(), read_suffix);

                let out_suffix = if let Some(granularity) = &granularity {
                    format!("_{}", granularity)
                } else {
                    "_day".to_string()
                };
                let alias = format!("{}{}", lambda_base.alias(), out_suffix);
                select_builder.add_projection_reference_member(
                    &dim,
                    QualifiedColumnName::new(None, name_in_table),
                    Some(alias),
                );
            }
            for meas in pre_aggregation.measures().iter() {
                let branch_meas = Self::find_branch_member(meas, &item.measures, branch_cube_name)?;
                select_builder.add_projection_reference_member(
                    &meas,
                    QualifiedColumnName::new(None, branch_meas.alias()),
                    Some(meas.alias()),
                );
            }
            let context = SqlNodesFactory::new();
            let select = select_builder.build(query_tools.clone(), context);
            let query_plan = QueryPlan::Select(Rc::new(select));
            union_sources.push(query_plan);
        }

        let plan = QueryPlan::Union(Rc::new(Union::new(union_sources)));
        let source = SingleSource::Subquery(Rc::new(plan));
        let alias = PlanSqlTemplates::member_alias_name(
            pre_aggregation.cube_name(),
            pre_aggregation.name(),
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
        let source = pre_aggregation.source();
        let from = match source.as_ref() {
            PreAggregationSource::Single(table) => {
                let table_source = self.make_pre_aggregation_table_source(&table)?;
                From::new(FromSource::Single(table_source))
            }
            PreAggregationSource::Union(union) => {
                let source = self.make_pre_aggregation_union_source(pre_aggregation, &union)?;
                source
            }
            PreAggregationSource::Join(join) => {
                let source = self.make_pre_aggregation_join_source(&join)?;
                source
            }
        };
        Ok(from)
    }
}

impl ProcessableNode for PreAggregation {
    type ProcessorType<'a> = PreAggregationProcessor<'a>;
}
