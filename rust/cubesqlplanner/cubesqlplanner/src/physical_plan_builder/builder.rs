use super::context::PushDownBuilderContext;
use super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::*;
use crate::plan::schema::QualifiedColumnName;
use crate::plan::*;
use crate::planner::query_properties::OrderByItem;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::ReferencesBuilder;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

const TOTAL_COUNT: &str = "total_count";
const ORIGINAL_QUERY: &str = "original_query";

pub struct PhysicalPlanBuilder {
    query_tools: Rc<QueryTools>,
    plan_sql_templates: PlanSqlTemplates,
}

impl PhysicalPlanBuilder {
    pub fn new(query_tools: Rc<QueryTools>, plan_sql_templates: PlanSqlTemplates) -> Self {
        Self {
            query_tools,
            plan_sql_templates,
        }
    }

    pub(super) fn query_tools(&self) -> &Rc<QueryTools> {
        &self.query_tools
    }

    pub(super) fn qtools_and_templates(&self) -> (&Rc<QueryTools>, &PlanSqlTemplates) {
        (&self.query_tools, &self.plan_sql_templates)
    }

    pub(super) fn process_node<T: ProcessableNode>(
        &self,
        logical_node: &T,
        context: &PushDownBuilderContext,
    ) -> Result<<T::ProcessorType<'_> as LogicalNodeProcessor<'_, T>>::PhysycalNode, CubeError>
    {
        let processor = T::ProcessorType::new(self);
        processor.process(logical_node, context)
    }

    pub fn build(
        &self,
        logical_plan: Rc<Query>,
        original_sql_pre_aggregations: HashMap<String, String>,
        total_query: bool,
    ) -> Result<Rc<Select>, CubeError> {
        let mut context = PushDownBuilderContext::default();
        context.original_sql_pre_aggregations = original_sql_pre_aggregations;
        let query = self.build_impl(logical_plan, &context)?;
        let query = if total_query {
            self.build_total_count(query, &context)?
        } else {
            query
        };
        Ok(query)
    }

    fn build_total_count(
        &self,
        source: Rc<Select>,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<Select>, CubeError> {
        let from = From::new_from_subselect(source.clone(), ORIGINAL_QUERY.to_string());
        let mut select_builder = SelectBuilder::new(from);
        select_builder.add_count_all(TOTAL_COUNT.to_string());
        let context_factory = context.make_sql_nodes_factory()?;
        Ok(Rc::new(
            select_builder.build(self.query_tools.clone(), context_factory),
        ))
    }

    fn build_impl(
        &self,
        logical_plan: Rc<Query>,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<Select>, CubeError> {
        self.process_node(logical_plan.as_ref(), context)
    }

    pub(super) fn measures_for_query(
        &self,
        node_measures: &Vec<Rc<MemberSymbol>>,
        context: &PushDownBuilderContext,
    ) -> Vec<(Rc<MemberSymbol>, bool)> {
        if context.dimensions_query {
            return vec![];
        }
        if let Some(required_measures) = &context.required_measures {
            required_measures
                .iter()
                .map(|member| (member.clone(), node_measures.iter().any(|m| m == member)))
                .collect_vec()
        } else {
            node_measures
                .iter()
                .map(|member| (member.clone(), true))
                .collect_vec()
        }
    }

    pub(super) fn add_subquery_join(
        &self,
        dimension_subquery: Rc<DimensionSubQuery>,
        join_builder: &mut JoinBuilder,
        context: &PushDownBuilderContext,
    ) -> Result<(), CubeError> {
        let mut context = context.clone();
        context.dimensions_query = false;
        context.measure_subquery = true;
        let sub_query = self.process_node(dimension_subquery.query.as_ref(), &context)?;
        let dim_name = dimension_subquery.subquery_dimension.name();
        let cube_name = dimension_subquery.subquery_dimension.cube_name();
        let primary_keys_dimensions = &dimension_subquery.primary_keys_dimensions;
        let sub_query_alias = format!("{cube_name}_{dim_name}_subquery");
        let conditions = primary_keys_dimensions
            .iter()
            .map(|dim| -> Result<_, CubeError> {
                let alias_in_sub_query = sub_query.schema().resolve_member_alias(&dim);
                let sub_query_ref = Expr::Reference(QualifiedColumnName::new(
                    Some(sub_query_alias.clone()),
                    alias_in_sub_query.clone(),
                ));

                Ok(vec![(sub_query_ref, Expr::new_member(dim.clone()))])
            })
            .collect::<Result<Vec<_>, _>>()?;

        join_builder.left_join_subselect(
            sub_query,
            sub_query_alias,
            JoinCondition::new_dimension_join(conditions, false),
        );
        Ok(())
    }

    pub(super) fn resolve_subquery_dimensions_references(
        &self,
        dimension_subqueries: &Vec<Rc<DimensionSubQuery>>,
        references_builder: &ReferencesBuilder,
        render_references: &mut HashMap<String, QualifiedColumnName>,
    ) -> Result<(), CubeError> {
        for dimension_subquery in dimension_subqueries.iter() {
            if let Some(dim_ref) = references_builder.find_reference_for_member(
                &dimension_subquery
                    .measure_for_subquery_dimension
                    .full_name(),
                &None,
            ) {
                render_references
                    .insert(dimension_subquery.subquery_dimension.full_name(), dim_ref);
            } else {
                return Err(CubeError::internal(format!(
                    "Can't find source for subquery dimension {}",
                    dimension_subquery.subquery_dimension.full_name()
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn make_order_by(
        &self,
        logical_schema: &LogicalSchema,
        order_by: &Vec<OrderByItem>,
    ) -> Result<Vec<OrderBy>, CubeError> {
        let mut result = Vec::new();
        for o in order_by.iter() {
            for position in logical_schema.find_member_positions(&o.name()) {
                result.push(OrderBy::new(
                    Expr::Member(MemberExpression::new(o.member_symbol())),
                    position + 1,
                    o.desc(),
                ));
            }
        }
        Ok(result)
    }
}
