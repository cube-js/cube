use super::context::PushDownBuilderContext;
use super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::*;
use crate::physical_plan::join::JoinType;
use crate::physical_plan::schema::QualifiedColumnName;
use crate::physical_plan::ReferencesBuilder;
use crate::physical_plan::VisitorContext;
use crate::physical_plan::*;
use crate::planner::query_properties::OrderByItem;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::symbols::transforms as symbol_transforms;
use crate::planner::{CubeId, MemberSymbol};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

const TOTAL_COUNT: &str = "total_count";
const ORIGINAL_QUERY: &str = "original_query";

pub struct PhysicalPlanBuilder {
    query_tools: Rc<QueryTools>,
    plan_sql_templates: PlanSqlTemplates,
    union_full_key_aggregate: bool,
}

impl PhysicalPlanBuilder {
    pub fn new(
        query_tools: Rc<QueryTools>,
        plan_sql_templates: PlanSqlTemplates,
        union_full_key_aggregate: bool,
    ) -> Self {
        Self {
            query_tools,
            plan_sql_templates,
            union_full_key_aggregate,
        }
    }

    pub(super) fn supports_union_full_key_aggregate(&self) -> bool {
        self.union_full_key_aggregate && self.plan_sql_templates.supports_union_full_key_aggregate()
    }

    pub(super) fn query_tools(&self) -> &Rc<QueryTools> {
        &self.query_tools
    }

    pub(super) fn qtools_and_templates(&self) -> (&Rc<QueryTools>, &PlanSqlTemplates) {
        (&self.query_tools, &self.plan_sql_templates)
    }

    pub(super) fn templates(&self) -> &PlanSqlTemplates {
        &self.plan_sql_templates
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
        logical_plan: Rc<RootQuery>,
        original_sql_pre_aggregations: HashMap<CubeId, String>,
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
        let query = collapse_trivial_subqueries(&query)?;
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
        logical_plan: Rc<RootQuery>,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<Select>, CubeError> {
        self.process_node(logical_plan.as_ref(), context)
    }

    pub(super) fn add_subquery_join(
        &self,
        dimension_subquery: Rc<DimensionSubQuery>,
        join_builder: &mut JoinBuilder,
        context: &PushDownBuilderContext,
    ) -> Result<(), CubeError> {
        let mut context = context.clone();
        context.measure_subquery = true;
        let sub_query = self.process_node(dimension_subquery.query.as_ref(), &context)?;
        let dim_name = dimension_subquery.subquery_dimension.name();
        let cube_id = dimension_subquery.subquery_dimension.cube_id();
        let cube_name = if cube_id.is_joined() {
            self.query_tools.alias_for_cube(&cube_id)?
        } else {
            cube_id.to_string()
        };
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

    pub(super) fn add_multistage_dimension_join(
        &self,
        input: &MultiStageDimensionInput,
        join_builder: &mut JoinBuilder,
        context: &PushDownBuilderContext,
    ) -> Result<(), CubeError> {
        let cte_name = input.subquery.name();
        let cte_schema = context.get_multi_stage_schema(cte_name)?;
        let original_join = From::new_from_join(join_builder.clone().build());
        let cube_references = SelectBuilder::make_cube_references(original_join.clone());
        let references_builder = ReferencesBuilder::new(original_join);
        let conditions = input
            .keys
            .iter()
            .map(|dim| -> Result<_, CubeError> {
                let alias_in_cte = cte_schema.resolve_member_alias(&dim);
                let sub_query_ref = Expr::Reference(QualifiedColumnName::new(
                    Some(cte_name.clone()),
                    alias_in_cte,
                ));

                if let Ok(dimension) = dim.as_dimension() {
                    if dimension.is_calc_group() {
                        // Left to the enclosing select's substitutions.
                        return Ok(vec![(sub_query_ref, Expr::new_member(dim.clone()))]);
                    }
                }

                let mut substitutions = ReferenceSubstitutions::new();
                references_builder.collect_substitutions_for_member(
                    dim.clone(),
                    &None,
                    &mut substitutions,
                )?;
                let dim = symbol_transforms::substitute_by_name(dim, &substitutions)?;

                // Rendered apart from the select, so it needs the aliases of
                // the cubes joined so far.
                let mut context_factory = context.make_sql_nodes_factory()?;
                context_factory.set_cube_name_references(cube_references.clone());
                let visitor_context =
                    VisitorContext::new(self.query_tools.clone(), &context_factory, None);

                Ok(vec![(
                    sub_query_ref,
                    Expr::new_member_with_context(dim, Rc::new(visitor_context)),
                )])
            })
            .collect::<Result<Vec<_>, _>>()?;

        join_builder.left_join_table_reference(
            cte_name.clone(),
            cte_schema.clone(),
            None,
            JoinCondition::new_dimension_join(conditions, true),
        );
        Ok(())
    }

    /// A subquery dimension is read under the alias of the measure that
    /// computes it: one member's id maps to another member's column.
    pub(super) fn collect_subquery_dimensions_substitutions(
        &self,
        dimension_subqueries: &Vec<Rc<DimensionSubQuery>>,
        references_builder: &ReferencesBuilder,
        substitutions: &mut ReferenceSubstitutions,
    ) -> Result<(), CubeError> {
        for dimension_subquery in dimension_subqueries.iter() {
            let Some(dim_ref) = references_builder.find_reference_for_member(
                &dimension_subquery.measure_for_subquery_dimension,
                &None,
            ) else {
                return Err(CubeError::internal(format!(
                    "Can't find source for subquery dimension {}",
                    dimension_subquery.subquery_dimension.full_name()
                )));
            };
            substitutions.insert(
                dimension_subquery.subquery_dimension.id().clone(),
                column_reference(&dimension_subquery.subquery_dimension, dim_ref),
            );
        }
        Ok(())
    }

    /// Builds the ORDER BY of a select. A measure absent from the schema
    /// carries its own symbol, so it gets the select's substitutions here.
    pub(crate) fn make_order_by(
        &self,
        logical_schema: &LogicalSchema,
        order_by: &Vec<OrderByItem>,
        substitutions: &ReferenceSubstitutions,
    ) -> Result<Vec<OrderBy>, CubeError> {
        let mut result = Vec::new();
        for o in order_by.iter() {
            let positions = logical_schema.find_member_positions(o.member_symbol().id());

            // TODO: Check for `is_measure` is temporary here until
            // correct processing of order by dimension that is not included in the
            // selection list will be implemented
            if positions.is_empty() && o.member_symbol().is_measure() {
                let symbol =
                    symbol_transforms::substitute_by_name(&o.member_symbol(), substitutions)?;
                result.push(OrderBy::new(
                    Expr::Member(MemberExpression::new(symbol)),
                    0,
                    o.desc(),
                ));
            } else {
                for position in positions {
                    // Use the symbol from schema at the found position instead of
                    // o.member_symbol() which may lack granularity context for time dimensions.
                    // This ensures ORDER BY uses the same symbol as GROUP BY.
                    let symbol = logical_schema
                        .get_member_at_position(position)
                        .unwrap_or_else(|| o.member_symbol());
                    result.push(OrderBy::new(
                        Expr::Member(MemberExpression::new(symbol)),
                        position + 1,
                        o.desc(),
                    ));
                }
            }
        }
        Ok(result)
    }

    /// A dimension a full join projects as `COALESCE` is read from no single
    /// source, so it stays computed.
    pub(super) fn collect_query_dimension_substitution(
        &self,
        dimension: &Rc<MemberSymbol>,
        references_builder: &ReferencesBuilder,
        from: &Rc<From>,
        substitutions: &mut ReferenceSubstitutions,
    ) -> Result<(), CubeError> {
        if self.dimension_coalesce_refs(dimension, from).is_some() {
            return Ok(());
        }
        references_builder.collect_substitutions_for_member(dimension.clone(), &None, substitutions)
    }

    pub(super) fn project_query_dimension(
        &self,
        dimension: &Rc<MemberSymbol>,
        select_builder: &mut SelectBuilder,
        context: &PushDownBuilderContext,
    ) -> Result<(), CubeError> {
        if let Some(coalesce_ref) = self.dimension_coalesce_refs(dimension, select_builder.from()) {
            select_builder.add_projection_coalesce_member(dimension, coalesce_ref, None)?;
        } else if context.measure_subquery {
            select_builder.add_projection_member_without_schema(dimension, None);
        } else {
            select_builder.add_projection_member(dimension, None);
        }
        Ok(())
    }

    fn dimension_coalesce_refs(
        &self,
        dimension: &Rc<MemberSymbol>,
        from: &Rc<From>,
    ) -> Option<Vec<QualifiedColumnName>> {
        match &from.source {
            FromSource::Join(join) => {
                if join.joins.iter().any(|i| i.join_type == JoinType::Full) {
                    let mut result = vec![];
                    let dim_alias = join.root.source.schema().resolve_member_alias(dimension);
                    result.push(QualifiedColumnName::new(
                        Some(join.root.alias.clone()),
                        dim_alias,
                    ));
                    for item in join.joins.iter() {
                        let dim_alias = item.from.source.schema().resolve_member_alias(dimension);
                        result.push(QualifiedColumnName::new(
                            Some(item.from.alias.clone()),
                            dim_alias,
                        ));
                    }
                    Some(result)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
