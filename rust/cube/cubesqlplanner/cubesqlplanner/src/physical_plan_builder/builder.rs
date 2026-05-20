use super::context::PushDownBuilderContext;
use super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::*;
use crate::physical_plan::join::JoinType;
use crate::physical_plan::schema::QualifiedColumnName;
use crate::physical_plan::sql_nodes::SqlNodesFactory;
use crate::physical_plan::ReferencesBuilder;
use crate::physical_plan::VisitorContext;
use crate::physical_plan::*;
use crate::physical_plan_builder::context::MultiStageDimensionContext;
use crate::planner::query_properties::OrderByItem;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
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

    pub(super) fn resolve_partition_refs(
        &self,
        partition_by: &[Rc<MemberSymbol>],
        references_builder: &ReferencesBuilder,
    ) -> Result<Vec<String>, CubeError> {
        let templates = &self.plan_sql_templates;
        partition_by
            .iter()
            .map(|dim| -> Result<_, CubeError> {
                let reference = references_builder
                    .find_reference_for_member(dim, &None)
                    .ok_or_else(|| {
                        CubeError::internal(format!(
                            "Alias not found for partition_by dimension {}",
                            dim.full_name()
                        ))
                    })?;
                let table_ref = if let Some(table_name) = reference.source() {
                    format!("{}.", templates.quote_identifier(table_name)?)
                } else {
                    String::new()
                };
                Ok(format!(
                    "{}{}",
                    table_ref,
                    templates.quote_identifier(&reference.name())?
                ))
            })
            .collect()
    }

    pub fn build(
        &self,
        logical_plan: Rc<LogicalPlan>,
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
        logical_plan: Rc<LogicalPlan>,
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

    /// Add a `LEFT JOIN <cte> ON ...` for a multi-stage-dim CTE ref to a
    /// cube-join chain. Used for the `OnPrimaryKeys` flavour: the cube
    /// the ref keys against has already been added to `join_builder`,
    /// and we LEFT-join the CTE on its primary keys.
    pub(super) fn add_multi_stage_dimension_pk_join(
        &self,
        ref_name: &str,
        pk_dimensions: &[Rc<MemberSymbol>],
        join_builder: &mut JoinBuilder,
        context: &PushDownBuilderContext,
    ) -> Result<(), CubeError> {
        // Body is rendered once on the top-level Query as a CTE; here we
        // just LEFT JOIN that CTE by name. Order contract: the top-level
        // `QueryProcessor` MUST publish the CTE (via `add_cte_schema`)
        // before any reference site gets to call this.
        let cte_schema = context.get_cte_schema(ref_name)?;
        let conditions = pk_dimensions
            .iter()
            .map(|dim| -> Result<_, CubeError> {
                let alias_in_sub_query = cte_schema.resolve_member_alias(dim);
                let sub_query_ref = Expr::Reference(QualifiedColumnName::new(
                    Some(ref_name.to_string()),
                    alias_in_sub_query,
                ));
                Ok(vec![(sub_query_ref, Expr::new_member(dim.clone()))])
            })
            .collect::<Result<Vec<_>, _>>()?;

        join_builder.left_join_table_reference(
            ref_name.to_string(),
            cte_schema,
            Some(ref_name.to_string()),
            JoinCondition::new_dimension_join(conditions, false),
        );
        Ok(())
    }

    pub(super) fn add_multistage_dimension_join(
        &self,
        dimension_schema: &Rc<MultiStageDimensionContext>,
        join_builder: &mut JoinBuilder,
        context: &PushDownBuilderContext,
    ) -> Result<(), CubeError> {
        let original_join = join_builder.clone().build();
        let references_builder = ReferencesBuilder::new(From::new_from_join(original_join));
        let conditions = dimension_schema
            .join_dimensions
            .iter()
            .map(|dim| -> Result<_, CubeError> {
                let alias_in_cte = dimension_schema.schema.resolve_member_alias(&dim);
                let sub_query_ref = Expr::Reference(QualifiedColumnName::new(
                    Some(dimension_schema.name.clone()),
                    alias_in_cte,
                ));

                if let Ok(dimension) = dim.as_dimension() {
                    if dimension.is_calc_group() {
                        return Ok(vec![(sub_query_ref, Expr::new_member(dim.clone()))]);
                    }
                }

                let mut context_factory = context.make_sql_nodes_factory()?;
                references_builder.resolve_references_for_member(
                    dim.clone(),
                    &None,
                    context_factory.render_references_mut(),
                )?;

                let visitor_context =
                    VisitorContext::new(self.query_tools.clone(), &context_factory, None);

                Ok(vec![(
                    sub_query_ref,
                    Expr::new_member_with_context(dim.clone(), Rc::new(visitor_context)),
                )])
            })
            .collect::<Result<Vec<_>, _>>()?;

        join_builder.left_join_table_reference(
            dimension_schema.name.clone(),
            dimension_schema.schema.clone(),
            None,
            JoinCondition::new_dimension_join(conditions, false),
        );
        Ok(())
    }

    /// Register outer-scope render references for each multi-stage-dim
    /// CTE this Query consumes — the body's projected column substitutes
    /// for `body_column.full_name()` in the outer scope. The synthetic
    /// body symbol is built so its `full_name` matches the dim symbol
    /// the outer scope references.
    pub(super) fn resolve_multi_stage_dimension_references(
        &self,
        multi_stage_dimensions: &Vec<Rc<MultiStageDimensionRef>>,
        references_builder: &ReferencesBuilder,
        context_factory: &mut SqlNodesFactory,
    ) -> Result<(), CubeError> {
        for ms_dim in multi_stage_dimensions.iter() {
            if let Some(dim_ref) =
                references_builder.find_reference_for_member(&ms_dim.body_column, &None)
            {
                context_factory.add_render_reference(ms_dim.body_column.full_name(), dim_ref);
            } else {
                return Err(CubeError::internal(format!(
                    "Can't find source for multi-stage dimension {}",
                    ms_dim.body_column.full_name()
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
            let positions = logical_schema.find_member_positions(&o.name());

            // TODO: Check for `is_measure` is temporary here until
            // correct processing of order by dimension that is not included in the
            // selection list will be implemented
            if positions.is_empty() && o.member_symbol().is_measure() {
                result.push(OrderBy::new(
                    Expr::Member(MemberExpression::new(o.member_symbol())),
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

    pub(super) fn process_query_dimension(
        &self,
        dimension: &Rc<MemberSymbol>,
        references_builder: &ReferencesBuilder,
        select_builder: &mut SelectBuilder,
        context_factory: &mut SqlNodesFactory,
        context: &PushDownBuilderContext,
    ) -> Result<(), CubeError> {
        if let Some(coalesce_ref) = self.dimension_coalesce_refs(dimension, select_builder.from()) {
            select_builder.add_projection_coalesce_member(dimension, coalesce_ref, None)?;
        } else {
            references_builder.resolve_references_for_member(
                dimension.clone(),
                &None,
                context_factory.render_references_mut(),
            )?;
            if context.measure_subquery {
                select_builder.add_projection_member_without_schema(dimension, None);
            } else {
                select_builder.add_projection_member(dimension, None);
            }
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
