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

    pub(super) fn join_dimension_input(
        &self,
        input: &DimensionInput,
        join_builder: &mut JoinBuilder,
        context: &PushDownBuilderContext,
    ) -> Result<(), CubeError> {
        if context.is_inline_subquery(input.subquery.id()) {
            self.add_subquery_join(input, join_builder, context)
        } else {
            self.add_multistage_dimension_join(input, join_builder, context)
        }
    }

    fn add_subquery_join(
        &self,
        input: &DimensionInput,
        join_builder: &mut JoinBuilder,
        context: &PushDownBuilderContext,
    ) -> Result<(), CubeError> {
        let body = Self::dimension_subquery_body(input, context)?;
        let plan = self.process_node(body.as_ref(), context)?;
        let alias = input.subquery.name().clone();
        let conditions =
            self.key_conditions(input, &alias, &plan.schema(), join_builder, context)?;

        // Readers see the dimensions the input exposes, never the keys.
        let schema = Self::exposed_schema(input);
        if let Some(missing) = input
            .subquery
            .schema()
            .all_members()
            .find(|member| !plan.schema().has_column(&member.alias()))
        {
            return Err(CubeError::internal(format!(
                "Dimension subquery {} has no column for {}",
                alias,
                missing.full_name()
            )));
        }
        let source = SingleSource::Subquery(Rc::new(QueryPlan::Select(plan)), schema);
        // The body groups the owning cube by its primary keys, so none is NULL.
        join_builder.left_join_aliased_source(
            SingleAliasedSource::new_from_source(source, alias),
            JoinCondition::new_dimension_join(conditions, false),
        );
        Ok(())
    }

    fn dimension_subquery_body(
        input: &DimensionInput,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<Query>, CubeError> {
        let subquery = context.get_inline_subquery(input.subquery.id())?;
        let SubqueryBody::Query(body) = &subquery.body else {
            return Err(CubeError::internal(format!(
                "Dimension subquery {} must be a query",
                subquery.name
            )));
        };
        Ok(body.clone())
    }

    // The exposed schema of a dimension input read from a CTE, checked against the
    // columns the CTE was rendered with.
    pub(super) fn cte_schema(
        input: &DimensionInput,
        context: &PushDownBuilderContext,
    ) -> Result<Rc<Schema>, CubeError> {
        let rendered = context.get_multi_stage_schema(input.subquery.name())?;
        let schema = Self::exposed_schema(input);
        if let Some(missing) = input
            .subquery
            .schema()
            .all_members()
            .find(|member| !rendered.has_column(&member.alias()))
        {
            return Err(CubeError::internal(format!(
                "Subquery {} has no column for {}",
                input.subquery.name(),
                missing.full_name()
            )));
        }
        Ok(schema)
    }

    // The columns a dimension input's readers resolve members against.
    pub(super) fn exposed_schema(input: &DimensionInput) -> Rc<Schema> {
        let columns = input
            .subquery
            .schema()
            .all_members()
            .map(|member| SchemaColumn::new(member.alias(), Some(member.clone())))
            .collect();
        Rc::new(Schema::new(columns))
    }

    // Pairs each key's column in the input with the key read from what is joined
    // before the input. The left side is resolved and rendered now: the select's
    // substitutions would read a key the input exposes from the input itself.
    fn key_conditions(
        &self,
        input: &DimensionInput,
        input_alias: &String,
        input_schema: &Schema,
        join_builder: &JoinBuilder,
        context: &PushDownBuilderContext,
    ) -> Result<Vec<Vec<(Expr, Expr)>>, CubeError> {
        let joined_before = From::new_from_join(join_builder.clone().build());
        let cube_references = SelectBuilder::make_cube_references(joined_before.clone());
        let references_builder = ReferencesBuilder::new(joined_before);
        input
            .keys
            .iter()
            .map(|key| -> Result<_, CubeError> {
                let input_column = Expr::Reference(QualifiedColumnName::new(
                    Some(input_alias.clone()),
                    input_schema.resolve_member_alias(key),
                ));

                if let Ok(dimension) = key.as_dimension() {
                    if dimension.is_calc_group() {
                        // Left to the enclosing select's substitutions.
                        return Ok(vec![(input_column, Expr::new_member(key.clone()))]);
                    }
                }

                let mut substitutions = ReferenceSubstitutions::new();
                references_builder.collect_substitutions_for_member(
                    key.clone(),
                    &None,
                    &mut substitutions,
                )?;
                let key = symbol_transforms::substitute_by_name(key, &substitutions)?;

                let mut context_factory = context.make_sql_nodes_factory()?;
                context_factory.set_cube_name_references(cube_references.clone());
                let visitor_context =
                    VisitorContext::new(self.query_tools.clone(), &context_factory, None);

                Ok(vec![(
                    input_column,
                    Expr::new_member_with_context(key, Rc::new(visitor_context)),
                )])
            })
            .collect()
    }

    fn add_multistage_dimension_join(
        &self,
        input: &DimensionInput,
        join_builder: &mut JoinBuilder,
        context: &PushDownBuilderContext,
    ) -> Result<(), CubeError> {
        let cte_name = input.subquery.name();
        let cte_schema = Self::cte_schema(input, context)?;
        let conditions =
            self.key_conditions(input, cte_name, &cte_schema, join_builder, context)?;

        // The CTE groups over the same outer joins: a key may be NULL on both sides.
        join_builder.left_join_table_reference(
            cte_name.clone(),
            cte_schema.clone(),
            None,
            JoinCondition::new_dimension_join(conditions, true),
        );
        Ok(())
    }

    // The dimensions an inline dimension subquery exposes are read from it.
    pub(super) fn collect_dimension_inputs_substitutions(
        &self,
        inputs: &[DimensionInput],
        references_builder: &ReferencesBuilder,
        context: &PushDownBuilderContext,
        substitutions: &mut ReferenceSubstitutions,
    ) -> Result<(), CubeError> {
        for input in inputs
            .iter()
            .filter(|input| context.is_inline_subquery(input.subquery.id()))
        {
            let source = Some(input.subquery.name().clone());
            for dimension in input.subquery.schema().all_dimensions() {
                let Some(column) = references_builder.find_reference_for_member(dimension, &source)
                else {
                    return Err(CubeError::internal(format!(
                        "Can't find source for subquery dimension {}",
                        dimension.full_name()
                    )));
                };
                substitutions.insert(dimension.id().clone(), column_reference(dimension, column));
            }
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
    ) -> Result<(), CubeError> {
        if let Some(coalesce_ref) = self.dimension_coalesce_refs(dimension, select_builder.from()) {
            select_builder.add_projection_coalesce_member(dimension, coalesce_ref, None)?;
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
