use crate::logical_plan::*;
use crate::plan::schema::QualifiedColumnName;
use crate::plan::*;
use crate::planner::query_properties::OrderByItem;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::MeasureTimeShift;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::ReferencesBuilder;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::BaseMemberHelper;
use crate::planner::SqlJoinCondition;
use crate::planner::{BaseMember, MemberSymbolRef};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

#[derive(Clone, Debug)]
struct PhysicalPlanBuilderContext {
    pub alias_prefix: Option<String>,
    pub render_measure_as_state: bool, //Render measure as state, for example hll state for count_approx
    pub render_measure_for_ungrouped: bool,
    pub time_shifts: HashMap<String, MeasureTimeShift>,
    pub original_sql_pre_aggregations: HashMap<String, String>,
}

impl Default for PhysicalPlanBuilderContext {
    fn default() -> Self {
        Self {
            alias_prefix: None,
            render_measure_as_state: false,
            render_measure_for_ungrouped: false,
            time_shifts: HashMap::new(),
            original_sql_pre_aggregations: HashMap::new(),
        }
    }
}

impl PhysicalPlanBuilderContext {
    pub fn make_sql_nodes_factory(&self) -> SqlNodesFactory {
        let mut factory = SqlNodesFactory::new();
        factory.set_time_shifts(self.time_shifts.clone());
        factory.set_count_approx_as_state(self.render_measure_as_state);
        factory.set_ungrouped_measure(self.render_measure_for_ungrouped);
        factory.set_original_sql_pre_aggregations(self.original_sql_pre_aggregations.clone());
        factory
    }
}

pub struct PhysicalPlanBuilder {
    query_tools: Rc<QueryTools>,
    plan_sql_templates: PlanSqlTemplates,
}

impl PhysicalPlanBuilder {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        let plan_sql_templates = PlanSqlTemplates::new(query_tools.templates_render());
        Self {
            query_tools,
            plan_sql_templates,
        }
    }

    pub fn build(
        &self,
        logical_plan: Rc<Query>,
        original_sql_pre_aggregations: HashMap<String, String>,
    ) -> Result<Rc<Select>, CubeError> {
        let mut context = PhysicalPlanBuilderContext::default();
        context.original_sql_pre_aggregations = original_sql_pre_aggregations;
        self.build_impl(logical_plan, &context)
    }

    fn build_impl(
        &self,
        logical_plan: Rc<Query>,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Rc<Select>, CubeError> {
        match logical_plan.as_ref() {
            Query::SimpleQuery(query) => self.build_simple_query(query, context),
            Query::FullKeyAggregateQuery(query) => {
                self.build_full_key_aggregate_query(query, context)
            }
        }
    }

    fn build_simple_query(
        &self,
        logical_plan: &SimpleQuery,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Rc<Select>, CubeError> {
        let mut render_references = HashMap::new();
        let mut measure_references = HashMap::new();
        let mut context_factory = context.make_sql_nodes_factory();
        let from = match &logical_plan.source {
            SimpleQuerySource::LogicalJoin(join) => self.process_logical_join(
                &join,
                context,
                &logical_plan.dimension_subqueries,
                &mut render_references,
            )?,
            SimpleQuerySource::PreAggregation(pre_aggregation) => {
                let res = self.process_pre_aggregation(
                    pre_aggregation,
                    context,
                    &mut render_references,
                    &mut measure_references,
                )?;
                for member in logical_plan.schema.time_dimensions.iter() {
                    context_factory.add_dimensions_with_ignored_timezone(member.full_name());
                }
                context_factory.set_use_local_tz_in_date_range(true);
                res
            }
        };

        let mut select_builder = SelectBuilder::new(from);
        context_factory.set_ungrouped(logical_plan.ungrouped);
        context_factory.set_pre_aggregation_measures_references(measure_references);

        let mut group_by = Vec::new();
        for member in logical_plan.schema.dimensions.iter() {
            let member_ref: Rc<dyn BaseMember> =
                MemberSymbolRef::try_new(member.clone(), self.query_tools.clone())?;
            select_builder.add_projection_member(&member_ref, None);
            if !logical_plan.ungrouped {
                group_by.push(Expr::Member(MemberExpression::new(member_ref.clone())));
            }
        }
        for member in logical_plan.schema.time_dimensions.iter() {
            let member_ref: Rc<dyn BaseMember> =
                MemberSymbolRef::try_new(member.clone(), self.query_tools.clone())?;
            select_builder.add_projection_member(&member_ref, None);
            if !logical_plan.ungrouped {
                group_by.push(Expr::Member(MemberExpression::new(member_ref.clone())));
            }
        }
        for member in logical_plan.schema.measures.iter() {
            select_builder.add_projection_member(
                &MemberSymbolRef::try_new(member.clone(), self.query_tools.clone())?,
                None,
            );
        }

        let filter = logical_plan.filter.all_filters();
        let having = if logical_plan.filter.measures_filter.is_empty() {
            None
        } else {
            Some(Filter {
                items: logical_plan.filter.measures_filter.clone(),
            })
        };

        select_builder.set_filter(filter);
        select_builder.set_group_by(group_by);
        select_builder
            .set_order_by(self.make_order_by(&logical_plan.schema, &logical_plan.order_by)?);
        select_builder.set_having(having);
        select_builder.set_limit(logical_plan.limit);
        select_builder.set_offset(logical_plan.offset);

        context_factory
            .set_rendered_as_multiplied_measures(logical_plan.schema.multiplied_measures.clone());
        context_factory.set_render_references(render_references);
        if logical_plan.ungrouped {
            context_factory.set_ungrouped(true);
        }

        let res = Rc::new(select_builder.build(context_factory));
        Ok(res)
    }

    fn process_pre_aggregation(
        &self,
        pre_aggregation: &Rc<PreAggregation>,
        _context: &PhysicalPlanBuilderContext,
        render_references: &mut HashMap<String, QualifiedColumnName>,
        measure_references: &mut HashMap<String, QualifiedColumnName>,
    ) -> Result<Rc<From>, CubeError> {
        let mut pre_aggregation_schema = Schema::empty();
        let pre_aggregation_alias = PlanSqlTemplates::memeber_alias_name(
            &pre_aggregation.cube_name,
            &pre_aggregation.name,
            &None,
        );
        for dim in pre_aggregation.dimensions.iter() {
            let alias = BaseMemberHelper::default_alias(
                &dim.cube_name(),
                &dim.name(),
                &dim.alias_suffix(),
                self.query_tools.clone(),
            )?;
            render_references.insert(
                dim.full_name(),
                QualifiedColumnName::new(Some(pre_aggregation_alias.clone()), alias.clone()),
            );
            pre_aggregation_schema.add_column(SchemaColumn::new(alias, Some(dim.full_name())));
        }
        for (dim, granularity) in pre_aggregation.time_dimensions.iter() {
            let alias = BaseMemberHelper::default_alias(
                &dim.cube_name(),
                &dim.name(),
                granularity,
                self.query_tools.clone(),
            )?;
            render_references.insert(
                dim.full_name(),
                QualifiedColumnName::new(Some(pre_aggregation_alias.clone()), alias.clone()),
            );
            if let Some(granularity) = &granularity {
                render_references.insert(
                    format!("{}_{}", dim.full_name(), granularity),
                    QualifiedColumnName::new(Some(pre_aggregation_alias.clone()), alias.clone()),
                );
            }
            pre_aggregation_schema.add_column(SchemaColumn::new(alias, Some(dim.full_name())));
        }
        for meas in pre_aggregation.measures.iter() {
            let alias = BaseMemberHelper::default_alias(
                &meas.cube_name(),
                &meas.name(),
                &meas.alias_suffix(),
                self.query_tools.clone(),
            )?;
            measure_references.insert(
                meas.full_name(),
                QualifiedColumnName::new(Some(pre_aggregation_alias.clone()), alias.clone()),
            );
            pre_aggregation_schema.add_column(SchemaColumn::new(alias, Some(meas.full_name())));
        }
        let from = From::new_from_table_reference(
            pre_aggregation.table_name.clone(),
            Rc::new(pre_aggregation_schema),
            Some(pre_aggregation_alias),
        );
        Ok(from)
    }

    fn build_full_key_aggregate_query(
        &self,
        logical_plan: &FullKeyAggregateQuery,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Rc<Select>, CubeError> {
        let mut multi_stage_schemas = HashMap::new();
        let mut ctes = Vec::new();
        for multi_stage_member in logical_plan.multistage_members.iter() {
            ctes.push(self.processs_multi_stage_member(
                multi_stage_member,
                &mut multi_stage_schemas,
                context,
            )?);
        }
        let (from, joins_len) =
            self.process_full_key_aggregate(&logical_plan.source, context, &multi_stage_schemas)?;

        let references_builder = ReferencesBuilder::new(from.clone());
        let mut render_references = HashMap::new();

        let mut select_builder = SelectBuilder::new(from.clone());
        let all_dimensions = logical_plan.schema.all_dimensions().cloned().collect_vec();

        self.process_full_key_aggregate_dimensions(
            &all_dimensions,
            &logical_plan.source,
            &mut select_builder,
            &references_builder,
            &mut render_references,
            joins_len,
            context,
        )?;

        for measure in logical_plan.schema.measures.iter() {
            references_builder.resolve_references_for_member(
                measure.clone(),
                &None,
                &mut render_references,
            )?;
            let alias = references_builder.resolve_alias_for_member(&measure.full_name(), &None);
            select_builder.add_projection_member(
                &measure.clone().as_base_member(self.query_tools.clone())?,
                alias,
            );
        }

        let having = if logical_plan.filter.measures_filter.is_empty() {
            None
        } else {
            let filter = Filter {
                items: logical_plan.filter.measures_filter.clone(),
            };
            references_builder.resolve_references_for_filter(&filter, &mut render_references)?;
            Some(filter)
        };

        select_builder
            .set_order_by(self.make_order_by(&logical_plan.schema, &logical_plan.order_by)?);
        select_builder.set_filter(having);
        select_builder.set_limit(logical_plan.limit);
        select_builder.set_offset(logical_plan.offset);
        select_builder.set_ctes(ctes);

        let mut context_factory = context.make_sql_nodes_factory();
        context_factory.set_render_references(render_references);

        Ok(Rc::new(select_builder.build(context_factory)))
    }

    //FIXME refactor required
    fn process_full_key_aggregate_dimensions(
        &self,
        dimensions: &Vec<Rc<MemberSymbol>>,
        full_key_aggregate: &Rc<FullKeyAggregate>,
        select_builder: &mut SelectBuilder,
        references_builder: &ReferencesBuilder,
        render_references: &mut HashMap<String, QualifiedColumnName>,
        joins_len: usize,
        _context: &PhysicalPlanBuilderContext,
    ) -> Result<(), CubeError> {
        let dimensions_for_join_names = full_key_aggregate
            .join_dimensions
            .iter()
            .map(|dim| dim.full_name())
            .collect::<HashSet<_>>();
        let source_for_join_dimensions = Some(format!("q_0"));
        for dim in dimensions.iter() {
            let dimension_ref = dim.clone().as_base_member(self.query_tools.clone())?;
            if dimensions_for_join_names.contains(&dim.full_name()) {
                references_builder.resolve_references_for_member(
                    dim.clone(),
                    &source_for_join_dimensions,
                    render_references,
                )?;
                let alias = references_builder
                    .resolve_alias_for_member(&dim.full_name(), &source_for_join_dimensions);
                if full_key_aggregate.use_full_join_and_coalesce {
                    let references = (0..joins_len)
                        .map(|i| {
                            let alias = format!("q_{}", i);
                            references_builder
                                .find_reference_for_member(&dim.full_name(), &Some(alias.clone()))
                                .ok_or_else(|| {
                                    CubeError::internal(format!(
                                        "Reference for join not found for {} in {}",
                                        dim.full_name(),
                                        alias
                                    ))
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    select_builder.add_projection_coalesce_member(
                        &dimension_ref,
                        references,
                        alias,
                    )?;
                } else {
                    select_builder.add_projection_member(&dimension_ref, alias);
                }
            } else {
                references_builder.resolve_references_for_member(
                    dim.clone(),
                    &None,
                    render_references,
                )?;
                select_builder.add_projection_member(&dimension_ref, None);
            }
        }
        Ok(())
    }

    fn process_full_key_aggregate(
        &self,
        full_key_aggregate: &Rc<FullKeyAggregate>,
        context: &PhysicalPlanBuilderContext,
        multi_stage_schemas: &HashMap<String, Rc<Schema>>,
    ) -> Result<(Rc<From>, usize), CubeError> {
        let mut joins = Vec::new();
        if let Some(resolver_multiplied_measures) = &full_key_aggregate.multiplied_measures_resolver
        {
            joins.append(
                &mut self
                    .process_resolved_multiplied_measures(resolver_multiplied_measures, context)?,
            );
        }
        for subquery_ref in full_key_aggregate.multi_stage_subquery_refs.iter() {
            if let Some(schema) = multi_stage_schemas.get(&subquery_ref.name) {
                joins.push(SingleSource::TableReference(
                    subquery_ref.name.clone(),
                    schema.clone(),
                ));
            } else {
                return Err(CubeError::internal(format!(
                    "MultiStageSubqueryRef not found: {}",
                    subquery_ref.name
                )));
            }
        }

        if joins.is_empty() {
            return Err(CubeError::internal(format!(
                "FullKeyAggregate should have at least one source: {}",
                pretty_print_rc(full_key_aggregate)
            )));
        }

        let dimensions_for_join = full_key_aggregate
            .join_dimensions
            .iter()
            .map(|dim| -> Result<Rc<dyn BaseMember>, CubeError> {
                dim.clone().as_base_member(self.query_tools.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut join_builder = JoinBuilder::new_from_source(joins[0].clone(), format!("q_0"));

        for (i, join) in joins.iter().enumerate().skip(1) {
            let right_alias = format!("q_{}", i);
            let left_schema = joins[i - 1].schema();
            let right_schema = joins[i].schema();
            // TODO every next join should join to all previous dimensions through OR: q_0.a = q_1.a, q_0.a = q_2.a OR q_1.a = q_2.a, ...
            let conditions = dimensions_for_join
                .iter()
                .map(|dim| {
                    (0..i)
                        .map(|left_i| {
                            let left_alias = format!("q_{}", left_i);
                            let alias_in_left_query = left_schema.resolve_member_alias(dim);
                            let left_ref = Expr::Reference(QualifiedColumnName::new(
                                Some(left_alias.clone()),
                                alias_in_left_query,
                            ));
                            let alias_in_right_query = right_schema.resolve_member_alias(dim);
                            let right_ref = Expr::Reference(QualifiedColumnName::new(
                                Some(right_alias.clone()),
                                alias_in_right_query,
                            ));
                            (left_ref, right_ref)
                        })
                        .collect::<Vec<_>>()
                })
                .collect_vec();
            let on = JoinCondition::new_dimension_join(conditions, true);
            let next_alias = format!("q_{}", i);
            if full_key_aggregate.use_full_join_and_coalesce
                && self.plan_sql_templates.supports_full_join()
            {
                join_builder.full_join_source(join.clone(), next_alias, on);
            } else {
                // TODO in case of full join is not supported there should be correct blending query that keeps NULL values
                join_builder.inner_join_source(join.clone(), next_alias, on);
            }
        }

        let result = From::new_from_join(join_builder.build());
        Ok((result, joins.len()))
    }

    fn process_resolved_multiplied_measures(
        &self,
        resolved_multiplied_measures: &ResolvedMultipliedMeasures,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Vec<SingleSource>, CubeError> {
        match resolved_multiplied_measures {
            ResolvedMultipliedMeasures::ResolveMultipliedMeasures(resolve_multiplied_measures) => {
                self.process_resolve_multiplied_measures(resolve_multiplied_measures, context)
            }
            ResolvedMultipliedMeasures::PreAggregation(pre_aggregation_query) => {
                let pre_aggregation_query =
                    self.build_simple_query(pre_aggregation_query, context)?;
                let source =
                    SingleSource::Subquery(Rc::new(QueryPlan::Select(pre_aggregation_query)));
                Ok(vec![source])
            }
        }
    }

    fn process_resolve_multiplied_measures(
        &self,
        resolve_multiplied_measures: &Rc<ResolveMultipliedMeasures>,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Vec<SingleSource>, CubeError> {
        let mut joins = Vec::new();
        for (i, regular_measure_subquery) in resolve_multiplied_measures
            .regular_measure_subqueries
            .iter()
            .enumerate()
        {
            let mut regular_measure_context = context.clone();
            regular_measure_context.alias_prefix = if i == 0 {
                Some(format!("main"))
            } else {
                Some(format!("main_{}", i))
            };
            let select =
                self.build_simple_query(regular_measure_subquery, &regular_measure_context)?;
            let source = SingleSource::Subquery(Rc::new(QueryPlan::Select(select)));
            joins.push(source);
        }
        for multiplied_measure_subquery in resolve_multiplied_measures
            .aggregate_multiplied_subqueries
            .iter()
        {
            let select =
                self.process_aggregate_multiplied_subquery(multiplied_measure_subquery, context)?;
            let source = SingleSource::Subquery(Rc::new(QueryPlan::Select(select)));
            joins.push(source);
        }
        Ok(joins)
    }

    fn process_logical_join(
        &self,
        logical_join: &LogicalJoin,
        context: &PhysicalPlanBuilderContext,
        dimension_subqueries: &Vec<Rc<DimensionSubQuery>>,
        render_references: &mut HashMap<String, QualifiedColumnName>,
    ) -> Result<Rc<From>, CubeError> {
        let root = logical_join.root.cube.clone();
        if logical_join.joins.is_empty() && dimension_subqueries.is_empty() {
            Ok(From::new_from_cube(
                root.clone(),
                Some(root.default_alias_with_prefix(&context.alias_prefix)),
            ))
        } else {
            let mut join_builder = JoinBuilder::new_from_cube(
                root.clone(),
                Some(root.default_alias_with_prefix(&context.alias_prefix)),
            );
            for dimension_subquery in dimension_subqueries
                .iter()
                .filter(|d| &d.subquery_dimension.cube_name() == root.name())
            {
                self.add_subquery_join(
                    dimension_subquery.clone(),
                    &mut join_builder,
                    render_references,
                    context,
                )?;
            }
            for join in logical_join.joins.iter() {
                match join {
                    LogicalJoinItem::CubeJoinItem(CubeJoinItem { cube, on_sql }) => {
                        join_builder.left_join_cube(
                            cube.cube.clone(),
                            Some(cube.cube.default_alias_with_prefix(&context.alias_prefix)),
                            JoinCondition::new_base_join(SqlJoinCondition::try_new(
                                self.query_tools.clone(),
                                on_sql.clone(),
                            )?),
                        );
                        for dimension_subquery in dimension_subqueries
                            .iter()
                            .filter(|d| &d.subquery_dimension.cube_name() == cube.cube.name())
                        {
                            self.add_subquery_join(
                                dimension_subquery.clone(),
                                &mut join_builder,
                                render_references,
                                context,
                            )?;
                        }
                    }
                }
            }
            Ok(From::new_from_join(join_builder.build()))
        }
    }

    fn add_subquery_join(
        &self,
        dimension_subquery: Rc<DimensionSubQuery>,
        join_builder: &mut JoinBuilder,
        render_references: &mut HashMap<String, QualifiedColumnName>,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<(), CubeError> {
        let sub_query = self.build_impl(dimension_subquery.query.clone(), context)?;
        let dim_name = dimension_subquery.subquery_dimension.name();
        let cube_name = dimension_subquery.subquery_dimension.cube_name();
        let primary_keys_dimensions = &dimension_subquery.primary_keys_dimensions;
        let sub_query_alias = format!("{cube_name}_{dim_name}_subquery");
        let conditions = primary_keys_dimensions
            .iter()
            .map(|dim| -> Result<_, CubeError> {
                let dim = dim.clone().as_base_member(self.query_tools.clone())?;
                let alias_in_sub_query = sub_query.schema().resolve_member_alias(&dim);
                let sub_query_ref = Expr::Reference(QualifiedColumnName::new(
                    Some(sub_query_alias.clone()),
                    alias_in_sub_query.clone(),
                ));

                Ok(vec![(sub_query_ref, Expr::new_member(dim))])
            })
            .collect::<Result<Vec<_>, _>>()?;

        if let Some(dim_ref) = sub_query.schema().resolve_member_reference(
            &dimension_subquery
                .measure_for_subquery_dimension
                .full_name(),
        ) {
            let qualified_column_name =
                QualifiedColumnName::new(Some(sub_query_alias.clone()), dim_ref);
            render_references.insert(
                dimension_subquery.subquery_dimension.full_name(),
                qualified_column_name,
            );
        } else {
            return Err(CubeError::internal(format!(
                "Can't find source for subquery dimension {}",
                dim_name
            )));
        }
        join_builder.left_join_subselect(
            sub_query,
            sub_query_alias,
            JoinCondition::new_dimension_join(conditions, false),
        );
        Ok(())
    }

    fn process_aggregate_multiplied_subquery(
        &self,
        aggregate_multiplied_subquery: &Rc<AggregateMultipliedSubquery>,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Rc<Select>, CubeError> {
        let mut render_references = HashMap::new();
        let keys_query =
            self.process_keys_sub_query(&aggregate_multiplied_subquery.keys_subquery, context)?;

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
                        let member_ref = dim.clone().as_base_member(self.query_tools.clone())?;
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
                for dimension_subquery in aggregate_multiplied_subquery.dimension_subqueries.iter()
                {
                    self.add_subquery_join(
                        dimension_subquery.clone(),
                        &mut join_builder,
                        &mut render_references,
                        context,
                    )?;
                }
            }
            AggregateMultipliedSubquerySouce::MeasureSubquery(measure_subquery) => {
                let subquery = self.process_measure_subquery(&measure_subquery, context)?;
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
                );
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
            let member_ref = member.clone().as_base_member(self.query_tools.clone())?;
            group_by.push(Expr::Member(MemberExpression::new(member_ref.clone())));
            select_builder.add_projection_member(&member_ref, alias);
        }
        for member in aggregate_multiplied_subquery.schema.measures.iter() {
            if matches!(
                aggregate_multiplied_subquery.source.as_ref(),
                AggregateMultipliedSubquerySouce::Cube
            ) {
                references_builder.resolve_references_for_member(
                    member.clone(),
                    &None,
                    &mut render_references,
                )?;
            }
            select_builder.add_projection_member(
                &member.clone().as_base_member(self.query_tools.clone())?,
                None,
            );
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

    fn process_measure_subquery(
        &self,
        measure_subquery: &Rc<MeasureSubquery>,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Rc<Select>, CubeError> {
        let mut render_references = HashMap::new();
        let from = self.process_logical_join(
            &measure_subquery.source,
            context,
            &measure_subquery.dimension_subqueries,
            &mut render_references,
        )?;
        let mut context_factory = context.make_sql_nodes_factory();
        let mut select_builder = SelectBuilder::new(from);

        context_factory.set_ungrouped_measure(true);
        context_factory.set_render_references(render_references);
        context_factory.set_rendered_as_multiplied_measures(
            measure_subquery
                .measures
                .iter()
                .map(|m| m.full_name())
                .collect(),
        );
        for dim in measure_subquery.primary_keys_dimensions.iter() {
            select_builder.add_projection_member(
                &dim.clone().as_base_member(self.query_tools.clone())?,
                None,
            );
        }
        for meas in measure_subquery.measures.iter() {
            select_builder.add_projection_member(
                &meas.clone().as_base_member(self.query_tools.clone())?,
                None,
            );
        }
        let select = Rc::new(select_builder.build(context_factory));
        Ok(select)
    }

    fn process_keys_sub_query(
        &self,
        keys_subquery: &Rc<KeysSubQuery>,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Rc<Select>, CubeError> {
        let mut render_references = HashMap::new();
        let alias_prefix = Some(format!(
            "{}_key",
            self.query_tools
                .alias_for_cube(&keys_subquery.key_cube_name)?
        ));

        let mut context = context.clone();
        context.alias_prefix = alias_prefix;
        let source = self.process_logical_join(
            &keys_subquery.source,
            &context,
            &keys_subquery.dimension_subqueries,
            &mut render_references,
        )?;
        let mut select_builder = SelectBuilder::new(source);
        for member in keys_subquery
            .dimensions
            .iter()
            .chain(keys_subquery.time_dimensions.iter())
            .chain(keys_subquery.primary_keys_dimensions.iter())
        {
            let member_ref: Rc<dyn BaseMember> =
                MemberSymbolRef::try_new(member.clone(), self.query_tools.clone())?;
            let alias = member_ref.alias_name();
            select_builder.add_projection_member(&member_ref, Some(alias.clone()));
        }

        select_builder.set_distinct();
        select_builder.set_filter(keys_subquery.filter.all_filters());
        let mut context_factory = context.make_sql_nodes_factory();
        context_factory.set_render_references(render_references);
        let res = Rc::new(select_builder.build(context_factory));
        Ok(res)
    }

    fn make_order_by(
        &self,
        logical_schema: &LogicalSchema,
        order_by: &Vec<OrderByItem>,
    ) -> Result<Vec<OrderBy>, CubeError> {
        let mut result = Vec::new();
        for o in order_by.iter() {
            for position in logical_schema.find_member_positions(&o.name()) {
                let member_ref: Rc<dyn BaseMember> =
                    MemberSymbolRef::try_new(o.member_symbol(), self.query_tools.clone())?;
                result.push(OrderBy::new(
                    Expr::Member(MemberExpression::new(member_ref)),
                    position + 1,
                    o.desc(),
                ));
            }
        }
        Ok(result)
    }

    fn processs_multi_stage_member(
        &self,
        logical_plan: &Rc<LogicalMultiStageMember>,
        multi_stage_schemas: &mut HashMap<String, Rc<Schema>>,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Rc<Cte>, CubeError> {
        let query = match &logical_plan.member_type {
            MultiStageMemberLogicalType::LeafMeasure(measure) => {
                self.process_multi_stage_leaf_measure(&measure, context)?
            }
            MultiStageMemberLogicalType::MeasureCalculation(calculation) => self
                .process_multi_stage_measure_calculation(
                    &calculation,
                    multi_stage_schemas,
                    context,
                )?,
            MultiStageMemberLogicalType::GetDateRange(get_date_range) => {
                self.process_multi_stage_get_date_range(&get_date_range, context)?
            }
            MultiStageMemberLogicalType::TimeSeries(time_series) => {
                self.process_multi_stage_time_series(&time_series, context)?
            }
            MultiStageMemberLogicalType::RollingWindow(rolling_window) => self
                .process_multi_stage_rolling_window(
                    &rolling_window,
                    multi_stage_schemas,
                    context,
                )?,
        };
        let alias = logical_plan.name.clone();
        multi_stage_schemas.insert(alias.clone(), query.schema().clone());
        Ok(Rc::new(Cte::new(query, alias)))
    }

    fn process_multi_stage_leaf_measure(
        &self,
        leaf_measure: &MultiStageLeafMeasure,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Rc<QueryPlan>, CubeError> {
        let mut context = context.clone();
        context.render_measure_as_state = leaf_measure.render_measure_as_state;
        context.render_measure_for_ungrouped = leaf_measure.render_measure_for_ungrouped;
        context.time_shifts = leaf_measure.time_shifts.clone();
        let select = self.build_impl(leaf_measure.query.clone(), &context)?;
        Ok(Rc::new(QueryPlan::Select(select)))
    }

    fn process_multi_stage_get_date_range(
        &self,
        get_date_range: &MultiStageGetDateRange,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Rc<QueryPlan>, CubeError> {
        let mut render_references = HashMap::new();
        let from = self.process_logical_join(
            &get_date_range.source,
            context,
            &get_date_range.dimension_subqueries,
            &mut render_references,
        )?;
        let mut select_builder = SelectBuilder::new(from);
        let mut context_factory = context.make_sql_nodes_factory();
        let args = vec![get_date_range
            .time_dimension
            .clone()
            .as_base_member(self.query_tools.clone())?];
        select_builder.add_projection_function_expression(
            "MAX",
            args.clone(),
            "date_to".to_string(),
        );

        select_builder.add_projection_function_expression(
            "MIN",
            args.clone(),
            "date_from".to_string(),
        );
        context_factory.set_render_references(render_references);
        let select = Rc::new(select_builder.build(context_factory));
        Ok(Rc::new(QueryPlan::Select(select)))
    }

    fn process_multi_stage_time_series(
        &self,
        time_series: &MultiStageTimeSeries,
        _context: &PhysicalPlanBuilderContext,
    ) -> Result<Rc<QueryPlan>, CubeError> {
        let time_dimension = time_series.time_dimension.clone();
        let time_dimension_symbol = time_dimension.as_time_dimension()?;
        let date_range = time_series.date_range.clone();
        let granularity_obj = if let Some(granularity_obj) = time_dimension_symbol.granularity_obj()
        {
            granularity_obj.clone()
        } else {
            return Err(CubeError::user(
                "Time dimension granularity is required for rolling window".to_string(),
            ));
        };

        let templates = PlanSqlTemplates::new(self.query_tools.templates_render());

        let ts_date_range = if templates.supports_generated_time_series() {
            if let Some(date_range) = time_dimension_symbol
                .get_range_for_time_series(date_range, self.query_tools.timezone())?
            {
                TimeSeriesDateRange::Filter(date_range.0.clone(), date_range.1.clone())
            } else {
                if let Some(date_range_cte) = &time_series.get_date_range_multistage_ref {
                    TimeSeriesDateRange::Generated(date_range_cte.clone())
                } else {
                    return Err(CubeError::internal(
                        "Date range cte is required for time series without date range".to_string(),
                    ));
                }
            }
        } else {
            if let Some(date_range) = &time_series.date_range {
                TimeSeriesDateRange::Filter(date_range[0].clone(), date_range[1].clone())
            } else {
                return Err(CubeError::internal(
                    "Date range is required for time series without date range".to_string(),
                ));
            }
        };

        let time_series = TimeSeries::new(
            self.query_tools.clone(),
            time_dimension.full_name(),
            ts_date_range,
            granularity_obj,
        );
        let query_plan = Rc::new(QueryPlan::TimeSeries(Rc::new(time_series)));
        Ok(query_plan)
    }

    fn process_multi_stage_rolling_window(
        &self,
        rolling_window: &MultiStageRollingWindow,
        multi_stage_schemas: &HashMap<String, Rc<Schema>>,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Rc<QueryPlan>, CubeError> {
        let time_dimension = rolling_window.rolling_time_dimension.clone();
        let time_series_ref = rolling_window.time_series_input.name.clone();
        let measure_input_ref = rolling_window.measure_input.name.clone();

        let time_series_schema = if let Some(schema) = multi_stage_schemas.get(&time_series_ref) {
            schema.clone()
        } else {
            return Err(CubeError::internal(format!(
                "Schema not found for cte {}",
                time_series_ref
            )));
        };
        let measure_input_schema = if let Some(schema) = multi_stage_schemas.get(&measure_input_ref)
        {
            schema.clone()
        } else {
            return Err(CubeError::internal(format!(
                "Schema not found for cte {}",
                measure_input_ref
            )));
        };

        let base_time_dimension_alias = measure_input_schema.resolve_member_alias(
            &rolling_window
                .time_dimension_in_measure_input
                .clone()
                .as_base_member(self.query_tools.clone())?,
        );

        let root_alias = format!("time_series");
        let measure_input_alias = format!("rolling_source");

        let mut join_builder = JoinBuilder::new_from_table_reference(
            time_series_ref.clone(),
            time_series_schema,
            Some(root_alias.clone()),
        );

        let on = match &rolling_window.rolling_window {
            MultiStageRollingWindowType::Regular(regular_rolling_window) => {
                JoinCondition::new_regular_rolling_join(
                    root_alias.clone(),
                    regular_rolling_window.trailing.clone(),
                    regular_rolling_window.leading.clone(),
                    regular_rolling_window.offset.clone(),
                    Expr::Reference(QualifiedColumnName::new(
                        Some(measure_input_alias.clone()),
                        base_time_dimension_alias,
                    )),
                )
            }
            MultiStageRollingWindowType::ToDate(to_date_rolling_window) => {
                JoinCondition::new_to_date_rolling_join(
                    root_alias.clone(),
                    to_date_rolling_window.granularity.clone(),
                    Expr::Reference(QualifiedColumnName::new(
                        Some(measure_input_alias.clone()),
                        base_time_dimension_alias,
                    )),
                    self.query_tools.clone(),
                )
            }
            MultiStageRollingWindowType::RunningTotal => JoinCondition::new_rolling_total_join(
                root_alias.clone(),
                Expr::Reference(QualifiedColumnName::new(
                    Some(measure_input_alias.clone()),
                    base_time_dimension_alias,
                )),
            ),
        };

        join_builder.left_join_table_reference(
            measure_input_ref.clone(),
            measure_input_schema.clone(),
            Some(measure_input_alias.clone()),
            on,
        );

        let mut context_factory = context.make_sql_nodes_factory();
        context_factory.set_rolling_window(true);
        let from = From::new_from_join(join_builder.build());
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut render_references = HashMap::new();
        let mut select_builder = SelectBuilder::new(from.clone());

        //We insert render reference for main time dimension (with the some granularity as in time series to avoid unnecessary date_tranc)
        render_references.insert(
            time_dimension.full_name(),
            QualifiedColumnName::new(Some(root_alias.clone()), format!("date_from")),
        );

        //We also insert render reference for the base dimension of the time dimension (i.e. without `_granularity` prefix to let other time dimensions make date_tranc)
        render_references.insert(
            time_dimension
                .as_time_dimension()?
                .base_symbol()
                .full_name(),
            QualifiedColumnName::new(Some(root_alias.clone()), format!("date_from")),
        );

        for dim in rolling_window.schema.time_dimensions.iter() {
            context_factory.add_dimensions_with_ignored_timezone(dim.full_name());
            let alias = references_builder
                .resolve_alias_for_member(&dim.full_name(), &Some(measure_input_alias.clone()));
            select_builder.add_projection_member(
                &dim.clone().as_base_member(self.query_tools.clone())?,
                alias,
            );
        }

        for dim in rolling_window.schema.dimensions.iter() {
            references_builder.resolve_references_for_member(
                dim.clone(),
                &Some(measure_input_alias.clone()),
                &mut render_references,
            )?;
            let alias = references_builder
                .resolve_alias_for_member(&dim.full_name(), &Some(measure_input_alias.clone()));
            select_builder.add_projection_member(
                &dim.clone().as_base_member(self.query_tools.clone())?,
                alias,
            );
        }

        for measure in rolling_window.schema.measures.iter() {
            let measure_ref = measure.clone().as_base_member(self.query_tools.clone())?;
            let name_in_base_query = measure_input_schema.resolve_member_alias(&measure_ref);
            context_factory.add_ungrouped_measure_reference(
                measure.full_name(),
                QualifiedColumnName::new(Some(measure_input_alias.clone()), name_in_base_query),
            );

            select_builder.add_projection_member(&measure_ref, None);
        }

        if !rolling_window.is_ungrouped {
            let group_by = rolling_window
                .schema
                .all_dimensions()
                .map(|dim| -> Result<_, CubeError> {
                    let member_ref: Rc<dyn BaseMember> =
                        MemberSymbolRef::try_new(dim.clone(), self.query_tools.clone())?;
                    Ok(Expr::Member(MemberExpression::new(member_ref.clone())))
                })
                .collect::<Result<Vec<_>, _>>()?;
            select_builder.set_group_by(group_by);
            select_builder.set_order_by(
                self.make_order_by(&rolling_window.schema, &rolling_window.order_by)?,
            );
        } else {
            context_factory.set_ungrouped(true);
        }

        context_factory.set_render_references(render_references);
        let select = Rc::new(select_builder.build(context_factory));
        Ok(Rc::new(QueryPlan::Select(select)))
    }

    fn process_multi_stage_measure_calculation(
        &self,
        measure_calculation: &MultiStageMeasureCalculation,
        multi_stage_schemas: &HashMap<String, Rc<Schema>>,
        context: &PhysicalPlanBuilderContext,
    ) -> Result<Rc<QueryPlan>, CubeError> {
        let (from, joins_len) = self.process_full_key_aggregate(
            &measure_calculation.source,
            context,
            multi_stage_schemas,
        )?;
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut render_references = HashMap::new();

        let mut select_builder = SelectBuilder::new(from.clone());
        let all_dimensions = measure_calculation
            .schema
            .all_dimensions()
            .cloned()
            .collect_vec();

        self.process_full_key_aggregate_dimensions(
            &all_dimensions,
            &measure_calculation.source,
            &mut select_builder,
            &references_builder,
            &mut render_references,
            joins_len,
            context,
        )?;

        for measure in measure_calculation.schema.measures.iter() {
            references_builder.resolve_references_for_member(
                measure.clone(),
                &None,
                &mut render_references,
            )?;
            let alias = references_builder.resolve_alias_for_member(&measure.full_name(), &None);
            select_builder.add_projection_member(
                &measure.clone().as_base_member(self.query_tools.clone())?,
                alias,
            );
        }

        if !measure_calculation.is_ungrouped {
            let group_by = all_dimensions
                .iter()
                .map(|dim| -> Result<_, CubeError> {
                    let member_ref: Rc<dyn BaseMember> =
                        MemberSymbolRef::try_new(dim.clone(), self.query_tools.clone())?;
                    Ok(Expr::Member(MemberExpression::new(member_ref.clone())))
                })
                .collect::<Result<Vec<_>, _>>()?;
            select_builder.set_group_by(group_by);
            select_builder.set_order_by(
                self.make_order_by(&measure_calculation.schema, &measure_calculation.order_by)?,
            );
        }

        let mut context_factory = context.make_sql_nodes_factory();
        let partition_by = measure_calculation
            .partition_by
            .iter()
            .map(|dim| -> Result<_, CubeError> {
                if let Some(reference) =
                    references_builder.find_reference_for_member(&dim.full_name(), &None)
                {
                    Ok(format!("{}", reference))
                } else {
                    Err(CubeError::internal(format!(
                        "Alias not found for partition_by dimension {}",
                        dim.full_name()
                    )))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        match &measure_calculation.window_function_to_use {
            MultiStageCalculationWindowFunction::Rank => {
                context_factory.set_multi_stage_rank(partition_by)
            }
            MultiStageCalculationWindowFunction::Window => {
                context_factory.set_multi_stage_window(partition_by)
            }
            MultiStageCalculationWindowFunction::None => {}
        }
        context_factory.set_render_references(render_references);
        let select = Rc::new(select_builder.build(context_factory));
        Ok(Rc::new(QueryPlan::Select(select)))
    }
}
