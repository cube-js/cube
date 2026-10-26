use super::{CommonUtils, DimensionSubqueryPlanner, JoinPlanner, JoinTreeBuilder};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::*;
use crate::planner::collectors::{
    collect_calc_group_dims_from_nodes, collect_cube_names, collect_join_hints,
    collect_join_hints_for_measures, collect_sub_query_dimensions_from_members,
    collect_sub_query_dimensions_from_symbols,
};
use crate::planner::planners::multi_stage::{EvaluationContext, PlanningScope};
use crate::planner::state::State;
use crate::planner::symbols::transforms;
use crate::planner::{CubeId, JoinHints, JoinTree};
use crate::planner::{FullKeyAggregateMeasures, QueryProperties};
use crate::planner::{MeasureRenderModifier, MemberSymbol};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

/// Plans the per-measure subqueries that feed `Merge` in
/// non-simple queries: regular measures become leaf-query
/// subqueries (one per multi-fact group), and multiplied measures become
/// `AggregateMultipliedSubquery` subqueries (one per owning cube and
/// multi-fact group). Both kinds are registered into the shared
/// `PlanningScope`.
pub struct MultipliedMeasuresQueryPlanner {
    query_tools: Rc<State>,
    query_properties: Rc<QueryProperties>,
    join_planner: JoinPlanner,
    common_utils: CommonUtils,
    full_key_aggregate_measures: FullKeyAggregateMeasures,
}

impl MultipliedMeasuresQueryPlanner {
    /// Constructs the planner and caches
    /// `full_key_aggregate_measures` for later use in
    /// `plan_queries`.
    pub fn try_new(
        query_tools: Rc<State>,
        query_properties: Rc<QueryProperties>,
    ) -> Result<Self, CubeError> {
        let full_key_aggregate_measures = query_properties.full_key_aggregate_measures()?;
        Ok(Self {
            query_tools: query_tools.clone(),
            join_planner: JoinPlanner::new(query_tools.clone()),
            common_utils: CommonUtils::new(query_tools.clone()),
            query_properties,
            full_key_aggregate_measures,
        })
    }

    /// Registers per-measure subqueries into `scope`: regular measures
    /// become leaf-measure subqueries grouped by multi-fact join, multiplied
    /// measures become `AggregateMultipliedSubquery` subqueries grouped by
    /// owning cube and then by multi-fact join. Returns the subquery
    /// refs the caller's `Merge` joins over. Errors if
    /// called on a simple query.
    pub fn plan_queries(
        &self,
        scope: &mut PlanningScope,
    ) -> Result<Vec<Rc<SubqueryRef>>, CubeError> {
        if self.query_properties.is_simple_query()? {
            return Err(CubeError::internal(format!(
                "MultipliedMeasuresQueryPlanner should not be used for simple query"
            )));
        }

        let full_key_aggregate_measures = &self.full_key_aggregate_measures;
        let mut subquery_refs = Vec::new();

        if !full_key_aggregate_measures.regular_measures.is_empty() {
            let join_multi_fact_groups = self
                .query_properties
                .compute_join_multi_fact_groups_with_measures(
                    &full_key_aggregate_measures.regular_measures,
                )?;
            for (join, measures) in join_multi_fact_groups.groups().iter() {
                let query = self.regular_measures_subquery(measures, join.clone(), scope)?;
                let (subquery_id, subquery_name) = scope.next_subquery();

                let leaf = query.with_evaluation_context(Some(EvaluationContext::default()));
                let member = Rc::new(Subquery::new(
                    subquery_id,
                    subquery_name.clone(),
                    SubqueryBody::LeafMeasure(leaf),
                ));
                scope.add_member(member)?;

                let ref_schema = query.schema().clone();
                let subquery_ref = Rc::new(
                    SubqueryRef::builder()
                        .id(subquery_id)
                        .name(subquery_name)
                        .schema(ref_schema)
                        .build(),
                );
                subquery_refs.push(subquery_ref);
            }
        }

        // `into_group_map_by` yields a HashMap; sort the groups so subquery
        // numbering stays deterministic across runs.
        for (cube_name, measures) in full_key_aggregate_measures
            .multiplied_measures
            .clone()
            .into_iter()
            .into_group_map_by(|m| m.cube_id().clone())
            .into_iter()
            .sorted_by(|(a, _), (b, _)| a.cmp(b))
        {
            let measures = measures
                .into_iter()
                .map(|m| m.measure().clone())
                .collect_vec();
            let join_multi_fact_groups = self
                .query_properties
                .compute_join_multi_fact_groups_with_measures(&measures)?;
            if join_multi_fact_groups.groups().is_empty() {
                return Err(CubeError::internal(format!(
                    "No join groups returned for aggregate measures of cube {cube_name}"
                )));
            }
            // Every measure of the bucket must land in some group, otherwise it
            // would be missing from the subquery refs the caller joins over.
            let grouped_count: usize = join_multi_fact_groups
                .groups()
                .iter()
                .map(|(_, ms)| ms.len())
                .sum();
            if grouped_count != measures.len() {
                return Err(CubeError::internal(format!(
                    "Join grouping dropped measures of cube {cube_name}: {grouped_count} grouped of {}",
                    measures.len()
                )));
            }
            // The key cube fixes the primary keys to deduplicate on, the join
            // tree fixes the joins to build. Measures of one cube can still
            // need different trees, so each tree gets its own subquery.
            for (join, group_measures) in join_multi_fact_groups.groups().iter() {
                let aggregate_subquery_logical_plan =
                    self.aggregate_subquery_plan(&cube_name, group_measures, join.clone(), scope)?;

                let (subquery_id, subquery_name) = scope.next_subquery();
                let member = Rc::new(Subquery::new(
                    subquery_id,
                    subquery_name.clone(),
                    SubqueryBody::MultipliedMeasure(aggregate_subquery_logical_plan.clone()),
                ));
                scope.add_member(member)?;

                let ref_schema = aggregate_subquery_logical_plan.schema.clone();
                let subquery_ref = Rc::new(
                    SubqueryRef::builder()
                        .id(subquery_id)
                        .name(subquery_name.clone())
                        .schema(ref_schema)
                        .build(),
                );
                subquery_refs.push(subquery_ref);
            }
        }

        Ok(subquery_refs)
    }

    fn aggregate_subquery_plan(
        &self,
        key_cube: &CubeId,
        measures: &Vec<Rc<MemberSymbol>>,
        key_join: Rc<JoinTree>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<AggregateMultipliedSubquery>, CubeError> {
        let pk_cube = self.common_utils.cube_from_path(key_cube)?;
        let pk_cube = Cube::new(pk_cube);
        let should_build_join_for_measure_select =
            self.check_should_build_join_for_measure_select(measures, key_cube)?;
        // A measure subquery joins the sub_query dimensions its measures read
        // itself; the aggregate over it reads only the subquery's columns.
        let subquery_dimension_queries = if should_build_join_for_measure_select {
            vec![]
        } else {
            let subquery_dimensions =
                collect_sub_query_dimensions_from_symbols(&measures, &key_join)?;
            let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
                &subquery_dimensions,
                self.query_tools.clone(),
                self.query_properties.clone(),
            )?;
            dimension_subquery_planner.plan_queries(&subquery_dimensions, scope)?
        };

        let primary_keys_dimensions = self.common_utils.primary_keys_dimensions(key_cube)?;
        let keys_subquery = self.key_query(
            &primary_keys_dimensions,
            key_join.clone(),
            pk_cube.clone(),
            scope,
        )?;

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .set_measures(measures.clone());
        // Hoisted out of an ungrouped multi-stage leaf, the aggregate feeds a
        // stage that aggregates its rows, so its measures render row-level.
        let schema = match scope.evaluation_context() {
            Some(context) if context.measure_for_ungrouped => {
                logical_transforms::measures_render_modifier_in_schema(
                    &schema,
                    &MeasureRenderModifier::RawValue,
                )?
            }
            _ => schema.into_rc(),
        };
        let source = if should_build_join_for_measure_select {
            let measure_subquery = self.aggregate_subquery_measure(
                key_join.clone(),
                key_cube,
                &measures,
                &primary_keys_dimensions,
                keys_subquery.filter().clone(),
                scope,
            )?;
            measure_subquery.into()
        } else {
            pk_cube.clone().into()
        };
        Ok(Rc::new(AggregateMultipliedSubquery {
            schema,
            keys_subquery,
            pk_cube,
            primary_keys_dimensions,
            dimension_subqueries: subquery_dimension_queries,
            source,
            evaluation_context: scope.evaluation_context().clone(),
            pre_aggregation_override: None,
        }))
    }

    // The query's WHERE-side filters, as every subquery of this flow sees
    // them. HAVING-style measure filters are applied by the enclosing query.
    fn query_filter(&self) -> Rc<LogicalFilter> {
        Rc::new(LogicalFilter {
            dimensions_filters: self.query_properties.dimensions_filters().clone(),
            time_dimensions_filters: self.query_properties.time_dimensions_filters().clone(),
            measures_filter: vec![],
            segments: self.query_properties.segments().clone(),
        })
    }

    fn check_should_build_join_for_measure_select(
        &self,
        measures: &Vec<Rc<MemberSymbol>>,
        key_cube: &CubeId,
    ) -> Result<bool, CubeError> {
        for measure in measures.iter() {
            let owned_measure = transforms::strip_join_prefix(measure);
            let member_expression_over_dimensions_cubes =
                if let Ok(member_expression) = owned_measure.as_member_expression() {
                    member_expression.cube_names_if_dimension_only_expression()?
                } else {
                    None
                };
            let cubes = if let Some(cubes) = member_expression_over_dimensions_cubes {
                cubes
            } else {
                collect_cube_names(&owned_measure)?
            };
            let join_hints = collect_join_hints(&owned_measure)?;
            if cubes.iter().any(|cube| cube != key_cube) {
                if self.is_multiplied_in_join(&join_hints, key_cube)? {
                    return Err(CubeError::user(format!("{} references cubes ({}) that lead to row multiplication. Please rewrite it using sub query.", measure.full_name(), cubes.iter().join(", "))));
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    // The join graph caches the trees it builds; only a tree with cube
    // instances is built here.
    fn is_multiplied_in_join(
        &self,
        join_hints: &JoinHints,
        key_cube: &CubeId,
    ) -> Result<bool, CubeError> {
        if key_cube.is_joined() {
            let (_, join) = JoinTreeBuilder::new(self.query_tools.clone())
                .build_rooted_at(key_cube, join_hints)?;
            return Ok(join.is_multiplied(key_cube));
        }
        let (graph_hints, joined) = join_hints.split_joined();
        if joined.is_empty() {
            let join = self.query_tools.join_graph().build_join(graph_hints)?;
            return Ok(*join
                .static_data()
                .multiplication_factor
                .get(key_cube.target())
                .unwrap_or(&false));
        }
        let (_, join) =
            JoinTreeBuilder::new(self.query_tools.clone()).build_for_hints(join_hints)?;
        Ok(join.is_multiplied(key_cube))
    }

    fn aggregate_subquery_measure(
        &self,
        key_join: Rc<JoinTree>,
        key_cube: &CubeId,
        measures: &Vec<Rc<MemberSymbol>>,
        primary_keys_dimensions: &Vec<Rc<MemberSymbol>>,
        keys_filter: Rc<LogicalFilter>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<Query>, CubeError> {
        // The subquery emits one row per key for the select above to aggregate;
        // a calculation group the keys' filter leaves more than one value of
        // would emit one row per value.
        let keys_where = keys_filter.all_filters();
        for measure in measures.iter() {
            if let Some(dimension) = collect_calc_group_dims_from_nodes(std::iter::once(measure))?
                .into_iter()
                .find(|dimension| transforms::get_filtered_values(dimension, &keys_where).len() > 1)
            {
                return Err(CubeError::user(format!(
                    "{} cannot be computed under a dimension that multiplies its rows: it \
                     reaches another cube and depends on calculation group {}. Please filter the \
                     calculation group to one value, drop the multiplying dimension or move the \
                     calculation group out of the measure.",
                    measure.full_name(),
                    dimension.full_name()
                )));
            }
        }
        let subquery_dimensions = collect_sub_query_dimensions_from_members(&measures, &key_join)?;
        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions, scope)?;
        let measure_join_hints = collect_join_hints_for_measures(&measures)?;
        let source = if key_cube.is_joined() {
            let (_, join) = JoinTreeBuilder::new(self.query_tools.clone())
                .build_rooted_at(key_cube, &measure_join_hints)?;
            self.join_planner
                .make_query_inputs(join, subquery_dimension_queries)
        } else {
            self.join_planner
                .make_query_inputs_with_join_hints(measure_join_hints, subquery_dimension_queries)?
        };

        // Row-level values per key: the aggregate joining this subquery back
        // to the keys applies the aggregation.
        let measures = measures
            .iter()
            .map(|measure| {
                transforms::measures_render_modifier(measure, &MeasureRenderModifier::RawValue)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let schema = LogicalSchema::default()
            .set_dimensions(primary_keys_dimensions.clone())
            .set_measures(measures)
            .into_rc();

        // No WHERE of its own - the keys subquery already restricts the rows -
        // but the sources' bindings resolve against the keys side's own filter,
        // so both copies of the fact source render one predicate.
        let query = Query::builder()
            .schema(schema)
            .filter(Rc::new(LogicalFilter::default()))
            .modifers(Rc::new(LogicalQueryModifiers {
                offset: None,
                limit: None,
                ungrouped: true,
                distinct: false,
                order_by: vec![],
            }))
            .source(QuerySource::new(source))
            .filter_params_filters(Some(keys_filter))
            .build();
        Ok(Rc::new(query))
    }

    fn regular_measures_subquery(
        &self,
        measures: &Vec<Rc<MemberSymbol>>,
        join: Rc<JoinTree>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<Query>, CubeError> {
        let all_symbols = self
            .query_properties
            .get_member_symbols(true, true, false, true, &measures);

        let subquery_dimensions = collect_sub_query_dimensions_from_symbols(&all_symbols, &join)?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions, scope)?;

        let source = self
            .join_planner
            .make_query_inputs(join, subquery_dimension_queries.clone());

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .set_measures(measures.clone())
            .into_rc();
        let source = QuerySource::new(source);
        let source = match scope.multi_stage_dimension_input(&schema)? {
            Some(input) => source.with_multi_stage_dimension(input),
            None => source,
        };

        let logical_filter = self.query_filter();

        let query = Query::builder()
            .schema(schema)
            .filter(logical_filter)
            .modifers(Rc::new(LogicalQueryModifiers {
                offset: None,
                limit: None,
                ungrouped: self.query_properties.ungrouped(),
                distinct: false,
                order_by: vec![],
            }))
            .source(source)
            .build();
        Ok(Rc::new(query))
    }

    fn key_query(
        &self,
        dimensions: &Vec<Rc<MemberSymbol>>,
        key_join: Rc<JoinTree>,
        key_cube: Rc<Cube>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<Query>, CubeError> {
        let all_symbols =
            self.query_properties
                .get_member_symbols(true, true, false, true, &dimensions);

        let subquery_dimensions =
            collect_sub_query_dimensions_from_symbols(&all_symbols, &key_join)?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions, scope)?;

        // The enclosing select joins the cubes again for the measure values,
        // so the ones the keys are read from take aliases of their own.
        let alias_prefix = format!(
            "{}_key",
            self.query_tools.alias_for_cube(key_cube.cube().cube_id())?
        );
        let source = self
            .join_planner
            .make_query_inputs(key_join, subquery_dimension_queries)
            .with_alias_prefix(alias_prefix);

        // A primary key that is also a query dimension is projected once:
        // a second column under the same alias would make every reference
        // to it from the enclosing re-join ambiguous.
        let query_dimensions = self.query_properties.dimensions();
        let primary_keys = dimensions.iter().filter(|pk| {
            let resolved = (*pk).clone().resolve_reference_chain();
            !query_dimensions
                .iter()
                .chain(self.query_properties.time_dimensions().iter())
                .any(|dim| dim.clone().resolve_reference_chain() == resolved)
        });
        let schema = LogicalSchema::default()
            .set_dimensions(
                query_dimensions
                    .iter()
                    .chain(primary_keys)
                    .cloned()
                    .collect(),
            )
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .into_rc();
        let source = QuerySource::new(source);
        let source = match scope.multi_stage_dimension_input(&schema)? {
            Some(input) => source.with_multi_stage_dimension(input),
            None => source,
        };

        let keys_query = Query::builder()
            .schema(schema)
            .filter(self.query_filter())
            .modifers(Rc::new(LogicalQueryModifiers {
                offset: None,
                limit: None,
                ungrouped: true,
                distinct: true,
                order_by: vec![],
            }))
            .source(source)
            .build();

        Ok(Rc::new(keys_query))
    }
}
