use super::{CommonUtils, DimensionSubqueryPlanner, JoinPlanner};
use crate::logical_plan::*;
use crate::planner::collectors::{
    collect_cube_names, collect_join_hints, collect_join_hints_for_measures,
    collect_sub_query_dimensions_from_members, collect_sub_query_dimensions_from_symbols,
};
use crate::planner::planners::multi_stage::{EvaluationContext, PlanningScope};
use crate::planner::query_tools::QueryTools;
use crate::planner::JoinTree;
use crate::planner::MemberSymbol;
use crate::planner::{FullKeyAggregateMeasures, QueryProperties};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

/// Plans the per-measure CTEs that feed `FullKeyAggregate` in
/// non-simple queries: regular measures become `MultiStageLeafMeasure`
/// CTEs (one per multi-fact group), and multiplied measures become
/// `AggregateMultipliedSubquery` CTEs (one per owning cube). Both
/// kinds are registered into the shared `PlanningScope`.
pub struct MultipliedMeasuresQueryPlanner {
    query_tools: Rc<QueryTools>,
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
        query_tools: Rc<QueryTools>,
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

    /// Registers per-measure CTEs into `scope`: regular measures
    /// become leaf-measure CTEs grouped by multi-fact join, multiplied
    /// measures become `AggregateMultipliedSubquery` CTEs grouped by
    /// owning cube. Returns the subquery refs the caller's
    /// `FullKeyAggregate` joins over. Errors if called on a simple
    /// query.
    pub fn plan_queries(
        &self,
        scope: &mut PlanningScope,
    ) -> Result<Vec<Rc<MultiStageSubqueryRef>>, CubeError> {
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
                let cte_name = scope.next_cte_name();

                let leaf = Rc::new(MultiStageLeafMeasure {
                    measures: measures.clone(),
                    evaluation_context: EvaluationContext::default(),
                    query: query.clone(),
                });
                let member = Rc::new(LogicalMultiStageMember {
                    name: cte_name.clone(),
                    member_type: MultiStageMemberLogicalType::LeafMeasure(leaf),
                });
                scope.add_member(member);

                let ref_schema = query.schema().clone();
                let subquery_ref = Rc::new(
                    MultiStageSubqueryRef::builder()
                        .name(cte_name)
                        .symbols(measures.clone())
                        .schema(ref_schema)
                        .build(),
                );
                subquery_refs.push(subquery_ref);
            }
        }

        // `into_group_map_by` yields a HashMap; sort the groups so CTE
        // numbering stays deterministic across runs.
        for (cube_name, measures) in full_key_aggregate_measures
            .multiplied_measures
            .clone()
            .into_iter()
            .into_group_map_by(|m| m.cube_name().clone())
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
            let join = join_multi_fact_groups.single_join()?.ok_or_else(|| {
                CubeError::internal("No join groups returned for aggregate measures".to_string())
            })?;
            let aggregate_subquery_logical_plan =
                self.aggregate_subquery_plan(&cube_name, &measures, join, scope)?;

            let cte_name = scope.next_cte_name();
            let member = Rc::new(LogicalMultiStageMember {
                name: cte_name.clone(),
                member_type: MultiStageMemberLogicalType::MultipliedMeasure(
                    aggregate_subquery_logical_plan.clone(),
                ),
            });
            scope.add_member(member);

            let ref_schema = aggregate_subquery_logical_plan.schema.clone();
            let subquery_ref = Rc::new(
                MultiStageSubqueryRef::builder()
                    .name(cte_name.clone())
                    .symbols(measures.clone())
                    .schema(ref_schema)
                    .build(),
            );
            subquery_refs.push(subquery_ref);
        }

        Ok(subquery_refs)
    }

    fn aggregate_subquery_plan(
        &self,
        key_cube_name: &String,
        measures: &Vec<Rc<MemberSymbol>>,
        key_join: Rc<JoinTree>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<AggregateMultipliedSubquery>, CubeError> {
        let pk_cube = self.common_utils.cube_from_path(key_cube_name.clone())?;
        let pk_cube = Cube::new(pk_cube);
        let subquery_dimensions = collect_sub_query_dimensions_from_symbols(&measures, &key_join)?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions, scope)?;

        let primary_keys_dimensions = self.common_utils.primary_keys_dimensions(key_cube_name)?;
        let keys_subquery = self.key_query(
            &primary_keys_dimensions,
            key_join.clone(),
            pk_cube.clone(),
            scope,
        )?;

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .set_measures(measures.clone())
            .into_rc();
        let should_build_join_for_measure_select =
            self.check_should_build_join_for_measure_select(measures, key_cube_name)?;
        let source = if should_build_join_for_measure_select {
            let measure_subquery = self.aggregate_subquery_measure(
                key_join.clone(),
                &measures,
                &primary_keys_dimensions,
                scope,
            )?;
            measure_subquery.into()
        } else {
            pk_cube.into()
        };
        Ok(Rc::new(AggregateMultipliedSubquery {
            schema,
            keys_subquery,
            dimension_subqueries: subquery_dimension_queries,
            source,
            evaluation_context: scope.evaluation_context().clone(),
            pre_aggregation_override: None,
        }))
    }

    fn check_should_build_join_for_measure_select(
        &self,
        measures: &Vec<Rc<MemberSymbol>>,
        key_cube_name: &String,
    ) -> Result<bool, CubeError> {
        for measure in measures.iter() {
            let owned_measure = measure.with_stripped_join_prefix();
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
            if cubes.iter().any(|cube| cube != key_cube_name) {
                let measures_join = self
                    .query_tools
                    .join_graph()
                    .build_join(join_hints.into_items())?;
                if *measures_join
                    .static_data()
                    .multiplication_factor
                    .get(key_cube_name)
                    .unwrap_or(&false)
                {
                    return Err(CubeError::user(format!("{}' references cubes ({}) that lead to row multiplication. Please rewrite it using sub query.", measure.full_name(), cubes.join(", "))));
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn aggregate_subquery_measure(
        &self,
        key_join: Rc<JoinTree>,
        measures: &Vec<Rc<MemberSymbol>>,
        primary_keys_dimensions: &Vec<Rc<MemberSymbol>>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<MeasureSubquery>, CubeError> {
        let subquery_dimensions = collect_sub_query_dimensions_from_members(&measures, &key_join)?;
        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let subquery_dimension_queries =
            dimension_subquery_planner.plan_queries(&subquery_dimensions, scope)?;
        let measure_join_hints = collect_join_hints_for_measures(&measures)?;
        let source = self.join_planner.make_join_logical_plan_with_join_hints(
            measure_join_hints,
            subquery_dimension_queries,
        )?;

        let schema = LogicalSchema::default()
            .set_dimensions(primary_keys_dimensions.clone())
            .set_measures(measures.clone())
            .into_rc();

        let result = MeasureSubquery { schema, source };
        Ok(Rc::new(result))
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
            .make_join_logical_plan(&join, subquery_dimension_queries.clone());

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .set_measures(measures.clone())
            .into_rc();

        let logical_filter = Rc::new(LogicalFilter {
            dimensions_filters: self.query_properties.dimensions_filters().clone(),
            time_dimensions_filters: self.query_properties.time_dimensions_filters().clone(),
            measures_filter: vec![],
            segments: self.query_properties.segments().clone(),
        });

        let query = Query::builder()
            .schema(schema)
            .filter(logical_filter)
            .modifers(Rc::new(LogicalQueryModifiers {
                offset: None,
                limit: None,
                ungrouped: self.query_properties.ungrouped(),
                order_by: vec![],
            }))
            .source(source.into())
            .build();
        Ok(Rc::new(query))
    }

    fn key_query(
        &self,
        dimensions: &Vec<Rc<MemberSymbol>>,
        key_join: Rc<JoinTree>,
        key_cube: Rc<Cube>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<KeysSubQuery>, CubeError> {
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

        let source = self
            .join_planner
            .make_join_logical_plan(&key_join, subquery_dimension_queries);

        let logical_filter = Rc::new(LogicalFilter {
            dimensions_filters: self.query_properties.dimensions_filters().clone(),
            time_dimensions_filters: self.query_properties.time_dimensions_filters().clone(),
            measures_filter: vec![],
            segments: self.query_properties.segments().clone(),
        });

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .into_rc();

        let keys_query = KeysSubQuery::builder()
            .schema(schema)
            .primary_keys_dimensions(dimensions.clone())
            .filter(logical_filter)
            .source(source)
            .pk_cube(key_cube)
            .build();

        Ok(Rc::new(keys_query))
    }
}
