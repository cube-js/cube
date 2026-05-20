use super::{CommonUtils, DimensionSubqueryPlanner, JoinPlanner};
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::logical_plan::*;
use crate::planner::collectors::{
    collect_cube_names, collect_join_hints, collect_join_hints_for_measures,
    collect_sub_query_dimensions_from_members, collect_sub_query_dimensions_from_symbols,
};
use crate::planner::planners::multi_stage::CteState;
use crate::planner::query_tools::QueryTools;
use crate::planner::MemberSymbol;
use crate::planner::{FullKeyAggregateMeasures, QueryProperties};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

/// Plans the per-measure CTEs that feed `FullKeyAggregate` in
/// non-simple queries: regular measures become `MultiStageLeafMeasure`
/// CTEs (one per multi-fact group), and multiplied measures become
/// `AggregateMultipliedSubquery` CTEs (one per owning cube). Both
/// kinds are registered into the shared `CteState`.
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

    /// Registers per-measure CTEs into `cte_state`: regular measures
    /// become leaf-measure CTEs grouped by multi-fact join, multiplied
    /// measures become `AggregateMultipliedSubquery` CTEs grouped by
    /// owning cube. Errors if called on a simple query.
    pub fn plan_queries(&self, cte_state: &mut CteState) -> Result<(), CubeError> {
        if self.query_properties.is_simple_query()? {
            return Err(CubeError::internal(format!(
                "MultipliedMeasuresQueryPlanner should not be used for simple query"
            )));
        }

        let full_key_aggregate_measures = &self.full_key_aggregate_measures;

        if !full_key_aggregate_measures.regular_measures.is_empty() {
            let join_multi_fact_groups = self
                .query_properties
                .compute_join_multi_fact_groups_with_measures(
                    &full_key_aggregate_measures.regular_measures,
                )?;
            for (join, measures) in join_multi_fact_groups.groups().iter() {
                let query = self.regular_measures_subquery(measures, join.clone(), cte_state)?;
                let cte_name = cte_state.next_cte_name();

                let member = Rc::new(LogicalMultiStageMember {
                    name: cte_name.clone(),
                    body: MultiStageMemberBody::Query(query.clone()),
                });
                cte_state.add_member(member);

                let ref_schema = query.schema().clone();
                let subquery_ref = Rc::new(
                    MultiStageSubqueryRef::builder()
                        .name(cte_name)
                        .symbols(measures.clone())
                        .schema(ref_schema)
                        .build(),
                );
                cte_state.add_subquery_ref(subquery_ref);
            }
        }

        for (cube_name, measures) in full_key_aggregate_measures
            .multiplied_measures
            .clone()
            .into_iter()
            .into_group_map_by(|m| m.cube_name().clone())
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
                self.aggregate_subquery_plan(&cube_name, &measures, join, cte_state)?;

            let cte_name = cte_state.next_cte_name();
            let ref_schema = aggregate_subquery_logical_plan.schema().clone();
            let member = Rc::new(LogicalMultiStageMember {
                name: cte_name.clone(),
                body: MultiStageMemberBody::Query(aggregate_subquery_logical_plan.clone()),
            });
            cte_state.add_member(member);

            let subquery_ref = Rc::new(
                MultiStageSubqueryRef::builder()
                    .name(cte_name.clone())
                    .symbols(measures.clone())
                    .schema(ref_schema)
                    .build(),
            );
            cte_state.add_subquery_ref(subquery_ref);
        }

        Ok(())
    }

    fn aggregate_subquery_plan(
        &self,
        key_cube_name: &String,
        measures: &Vec<Rc<MemberSymbol>>,
        key_join: Rc<dyn JoinDefinition>,
        cte_state: &mut CteState,
    ) -> Result<Rc<Query>, CubeError> {
        // FIXME: subquery dimensions for the outer aggregate SELECT are
        // currently flowed through `MeasureSubquery` (its inner LogicalJoin
        // owns the DSQ joins). Revisit if outer dimensions ever need DSQ
        // refs at this level.
        //
        // let subquery_dimensions =
        //     collect_sub_query_dimensions_from_symbols(&measures, &self.join_planner, &key_join)?;
        // let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
        //     &subquery_dimensions,
        //     self.query_tools.clone(),
        //     self.query_properties.clone(),
        // )?;
        // let subquery_dimension_queries =
        //     dimension_subquery_planner.plan_queries(&subquery_dimensions)?;

        let primary_keys_dimensions = self.common_utils.primary_keys_dimensions(key_cube_name)?;
        self.assert_measures_not_multiplied(measures, key_cube_name)?;

        // Build the KeysSubQuery-shaped body and publish it as a top-level
        // CTE. The same pk cube may need distinct bodies (e.g. shifted vs
        // unshifted leaf in time-shifted multiplied measures produces
        // different filter sets), so the CTE name carries a global sequence
        // id from `QueryTools` to disambiguate.
        let keys_query = self.key_query(&primary_keys_dimensions, key_join.clone(), cte_state)?;
        let keys_cte_name = format!(
            "{}_keys_subquery_{}",
            key_cube_name,
            self.query_tools.next_cte_seq_id()
        );
        let keys_ref = Rc::new(
            MultiStageSubqueryRef::builder()
                .name(keys_cte_name.clone())
                .symbols(primary_keys_dimensions.clone())
                .schema(keys_query.schema().clone())
                .build(),
        );
        cte_state.add_member(Rc::new(LogicalMultiStageMember {
            name: keys_cte_name,
            body: MultiStageMemberBody::Query(keys_query),
        }));

        // Build the MeasureSubquery-shaped body and publish it as a
        // top-level CTE. The same pk cube may need distinct bodies (e.g.
        // shifted vs unshifted leaf in time-shifted multiplied measures
        // produces different filter sets), so the CTE name carries a
        // global sequence id from `QueryTools` to disambiguate.
        let measure_query = self.aggregate_subquery_measure(
            &measures,
            &primary_keys_dimensions,
            key_join.clone(),
            cte_state,
        )?;
        let measure_cte_name = format!(
            "{}_measure_subquery_{}",
            key_cube_name,
            self.query_tools.next_cte_seq_id()
        );
        // The CTE body projects measures as raw ungrouped columns; the outer
        // aggregate-multiplied SELECT wraps them in the right aggregate via
        // `ungrouped_measure_reference`.
        let measure_ref = Rc::new(
            MultiStageSubqueryRef::builder()
                .name(measure_cte_name.clone())
                .symbols(measures.clone())
                .schema(measure_query.schema().clone())
                .is_ungrouped(true)
                .build(),
        );
        cte_state.add_member(Rc::new(LogicalMultiStageMember {
            name: measure_cte_name,
            body: MultiStageMemberBody::Query(measure_query),
        }));

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .set_measures(measures.clone())
            .set_multiplied_measures(
                self.full_key_aggregate_measures
                    .rendered_as_multiplied_measures
                    .clone(),
            )
            .into_rc();

        // Aggregate-multiplied subquery shape: FullKeyAggregate joins the
        // MeasureSubquery CTE to the KeysSubQuery CTE on the pk cube's
        // primary-key dimensions. Outer `Query` re-aggregates measures over
        // the outer dimensions.
        let full_key_aggregate = Rc::new(
            FullKeyAggregate::builder()
                .schema(schema.clone())
                .data_inputs(vec![measure_ref])
                .keys_subquery_ref(Some(keys_ref))
                .join_keys(primary_keys_dimensions.clone())
                .build(),
        );

        let query = Query::builder()
            .schema(schema)
            .filter(Rc::new(LogicalFilter::default()))
            .modifers(Rc::new(LogicalQueryModifiers::default()))
            .source(full_key_aggregate.into())
            .kind(QueryKind::AggregateMultiplied)
            .build();
        Ok(Rc::new(query))
    }

    fn assert_measures_not_multiplied(
        &self,
        measures: &Vec<Rc<MemberSymbol>>,
        key_cube_name: &String,
    ) -> Result<(), CubeError> {
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
            if !cubes.iter().any(|cube| cube != key_cube_name) {
                continue;
            }
            let join_hints = collect_join_hints(&owned_measure)?;
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
        }
        Ok(())
    }

    fn aggregate_subquery_measure(
        &self,
        measures: &Vec<Rc<MemberSymbol>>,
        primary_keys_dimensions: &Vec<Rc<MemberSymbol>>,
        key_join: Rc<dyn JoinDefinition>,
        cte_state: &mut CteState,
    ) -> Result<Rc<Query>, CubeError> {
        let subquery_dimensions =
            collect_sub_query_dimensions_from_members(&measures, &self.join_planner, &key_join)?;
        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let multi_stage_dimensions =
            dimension_subquery_planner.plan_queries(&subquery_dimensions, cte_state)?;
        let measure_join_hints = collect_join_hints_for_measures(&measures)?;
        let source = self
            .join_planner
            .make_join_logical_plan_with_join_hints(measure_join_hints)?;

        let schema = LogicalSchema::default()
            .set_dimensions(primary_keys_dimensions.clone())
            .set_measures(measures.clone())
            .into_rc();

        // MeasureSubquery shape: raw column projection of pk + measures over
        // the source join; the upstream `Query{FullKeyAggregate}` re-aggregates.
        let query = Query::builder()
            .schema(schema)
            .filter(Rc::new(LogicalFilter::default()))
            .modifers(Rc::new(LogicalQueryModifiers::default()))
            .source(source.into())
            .multi_stage_dimensions(multi_stage_dimensions)
            .kind(QueryKind::InternalFact(FactKind::Measures))
            .build();
        Ok(Rc::new(query))
    }

    fn regular_measures_subquery(
        &self,
        measures: &Vec<Rc<MemberSymbol>>,
        join: Rc<dyn JoinDefinition>,
        cte_state: &mut CteState,
    ) -> Result<Rc<Query>, CubeError> {
        let all_symbols = self
            .query_properties
            .get_member_symbols(true, true, false, true, &measures);

        let subquery_dimensions =
            collect_sub_query_dimensions_from_symbols(&all_symbols, &self.join_planner, &join)?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let multi_stage_dimensions =
            dimension_subquery_planner.plan_queries(&subquery_dimensions, cte_state)?;

        let source = self.join_planner.make_join_logical_plan(join)?;

        let schema = LogicalSchema::default()
            .set_dimensions(self.query_properties.dimensions().clone())
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .set_measures(measures.clone())
            .set_multiplied_measures(
                self.full_key_aggregate_measures
                    .rendered_as_multiplied_measures
                    .clone(),
            )
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
                ungrouped: self.query_properties.ungrouped(),
                ..Default::default()
            }))
            .source(source.into())
            .multi_stage_dimensions(multi_stage_dimensions)
            .build();
        Ok(Rc::new(query))
    }

    fn key_query(
        &self,
        dimensions: &Vec<Rc<MemberSymbol>>,
        key_join: Rc<dyn JoinDefinition>,
        cte_state: &mut CteState,
    ) -> Result<Rc<Query>, CubeError> {
        let all_symbols =
            self.query_properties
                .get_member_symbols(true, true, false, true, &dimensions);

        let subquery_dimensions =
            collect_sub_query_dimensions_from_symbols(&all_symbols, &self.join_planner, &key_join)?;

        let dimension_subquery_planner = DimensionSubqueryPlanner::try_new(
            &subquery_dimensions,
            self.query_tools.clone(),
            self.query_properties.clone(),
        )?;
        let multi_stage_dimensions =
            dimension_subquery_planner.plan_queries(&subquery_dimensions, cte_state)?;

        let source = self.join_planner.make_join_logical_plan(key_join.clone())?;

        let logical_filter = Rc::new(LogicalFilter {
            dimensions_filters: self.query_properties.dimensions_filters().clone(),
            time_dimensions_filters: self.query_properties.time_dimensions_filters().clone(),
            measures_filter: vec![],
            segments: self.query_properties.segments().clone(),
        });

        // pk dimensions are projected as ordinary schema dimensions: the
        // CTE consumer reads them off `schema` to build join keys.
        let mut schema_dimensions = self.query_properties.dimensions().clone();
        for pk in dimensions.iter() {
            if !schema_dimensions
                .iter()
                .any(|d| d.full_name() == pk.full_name())
            {
                schema_dimensions.push(pk.clone());
            }
        }
        let schema = LogicalSchema::default()
            .set_dimensions(schema_dimensions)
            .set_time_dimensions(self.query_properties.time_dimensions().clone())
            .into_rc();

        // KeysSubQuery shape: a `SELECT DISTINCT` projection of outer +
        // pk-cube dimensions over the keys join, with the leaf time-shift
        // snapshot pinned on the modifiers. The upstream
        // `Query{FullKeyAggregate}` joins to its CTE by name.
        let query = Query::builder()
            .schema(schema)
            .filter(logical_filter)
            .modifers(Rc::new(LogicalQueryModifiers {
                time_shifts: self.query_properties.time_shifts().clone(),
                ..Default::default()
            }))
            .source(source.into())
            .multi_stage_dimensions(multi_stage_dimensions)
            .kind(QueryKind::InternalFact(FactKind::Keys))
            .build();
        Ok(Rc::new(query))
    }
}
