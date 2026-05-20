use super::{CommonUtils, DimensionSubqueryPlanner, JoinPlanner};
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::logical_plan::*;
use crate::planner::collectors::{
    collect_cube_names, collect_join_hints, collect_join_hints_for_measures,
    collect_sub_query_dimensions_from_members, collect_sub_query_dimensions_from_symbols,
};
use crate::planner::planners::multi_stage::{CteRole, CteState};
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
                let cte_name = cte_state.next_cte_name(CteRole::FactMeasure);

                let member = Rc::new(LogicalMultiStageMember {
                    name: cte_name.clone(),
                    body: MultiStageMemberBody::Query(query.clone()),
                });
                let registered_name = cte_state.add_member(
                    CteRole::FactMeasure,
                    measures.clone(),
                    self.query_properties.clone(),
                    member,
                );

                let ref_schema = query.schema().clone();
                let subquery_ref = Rc::new(
                    MultiStageSubqueryRef::builder()
                        .name(registered_name)
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

            let cte_name = cte_state.next_cte_name(CteRole::MultipliedMeasureSubquery);
            let ref_schema = aggregate_subquery_logical_plan.schema().clone();
            let member = Rc::new(LogicalMultiStageMember {
                name: cte_name.clone(),
                body: MultiStageMemberBody::Query(aggregate_subquery_logical_plan.clone()),
            });
            let registered_name = cte_state.add_member(
                CteRole::MultipliedMeasureSubquery,
                measures.clone(),
                self.query_properties.clone(),
                member,
            );

            let subquery_ref = Rc::new(
                MultiStageSubqueryRef::builder()
                    .name(registered_name)
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
        let primary_keys_dimensions = self.common_utils.primary_keys_dimensions(key_cube_name)?;
        self.assert_measures_not_multiplied(measures, key_cube_name)?;

        // KeysSubQuery body.
        let keys_query = self.key_query(&primary_keys_dimensions, key_join.clone(), cte_state)?;
        let keys_cte_name = cte_state.next_cte_name(CteRole::Keys);
        let registered_keys_name = cte_state.add_member(
            CteRole::Keys,
            primary_keys_dimensions.clone(),
            self.query_properties.clone(),
            Rc::new(LogicalMultiStageMember {
                name: keys_cte_name,
                body: MultiStageMemberBody::Query(keys_query.clone()),
            }),
        );
        let keys_ref = Rc::new(
            MultiStageSubqueryRef::builder()
                .name(registered_keys_name)
                .symbols(primary_keys_dimensions.clone())
                .schema(keys_query.schema().clone())
                .build(),
        );

        // MeasureSubQuery body — projects raw ungrouped columns; the outer
        // aggregate-multiplied SELECT wraps them in the right aggregate via
        // `ungrouped_measure_reference`.
        let measure_query = self.aggregate_subquery_measure(
            &measures,
            &primary_keys_dimensions,
            key_join.clone(),
            cte_state,
        )?;
        let measure_cte_name = cte_state.next_cte_name(CteRole::MultipliedMeasureSubquery);
        // `symbols` drive `ungrouped_measure_reference` setup on the outer
        // Query: we only want the wrap-in-aggregate behaviour for what the
        // subquery actually projects as a measure column — native measures
        // and the measure-deps surfaced from member-expressions. The
        // member-expressions themselves stay on the outer schema and
        // resolve their inner deps via the standard render-reference
        // mechanism (which finds them by origin_member in the CTE schema).
        let measure_symbols = measure_query.schema().measures.clone();
        let registered_measure_name = cte_state.add_member(
            CteRole::MultipliedMeasureSubquery,
            measure_symbols.clone(),
            self.query_properties.clone(),
            Rc::new(LogicalMultiStageMember {
                name: measure_cte_name,
                body: MultiStageMemberBody::Query(measure_query.clone()),
            }),
        );
        let measure_ref = Rc::new(
            MultiStageSubqueryRef::builder()
                .name(registered_measure_name)
                .symbols(measure_symbols)
                .schema(measure_query.schema().clone())
                .is_ungrouped(true)
                .build(),
        );

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

    /// Splits multiplied measures into the parts the MeasureSubquery
    /// actually needs to project:
    /// - `native_measures`: declared cube measures, projected raw and
    ///   re-aggregated by the outer FullKeyAggregate.
    /// - `extra_dim_deps`: dim symbols referenced by member-expressions;
    ///   projected as dimensions on the subquery so the outer rendering
    ///   of the member-expression resolves its dim refs against the CTE
    ///   instead of the multiplied join chain.
    /// - `extra_measure_deps`: native measure symbols referenced by
    ///   member-expressions; projected raw alongside `native_measures`.
    ///
    /// The member-expression itself stays on the outer
    /// `Query{AggregateMultiplied}` schema — its inner aggregate is
    /// computed against the dedup-by-pk subquery output.
    fn split_member_expression_deps(
        measures: &Vec<Rc<MemberSymbol>>,
    ) -> (
        Vec<Rc<MemberSymbol>>, // native_measures
        Vec<Rc<MemberSymbol>>, // extra_dim_deps
        Vec<Rc<MemberSymbol>>, // extra_measure_deps
    ) {
        let mut native_measures = Vec::new();
        let mut extra_dim_deps: Vec<Rc<MemberSymbol>> = Vec::new();
        let mut extra_measure_deps: Vec<Rc<MemberSymbol>> = Vec::new();
        for measure in measures.iter() {
            if let Ok(me) = measure.as_member_expression() {
                for dep in me.get_dependencies() {
                    let resolved = dep.resolve_reference_chain();
                    match resolved.as_ref() {
                        MemberSymbol::Dimension(_) | MemberSymbol::TimeDimension(_) => {
                            if !extra_dim_deps
                                .iter()
                                .any(|d| d.full_name() == resolved.full_name())
                            {
                                extra_dim_deps.push(resolved);
                            }
                        }
                        MemberSymbol::Measure(_) => {
                            if !extra_measure_deps
                                .iter()
                                .any(|d| d.full_name() == resolved.full_name())
                            {
                                extra_measure_deps.push(resolved);
                            }
                        }
                        _ => {}
                    }
                }
                let _ = me;
            } else {
                native_measures.push(measure.clone());
            }
        }
        (native_measures, extra_dim_deps, extra_measure_deps)
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

        let (native_measures, extra_dim_deps, extra_measure_deps) =
            Self::split_member_expression_deps(measures);
        let projected_measures = native_measures
            .iter()
            .cloned()
            .chain(extra_measure_deps.iter().cloned())
            .collect_vec();
        let projected_dimensions = primary_keys_dimensions
            .iter()
            .cloned()
            .chain(extra_dim_deps.iter().cloned())
            .collect_vec();

        // Strip the join-chain prefix from each projected measure before
        // collecting join hints: the MeasureSubquery is the dedup-by-pk
        // subselect of the owning cube alone — including the outer cubes
        // the measure was reached through (e.g. a view's `join_path`)
        // would multiply rows back, defeating the dedup. Member-
        // expressions are decomposed into their deps (the member-
        // expression itself emits no hints — see `JoinHintsCollector`);
        // those deps drive the join from their own owning cubes.
        let hint_sources = projected_measures
            .iter()
            .map(|m| m.with_stripped_join_prefix())
            .collect_vec();
        let measure_join_hints = collect_join_hints_for_measures(&hint_sources)?;
        let source = self
            .join_planner
            .make_join_logical_plan_with_join_hints(measure_join_hints)?;

        let schema = LogicalSchema::default()
            .set_dimensions(projected_dimensions)
            .set_measures(projected_measures)
            .into_rc();

        // MeasureSubquery shape: raw column projection of pk + dim-deps +
        // native measures + measure-deps over the (stripped) source join.
        // The outer `Query{FullKeyAggregate}` re-aggregates measure
        // columns and renders member-expressions against this CTE.
        let query = Query::builder()
            .schema(schema)
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

        let schema_dimensions = self
            .query_properties
            .dimensions()
            .iter()
            .chain(dimensions.iter())
            .cloned()
            .unique_by(|d| d.full_name())
            .collect_vec();
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
