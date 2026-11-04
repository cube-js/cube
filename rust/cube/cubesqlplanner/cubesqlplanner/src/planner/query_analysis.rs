use super::query_properties::QueryProperties;
use super::MemberSymbol;
use crate::planner::collectors::{collect_multiplied_measures, has_multi_stage_members};
use crate::planner::multi_fact_join_groups::{MeasuresJoinHints, MultiFactJoinGroups};
use crate::planner::symbols::transforms;
use crate::planner::{CubeId, JoinTree, MemberId};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::cell::OnceCell;
use std::collections::HashMap;
use std::rc::Rc;

/// A measure paired with the cube it should be aggregated under. The bound
/// cube can differ from the measure's own cube for member expressions
/// referencing a dimension.
#[derive(Debug, Clone)]
pub struct MultipliedMeasure {
    measure: Rc<MemberSymbol>,
    cube_id: CubeId,
}

impl MultipliedMeasure {
    pub fn new(measure: Rc<MemberSymbol>, cube_id: CubeId) -> Rc<Self> {
        Rc::new(Self { measure, cube_id })
    }

    pub fn measure(&self) -> &Rc<MemberSymbol> {
        &self.measure
    }

    pub fn cube_id(&self) -> &CubeId {
        &self.cube_id
    }
}

/// Measures classified by how the planner must compute them: directly
/// aggregated alongside the rest of the query, wrapped in a multiplied
/// subquery, or planned as a multi-stage CTE.
#[derive(Default, Clone, Debug)]
pub struct FullKeyAggregateMeasures {
    pub multiplied_measures: Vec<Rc<MultipliedMeasure>>,
    pub regular_measures: Vec<Rc<MemberSymbol>>,
    pub multi_stage_measures: Vec<Rc<MemberSymbol>>,
    // A leaf measure's distinct render form (a `count` rewritten to `MultipliedCount`).
    render_forms: HashMap<MemberId, Rc<MemberSymbol>>,
}

impl FullKeyAggregateMeasures {
    /// Rewrite a selected measure into its render form: every multiplied
    /// `count` in its dependency tree is substituted with its distinct form.
    pub fn render(&self, measure: &Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> {
        transforms::substitute_by_name(measure, &self.render_forms)
    }
}

/// What planning needs to know about one built query: its join groups, how
/// its measures are computed, and whether it plans as a single simple query.
pub struct QueryAnalysis {
    query: Rc<QueryProperties>,
    // Lazy: a query whose measures are all multi-stage may have no valid join
    // of its own, and its planning never asks for one.
    join_groups: OnceCell<MultiFactJoinGroups>,
    measures: FullKeyAggregateMeasures,
    select_measures: Vec<Rc<MemberSymbol>>,
    is_simple: bool,
}

impl QueryAnalysis {
    pub fn try_new(query: Rc<QueryProperties>) -> Result<Rc<Self>, CubeError> {
        let join_groups = OnceCell::new();
        let measures = Self::classify_measures(&query, &join_groups)?;
        let select_measures = query
            .measures()
            .iter()
            .map(|m| measures.render(m))
            .collect::<Result<Vec<_>, _>>()?;
        let is_simple = measures.multiplied_measures.is_empty()
            && (measures.multi_stage_measures.is_empty() || !query.allow_multi_stage())
            && !Self::join_groups_in(&query, &join_groups)?.is_multi_fact()
            && (!Self::has_multi_stage_dimensions(&query)? || !query.allow_multi_stage());
        Ok(Rc::new(Self {
            query,
            join_groups,
            measures,
            select_measures,
            is_simple,
        }))
    }

    pub fn query(&self) -> &Rc<QueryProperties> {
        &self.query
    }

    pub fn is_simple(&self) -> bool {
        self.is_simple
    }

    pub fn measures(&self) -> &FullKeyAggregateMeasures {
        &self.measures
    }

    /// The query's selected measures in render form.
    pub fn select_measures(&self) -> &[Rc<MemberSymbol>] {
        &self.select_measures
    }

    pub fn simple_query_join(&self) -> Result<Option<Rc<JoinTree>>, CubeError> {
        Self::join_groups_in(&self.query, &self.join_groups)?.single_join()
    }

    pub fn join_groups_for_measures(
        &self,
        measures: &[Rc<MemberSymbol>],
    ) -> Result<MultiFactJoinGroups, CubeError> {
        Self::join_groups_in(&self.query, &self.join_groups)?.for_measures(measures)
    }

    fn join_groups_in<'a>(
        query: &QueryProperties,
        cell: &'a OnceCell<MultiFactJoinGroups>,
    ) -> Result<&'a MultiFactJoinGroups, CubeError> {
        if let Some(groups) = cell.get() {
            return Ok(groups);
        }
        let computed = Self::compute_join_groups(query)?;
        Ok(cell.get_or_init(move || computed))
    }

    fn compute_join_groups(query: &QueryProperties) -> Result<MultiFactJoinGroups, CubeError> {
        let measures_join_hints = MeasuresJoinHints::builder(query.query_join_hints())
            .add_dimensions(query.dimensions())
            .add_dimensions(&query.extract_dimensions_from_order())
            .add_dimensions(query.time_dimensions())
            .add_filters(query.time_dimensions_filters())
            .add_filters(query.dimensions_filters())
            .add_filters(query.segments())
            .build(&query.all_used_measures()?)?;
        let query_tools = query.query_tools().clone();
        // A pre-aggregation query describes the rollup to build, and matching
        // compares its groups against the query's, so both sides have to be
        // grouped the same way.
        if query.is_pre_aggregation_query() {
            MultiFactJoinGroups::try_new(query_tools, measures_join_hints)
        } else if query.ungrouped() {
            MultiFactJoinGroups::try_new_merging_nested_raw_rows(query_tools, measures_join_hints)
        } else {
            MultiFactJoinGroups::try_new_merging_nested(query_tools, measures_join_hints)
        }
    }

    fn has_multi_stage_dimensions(query: &QueryProperties) -> Result<bool, CubeError> {
        for dim in query.dimensions().iter().chain(query.time_dimensions()) {
            if has_multi_stage_members(dim, true)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn classify_measures(
        query: &QueryProperties,
        join_groups: &OnceCell<MultiFactJoinGroups>,
    ) -> Result<FullKeyAggregateMeasures, CubeError> {
        let mut result = FullKeyAggregateMeasures::default();
        let measures = query.all_used_measures()?;
        for m in measures.iter() {
            if has_multi_stage_members(
                m,
                query.ignore_cumulative() || query.is_pre_aggregation_query(),
            )? {
                result.multi_stage_measures.push(m.clone())
            } else if query.ungrouped() && m.is_dimension_only_expression()? {
                // Without aggregation an expression over dimensions is a
                // per-row value: row multiplication has nothing to distort.
                result.regular_measures.push(m.clone());
            } else {
                let join = Self::join_groups_in(query, join_groups)?
                    .for_measures(std::slice::from_ref(m))?
                    .single_join()?
                    .ok_or_else(|| {
                        CubeError::internal(format!(
                            "No join groups returned for measure {}",
                            m.full_name()
                        ))
                    })?;
                for item in collect_multiplied_measures(m, &join)? {
                    if !item.multiplied {
                        result.regular_measures.push(item.measure.clone());
                        continue;
                    }
                    // An expression over dimensions has no other form: it
                    // moves to a multiplied subquery as it is.
                    let Ok(measure) = item.measure.as_measure() else {
                        result
                            .multiplied_measures
                            .push(MultipliedMeasure::new(item.measure.clone(), item.cube_id));
                        continue;
                    };
                    // The leaf's render form (a distinct `MultipliedCount`
                    // for a count) — the same symbol whether it stays in the
                    // main query or moves to a multiplied subquery.
                    let rendered = match transforms::regular_in_multiplied(&measure) {
                        Some(regular) => {
                            result.regular_measures.push(regular.clone());
                            regular
                        }
                        None => {
                            let rendered = transforms::into_multiplied(&measure);
                            result
                                .multiplied_measures
                                .push(MultipliedMeasure::new(rendered.clone(), item.cube_id));
                            rendered
                        }
                    };
                    result
                        .render_forms
                        .insert(item.measure.id().clone(), rendered);
                }
            }
        }
        result.multi_stage_measures = result
            .multi_stage_measures
            .into_iter()
            .unique_by(|itm| itm.id().clone())
            .collect();
        result.regular_measures = result
            .regular_measures
            .into_iter()
            .unique_by(|itm| itm.id().clone())
            .collect();
        result.multiplied_measures = result
            .multiplied_measures
            .into_iter()
            .unique_by(|itm| itm.measure.id().clone())
            .collect();

        Ok(result)
    }
}
