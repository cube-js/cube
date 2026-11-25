use super::query_properties::QueryProperties;
use super::symbol_classification::{FullKeyAggregateMeasures, SymbolClassification};
use super::MemberSymbol;
use crate::logical_plan::{FilterPlacement, LogicalFilter};
use crate::planner::collectors::has_multi_stage_members;
use crate::planner::dimension_overrides::{self, partition_by_dimension_overrides};
use crate::planner::filter::{FilterGroupOperator, FilterItem};
use crate::planner::multi_fact_join_groups::{MeasuresJoinHints, MultiFactJoinGroups};
use crate::planner::symbols::transforms;
use crate::planner::JoinTree;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::cell::OnceCell;
use std::collections::HashSet;
use std::rc::Rc;

/// What planning needs to know about one built query: its join groups, how
/// its measures are computed, and whether it plans as a single simple query.
pub struct QueryAnalysis {
    request: Rc<QueryProperties>,
    // The request specialized against its own filters; everything below is
    // derived from it.
    query: Rc<QueryProperties>,
    // Lazy: a query whose measures are all multi-stage may have no valid join
    // of its own, and its planning never asks for one.
    join_groups: OnceCell<MultiFactJoinGroups>,
    classification: SymbolClassification,
    select_measures: Vec<Rc<MemberSymbol>>,
    is_simple: bool,
    // Planned on their own and merged when the query's measures need
    // different dimensions; empty otherwise.
    override_legs: Vec<Rc<QueryProperties>>,
}

impl QueryAnalysis {
    pub fn try_new(request: Rc<QueryProperties>) -> Result<Rc<Self>, CubeError> {
        let partitions = partition_by_dimension_overrides(&request)?;
        let view_composites = request
            .all_used_measures()?
            .into_iter()
            .filter(dimension_overrides::is_view_composite)
            .collect_vec();
        // A view composite is computed over legs only when it can't be planned
        // as one measure: its leaves need different overrides, or no single
        // join reaches them all.
        let composites_need_legs = !view_composites.is_empty()
            && match partitions.as_slice() {
                [single] => !Self::has_single_join(&*single.apply(&request)?, &view_composites)?,
                _ => true,
            };
        if composites_need_legs && request.ungrouped() {
            return Err(CubeError::user(format!(
                "An ungrouped query can't compute {}: its measures can't be read in one query",
                view_composites.iter().map(|m| m.full_name()).join(", ")
            )));
        }
        let (request, override_legs) = match partitions.as_slice() {
            [single] if single.has_overrides() && !composites_need_legs => {
                (single.apply(&request)?, vec![])
            }
            [_, ..] if composites_need_legs || partitions.len() > 1 => {
                let legs = partitions
                    .iter()
                    .map(|partition| partition.leg(&request))
                    .collect::<Result<Vec<_>, _>>()?;
                (request, legs)
            }
            _ => (request, vec![]),
        };
        let specializer = transforms::Specializer::for_query(&request, request.allow_multi_stage());
        let query = request.map_symbols(&|symbol| specializer.specialize(symbol))?;
        // Over legs the query only reads their measures' columns.
        let classified = if override_legs.is_empty() {
            query.clone()
        } else {
            query.leg_for_measures(vec![])
        };
        let join_groups = OnceCell::new();
        let leg_join_groups = override_legs.iter().map(|_| OnceCell::new()).collect_vec();
        let mut classification = SymbolClassification::try_new(&classified, |measure| {
            let groups = match override_legs
                .iter()
                .position(|leg| leg.measures().iter().any(|m| m.id() == measure.id()))
            {
                Some(leg) => Self::join_groups_in(&override_legs[leg], &leg_join_groups[leg])?,
                None => Self::join_groups_in(&query, &join_groups)?,
            };
            groups
                .for_measures(std::slice::from_ref(measure))?
                .single_join()?
                .ok_or_else(|| {
                    CubeError::internal(format!(
                        "No join groups returned for measure {}",
                        measure.full_name()
                    ))
                })
        })?;
        let measures = classification.measures();
        let select_measures = query
            .measures()
            .iter()
            .map(|m| measures.render(m))
            .collect::<Result<Vec<_>, _>>()?;
        let mut has_multi_stage_dimensions = false;
        for dim in query.dimensions().iter().chain(query.time_dimensions()) {
            has_multi_stage_dimensions |= classification.has_multi_stage(dim, true)?;
        }
        let is_simple = override_legs.is_empty()
            && measures.multiplied_measures.is_empty()
            && (measures.multi_stage_measures.is_empty() || !query.allow_multi_stage())
            && !Self::join_groups_in(&query, &join_groups)?.is_multi_fact()
            && (!has_multi_stage_dimensions || !query.allow_multi_stage());
        // Rolling windows left as plain leaves (a rollup build) are computed
        // as stages once the query is not simple, so they are not leaves too.
        if !is_simple && query.allow_multi_stage() {
            let mut stages = Vec::new();
            for measure in classified.all_used_measures()? {
                if classification.has_multi_stage(&measure, false)? {
                    stages.push(measure);
                }
            }
            classification.compute_as_stages(&stages);
        }
        Ok(Rc::new(Self {
            request,
            query,
            join_groups,
            classification,
            select_measures,
            is_simple,
            override_legs,
        }))
    }

    /// The queries computing the measures that need different dimensions, to
    /// be merged; empty when the measures agree on their dimensions.
    pub fn override_legs(&self) -> &[Rc<QueryProperties>] {
        &self.override_legs
    }

    /// The query as requested, before specialization.
    pub fn request(&self) -> &Rc<QueryProperties> {
        &self.request
    }

    /// The query specialized against its own filters.
    pub fn query(&self) -> &Rc<QueryProperties> {
        &self.query
    }

    pub fn is_simple(&self) -> bool {
        self.is_simple
    }

    pub fn measures(&self) -> &FullKeyAggregateMeasures {
        self.classification.measures()
    }

    pub fn classification(&self) -> &SymbolClassification {
        &self.classification
    }

    /// The query's selected measures in render form.
    pub fn select_measures(&self) -> &[Rc<MemberSymbol>] {
        &self.select_measures
    }

    /// The part of the query's filters a select reading its subqueries applies
    /// itself; the subqueries applied the rest.
    pub fn filter_placement(
        &self,
        merges_legs: bool,
        selected_multi_stage_dimensions: &[Rc<MemberSymbol>],
    ) -> Result<FilterPlacement, CubeError> {
        // Merged legs are aggregated already: their measure filters filter rows.
        if merges_legs {
            return Ok(FilterPlacement::over_merged_legs(
                self.query.measures_filters().clone(),
            ));
        }
        Ok(
            match self.filter_on_dimension_subquery(selected_multi_stage_dimensions)? {
                DimensionSubqueryFilter::Applies(own) => FilterPlacement::aggregating(own),
                DimensionSubqueryFilter::Unrenderable(reason) => {
                    FilterPlacement::unrenderable(reason)
                }
            },
        )
    }

    // Over the multi-stage dimension subquery alone the select applies, besides
    // the measure filters, the filters on its selected multi-stage dimensions.
    fn filter_on_dimension_subquery(
        &self,
        selected_multi_stage_dimensions: &[Rc<MemberSymbol>],
    ) -> Result<DimensionSubqueryFilter, CubeError> {
        let measures_filter = self.query.measures_filters().clone();
        let selected = selected_multi_stage_dimensions
            .iter()
            .map(|d| d.clone().resolve_reference_chain().full_name())
            .collect::<HashSet<_>>();
        let all_items = self
            .query
            .time_dimensions_filters()
            .iter()
            .chain(self.query.dimensions_filters().iter())
            .chain(self.query.segments().iter())
            .collect_vec();
        let mut multi_stage_members = HashSet::new();
        for item in all_items.iter() {
            for member in item.all_member_evaluators() {
                if has_multi_stage_members(&member, true)? {
                    multi_stage_members.insert(member.resolve_reference_chain().full_name());
                }
            }
        }
        if multi_stage_members.is_empty() {
            return Ok(DimensionSubqueryFilter::Applies(LogicalFilter {
                measures_filter,
                ..Default::default()
            }));
        }
        // FIXME: a filter on a multi-stage dimension that is not selected has no
        // column to resolve to; it should be planned instead of rejected.
        if let Some(member) = multi_stage_members
            .iter()
            .sorted()
            .find(|name| !selected.contains(*name))
        {
            return Ok(DimensionSubqueryFilter::Unrenderable(format!(
                "Filtering on multi-stage dimension {} requires it to be selected as a dimension",
                member
            )));
        }
        if let Some(reason) = all_items
            .iter()
            .find_map(|item| mixed_or_group(item, &multi_stage_members))
        {
            return Ok(DimensionSubqueryFilter::Unrenderable(reason));
        }
        let targets = multi_stage_members.iter().collect_vec();
        let on_targets = |items: &[FilterItem]| {
            items
                .iter()
                .filter_map(|item| item.find_subtree_for_members(&targets))
                .collect_vec()
        };
        Ok(DimensionSubqueryFilter::Applies(LogicalFilter {
            dimensions_filters: on_targets(self.query.dimensions_filters()),
            time_dimensions_filters: on_targets(self.query.time_dimensions_filters()),
            segments: on_targets(self.query.segments()),
            measures_filter,
        }))
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

    // Only a missing join path means no single join; any other failure is the
    // query's own error.
    fn has_single_join(
        query: &QueryProperties,
        measures: &[Rc<MemberSymbol>],
    ) -> Result<bool, CubeError> {
        let hints = Self::measures_join_hints(query)?;
        let Ok(groups) =
            MultiFactJoinGroups::try_new_if_joinable(query.query_tools().clone(), hints)?
        else {
            return Ok(false);
        };
        for measure in measures {
            let measure_groups = groups.for_measures(std::slice::from_ref(measure))?;
            if !matches!(measure_groups.single_join(), Ok(Some(_))) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn measures_join_hints(query: &QueryProperties) -> Result<MeasuresJoinHints, CubeError> {
        MeasuresJoinHints::builder(query.query_join_hints())
            .add_dimensions(query.dimensions())
            .add_dimensions(&query.extract_dimensions_from_order())
            .add_dimensions(query.time_dimensions())
            .add_filters(query.time_dimensions_filters())
            .add_filters(query.dimensions_filters())
            .add_filters(query.segments())
            .build(&query.all_used_measures()?)
    }

    fn compute_join_groups(query: &QueryProperties) -> Result<MultiFactJoinGroups, CubeError> {
        let measures_join_hints = Self::measures_join_hints(query)?;
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
}

// Why an OR group mixing multi-stage dimensions with other members can't be
// applied over the multi-stage dimension subquery, if `item` holds one.
fn mixed_or_group(item: &FilterItem, multi_stage_members: &HashSet<String>) -> Option<String> {
    let FilterItem::Group(group) = item else {
        return None;
    };
    if group.operator == FilterGroupOperator::Or {
        let members = item.all_member_evaluators();
        let multi_stage_count = members
            .iter()
            .filter(|m| {
                multi_stage_members.contains(&(*m).clone().resolve_reference_chain().full_name())
            })
            .count();
        if multi_stage_count > 0 && multi_stage_count < members.len() {
            return Some(format!(
                "An OR filter group can't mix multi-stage dimensions with other members: {}",
                members.iter().map(|m| m.full_name()).join(", ")
            ));
        }
        return None;
    }
    group
        .items
        .iter()
        .find_map(|child| mixed_or_group(child, multi_stage_members))
}

enum DimensionSubqueryFilter {
    Applies(LogicalFilter),
    Unrenderable(String),
}
