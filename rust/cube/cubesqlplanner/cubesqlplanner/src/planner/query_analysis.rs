use super::query_properties::QueryProperties;
use super::symbol_classification::{FullKeyAggregateMeasures, SymbolClassification};
use super::MemberSymbol;
use crate::planner::multi_fact_join_groups::{MeasuresJoinHints, MultiFactJoinGroups};
use crate::planner::symbols::transforms;
use crate::planner::JoinTree;
use cubenativeutils::CubeError;
use std::cell::OnceCell;
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
}

impl QueryAnalysis {
    pub fn try_new(request: Rc<QueryProperties>) -> Result<Rc<Self>, CubeError> {
        let specializer = transforms::Specializer::for_query(
            request.dimensions_filters(),
            &request.all_filter_items(),
            request.allow_multi_stage(),
        );
        let query = request.map_symbols(&|symbol| specializer.specialize(symbol))?;
        let join_groups = OnceCell::new();
        let classification = SymbolClassification::try_new(&query, |measure| {
            Self::join_groups_in(&query, &join_groups)?
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
        let is_simple = measures.multiplied_measures.is_empty()
            && (measures.multi_stage_measures.is_empty() || !query.allow_multi_stage())
            && !Self::join_groups_in(&query, &join_groups)?.is_multi_fact()
            && (!has_multi_stage_dimensions || !query.allow_multi_stage());
        Ok(Rc::new(Self {
            request,
            query,
            join_groups,
            classification,
            select_measures,
            is_simple,
        }))
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
}
