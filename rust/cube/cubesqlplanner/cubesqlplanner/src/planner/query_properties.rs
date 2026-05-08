use super::query_tools::QueryTools;
use super::MemberSymbol;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::planner::collectors::{collect_multiplied_measures, has_multi_stage_members};
use crate::planner::filter::{Filter, FilterItem};
use crate::planner::join_hints::JoinHints;
use crate::planner::multi_fact_join_groups::{MeasuresJoinHints, MultiFactJoinGroups};
use crate::planner::{apply_static_filter_to_filter_item, apply_static_filter_to_symbol};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashSet;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct OrderByItem {
    member_evaluator: Rc<MemberSymbol>,
    desc: bool,
}

impl OrderByItem {
    pub fn new(member_evaluator: Rc<MemberSymbol>, desc: bool) -> Self {
        Self {
            member_evaluator,
            desc,
        }
    }

    pub fn name(&self) -> String {
        self.member_evaluator.full_name()
    }

    pub fn member_symbol(&self) -> Rc<MemberSymbol> {
        self.member_evaluator.clone()
    }

    pub fn desc(&self) -> bool {
        self.desc
    }
}

#[derive(Debug, Clone)]
pub struct MultipliedMeasure {
    measure: Rc<MemberSymbol>,
    cube_name: String, //May differ from cube_name of the measure for a member_expression that refers to a dimension.
}

impl MultipliedMeasure {
    pub fn new(measure: Rc<MemberSymbol>, cube_name: String) -> Rc<Self> {
        Rc::new(Self { measure, cube_name })
    }

    pub fn measure(&self) -> &Rc<MemberSymbol> {
        &self.measure
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

#[derive(Default, Clone, Debug)]
pub struct FullKeyAggregateMeasures {
    pub multiplied_measures: Vec<Rc<MultipliedMeasure>>,
    pub regular_measures: Vec<Rc<MemberSymbol>>,
    pub multi_stage_measures: Vec<Rc<MemberSymbol>>,
    pub rendered_as_multiplied_measures: HashSet<String>,
}

impl FullKeyAggregateMeasures {
    pub fn has_multiplied_measures(&self) -> bool {
        !self.multiplied_measures.is_empty()
    }

    pub fn has_multi_stage_measures(&self) -> bool {
        !self.multi_stage_measures.is_empty()
    }
}

#[derive(Clone)]
pub struct QueryProperties {
    measures: Vec<Rc<MemberSymbol>>,
    dimensions: Vec<Rc<MemberSymbol>>,
    dimensions_filters: Vec<FilterItem>,
    time_dimensions_filters: Vec<FilterItem>,
    measures_filters: Vec<FilterItem>,
    segments: Vec<FilterItem>,
    time_dimensions: Vec<Rc<MemberSymbol>>,
    order_by: Vec<OrderByItem>,
    row_limit: Option<usize>,
    offset: Option<usize>,
    query_tools: Rc<QueryTools>,
    ignore_cumulative: bool,
    ungrouped: bool,
    multi_fact_join_groups: MultiFactJoinGroups,
    pre_aggregation_query: bool,
    total_query: bool,
    query_join_hints: Rc<JoinHints>,
    allow_multi_stage: bool,
    disable_external_pre_aggregations: bool,
    pre_aggregation_id: Option<String>,
}

impl QueryProperties {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        measures: Vec<Rc<MemberSymbol>>,
        dimensions: Vec<Rc<MemberSymbol>>,
        time_dimensions: Vec<Rc<MemberSymbol>>,
        time_dimensions_filters: Vec<FilterItem>,
        dimensions_filters: Vec<FilterItem>,
        measures_filters: Vec<FilterItem>,
        segments: Vec<FilterItem>,
        order_by: Vec<OrderByItem>,
        row_limit: Option<usize>,
        offset: Option<usize>,
        ignore_cumulative: bool,
        ungrouped: bool,
        pre_aggregation_query: bool,
        total_query: bool,
        query_join_hints: Rc<JoinHints>,
        allow_multi_stage: bool,
        disable_external_pre_aggregations: bool,
        pre_aggregation_id: Option<String>,
    ) -> Result<Rc<Self>, CubeError> {
        let order_by = if order_by.is_empty() {
            Self::default_order(&dimensions, &time_dimensions, &measures)
        } else {
            order_by
        };

        let mut res = Self {
            measures,
            dimensions,
            time_dimensions,
            time_dimensions_filters,
            dimensions_filters,
            segments,
            measures_filters,
            order_by,
            row_limit,
            offset,
            multi_fact_join_groups: MultiFactJoinGroups::empty(query_tools.clone()),
            query_tools,
            ignore_cumulative,
            ungrouped,
            pre_aggregation_query,
            total_query,
            query_join_hints,
            allow_multi_stage,
            disable_external_pre_aggregations,
            pre_aggregation_id,
        };
        res.apply_static_filters()?;

        Ok(Rc::new(res))
    }

    pub fn allow_multi_stage(&self) -> bool {
        self.allow_multi_stage
    }

    fn apply_static_filters(&mut self) -> Result<(), CubeError> {
        let dimensions_filters = self.dimensions_filters.clone();
        for dim in self.dimensions.iter_mut() {
            *dim = apply_static_filter_to_symbol(dim, &dimensions_filters)?;
        }
        for dim in self.time_dimensions.iter_mut() {
            *dim = apply_static_filter_to_symbol(dim, &dimensions_filters)?;
        }
        for meas in self.measures.iter_mut() {
            *meas = apply_static_filter_to_symbol(meas, &dimensions_filters)?;
        }
        for filter_item in self.dimensions_filters.iter_mut() {
            *filter_item = apply_static_filter_to_filter_item(filter_item, &dimensions_filters)?;
        }
        for filter_item in self.measures_filters.iter_mut() {
            *filter_item = apply_static_filter_to_filter_item(filter_item, &dimensions_filters)?;
        }
        for filter_item in self.time_dimensions_filters.iter_mut() {
            *filter_item = apply_static_filter_to_filter_item(filter_item, &dimensions_filters)?;
        }
        for filter_item in self.segments.iter_mut() {
            *filter_item = apply_static_filter_to_filter_item(filter_item, &dimensions_filters)?;
        }
        for order_item in self.order_by.iter_mut() {
            order_item.member_evaluator =
                apply_static_filter_to_symbol(&order_item.member_evaluator, &dimensions_filters)?;
        }

        let measures_join_hints = MeasuresJoinHints::builder(&self.query_join_hints)
            .add_dimensions(&self.dimensions)
            .add_dimensions(&self.extract_dimensions_from_order())
            .add_dimensions(&self.time_dimensions)
            .add_filters(&self.time_dimensions_filters)
            .add_filters(&self.dimensions_filters)
            .add_filters(&self.segments)
            .build(&self.all_used_measures()?)?;
        self.multi_fact_join_groups =
            MultiFactJoinGroups::try_new(self.query_tools.clone(), measures_join_hints)?;
        Ok(())
    }

    pub fn compute_join_multi_fact_groups_with_measures(
        &self,
        measures: &[Rc<MemberSymbol>],
    ) -> Result<MultiFactJoinGroups, CubeError> {
        self.multi_fact_join_groups.for_measures(measures)
    }

    pub fn is_total_query(&self) -> bool {
        self.total_query
    }

    fn extract_dimensions_from_order(&self) -> Vec<Rc<MemberSymbol>> {
        self.order_by
            .iter()
            .filter_map(|order| {
                if order.member_evaluator.as_dimension().is_ok() {
                    Some(order.member_evaluator.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn is_multi_fact_join(&self) -> bool {
        self.multi_fact_join_groups.is_multi_fact()
    }

    pub fn simple_query_join(&self) -> Result<Option<Rc<dyn JoinDefinition>>, CubeError> {
        self.multi_fact_join_groups.single_join()
    }

    pub fn measures(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.measures
    }

    pub fn dimensions(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.dimensions
    }

    pub fn time_dimensions(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.time_dimensions
    }

    pub fn time_dimensions_filters(&self) -> &Vec<FilterItem> {
        &self.time_dimensions_filters
    }

    pub fn dimensions_filters(&self) -> &Vec<FilterItem> {
        &self.dimensions_filters
    }

    pub fn measures_filters(&self) -> &Vec<FilterItem> {
        &self.measures_filters
    }

    pub fn row_limit(&self) -> Option<usize> {
        self.row_limit
    }

    pub fn query_join_hints(&self) -> &Rc<JoinHints> {
        &self.query_join_hints
    }

    pub fn offset(&self) -> Option<usize> {
        self.offset
    }

    pub fn order_by(&self) -> &Vec<OrderByItem> {
        &self.order_by
    }

    pub fn set_order_by_to_default(&mut self) {
        self.order_by =
            Self::default_order(&self.dimensions, &self.time_dimensions, &self.measures);
    }

    pub fn ungrouped(&self) -> bool {
        self.ungrouped
    }

    pub fn is_pre_aggregation_query(&self) -> bool {
        self.pre_aggregation_query
    }

    pub fn disable_external_pre_aggregations(&self) -> bool {
        self.disable_external_pre_aggregations
    }

    pub fn pre_aggregation_id(&self) -> Option<&str> {
        self.pre_aggregation_id.as_deref()
    }

    pub fn all_filters(&self) -> Option<Filter> {
        let items = self
            .time_dimensions_filters
            .iter()
            .chain(self.dimensions_filters.iter())
            .chain(self.segments.iter())
            .cloned()
            .collect_vec();
        if items.is_empty() {
            None
        } else {
            Some(Filter { items })
        }
    }

    pub fn segments(&self) -> &Vec<FilterItem> {
        &self.segments
    }

    pub fn all_members(&self, exclude_time_dimensions: bool) -> Vec<Rc<MemberSymbol>> {
        let dimensions = self.dimensions.iter().cloned();
        let measures = self.measures.iter().cloned();
        if exclude_time_dimensions {
            dimensions.chain(measures).collect_vec()
        } else {
            let time_dimensions = self.time_dimensions.iter().map(|d| d.clone());
            dimensions
                .chain(time_dimensions)
                .chain(measures)
                .collect_vec()
        }
    }

    pub fn all_used_symbols(&self) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let mut members = vec![];
        members.extend(self.time_dimensions.iter().cloned());
        members.extend(self.dimensions.iter().cloned());
        self.fill_all_filter_symbols(&mut members);
        members.extend(self.all_used_measures()?);

        let res = members
            .into_iter()
            .unique_by(|m| m.full_name())
            .collect_vec();
        Ok(res)
    }

    pub fn get_member_symbols(
        &self,
        include_time_dimensions: bool,
        include_dimensions: bool,
        include_measures: bool,
        include_filters: bool,
        additional_symbols: &Vec<Rc<MemberSymbol>>,
    ) -> Vec<Rc<MemberSymbol>> {
        let mut members = additional_symbols.clone();
        if include_time_dimensions {
            members.extend(self.time_dimensions.iter().cloned());
        }
        if include_dimensions {
            members.extend(self.dimensions.iter().cloned());
        }
        if include_measures {
            members.extend(self.measures.iter().cloned());
        }
        if include_filters {
            self.fill_all_filter_symbols(&mut members);
        }
        members
            .into_iter()
            .unique_by(|m| m.full_name())
            .collect_vec()
    }

    pub fn fill_all_filter_symbols(&self, members: &mut Vec<Rc<MemberSymbol>>) {
        if let Some(all_filters) = self.all_filters() {
            for filter_item in all_filters.items.iter() {
                filter_item.find_all_member_evaluators(members);
            }
        }
    }

    pub fn default_order(
        dimensions: &[Rc<MemberSymbol>],
        time_dimensions: &[Rc<MemberSymbol>],
        measures: &[Rc<MemberSymbol>],
    ) -> Vec<OrderByItem> {
        let mut result = Vec::new();
        if let Some(granularity_dim) = time_dimensions.iter().find(|d| {
            if let Ok(td) = d.as_time_dimension() {
                td.has_granularity()
            } else {
                false
            }
        }) {
            result.push(OrderByItem::new(granularity_dim.clone(), false));
        } else if !measures.is_empty() && !dimensions.is_empty() {
            result.push(OrderByItem::new(measures[0].clone(), true));
        } else if !dimensions.is_empty() {
            result.push(OrderByItem::new(dimensions[0].clone(), false));
        }
        result
    }

    pub fn is_simple_query(&self) -> Result<bool, CubeError> {
        let full_aggregate_measure = self.full_key_aggregate_measures()?;
        if full_aggregate_measure.multiplied_measures.is_empty()
            && (full_aggregate_measure.multi_stage_measures.is_empty() || !self.allow_multi_stage)
            && !self.is_multi_fact_join()
            && (!self.has_multi_stage_dimensions()? || !self.allow_multi_stage)
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn has_multi_stage_dimensions(&self) -> Result<bool, CubeError> {
        for dim in self.dimensions.iter() {
            if has_multi_stage_members(dim, true)? {
                return Ok(true);
            }
        }
        for dim in self.time_dimensions.iter() {
            if has_multi_stage_members(dim, true)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn full_key_aggregate_measures(&self) -> Result<FullKeyAggregateMeasures, CubeError> {
        let mut result = FullKeyAggregateMeasures::default();
        let measures = self.all_used_measures()?;
        for m in measures.iter() {
            if has_multi_stage_members(m, self.ignore_cumulative || self.pre_aggregation_query)? {
                result.multi_stage_measures.push(m.clone())
            } else {
                let join = self
                    .compute_join_multi_fact_groups_with_measures(std::slice::from_ref(m))?
                    .single_join()?
                    .expect("No join groups returned for single measure multi-fact join group");
                for item in collect_multiplied_measures(m, join)? {
                    if item.multiplied {
                        result
                            .rendered_as_multiplied_measures
                            .insert(item.measure.full_name());
                    }
                    let is_multiplied_measure = if item.multiplied {
                        if let Ok(measure) = item.measure.as_measure() {
                            if measure.can_used_as_addictive_in_multplied() {
                                false
                            } else {
                                true
                            }
                        } else {
                            true
                        }
                    } else {
                        false
                    };
                    if is_multiplied_measure {
                        result
                            .multiplied_measures
                            .push(MultipliedMeasure::new(item.measure.clone(), item.cube_name));
                    } else {
                        result.regular_measures.push(item.measure.clone());
                    }
                }
            }
        }
        result.multi_stage_measures = result
            .multi_stage_measures
            .into_iter()
            .unique_by(|itm| itm.full_name())
            .collect();
        result.regular_measures = result
            .regular_measures
            .into_iter()
            .unique_by(|itm| itm.full_name())
            .collect();
        result.multiplied_measures = result
            .multiplied_measures
            .into_iter()
            .unique_by(|itm| itm.measure.full_name())
            .collect();

        Ok(result)
    }

    fn all_used_measures(&self) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let mut measures = self.measures.clone();
        for item in self.measures_filters.iter() {
            self.fill_missed_measures_from_filter(item, &mut measures)?;
        }
        for item in self.order_by.iter() {
            if let Ok(measure) = item.member_evaluator.as_measure() {
                if !measures
                    .iter()
                    .any(|m| m.full_name() == measure.full_name())
                {
                    measures.push(item.member_evaluator.clone());
                }
            }
        }
        Ok(measures)
    }

    fn fill_missed_measures_from_filter(
        &self,
        item: &FilterItem,
        measures: &mut Vec<Rc<MemberSymbol>>,
    ) -> Result<(), CubeError> {
        match item {
            FilterItem::Group(group) => {
                for item in group.items.iter() {
                    self.fill_missed_measures_from_filter(item, measures)?
                }
            }
            FilterItem::Item(item) => {
                let item_member_name = item.member_name();
                if !measures.iter().any(|m| m.full_name() == item_member_name) {
                    measures.push(item.member_evaluator().clone());
                }
            }
            FilterItem::Segment(_) => {}
        }
        Ok(())
    }
}
