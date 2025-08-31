use super::CompiledPreAggregation;
use crate::plan::filter::FilterGroupOperator;
use crate::plan::FilterItem;
use crate::planner::filter::BaseFilter;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::DimensionSymbol;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::TimeDimensionSymbol;
use crate::planner::GranularityHelper;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone, Debug, PartialEq)]
pub enum MatchState {
    Partial,
    Full,
    NotMatched,
}

impl MatchState {
    pub fn combine(&self, other: &MatchState) -> MatchState {
        if matches!(self, MatchState::NotMatched) || matches!(other, MatchState::NotMatched) {
            return MatchState::NotMatched;
        }
        if matches!(self, MatchState::Partial) || matches!(other, MatchState::Partial) {
            return MatchState::Partial;
        }
        MatchState::Full
    }
}

pub struct DimensionMatcher<'a> {
    query_tools: Rc<QueryTools>,
    pre_aggregation: &'a CompiledPreAggregation,
    pre_aggregation_dimensions: HashMap<String, bool>,
    pre_aggregation_time_dimensions: HashMap<String, (Option<String>, bool)>,
    result: MatchState,
}

impl<'a> DimensionMatcher<'a> {
    pub fn new(query_tools: Rc<QueryTools>, pre_aggregation: &'a CompiledPreAggregation) -> Self {
        let pre_aggregation_dimensions = pre_aggregation
            .dimensions
            .iter()
            .map(|d| (d.full_name(), false))
            .collect();
        let pre_aggregation_time_dimensions = pre_aggregation
            .time_dimensions
            .iter()
            .map(|(dim, granularity)| (dim.full_name(), (granularity.clone(), false)))
            .collect::<HashMap<_, _>>();
        Self {
            query_tools,
            pre_aggregation,
            pre_aggregation_dimensions,
            pre_aggregation_time_dimensions,
            result: MatchState::Full,
        }
    }

    pub fn try_match(
        &mut self,
        dimensions: &Vec<Rc<MemberSymbol>>,
        time_dimensions: &Vec<Rc<MemberSymbol>>,
        filters: &Vec<FilterItem>,
        time_dimension_filters: &Vec<FilterItem>,
        segments: &Vec<FilterItem>,
    ) -> Result<(), CubeError> {
        for dimension in dimensions.iter() {
            let dimension_match = self.try_match_symbol(dimension, true)?;
            self.result = self.result.combine(&dimension_match);
            if self.result == MatchState::NotMatched {
                return Ok(());
            }
        }
        for time_dimension in time_dimensions.iter() {
            let time_dimension_match = self.try_match_symbol(time_dimension, true)?;
            self.result = self.result.combine(&time_dimension_match);
            if self.result == MatchState::NotMatched {
                return Ok(());
            }
        }

        for filter in filters.iter() {
            let filter_match = self.try_match_filter_item(filter, true)?;
            self.result = self.result.combine(&filter_match);
            if self.result == MatchState::NotMatched {
                return Ok(());
            }
        }

        for filter in time_dimension_filters.iter() {
            let filter_match = self.try_match_filter_item(filter, true)?;
            self.result = self.result.combine(&filter_match);
            if self.result == MatchState::NotMatched {
                return Ok(());
            }
        }

        for segment in segments.iter() {
            let segment_match = self.try_match_filter_item(segment, true)?;
            self.result = self.result.combine(&segment_match);
            if self.result == MatchState::NotMatched {
                return Ok(());
            }
        }
        Ok(())
    }

    pub fn result(mut self) -> MatchState {
        let dimension_coverage_result = if self.pre_aggregation_dimensions.values().all(|v| *v) {
            MatchState::Full
        } else {
            MatchState::Partial
        };
        self.result = self.result.combine(&dimension_coverage_result);
        let time_dimension_coverage_result =
            if self.pre_aggregation_time_dimensions.values().all(|v| v.1) {
                MatchState::Full
            } else {
                MatchState::Partial
            };
        self.result = self.result.combine(&time_dimension_coverage_result);
        self.result
    }

    fn try_match_symbol(
        &mut self,
        symbol: &Rc<MemberSymbol>,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        match symbol.as_ref() {
            MemberSymbol::Dimension(dimension) => {
                self.try_match_dimension(dimension, add_to_matched_dimension)
            }
            MemberSymbol::TimeDimension(time_dimension) => {
                self.try_match_time_dimension(time_dimension, add_to_matched_dimension)
            }
            MemberSymbol::MemberExpression(_member_expression) => Ok(MatchState::NotMatched), //TODO We don't allow to use pre-aggregations with member expressions before SQL API is ready for it
            _ => Ok(MatchState::NotMatched),
        }
    }

    fn try_match_dimension(
        &mut self,
        dimension: &DimensionSymbol,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        if let Some(found) = self
            .pre_aggregation_dimensions
            .get_mut(&dimension.full_name())
        {
            if add_to_matched_dimension {
                *found = true;
            }
            Ok(MatchState::Full)
        } else if dimension.owned_by_cube() {
            Ok(MatchState::NotMatched)
        } else {
            let dependencies = dimension.get_dependencies();
            if dependencies.is_empty() {
                Ok(MatchState::NotMatched)
            } else {
                let mut result = if dimension.is_reference() {
                    MatchState::Full
                } else {
                    MatchState::Partial
                };
                for dep in dimension.get_dependencies() {
                    let dep_match = self.try_match_symbol(&dep, add_to_matched_dimension)?;
                    if dep_match == MatchState::NotMatched {
                        return Ok(MatchState::NotMatched);
                    }
                    result = result.combine(&dep_match);
                }
                Ok(result)
            }
        }
    }

    fn try_match_time_dimension(
        &mut self,
        time_dimension: &TimeDimensionSymbol,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        let granularity = if self.pre_aggregation.allow_non_strict_date_range_match {
            time_dimension.granularity().clone()
        } else {
            time_dimension.rollup_granularity(self.query_tools.clone())?
        };
        let base_symbol_name = time_dimension.base_symbol().full_name();
        if let Some(found) = self
            .pre_aggregation_time_dimensions
            .get_mut(&base_symbol_name)
        {
            if add_to_matched_dimension {
                found.1 = true;
            }
            let pre_aggr_granularity = &found.0;
            if granularity.is_none() || pre_aggr_granularity == &granularity {
                Ok(MatchState::Full)
            } else if pre_aggr_granularity.is_none()
                || GranularityHelper::is_predefined_granularity(
                    pre_aggr_granularity.as_ref().unwrap(),
                )
            {
                let min_granularity =
                    GranularityHelper::min_granularity(&granularity, &pre_aggr_granularity)?;
                if &min_granularity == pre_aggr_granularity {
                    Ok(MatchState::Partial)
                } else {
                    Ok(MatchState::NotMatched)
                }
            } else {
                Ok(MatchState::NotMatched) //TODO Custom granularities!!!
            }
        } else {
            if time_dimension.owned_by_cube() {
                Ok(MatchState::NotMatched)
            } else {
                let mut result = if time_dimension.is_reference() {
                    MatchState::Full
                } else {
                    MatchState::Partial
                };
                for dep in time_dimension.get_dependencies_as_time_dimensions() {
                    let dep_match = self.try_match_symbol(&dep, add_to_matched_dimension)?;
                    if dep_match == MatchState::NotMatched {
                        return Ok(MatchState::NotMatched);
                    }
                    result = result.combine(&dep_match);
                }
                Ok(result)
            }
        }
    }

    fn try_match_filter_item(
        &mut self,
        filter_item: &FilterItem,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        match filter_item {
            FilterItem::Item(filter) => self.try_match_filter(filter, add_to_matched_dimension),
            FilterItem::Group(group) => {
                let add_to_matched_dimension =
                    add_to_matched_dimension && group.operator == FilterGroupOperator::And;
                let mut result = MatchState::Full;
                for item in group.items.iter() {
                    result = result
                        .combine(&self.try_match_filter_item(item, add_to_matched_dimension)?);
                }
                Ok(result)
            }
            FilterItem::Segment(segment) => {
                self.try_match_symbol(&segment.member_evaluator(), add_to_matched_dimension)
            }
        }
    }

    fn try_match_filter(
        &mut self,
        filter: &Rc<BaseFilter>,
        add_to_matched_dimension: bool,
    ) -> Result<MatchState, CubeError> {
        let symbol = if let Some(time_dimension) = filter.time_dimension_symbol() {
            time_dimension
        } else {
            filter.member_evaluator().clone()
        };
        let add_to_matched_dimension = add_to_matched_dimension && filter.is_single_value_equal();
        self.try_match_symbol(&symbol, add_to_matched_dimension)
    }
}
