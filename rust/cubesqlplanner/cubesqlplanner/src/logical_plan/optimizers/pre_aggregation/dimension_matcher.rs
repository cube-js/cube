use super::CompiledPreAggregation;
use super::MatchState;
use crate::plan::{Filter, FilterItem};
use crate::plan::filter::FilterGroupOperator;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use crate::planner::filter::BaseFilter;
use std::rc::Rc;

pub struct DimensionMatcher {
}

impl DimensionMatcher {
    pub fn new() -> Self {
        Self {}
    }

    pub fn try_match(
        &self,
        symbols: &Vec<Rc<MemberSymbol>>,
        filters: &Vec<FilterItem>,
        segments: &Vec<FilterItem>,
        pre_aggregation: &CompiledPreAggregation,
    ) -> Result<MatchState, CubeError> {
        let mut pre_aggregation_dimensions = pre_aggregation
            .dimensions
            .iter()
            .map(|d| (d.full_name(), false))
            .collect();
        let mut result = MatchState::Full;
        for symbol in symbols.iter() {
            let symbol_match = self.try_match_symbol(symbol, true, &mut pre_aggregation_dimensions)?;
            result = result.combine(&symbol_match);
            if result == MatchState::NotMatched {
                return Ok(result);
            }
        }

        for filter in filters.iter() {
            let filter_match = self.try_match_filter_item(filter, true, &mut pre_aggregation_dimensions)?;
            result = result.combine(&filter_match);
            if result == MatchState::NotMatched {
                return Ok(result);
            }
        }

        for segment in segments.iter() {
            let segment_match = self.try_match_filter_item(segment, true, &mut pre_aggregation_dimensions)?;
            result = result.combine(&segment_match);
            if result == MatchState::NotMatched {
                return Ok(result);
            }
        }

        let coverage_result = if pre_aggregation_dimensions.values().all(|v| *v) {
            MatchState::Full
        } else {
            MatchState::Partial
        };

        Ok(result.combine(&coverage_result))
    }

    fn try_match_symbol(
        &self,
        symbol: &Rc<MemberSymbol>,
        add_to_matched_dimension: bool,
        pre_aggregation_dimensions: &mut HashMap<String, bool>,
    ) -> Result<MatchState, CubeError> {
        let mut result = match symbol.as_ref() {
            MemberSymbol::Dimension(dimension) => {
                if let Some(found) = pre_aggregation_dimensions.get_mut(&dimension.full_name()) {
                    if add_to_matched_dimension {
                        *found = true;
                    }
                    return Ok(MatchState::Full);
                } else if dimension.owned_by_cube() {
                    return Ok(MatchState::NotMatched);
                }
                MatchState::Full
            }
            MemberSymbol::MemberExpression(_member_expression) => MatchState::NotMatched, //TODO We not allow to use pre-aggregations with member expressions before sqlapi ready for it
            _ => return Ok(MatchState::NotMatched),
        };

        if symbol.get_dependencies().is_empty() {
            return Ok(MatchState::NotMatched);
        }

        if !symbol.is_reference() {
            result = result.combine(&MatchState::Partial);
        }

        for dep in symbol.get_dependencies() {
            let dep_match = self.try_match_symbol(&dep, add_to_matched_dimension, pre_aggregation_dimensions)?;
            if dep_match == MatchState::NotMatched {
                return Ok(MatchState::NotMatched);
            }
            result = result.combine(&dep_match);
        }
        Ok(result)
    }

    fn try_match_filter_item(
        &self,
        filter_item: &FilterItem,
        add_to_matched_dimension: bool,
        pre_aggregation_dimensions: &mut HashMap<String, bool>,
    ) -> Result<MatchState, CubeError> {
        match filter_item {
            FilterItem::Item(filter) => {
                self.try_match_filter(filter, add_to_matched_dimension, pre_aggregation_dimensions)
            }
            FilterItem::Group(group) => {
                let add_to_matched_dimension = add_to_matched_dimension && group.operator == FilterGroupOperator::And;
                let mut result = MatchState::Full;
                for item in group.items.iter() {
                    result = result.combine(&self.try_match_filter_item(item, add_to_matched_dimension, pre_aggregation_dimensions)?);
                }
                Ok(result)
            },
            FilterItem::Segment(segment) => {
                self.try_match_symbol(&segment.member_evaluator(), add_to_matched_dimension, pre_aggregation_dimensions)
            },
        }
    }

    fn try_match_filter(
        &self,
        filter: &Rc<BaseFilter>,
        add_to_matched_dimension: bool,
        pre_aggregation_dimensions: &mut HashMap<String, bool>,
    ) -> Result<MatchState, CubeError> {
        let symbol = filter.member_evaluator().clone();
        let add_to_matched_dimension = add_to_matched_dimension && filter.is_single_value_equal();
        self.try_match_symbol(&symbol, add_to_matched_dimension, pre_aggregation_dimensions)
    }



}
