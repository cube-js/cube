use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;
use super::CompiledPreAggregation;
use std::collections::HashSet;
use super::MatchState;

pub struct DimensionMatcher {
    only_addictive: bool,
    pre_aggregation_dimensions: HashSet<String>,
}

impl DimensionMatcher {
    pub fn new(pre_aggregation: &CompiledPreAggregation, only_addictive: bool) -> Self {
        let pre_aggregation_dimensions = pre_aggregation.dimensions.iter().map(|d| d.full_name()).collect();
        Self { only_addictive, pre_aggregation_dimensions }
    }

    pub fn try_match(&self, symbol: &Rc<MemberSymbol>) -> Result<MatchState, CubeError> {
        let mut result = match symbol.as_ref() {
            MemberSymbol::Dimension(dimension) => {
                if dimension.owned_by_cube() {
                    if self.pre_aggregation_dimensions.contains(&dimension.full_name()) {
                        return Ok(MatchState::Full);
                    } else {
                        return Ok(MatchState::NotMatched);
                    }
                }
                MatchState::Full
            }
            MemberSymbol::MemberExpression(member_expression) => {
                MatchState::Partial
            }
            _ => return Ok(MatchState::NotMatched)
        };

        if symbol.get_dependencies().is_empty() {
            return Ok(MatchState::NotMatched);
        }

        for dep in symbol.get_dependencies() {
            let dep_match = self.try_match(&dep)?;
            if dep_match == MatchState::NotMatched {
                return Ok(MatchState::NotMatched);
            }
            result = result.combine(&dep_match);
        }
        Ok(result)
    }


}