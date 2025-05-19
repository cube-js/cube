use super::CompiledPreAggregation;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;
pub struct MeasureMatcher {
    only_addictive: bool,
    pre_aggregation_measures: HashSet<String>,
}

impl MeasureMatcher {
    pub fn new(pre_aggregation: &CompiledPreAggregation, only_addictive: bool) -> Self {
        let pre_aggregation_measures = pre_aggregation
            .measures
            .iter()
            .map(|m| m.full_name())
            .collect();
        Self {
            only_addictive,
            pre_aggregation_measures,
        }
    }

    pub fn try_match(&self, symbol: &Rc<MemberSymbol>) -> Result<bool, CubeError> {
        match symbol.as_ref() {
            MemberSymbol::Measure(measure) => {
                if self.pre_aggregation_measures.contains(&measure.full_name()) {
                    if !self.only_addictive || measure.is_addictive() {
                        return Ok(true);
                    }
                }
            }
            MemberSymbol::MemberExpression(_) => {
                return Ok(false); //TODO We not allow to use pre-aggregations with member expressions before sqlapi ready for it
            }
            _ => return Ok(false),
        }

        if symbol.get_dependencies().is_empty() {
            return Ok(false);
        }

        for dep in symbol.get_dependencies() {
            if !self.try_match(&dep)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
