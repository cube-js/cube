use super::CompiledPreAggregation;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;
pub struct MeasureMatcher {
    only_addictive: bool,
    pre_aggregation_measures: HashSet<String>,
    matched_measures: HashSet<String>,
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
            matched_measures: HashSet::new(),
        }
    }

    pub fn matched_measures(&self) -> &HashSet<String> {
        &self.matched_measures
    }

    pub fn try_match(&mut self, symbol: &Rc<MemberSymbol>) -> Result<bool, CubeError> {
        match symbol.as_ref() {
            MemberSymbol::Measure(measure) => {
                if self.pre_aggregation_measures.contains(&measure.full_name())
                    && (!self.only_addictive || measure.is_addictive())
                {
                    self.matched_measures.insert(measure.full_name());
                    return Ok(true);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::optimizers::pre_aggregation::{
        PreAggregationFullName, PreAggregationsCompiler,
    };
    use crate::test_fixtures::cube_bridge::MockSchema;
    use crate::test_fixtures::test_utils::TestContext;

    fn create_test_context() -> TestContext {
        let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml");
        TestContext::new(schema).unwrap()
    }

    fn compile_pre_agg(ctx: &TestContext, pre_agg_name: &str) -> Rc<CompiledPreAggregation> {
        let cube_names = vec!["orders".to_string()];
        let mut compiler =
            PreAggregationsCompiler::try_new(ctx.query_tools().clone(), &cube_names).unwrap();
        let name = PreAggregationFullName::new("orders".to_string(), pre_agg_name.to_string());
        compiler.compile_pre_aggregation(&name).unwrap()
    }

    #[test]
    fn test_measure_matching() {
        let ctx = create_test_context();
        let pre_agg = compile_pre_agg(&ctx, "all_base_measures_rollup");
        let mut matcher = MeasureMatcher::new(&pre_agg, false);

        assert!(matcher
            .try_match(&ctx.create_measure("orders.count").unwrap())
            .unwrap());
        assert!(matcher
            .try_match(&ctx.create_measure("orders.total_amount").unwrap())
            .unwrap());
        assert!(matcher
            .try_match(&ctx.create_measure("orders.min_amount").unwrap())
            .unwrap());
        assert!(matcher
            .try_match(&ctx.create_measure("orders.avg_amount").unwrap())
            .unwrap());
        assert!(matcher
            .try_match(&ctx.create_measure("orders.unique_status_count").unwrap())
            .unwrap());
        assert!(matcher
            .try_match(&ctx.create_measure("orders.amount_per_count").unwrap())
            .unwrap());
        assert!(matcher
            .try_match(&ctx.create_measure("orders.multi_level_measure").unwrap())
            .unwrap());
    }

    #[test]
    fn test_measure_matching_only_additive() {
        let ctx = create_test_context();
        let pre_agg = compile_pre_agg(&ctx, "all_base_measures_rollup");
        let mut matcher = MeasureMatcher::new(&pre_agg, true);

        assert!(matcher
            .try_match(&ctx.create_measure("orders.count").unwrap())
            .unwrap());
        assert!(matcher
            .try_match(&ctx.create_measure("orders.total_amount").unwrap())
            .unwrap());
        assert!(matcher
            .try_match(&ctx.create_measure("orders.min_amount").unwrap())
            .unwrap());
        assert!(!matcher
            .try_match(&ctx.create_measure("orders.avg_amount").unwrap())
            .unwrap());
        assert!(!matcher
            .try_match(&ctx.create_measure("orders.unique_status_count").unwrap())
            .unwrap());
        assert!(matcher
            .try_match(&ctx.create_measure("orders.amount_per_count").unwrap())
            .unwrap());
        assert!(matcher
            .try_match(&ctx.create_measure("orders.multi_level_measure").unwrap())
            .unwrap());
    }

    #[test]
    fn test_calculated_measure_in_pre_agg_not_additive() {
        let ctx = create_test_context();
        let pre_agg = compile_pre_agg(&ctx, "calculated_measure_rollup");

        let mut matcher = MeasureMatcher::new(&pre_agg, false);
        assert!(matcher
            .try_match(&ctx.create_measure("orders.multi_level_measure").unwrap())
            .unwrap());

        let mut additive_matcher = MeasureMatcher::new(&pre_agg, true);
        assert!(!additive_matcher
            .try_match(&ctx.create_measure("orders.multi_level_measure").unwrap())
            .unwrap());
    }

    #[test]
    fn test_mixed_measure_rollup() {
        let ctx = create_test_context();
        let pre_agg = compile_pre_agg(&ctx, "mixed_measure_rollup");

        let mut matcher = MeasureMatcher::new(&pre_agg, false);
        assert!(matcher
            .try_match(&ctx.create_measure("orders.amount_per_count").unwrap())
            .unwrap());
        assert!(matcher
            .try_match(&ctx.create_measure("orders.max_amount").unwrap())
            .unwrap());
        assert!(matcher
            .try_match(&ctx.create_measure("orders.multi_level_measure").unwrap())
            .unwrap());

        let mut additive_matcher = MeasureMatcher::new(&pre_agg, true);
        assert!(!additive_matcher
            .try_match(&ctx.create_measure("orders.amount_per_count").unwrap())
            .unwrap());
        assert!(additive_matcher
            .try_match(&ctx.create_measure("orders.max_amount").unwrap())
            .unwrap());
        assert!(!additive_matcher
            .try_match(&ctx.create_measure("orders.multi_level_measure").unwrap())
            .unwrap());
    }

    #[test]
    fn test_matched_measures_full_match() {
        let ctx = create_test_context();
        let pre_agg = compile_pre_agg(&ctx, "base_and_calculated_measure_rollup");

        // Full match (not only_additive) — calculated measure consumed directly
        let mut matcher = MeasureMatcher::new(&pre_agg, false);
        assert!(matcher
            .try_match(&ctx.create_measure("orders.amount_per_count").unwrap())
            .unwrap());
        assert!(matcher
            .matched_measures()
            .contains("orders.amount_per_count"));
        assert!(!matcher.matched_measures().contains("orders.count"));
        assert!(!matcher.matched_measures().contains("orders.total_amount"));
    }

    #[test]
    fn test_matched_measures_partial_match() {
        let ctx = create_test_context();
        let pre_agg = compile_pre_agg(&ctx, "base_and_calculated_measure_rollup");

        // Partial match (only_additive) — calculated measure decomposed to base deps
        let mut matcher = MeasureMatcher::new(&pre_agg, true);
        assert!(matcher
            .try_match(&ctx.create_measure("orders.amount_per_count").unwrap())
            .unwrap());
        assert!(!matcher
            .matched_measures()
            .contains("orders.amount_per_count"));
        assert!(matcher.matched_measures().contains("orders.count"));
        assert!(matcher.matched_measures().contains("orders.total_amount"));
    }
}
