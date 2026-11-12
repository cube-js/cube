use super::DimensionSubqueryPlanner;
use crate::logical_plan::*;
use crate::planner::collectors::collect_sub_query_dimensions_from_symbols;
use crate::planner::measure_groups::RegularGroup;
use crate::planner::planners::multi_stage::{EvaluationContext, PlanningScope};
use crate::planner::state::State;
use crate::planner::QueryAnalysis;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Plans a regular measure group as a subquery aggregating over its join.
pub struct RegularGroupPlanner {
    query_tools: Rc<State>,
    analysis: Rc<QueryAnalysis>,
}

impl RegularGroupPlanner {
    pub fn new(query_tools: Rc<State>, analysis: Rc<QueryAnalysis>) -> Self {
        Self {
            query_tools,
            analysis,
        }
    }

    /// Registers the group's subquery into `scope` and returns its ref.
    pub fn plan(
        &self,
        group: &RegularGroup,
        scope: &mut PlanningScope,
    ) -> Result<Rc<SubqueryRef>, CubeError> {
        let query = self.subquery(group, scope)?;
        let (subquery_id, subquery_name) = scope.next_subquery();

        let leaf = query.with_evaluation_context(Some(EvaluationContext::default()));
        let member = Rc::new(Subquery::new(
            subquery_id,
            subquery_name.clone(),
            SubqueryBody::Query(leaf),
        ));
        scope.add_member(member)?;

        Ok(Rc::new(
            SubqueryRef::builder()
                .id(subquery_id)
                .name(subquery_name)
                .schema(query.schema().clone())
                .build(),
        ))
    }

    fn subquery(
        &self,
        group: &RegularGroup,
        scope: &mut PlanningScope,
    ) -> Result<Rc<Query>, CubeError> {
        let measures = group.measures();
        let join = group.join().clone();
        let all_symbols = self
            .analysis
            .query()
            .get_member_symbols(true, true, false, true, measures);

        let subquery_dimensions = collect_sub_query_dimensions_from_symbols(&all_symbols, &join)?;

        let dimension_inputs =
            DimensionSubqueryPlanner::new(self.query_tools.clone(), self.analysis.query().clone())
                .plan_queries(&subquery_dimensions, scope)?;

        let schema = LogicalSchema::default()
            .set_dimensions(self.analysis.query().dimensions().clone())
            .set_time_dimensions(self.analysis.query().time_dimensions().clone())
            .set_measures(measures.clone())
            .into_rc();
        let (from, join) = LogicalFrom::over_join(Some(join))
            .dimension_inputs(dimension_inputs)
            .grid_input(scope.multi_stage_dimension_input(&schema)?)
            .build()?;

        Query::builder()
            .schema(schema)
            .filter(self.analysis.group_filter())
            .modifers(Rc::new(LogicalQueryModifiers {
                offset: None,
                limit: None,
                ungrouped: self.analysis.query().ungrouped(),
                distinct: false,
                order_by: vec![],
            }))
            .from(from)
            .join(join)
            .build()
    }
}
