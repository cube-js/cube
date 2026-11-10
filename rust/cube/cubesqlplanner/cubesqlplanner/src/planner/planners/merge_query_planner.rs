use crate::logical_plan::*;
use crate::planner::planners::multi_stage::PlanningScope;
use crate::planner::QueryAnalysis;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Final assembly step for non-simple queries: takes the collected
/// multi-stage / multiplied subquery refs and members and wraps
/// them in a `Query` whose source is a `Merge`.
pub struct MergeQueryPlanner {
    analysis: Rc<QueryAnalysis>,
}

impl MergeQueryPlanner {
    pub fn new(analysis: Rc<QueryAnalysis>) -> Self {
        Self { analysis }
    }

    /// Builds the `Merge` source from the collected
    /// multi-stage subquery refs. Always renders as FULL OUTER JOIN
    /// + COALESCE.
    pub fn plan_logical_source(
        &self,
        multi_stage_subqueries: Vec<Rc<SubqueryRef>>,
    ) -> Result<Rc<Merge>, CubeError> {
        let schema = LogicalSchema::default()
            .set_dimensions(self.analysis.query().dimensions().clone())
            .set_time_dimensions(self.analysis.query().time_dimensions().clone())
            .into_rc();
        Ok(Rc::new(
            Merge::builder()
                .subqueries(multi_stage_subqueries)
                .schema(schema)
                .build(),
        ))
    }

    /// Wraps `plan_logical_source` in a `Query` with the request's
    /// filters and modifiers.
    pub fn plan_logical_plan(
        &self,
        multi_stage_subqueries: Vec<Rc<SubqueryRef>>,
        scope: &PlanningScope,
    ) -> Result<Rc<Query>, CubeError> {
        let source = self.plan_logical_source(multi_stage_subqueries)?;

        let schema = LogicalSchema::default()
            .set_dimensions(self.analysis.query().dimensions().clone())
            .set_time_dimensions(self.analysis.query().time_dimensions().clone())
            .set_measures(self.analysis.select_measures().to_vec())
            .into_rc();
        let merges_legs = !source.is_empty();
        let placement = self
            .analysis
            .filter_placement(merges_legs, &schema.multi_stage_dimensions()?)?;
        // Over subqueries the multi-stage dimensions are read from them.
        let from = if !merges_legs {
            LogicalFrom::over_join(None)
                .grid_input(scope.multi_stage_dimension_input(&schema)?)
                .build()?
                .0
        } else {
            LogicalFrom::merge(source)?
        };

        let logical_filter = Rc::new(LogicalFilter {
            dimensions_filters: self.analysis.query().dimensions_filters().clone(),
            time_dimensions_filters: self.analysis.query().time_dimensions_filters().clone(),
            measures_filter: self.analysis.query().measures_filters().clone(),
            segments: self.analysis.query().segments().clone(),
        });
        let result = Query::builder()
            .schema(schema)
            .filter(logical_filter)
            .modifers(Rc::new(LogicalQueryModifiers {
                offset: self.analysis.query().offset(),
                limit: self.analysis.query().row_limit(),
                ungrouped: self.analysis.query().ungrouped(),
                distinct: false,
                order_by: self.analysis.query().order_by().to_vec(),
            }))
            .from(from)
            .placement(Some(Rc::new(placement)))
            .build();
        Ok(Rc::new(result))
    }
}
