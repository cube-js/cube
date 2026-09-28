use crate::logical_plan::*;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Date-axis CTE for a rolling window — generates the series of
/// points the rolling computation walks over. The range comes
/// either from a literal `date_range` or from a sibling
/// `MultiStageGetDateRange` CTE (`get_date_range_multistage_ref`).
#[derive(TypedBuilder)]
pub struct MultiStageTimeSeries {
    time_dimension: Rc<MemberSymbol>,
    #[builder(default)]
    date_range: Option<Vec<String>>,
    #[builder(default)]
    get_date_range_multistage_ref: Option<String>,
    /// Query over the calendar cube supplying the period each series point
    /// belongs to, for `to_date` windows whose granularity defines its own SQL.
    /// `None` when every window on this series bounds itself by interval math.
    #[builder(default)]
    calendar_source: Option<Rc<Query>>,
    /// The time dimension at each granularity `calendar_source` projects a
    /// period for.
    #[builder(default)]
    period_dimensions: Vec<Rc<MemberSymbol>>,
}

impl MultiStageTimeSeries {
    pub fn time_dimension(&self) -> &Rc<MemberSymbol> {
        &self.time_dimension
    }

    pub fn date_range(&self) -> &Option<Vec<String>> {
        &self.date_range
    }

    pub fn get_date_range_multistage_ref(&self) -> &Option<String> {
        &self.get_date_range_multistage_ref
    }

    pub fn calendar_source(&self) -> &Option<Rc<Query>> {
        &self.calendar_source
    }

    pub fn period_dimensions(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.period_dimensions
    }
}

impl PrettyPrint for MultiStageTimeSeries {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Time Series", state);
        let state = state.new_level();
        result.println(
            &format!("time_dimension: {}", self.time_dimension().full_name()),
            &state,
        );
        if let Some(date_range) = self.date_range() {
            result.println(
                &format!("date_range: [{}, {}]", date_range[0], date_range[1]),
                &state,
            );
        }
        if !self.period_dimensions.is_empty() {
            result.println(
                &format!(
                    "period_dimensions: {}",
                    self.period_dimensions
                        .iter()
                        .map(|d| d.full_name())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                &state,
            );
        }
        if let Some(calendar_source) = self.calendar_source() {
            result.println("calendar_source:", &state);
            calendar_source.pretty_print(result, &state.new_level());
        }
        if let Some(get_date_range_multistage_ref) = self.get_date_range_multistage_ref() {
            result.println(
                &format!(
                    "get_date_range_multistage_ref: {}",
                    get_date_range_multistage_ref
                ),
                &state,
            );
        }
    }
}

impl LogicalNode for MultiStageTimeSeries {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::MultiStageTimeSeries(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        self.calendar_source
            .iter()
            .map(|source| source.as_plan_node())
            .collect()
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        let expected = if self.calendar_source.is_some() { 1 } else { 0 };
        check_inputs_len(&inputs, expected, self.node_name())?;
        if let Some(source) = inputs.into_iter().next() {
            Ok(Rc::new(Self {
                time_dimension: self.time_dimension.clone(),
                date_range: self.date_range.clone(),
                get_date_range_multistage_ref: self.get_date_range_multistage_ref.clone(),
                calendar_source: Some(source.into_logical_node()?),
                period_dimensions: self.period_dimensions.clone(),
            }))
        } else {
            Ok(self)
        }
    }

    fn referenced_cte_names(&self) -> Vec<String> {
        self.get_date_range_multistage_ref.iter().cloned().collect()
    }

    fn node_name(&self) -> &'static str {
        "MultiStageTimeSeries"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::MultiStageTimeSeries(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "MultiStageTimeSeries"))
        }
    }
}
