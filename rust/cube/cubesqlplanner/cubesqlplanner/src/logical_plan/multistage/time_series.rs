use crate::logical_plan::*;
use crate::planner::{InputId, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Date-axis subquery for a rolling window — generates the series of
/// points the rolling computation walks over. The range comes
/// either from a literal `date_range` or from a sibling
/// `MultiStageGetDateRange` subquery (`get_date_range_multistage_ref`).
#[derive(TypedBuilder)]
pub struct MultiStageTimeSeries {
    /// Name of the subquery the series is rendered as. A recursive series refers
    /// to itself by it.
    name: String,
    time_dimension: Rc<MemberSymbol>,
    #[builder(default)]
    date_range: Option<Vec<String>>,
    #[builder(default)]
    get_date_range_multistage_ref: Option<Rc<SubqueryRef>>,
    /// Inline query over the calendar cube supplying the period each series point
    /// belongs to, for `to_date` windows whose granularity defines its own SQL.
    /// `None` when every window on this series bounds itself by interval math.
    #[builder(default)]
    calendar_source: Option<Rc<SubqueryRef>>,
    /// The time dimension at each granularity `calendar_source` projects a
    /// period for.
    #[builder(default)]
    period_dimensions: Vec<Rc<MemberSymbol>>,
}

impl MultiStageTimeSeries {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn time_dimension(&self) -> &Rc<MemberSymbol> {
        &self.time_dimension
    }

    pub fn date_range(&self) -> &Option<Vec<String>> {
        &self.date_range
    }

    pub fn get_date_range_multistage_ref(&self) -> &Option<Rc<SubqueryRef>> {
        &self.get_date_range_multistage_ref
    }

    pub fn calendar_source(&self) -> &Option<Rc<SubqueryRef>> {
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
                    "get_date_range_multistage_ref: {} {}",
                    get_date_range_multistage_ref.name(),
                    get_date_range_multistage_ref.id()
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
        vec![]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;
        Ok(self)
    }

    fn referenced_inputs(&self) -> Vec<InputId> {
        self.get_date_range_multistage_ref
            .iter()
            .chain(self.calendar_source.iter())
            .map(|r| r.id())
            .collect()
    }

    fn with_forwarded_input(self: Rc<Self>, from: InputId, to: &Rc<SubqueryRef>) -> Rc<Self> {
        if !self.referenced_inputs().contains(&from) {
            return self;
        }
        Rc::new(Self {
            name: self.name.clone(),
            time_dimension: self.time_dimension.clone(),
            date_range: self.date_range.clone(),
            get_date_range_multistage_ref: self
                .get_date_range_multistage_ref
                .as_ref()
                .map(|input| input.forwarded(from, to)),
            calendar_source: self
                .calendar_source
                .as_ref()
                .map(|input| input.forwarded(from, to)),
            period_dimensions: self.period_dimensions.clone(),
        })
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
