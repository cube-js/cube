use super::query_tools::QueryTools;
use super::sql_evaluator::{EvaluationNode, MemberSymbol, MemberSymbolType};
use super::{evaluate_with_context, BaseMember, VisitorContext};
use crate::cube_bridge::measure_definition::{MeasureDefinition, TimeShiftReference};
use crate::plan::Schema;
use cubenativeutils::CubeError;
use lazy_static::lazy_static;
use regex::Regex;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct MeasureTimeShift {
    pub interval: String,
    pub time_dimension: String,
}

lazy_static! {
    static ref INTERVAL_MATCH_RE: Regex =
        Regex::new(r"^(-?\d+) (second|minute|hour|day|week|month|quarter|year)s?$").unwrap();
}
impl MeasureTimeShift {
    pub fn try_from_reference(reference: &TimeShiftReference) -> Result<Self, CubeError> {
        let parsed_interval =
            if let Some(captures) = INTERVAL_MATCH_RE.captures(&reference.interval) {
                let duration = if let Some(duration) = captures.get(1) {
                    duration.as_str().parse::<i64>().ok()
                } else {
                    None
                };
                let granularity = if let Some(granularity) = captures.get(2) {
                    Some(granularity.as_str().to_owned())
                } else {
                    None
                };
                if let Some((duration, granularity)) = duration.zip(granularity) {
                    Some((duration, granularity))
                } else {
                    None
                }
            } else {
                None
            };
        if let Some((duration, granularity)) = parsed_interval {
            let duration = if reference.shift_type.as_ref().unwrap_or(&format!("prior")) == "next" {
                duration * (-1)
            } else {
                duration
            };

            Ok(Self {
                interval: format!("{duration} {granularity}"),
                time_dimension: reference.time_dimension.clone(),
            })
        } else {
            Err(CubeError::user(format!(
                "Invalid interval: {}",
                reference.interval
            )))
        }
    }
}

pub struct BaseMeasure {
    measure: String,
    query_tools: Rc<QueryTools>,
    member_evaluator: Rc<EvaluationNode>,
    definition: Rc<dyn MeasureDefinition>,
    time_shifts: Vec<MeasureTimeShift>,
    cube_name: String,
    name: String,
}

impl BaseMember for BaseMeasure {
    fn to_sql(&self, context: Rc<VisitorContext>, schema: Rc<Schema>) -> Result<String, CubeError> {
        evaluate_with_context(
            &self.member_evaluator,
            self.query_tools.clone(),
            context,
            schema,
        )
    }

    fn alias_name(&self) -> String {
        self.query_tools
            .escape_column_name(&self.unescaped_alias_name())
    }

    fn member_evaluator(&self) -> Rc<EvaluationNode> {
        self.member_evaluator.clone()
    }

    fn as_base_member(self: Rc<Self>) -> Rc<dyn BaseMember> {
        self.clone()
    }

    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn name(&self) -> &String {
        &self.name
    }
}

impl BaseMeasure {
    pub fn try_new(
        evaluation_node: Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<Option<Rc<Self>>, CubeError> {
        let res = match evaluation_node.symbol() {
            MemberSymbolType::Measure(s) => {
                let time_shifts = Self::parse_time_shifts(&s.definition())?;
                Some(Rc::new(Self {
                    measure: s.full_name(),
                    query_tools: query_tools.clone(),
                    member_evaluator: evaluation_node.clone(),
                    definition: s.definition().clone(),
                    cube_name: s.cube_name().clone(),
                    name: s.name().clone(),
                    time_shifts,
                }))
            }
            _ => None,
        };
        Ok(res)
    }

    pub fn try_new_required(
        evaluation_node: Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<Rc<Self>, CubeError> {
        if let Some(result) = Self::try_new(evaluation_node, query_tools)? {
            Ok(result)
        } else {
            Err(CubeError::internal(format!(
                "MeasureSymbol expected as evaluation node for BaseMeasure"
            )))
        }
    }

    fn parse_time_shifts(
        definition: &Rc<dyn MeasureDefinition>,
    ) -> Result<Vec<MeasureTimeShift>, CubeError> {
        if let Some(time_shifts) = &definition.static_data().time_shift_references {
            time_shifts
                .iter()
                .map(|t| MeasureTimeShift::try_from_reference(t))
                .collect::<Result<Vec<_>, _>>()
        } else {
            Ok(vec![])
        }
    }

    pub fn member_evaluator(&self) -> &Rc<EvaluationNode> {
        &self.member_evaluator
    }

    pub fn measure(&self) -> &String {
        &self.measure
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }

    pub fn reduce_by(&self) -> &Option<Vec<String>> {
        &self.definition.static_data().reduce_by_references
    }

    pub fn add_group_by(&self) -> &Option<Vec<String>> {
        &self.definition.static_data().add_group_by_references
    }

    pub fn group_by(&self) -> &Option<Vec<String>> {
        &self.definition.static_data().group_by_references
    }

    //FIXME dublicate with symbol
    pub fn is_calculated(&self) -> bool {
        match self.definition.static_data().measure_type.as_str() {
            "number" | "string" | "time" | "boolean" => true,
            _ => false,
        }
    }

    pub fn time_shift_references(&self) -> &Option<Vec<TimeShiftReference>> {
        &self.definition.static_data().time_shift_references
    }

    pub fn time_shifts(&self) -> &Vec<MeasureTimeShift> {
        &self.time_shifts
    }

    pub fn is_multi_stage(&self) -> bool {
        self.definition.static_data().multi_stage.unwrap_or(false)
    }

    //FIXME dublicate with symbol
    pub fn measure_type(&self) -> &String {
        &self.definition.static_data().measure_type
    }

    pub fn is_multi_stage_ungroupped(&self) -> bool {
        self.is_calculated() || self.definition.static_data().measure_type == "rank"
    }

    fn unescaped_alias_name(&self) -> String {
        self.query_tools.alias_name(&self.measure)
    }
}
