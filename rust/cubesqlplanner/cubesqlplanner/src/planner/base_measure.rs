use super::query_tools::QueryTools;
use super::sql_evaluator::{MeasureTimeShift, MemberExpressionSymbol, MemberSymbol, SqlCall};
use super::{evaluate_with_context, BaseMember, BaseMemberHelper, VisitorContext};
use crate::cube_bridge::measure_definition::{
    MeasureDefinition, RollingWindow, TimeShiftReference,
};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::fmt::{Debug, Formatter};
use std::rc::Rc;

pub struct BaseMeasure {
    measure: String,
    query_tools: Rc<QueryTools>,
    member_evaluator: Rc<MemberSymbol>,
    definition: Option<Rc<dyn MeasureDefinition>>,
    #[allow(dead_code)]
    member_expression_definition: Option<String>,
    cube_name: String,
    name: String,
    default_alias: String,
}

impl Debug for BaseMeasure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseMeasure")
            .field("measure", &self.measure)
            .field("default_alias", &self.default_alias)
            .finish()
    }
}

impl BaseMember for BaseMeasure {
    fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        evaluate_with_context(
            &self.member_evaluator,
            self.query_tools.clone(),
            context,
            templates,
        )
    }

    fn alias_name(&self) -> String {
        self.default_alias.clone()
    }

    fn member_evaluator(&self) -> Rc<MemberSymbol> {
        self.member_evaluator.clone()
    }

    fn as_base_member(self: Rc<Self>) -> Rc<dyn BaseMember> {
        self.clone()
    }

    fn full_name(&self) -> String {
        self.member_evaluator.full_name()
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
        evaluation_node: Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
    ) -> Result<Option<Rc<Self>>, CubeError> {
        let res = match evaluation_node.as_ref() {
            MemberSymbol::Measure(s) => {
                let default_alias = BaseMemberHelper::default_alias(
                    &s.cube_name(),
                    &s.name(),
                    &None,
                    query_tools.clone(),
                )?;
                Some(Rc::new(Self {
                    measure: s.full_name(),
                    query_tools: query_tools.clone(),
                    member_evaluator: evaluation_node.clone(),
                    definition: Some(s.definition().clone()),
                    member_expression_definition: None,
                    cube_name: s.cube_name().clone(),
                    name: s.name().clone(),
                    default_alias,
                }))
            }
            MemberSymbol::MemberExpression(expression_symbol) => {
                let full_name = expression_symbol.full_name();
                let cube_name = expression_symbol.cube_name().clone();
                let name = expression_symbol.name().clone();
                let member_expression_definition = expression_symbol.definition().clone();
                let default_alias = PlanSqlTemplates::alias_name(&name);
                Some(Rc::new(Self {
                    measure: full_name,
                    query_tools: query_tools.clone(),
                    member_evaluator: evaluation_node,
                    definition: None,
                    cube_name,
                    name,
                    member_expression_definition,
                    default_alias,
                }))
            }
            _ => None,
        };
        Ok(res)
    }

    pub fn try_new_required(
        evaluation_node: Rc<MemberSymbol>,
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

    pub fn try_new_from_expression(
        expression: Rc<SqlCall>,
        cube_name: String,
        name: String,
        member_expression_definition: Option<String>,
        query_tools: Rc<QueryTools>,
    ) -> Result<Rc<Self>, CubeError> {
        let member_expression_symbol = MemberExpressionSymbol::new(
            cube_name.clone(),
            name.clone(),
            expression,
            member_expression_definition.clone(),
        );
        let full_name = member_expression_symbol.full_name();
        let member_evaluator = Rc::new(MemberSymbol::MemberExpression(member_expression_symbol));
        let default_alias = PlanSqlTemplates::alias_name(&name);
        Ok(Rc::new(Self {
            measure: full_name,
            query_tools,
            member_evaluator,
            definition: None,
            cube_name,
            name,
            member_expression_definition,
            default_alias,
        }))
    }

    pub fn can_used_as_addictive_in_multplied(&self) -> Result<bool, CubeError> {
        let measure_type = self.measure_type();
        let res = if measure_type == "countDistinct" || measure_type == "countDistinctApprox" {
            true
        } else if measure_type == "count" {
            if let Some(definition) = &self.definition {
                !definition.has_sql()?
            } else {
                false
            }
        } else {
            false
        };
        Ok(res)
    }

    pub fn member_evaluator(&self) -> &Rc<MemberSymbol> {
        &self.member_evaluator
    }

    pub fn measure(&self) -> &String {
        &self.measure
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }

    pub fn reduce_by(&self) -> Option<Vec<String>> {
        self.definition
            .as_ref()
            .map_or(None, |d| d.static_data().reduce_by_references.clone())
    }

    pub fn add_group_by(&self) -> Option<Vec<String>> {
        self.definition
            .as_ref()
            .map_or(None, |d| d.static_data().add_group_by_references.clone())
    }

    pub fn group_by(&self) -> Option<Vec<String>> {
        self.definition
            .as_ref()
            .map_or(None, |d| d.static_data().group_by_references.clone())
    }

    //FIXME dublicate with symbol
    pub fn is_calculated(&self) -> bool {
        match self.measure_type() {
            "number" | "string" | "time" | "boolean" => true,
            _ => false,
        }
    }

    pub fn time_shift_references(&self) -> Option<Vec<TimeShiftReference>> {
        self.definition
            .as_ref()
            .map_or(None, |d| d.static_data().time_shift_references.clone())
    }

    pub fn time_shifts(&self) -> Vec<MeasureTimeShift> {
        match self.member_evaluator.as_ref() {
            MemberSymbol::Measure(measure_symbol) => measure_symbol.time_shifts().clone(),
            _ => vec![],
        }
    }

    pub fn is_multi_stage(&self) -> bool {
        self.definition
            .as_ref()
            .map_or(false, |d| d.static_data().multi_stage.unwrap_or(false))
    }

    pub fn rolling_window(&self) -> Option<RollingWindow> {
        self.definition
            .as_ref()
            .map_or(None, |d| d.static_data().rolling_window.clone())
    }

    pub fn is_rolling_window(&self) -> bool {
        self.rolling_window().is_some()
    }

    pub fn is_running_total(&self) -> bool {
        self.measure_type() == "runningTotal"
    }

    pub fn is_cumulative(&self) -> bool {
        self.is_rolling_window() || self.is_running_total()
    }

    //FIXME dublicate with symbol
    pub fn measure_type(&self) -> &str {
        self.definition
            .as_ref()
            .map_or("number", |d| &d.static_data().measure_type)
    }

    pub fn is_multi_stage_ungroupped(&self) -> bool {
        self.is_calculated() || self.measure_type() == "rank"
    }
}
