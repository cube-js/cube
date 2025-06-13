use super::query_tools::QueryTools;
use super::sql_evaluator::{MeasureTimeShifts, MemberExpressionSymbol, MemberSymbol};
use super::{evaluate_with_context, BaseMember, BaseMemberHelper, VisitorContext};
use crate::cube_bridge::measure_definition::RollingWindow;
use crate::planner::sql_evaluator::MemberExpressionExpression;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::fmt::{Debug, Formatter};
use std::rc::Rc;

pub struct BaseMeasure {
    measure: String,
    query_tools: Rc<QueryTools>,
    member_evaluator: Rc<MemberSymbol>,
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
        expression: MemberExpressionExpression,
        cube_name: String,
        name: String,
        member_expression_definition: Option<String>,
        query_tools: Rc<QueryTools>,
    ) -> Result<Rc<Self>, CubeError> {
        let member_expression_symbol = MemberExpressionSymbol::try_new(
            cube_name.clone(),
            name.clone(),
            expression,
            member_expression_definition.clone(),
            query_tools.base_tools().clone(),
        )?;
        let full_name = member_expression_symbol.full_name();
        let member_evaluator = MemberSymbol::new_member_expression(member_expression_symbol);
        let default_alias = PlanSqlTemplates::alias_name(&name);
        Ok(Rc::new(Self {
            measure: full_name,
            query_tools,
            member_evaluator,
            cube_name,
            name,
            member_expression_definition,
            default_alias,
        }))
    }

    pub fn can_be_used_as_additive_in_multplied(&self) -> bool {
        if let Ok(measure_symbol) = self.member_evaluator.as_measure() {
            measure_symbol.can_used_as_addictive_in_multplied()
        } else {
            false
        }
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

    pub fn is_calculated(&self) -> bool {
        if let Ok(measure_symbol) = self.member_evaluator.as_measure() {
            measure_symbol.is_calculated()
        } else {
            true
        }
    }

    pub fn time_shift(&self) -> Option<MeasureTimeShifts> {
        match self.member_evaluator.as_ref() {
            MemberSymbol::Measure(measure_symbol) => measure_symbol.time_shift().clone(),
            _ => None,
        }
    }

    pub fn is_multi_stage(&self) -> bool {
        if let Ok(measure_symbol) = self.member_evaluator.as_measure() {
            measure_symbol.is_multi_stage()
        } else {
            false
        }
    }

    pub fn rolling_window(&self) -> Option<RollingWindow> {
        if let Ok(measure_symbol) = self.member_evaluator.as_measure() {
            measure_symbol.rolling_window().clone()
        } else {
            None
        }
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

    pub fn measure_type(&self) -> String {
        if let Ok(measure_symbol) = self.member_evaluator.as_measure() {
            measure_symbol.measure_type().clone()
        } else {
            "number".to_string()
        }
    }

    pub fn is_multi_stage_ungroupped(&self) -> bool {
        self.is_calculated() || self.measure_type() == "rank"
    }
}
