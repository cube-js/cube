use super::query_tools::QueryTools;
use super::sql_evaluator::{EvaluationNode, MemberSymbol, MemberSymbolType};
use super::{evaluate_with_context, BaseMember, VisitorContext};
use crate::cube_bridge::measure_definition::MeasureDefinition;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseMeasure {
    measure: String,
    query_tools: Rc<QueryTools>,
    member_evaluator: Rc<EvaluationNode>,
    definition: Rc<dyn MeasureDefinition>,
    cube_name: String,
}

impl BaseMember for BaseMeasure {
    fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        let sql = evaluate_with_context(&self.member_evaluator, self.query_tools.clone(), context)?;
        let alias_name = self.alias_name();

        Ok(format!("{} {}", sql, alias_name))
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
}

impl BaseMeasure {
    pub fn try_new(
        measure: String,
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<EvaluationNode>,
    ) -> Result<Rc<Self>, CubeError> {
        let cube_name = query_tools
            .cube_evaluator()
            .cube_from_path(measure.clone())?
            .static_data()
            .name
            .clone();
        let definition = match member_evaluator.symbol() {
            MemberSymbolType::Measure(m) => Ok(m.definition().clone()),
            _ => Err(CubeError::internal(format!(
                "wrong type of member_evaluator for measure: {}",
                measure
            ))),
        }?;
        Ok(Rc::new(Self {
            measure,
            query_tools,
            definition,
            member_evaluator,
            cube_name,
        }))
    }

    pub fn try_new_from_precompiled(
        evaluation_node: Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Option<Rc<Self>> {
        match evaluation_node.symbol() {
            MemberSymbolType::Measure(s) => Some(Rc::new(Self {
                measure: s.full_name(),
                query_tools: query_tools.clone(),
                member_evaluator: evaluation_node.clone(),
                definition: s.definition().clone(),
                cube_name: s.cube_name().clone(),
            })),
            _ => None,
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
